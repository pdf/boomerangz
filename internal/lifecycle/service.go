package lifecycle

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/pdf/boomerangz/internal/policy"
	"github.com/pdf/boomerangz/internal/zfs"
)

// Backend is the bounded ZFS interface used by snapshot management.
type Backend interface {
	InspectState(context.Context, string, bool) (zfs.State, error)
	SetProperties(context.Context, string, map[string]string) error
	Snapshot(context.Context, string, string, bool, map[string]string) error
	DestroySnapshot(context.Context, string) error
}

// Service serializes its own lifecycle operations. Callers must additionally
// coordinate with other processes and stop submitting work on deactivation.
// ZFS CLI commands do not offer compare-and-swap against external administrators.
type Service struct {
	backend      Backend
	installation string
	locks        *hierarchyLocks
}

type hierarchyLocks struct {
	mu      sync.Mutex
	active  map[string]int
	changed chan struct{}
}

func newHierarchyLocks() *hierarchyLocks {
	return &hierarchyLocks{active: make(map[string]int), changed: make(chan struct{})}
}

func overlappingScope(a, b string) bool {
	return a == b || strings.HasPrefix(a, b+"/") || strings.HasPrefix(b, a+"/")
}

func (l *hierarchyLocks) acquire(ctx context.Context, dataset string) (func(), error) {
	for {
		l.mu.Lock()
		blocked := false
		for active := range l.active {
			if overlappingScope(active, dataset) {
				blocked = true
				break
			}
		}
		if !blocked {
			l.active[dataset]++
			l.mu.Unlock()
			return func() {
				l.mu.Lock()
				l.active[dataset]--
				if l.active[dataset] == 0 {
					delete(l.active, dataset)
				}
				close(l.changed)
				l.changed = make(chan struct{})
				l.mu.Unlock()
			}, nil
		}
		changed := l.changed
		l.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-changed:
		}
	}
}

func (s *Service) lock(ctx context.Context, dataset string) (func(), error) {
	if err := zfs.ValidateDataset(dataset); err != nil {
		return nil, err
	}
	return s.locks.acquire(ctx, dataset)
}

// NewService constructs an operational lifecycle service.
func NewService(backend Backend, installation string) (*Service, error) {
	if backend == nil {
		return nil, fmt.Errorf("lifecycle backend is required")
	}
	if !ValidID(installation) {
		return nil, fmt.Errorf("valid installation identity required")
	}
	return &Service{backend: backend, installation: installation, locks: newHierarchyLocks()}, nil
}

// NewCleanService constructs the explicitly administrative clean surface.
// It cannot authorize snapshot, reference, transfer, or adoption operations.
func NewCleanService(backend Backend) (*Service, error) {
	if backend == nil {
		return nil, fmt.Errorf("lifecycle backend is required")
	}
	return &Service{backend: backend, locks: newHierarchyLocks()}, nil
}

// storedLineage never treats inherited lineage as ownership of another dataset.
func storedLineage(state zfs.State, dataset string) (string, error) {
	var lineage string
	for _, p := range state.Properties {
		if p.Dataset != dataset || p.Name != LineageProperty {
			continue
		}
		if lineage != "" || !ValidID(p.Value) || (p.Source != zfs.SourceLocal && p.Source != zfs.SourceReceived) {
			return "", fmt.Errorf("ambiguous or invalid lineage on %s", dataset)
		}
		lineage = p.Value
	}
	if hidden := state.Received[dataset][LineageProperty]; hidden != "" && hidden != lineage {
		return "", fmt.Errorf("conflicting or hidden received lineage on %s", dataset)
	}
	return lineage, nil
}

func snapshotsIn(state zfs.State, dataset string) []Snapshot {
	var snapshots []Snapshot
	for _, object := range state.Objects {
		if object.Type != "snapshot" || !strings.HasPrefix(object.Name, dataset+"@") {
			continue
		}
		snapshot := Snapshot{Name: object.Name, GUID: object.GUID, Holds: state.Holds[object.Name], Clones: state.Clones[object.Name], ResumeRequired: len(state.ResumeTokens) != 0}
		for _, p := range state.Properties {
			if p.Dataset == object.Name {
				snapshot.Properties = append(snapshot.Properties, p)
			}
		}
		snapshots = append(snapshots, snapshot)
	}
	return snapshots
}

func scopeReady(state zfs.State, dataset string) error {
	if len(state.ResumeTokens) != 0 {
		return fmt.Errorf("scope has resumable receive state")
	}
	for _, object := range state.Objects {
		if object.Name == dataset && (object.Type == "filesystem" || object.Type == "volume") && object.GUID != 0 {
			return nil
		}
	}
	return fmt.Errorf("dataset missing from lifecycle inventory")
}

func datasetObjects(state zfs.State) []zfs.Object {
	var objects []zfs.Object
	for _, object := range state.Objects {
		if object.Type == "filesystem" || object.Type == "volume" {
			objects = append(objects, object)
		}
	}
	return objects
}

func (s *Service) unchanged(ctx context.Context, dataset string, recursive bool, expected zfs.State) error {
	current, err := s.backend.InspectState(ctx, dataset, recursive)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(expected, current) {
		return fmt.Errorf("ZFS state changed; retry with a fresh plan")
	}
	return nil
}

// AdoptionLineage previews restoration of a missing exact-dataset lineage.
func AdoptionLineage(state zfs.State, dataset string) (string, error) {
	if err := zfs.ValidateDataset(dataset); err != nil {
		return "", err
	}
	if err := scopeReady(state, dataset); err != nil {
		return "", err
	}
	lineage, err := storedLineage(state, dataset)
	if err != nil {
		return "", err
	}
	if lineage != "" {
		local := false
		for _, row := range state.Properties {
			local = local || row.Dataset == dataset && row.Name == LineageProperty && row.Source == zfs.SourceLocal && row.Value == lineage
		}
		if !local {
			return "", fmt.Errorf("received lineage is provenance only; local lineage or unique owned snapshot proof required")
		}
		return lineage, nil
	}
	return Adopt(snapshotsIn(state, dataset))
}

// AdoptDataset applies an already reviewed adoption, restoring a missing exact
// lineage when uniquely proven and changing the owner. Callers must first show
// and verify every configured target; this low-level mutation never probes them.
func (s *Service) AdoptDataset(ctx context.Context, dataset string, effective policy.Effective) (string, error) {
	unlock, err := s.lock(ctx, dataset)
	if err != nil {
		return "", err
	}
	defer unlock()
	if err := zfs.ValidateDataset(dataset); err != nil {
		return "", err
	}
	if err := ActiveRoot(effective, dataset); err != nil {
		return "", err
	}
	state, err := s.backend.InspectState(ctx, dataset, false)
	if err != nil {
		return "", err
	}
	lineage, err := storedLineage(state, dataset)
	if err != nil {
		return "", err
	}
	if lineage == "" {
		lineage, err = AdoptionLineage(state, dataset)
		if err != nil {
			return "", err
		}
	}
	if current, authorityErr := RootAuthority(state, dataset, s.installation); authorityErr == nil && current == lineage {
		return "", fmt.Errorf("dataset is already owned by this installation")
	}
	if err := ValidateLineageEvidence(state, dataset, lineage); err != nil {
		return "", err
	}
	if err := s.unchanged(ctx, dataset, false, state); err != nil {
		return "", err
	}
	if err := s.backend.SetProperties(ctx, dataset, map[string]string{LineageProperty: lineage, OwnerProperty: s.installation}); err != nil {
		return "", err
	}
	after, err := s.backend.InspectState(ctx, dataset, false)
	if err != nil {
		return "", err
	}
	actual, err := RootAuthority(after, dataset, s.installation)
	if err != nil || actual != lineage {
		return "", fmt.Errorf("adopted lineage could not be verified")
	}
	return lineage, nil
}

// CreateSnapshot creates one snapshot or recursive snapshot set with metadata
// attached atomically by zfs snapshot. Scheduling/cadence is the caller's concern.
// Existing snapshot metadata requires explicit adoption if root lineage is lost.
func (s *Service) CreateSnapshot(ctx context.Context, dataset string, recursive bool, now time.Time, effective policy.Effective) (Metadata, error) {
	unlock, err := s.lock(ctx, dataset)
	if err != nil {
		return Metadata{}, err
	}
	defer unlock()
	if err := zfs.ValidateDataset(dataset); err != nil {
		return Metadata{}, err
	}
	if err := ActiveRoot(effective, dataset); err != nil {
		return Metadata{}, err
	}
	state, err := s.backend.InspectState(ctx, dataset, recursive)
	if err != nil {
		return Metadata{}, err
	}
	if err := scopeReady(state, dataset); err != nil {
		return Metadata{}, err
	}
	lineage, err := storedLineage(state, dataset)
	if err != nil {
		return Metadata{}, err
	}
	fresh := lineage == ""
	if fresh {
		// Do not silently fork a lineage after reinstall or property removal.
		for _, p := range state.Properties {
			if strings.HasPrefix(p.Name, policy.StateNamespace) {
				return Metadata{}, fmt.Errorf("existing internal metadata requires explicit adoption or clean")
			}
		}
		for _, values := range state.Received {
			for name := range values {
				if strings.HasPrefix(name, policy.StateNamespace) {
					return Metadata{}, fmt.Errorf("hidden received metadata requires explicit resolution")
				}
			}
		}
		lineage, err = NewID()
		if err != nil {
			return Metadata{}, err
		}
	}
	if !fresh {
		lineage, err = RootAuthority(state, dataset, s.installation)
		if err != nil {
			return Metadata{}, err
		}
	}
	for _, object := range state.Objects {
		if object.Type != "filesystem" && object.Type != "volume" {
			continue
		}
		child, err := storedLineage(state, object.Name)
		if err != nil {
			return Metadata{}, err
		}
		if child != "" && child != lineage {
			return Metadata{}, fmt.Errorf("conflicting descendant lineage on %s", object.Name)
		}
	}
	metadata, err := NewMetadata(lineage, now)
	if err != nil {
		return Metadata{}, err
	}
	if err := s.unchanged(ctx, dataset, recursive, state); err != nil {
		return Metadata{}, err
	}
	if fresh {
		if err := s.backend.SetProperties(ctx, dataset, map[string]string{LineageProperty: lineage, OwnerProperty: s.installation}); err != nil {
			return Metadata{}, err
		}
		current, err := s.backend.InspectState(ctx, dataset, recursive)
		if err != nil {
			return Metadata{}, err
		}
		actual, err := RootAuthority(current, dataset, s.installation)
		if err != nil || actual != lineage || !reflect.DeepEqual(datasetObjects(state), datasetObjects(current)) || len(current.ResumeTokens) > 0 {
			return Metadata{}, fmt.Errorf("scope changed during lineage initialization")
		}
	}
	if err := s.backend.Snapshot(ctx, dataset, metadata.Name(), recursive, metadata.Properties()); err != nil {
		return Metadata{}, err
	}
	after, err := s.backend.InspectState(ctx, dataset, recursive)
	if err != nil {
		return Metadata{}, err
	}
	if !reflect.DeepEqual(datasetObjects(state), datasetObjects(after)) {
		return Metadata{}, fmt.Errorf("dataset scope changed during snapshot creation")
	}
	for _, object := range state.Objects {
		if object.Type != "filesystem" && object.Type != "volume" {
			continue
		}
		verified := false
		for _, snapshot := range snapshotsIn(after, object.Name) {
			if snapshot.Name != object.Name+"@"+metadata.Name() {
				continue
			}
			actual, err := Ownership(snapshot, lineage)
			verified = err == nil && actual == metadata
		}
		if !verified {
			return Metadata{}, fmt.Errorf("created snapshot could not be verified on %s", object.Name)
		}
	}
	return metadata, nil
}

// Prune returns the initial preview and, when apply is true, re-plans before
// each exact deletion. Previously completed deletions are not rolled back on error.
func (s *Service) Prune(ctx context.Context, dataset string, effective policy.Effective, apply bool) ([]Decision, error) {
	unlock, err := s.lock(ctx, dataset)
	if err != nil {
		return nil, err
	}
	defer unlock()
	if err := zfs.ValidateDataset(dataset); err != nil {
		return nil, err
	}
	if err := ActiveRoot(effective, dataset); err != nil {
		return nil, err
	}
	state, err := s.backend.InspectState(ctx, dataset, false)
	if err != nil {
		return nil, err
	}
	if err := scopeReady(state, dataset); err != nil {
		return nil, err
	}
	lineage, err := RootAuthority(state, dataset, s.installation)
	if err != nil {
		return nil, err
	}
	plan, err := PlanPrune(dataset, lineage, effective.Grid, snapshotsIn(state, dataset))
	if err != nil || !apply {
		return plan, err
	}
	for _, decision := range plan {
		if !decision.Destroy {
			continue
		}
		current, err := s.backend.InspectState(ctx, dataset, false)
		if err != nil {
			return plan, err
		}
		if err := scopeReady(current, dataset); err != nil {
			return plan, err
		}
		if !reflect.DeepEqual(datasetObjects(state), datasetObjects(current)) {
			return plan, fmt.Errorf("dataset identity changed before pruning")
		}
		actual, err := RootAuthority(current, dataset, s.installation)
		if err != nil || actual != lineage {
			return plan, fmt.Errorf("lineage changed before pruning")
		}
		decisions, err := PlanPrune(dataset, lineage, effective.Grid, snapshotsIn(current, dataset))
		if err != nil {
			return plan, err
		}
		eligible := false
		for _, next := range decisions {
			if next.Snapshot == decision.Snapshot && next.GUID == decision.GUID && next.Destroy {
				eligible = true
			}
		}
		if !eligible {
			return plan, fmt.Errorf("snapshot eligibility changed before pruning %s", decision.Snapshot)
		}
		if err := s.backend.DestroySnapshot(ctx, decision.Snapshot); err != nil {
			return plan, err
		}
	}
	return plan, nil
}
