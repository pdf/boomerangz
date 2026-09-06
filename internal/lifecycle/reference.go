package lifecycle

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/pdf/boomerangz/internal/policy"
	"github.com/pdf/boomerangz/internal/zfs"
)

// ReferencePrefix identifies per-target, per-snapshot recovery proofs. Keep
// old records until their corresponding hold and bookmark have been removed.
const ReferencePrefix = policy.StateNamespace + "reference:"

// Reference binds a target-specific hold/bookmark to proven snapshot metadata
// and GUID. Target is a canonical non-secret destination identity supplied by
// the transport planner, not a credential or arbitrary command.
type Reference struct {
	Target   string            `json:"target"`
	Dataset  string            `json:"dataset,omitempty"`
	GUID     uint64            `json:"guid"`
	Metadata Metadata          `json:"metadata"`
	Sources  []ReferenceSource `json:"sources,omitempty"`
}

// ReferenceSource is one member of a recursive snapshot set protected by the
// single root reference property for its shared snapshot UUID.
type ReferenceSource struct {
	Dataset  string   `json:"dataset"`
	GUID     uint64   `json:"guid"`
	Metadata Metadata `json:"metadata"`
}

// TargetID is deterministic and does not put arbitrary target text in ZFS names.
func TargetID(target string) string {
	sum := sha256.Sum256([]byte(target))
	return hex.EncodeToString(sum[:])
}
func (r Reference) property() string {
	return ReferencePrefix + TargetID(r.Target) + ":" + r.Metadata.Snapshot
}
func (r Reference) hold() string { return SnapshotPrefix + TargetID(r.Target) }
func (r Reference) bookmark() string {
	return r.Dataset + "#" + r.hold() + "-" + r.Metadata.Snapshot
}
func (r Reference) snapshot() string { return r.Dataset + "@" + r.Metadata.Name() }

func (r Reference) sources() []ReferenceSource {
	if len(r.Sources) == 0 {
		return []ReferenceSource{{Dataset: r.Dataset, GUID: r.GUID, Metadata: r.Metadata}}
	}
	return r.Sources
}

func (r Reference) sourceSnapshot(source ReferenceSource) string {
	return source.Dataset + "@" + source.Metadata.Name()
}

func (r Reference) sourceBookmark(source ReferenceSource) string {
	return source.Dataset + "#" + r.hold() + "-" + source.Metadata.Snapshot
}

type referenceBackend interface {
	Hold(context.Context, string, string) error
	Release(context.Context, string, string) error
	Bookmark(context.Context, string, string) error
	DestroyBookmark(context.Context, string) error
	InheritProperty(context.Context, string, string) error
}

func referenceRecords(state zfs.State, dataset, lineage string) ([]Reference, error) {
	var refs []Reference
	seen := map[string]bool{}
	for _, p := range state.Properties {
		if p.Dataset != dataset || !strings.HasPrefix(p.Name, ReferencePrefix) {
			continue
		}
		// A received proof describes another source's targets. It is clean
		// metadata, not authority to claim local holds or bookmarks.
		if p.Source != zfs.SourceLocal {
			continue
		}
		var r Reference
		if json.Unmarshal([]byte(p.Value), &r) != nil || r.Target == "" || r.GUID == 0 || r.Metadata.Lineage != lineage || !ValidID(r.Metadata.Snapshot) || r.Metadata.Created.IsZero() || r.property() != p.Name || seen[p.Name] {
			return nil, fmt.Errorf("invalid reference proof %s", p.Name)
		}
		if r.Dataset == "" { // Accept pre-authority records on their exact root.
			r.Dataset = dataset
		}
		if err := zfs.ValidateDataset(r.Dataset); err != nil || (r.Dataset != dataset && !strings.HasPrefix(r.Dataset, dataset+"/")) {
			return nil, fmt.Errorf("invalid reference dataset in %s", p.Name)
		}
		for _, source := range r.sources() {
			if err := zfs.ValidateDataset(source.Dataset); err != nil || (source.Dataset != dataset && !strings.HasPrefix(source.Dataset, dataset+"/")) || source.GUID == 0 || source.Metadata.Lineage != lineage || source.Metadata.Snapshot != r.Metadata.Snapshot || source.Metadata.Created != r.Metadata.Created {
				return nil, fmt.Errorf("invalid recursive reference proof %s", p.Name)
			}
		}
		seen[p.Name] = true
		refs = append(refs, r)
	}
	return refs, nil
}

func findOwned(state zfs.State, dataset, name, lineage string) (Snapshot, Metadata, error) {
	for _, snapshot := range snapshotsIn(state, dataset) {
		if snapshot.Name != name {
			continue
		}
		metadata, err := Ownership(snapshot, lineage)
		return snapshot, metadata, err
	}
	return Snapshot{}, Metadata{}, fmt.Errorf("source snapshot not found")
}

func containsReference(refs []Reference, wanted Reference) bool {
	return slices.ContainsFunc(refs, func(candidate Reference) bool { return reflect.DeepEqual(candidate, wanted) })
}

// Protect records a recovery proof before placing the target hold. If interrupted
// between writes, the record remains reconstructable and a retry is idempotent.
func (s *Service) Protect(ctx context.Context, dataset, snapshot, target string) (Reference, error) {
	return s.ProtectSet(ctx, dataset, []string{snapshot}, target)
}

// ProtectSet records one root property and protects every member of a recursive
// snapshot set. Recursive snapshots share one snapshot UUID, so separate
// properties would collide and cannot serve as independent proofs.
func (s *Service) ProtectSet(ctx context.Context, dataset string, snapshots []string, target string) (Reference, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	backend, ok := s.backend.(referenceBackend)
	if !ok {
		return Reference{}, fmt.Errorf("reference operations unavailable")
	}
	if err := zfs.ValidateDataset(dataset); err != nil {
		return Reference{}, err
	}
	if strings.TrimSpace(target) == "" || strings.ContainsAny(target, "\x00\r\n") {
		return Reference{}, fmt.Errorf("canonical non-secret target identity required")
	}
	if len(snapshots) == 0 {
		return Reference{}, fmt.Errorf("at least one source snapshot is required")
	}
	state, err := s.backend.InspectState(ctx, dataset, true)
	if err != nil {
		return Reference{}, err
	}
	if err := scopeReady(state, dataset); err != nil {
		return Reference{}, err
	}
	lineage, err := RootAuthority(state, dataset, s.installation)
	if err != nil {
		return Reference{}, err
	}
	var sources []ReferenceSource
	seenSnapshots := make(map[string]bool)
	for _, snapshot := range snapshots {
		if seenSnapshots[snapshot] {
			return Reference{}, fmt.Errorf("duplicate source snapshot")
		}
		seenSnapshots[snapshot] = true
		snapshotDataset := strings.Split(snapshot, "@")[0]
		if snapshotDataset != dataset && !strings.HasPrefix(snapshotDataset, dataset+"/") {
			return Reference{}, fmt.Errorf("source snapshot is outside authoritative root")
		}
		owned, metadata, findErr := findOwned(state, snapshotDataset, snapshot, lineage)
		if findErr != nil {
			return Reference{}, findErr
		}
		sources = append(sources, ReferenceSource{Dataset: snapshotDataset, GUID: owned.GUID, Metadata: metadata})
	}
	slices.SortFunc(sources, func(a, b ReferenceSource) int { return strings.Compare(a.Dataset, b.Dataset) })
	rootIndex := slices.IndexFunc(sources, func(source ReferenceSource) bool { return source.Dataset == dataset })
	if rootIndex < 0 {
		return Reference{}, fmt.Errorf("recursive protection lacks root snapshot")
	}
	root := sources[rootIndex]
	if rootIndex != 0 {
		sources[0], sources[rootIndex] = sources[rootIndex], sources[0]
	}
	for _, source := range sources[1:] {
		if source.Metadata.Snapshot != root.Metadata.Snapshot || source.Metadata.Created != root.Metadata.Created || source.Metadata.Lineage != root.Metadata.Lineage {
			return Reference{}, fmt.Errorf("recursive snapshot metadata is inconsistent")
		}
	}
	r := Reference{Target: target, Dataset: root.Dataset, GUID: root.GUID, Metadata: root.Metadata}
	if len(sources) > 1 {
		r.Sources = sources
	}
	refs, err := referenceRecords(state, dataset, lineage)
	if err != nil {
		return Reference{}, err
	}
	exists := false
	for _, prior := range refs {
		if prior.property() == r.property() {
			if !reflect.DeepEqual(prior, r) {
				return Reference{}, fmt.Errorf("conflicting reference proof")
			}
			exists = true
		}
	}
	if state.Received[dataset][r.property()] != "" {
		return Reference{}, fmt.Errorf("received reference proof requires explicit resolution")
	}
	if !exists {
		for _, source := range r.sources() {
			if slices.Contains(state.Holds[r.sourceSnapshot(source)], r.hold()) {
				return Reference{}, fmt.Errorf("unproven existing target hold")
			}
		}
	}
	if err := s.unchanged(ctx, dataset, true, state); err != nil {
		return Reference{}, err
	}
	if !exists {
		value, err := json.Marshal(r)
		if err != nil {
			return Reference{}, err
		}
		if err := s.backend.SetProperties(ctx, dataset, map[string]string{r.property(): string(value)}); err != nil {
			return Reference{}, err
		}
	}
	for _, source := range r.sources() {
		snapshot := r.sourceSnapshot(source)
		if !slices.Contains(state.Holds[snapshot], r.hold()) {
			if err := backend.Hold(ctx, r.hold(), snapshot); err != nil {
				return Reference{}, err
			}
		}
	}
	return r, nil
}

// Checkpoint creates a versioned bookmark only after the caller supplies the
// destination snapshot GUID obtained by a successful receive verification.
// It deliberately retains the hold and previous checkpoints; the transfer layer
// releases obsolete references only once recovery no longer needs them.
func (s *Service) Checkpoint(ctx context.Context, dataset string, r Reference, verifiedGUID uint64) error {
	return s.CheckpointSet(ctx, dataset, r, map[string]uint64{r.snapshot(): verifiedGUID})
}

// CheckpointSet creates every versioned bookmark only after the caller supplies
// the verified destination GUID for every protected source snapshot.
func (s *Service) CheckpointSet(ctx context.Context, dataset string, r Reference, verifiedGUIDs map[string]uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	backend, ok := s.backend.(referenceBackend)
	if !ok {
		return fmt.Errorf("reference operations unavailable")
	}
	if err := zfs.ValidateDataset(dataset); err != nil {
		return err
	}
	state, err := s.backend.InspectState(ctx, dataset, true)
	if err != nil {
		return err
	}
	if err := scopeReady(state, dataset); err != nil {
		return err
	}
	lineage, err := RootAuthority(state, dataset, s.installation)
	if err != nil {
		return err
	}
	refs, err := referenceRecords(state, dataset, lineage)
	if err != nil {
		return err
	}
	if !containsReference(refs, r) {
		return fmt.Errorf("reference proof is missing or changed")
	}
	missing := make([]ReferenceSource, 0, len(r.sources()))
	for _, source := range r.sources() {
		snapshotName := r.sourceSnapshot(source)
		if verifiedGUIDs[snapshotName] == 0 || verifiedGUIDs[snapshotName] != source.GUID {
			return fmt.Errorf("destination snapshot GUID does not match source: %s", snapshotName)
		}
		owned, metadata, findErr := findOwned(state, source.Dataset, snapshotName, lineage)
		if findErr != nil {
			return findErr
		}
		if owned.GUID != source.GUID || metadata != source.Metadata || !slices.Contains(owned.Holds, r.hold()) {
			return fmt.Errorf("source GUID or protecting hold changed")
		}
		found := false
		for _, object := range state.Objects {
			if object.Name == r.sourceBookmark(source) {
				if object.Type != "bookmark" || object.GUID != source.GUID {
					return fmt.Errorf("conflicting bookmark")
				}
				found = true
			}
		}
		if !found {
			missing = append(missing, source)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	if err := s.unchanged(ctx, dataset, true, state); err != nil {
		return err
	}
	for _, source := range missing {
		if err := backend.Bookmark(ctx, r.sourceSnapshot(source), r.sourceBookmark(source)); err != nil {
			return err
		}
	}
	return nil
}

// ReleaseReference removes only exactly proven references. Resume state blocks
// release. Callers must also establish that no active transfer needs the hold.
func (s *Service) ReleaseReference(ctx context.Context, dataset string, r Reference) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	backend, ok := s.backend.(referenceBackend)
	if !ok {
		return fmt.Errorf("reference operations unavailable")
	}
	if err := zfs.ValidateDataset(dataset); err != nil {
		return err
	}
	state, err := s.backend.InspectState(ctx, dataset, true)
	if err != nil {
		return err
	}
	if err := scopeReady(state, dataset); err != nil {
		return err
	}
	lineage, err := RootAuthority(state, dataset, s.installation)
	if err != nil {
		return err
	}
	refs, err := referenceRecords(state, dataset, lineage)
	if err != nil {
		return err
	}
	if !containsReference(refs, r) {
		return fmt.Errorf("reference proof is missing or changed")
	}
	type sourceState struct {
		source      ReferenceSource
		hasBookmark bool
		hasHold     bool
	}
	var found []sourceState
	for _, source := range r.sources() {
		item := sourceState{source: source, hasHold: slices.Contains(state.Holds[r.sourceSnapshot(source)], r.hold())}
		for _, object := range state.Objects {
			if object.Name == r.sourceBookmark(source) {
				if object.Type != "bookmark" || object.GUID != source.GUID {
					return fmt.Errorf("bookmark GUID changed")
				}
				item.hasBookmark = true
			}
		}
		if item.hasHold {
			owned, metadata, findErr := findOwned(state, source.Dataset, r.sourceSnapshot(source), lineage)
			if findErr != nil || owned.GUID != source.GUID || metadata != source.Metadata {
				return fmt.Errorf("held snapshot ownership changed")
			}
		}
		found = append(found, item)
	}
	if err := s.unchanged(ctx, dataset, true, state); err != nil {
		return err
	}
	for _, item := range found {
		if item.hasBookmark {
			if err := backend.DestroyBookmark(ctx, r.sourceBookmark(item.source)); err != nil {
				return err
			}
		}
		if item.hasHold {
			if err := backend.Release(ctx, r.hold(), r.sourceSnapshot(item.source)); err != nil {
				return err
			}
		}
	}
	if err := backend.InheritProperty(ctx, dataset, r.property()); err != nil {
		return err
	}
	return nil
}
