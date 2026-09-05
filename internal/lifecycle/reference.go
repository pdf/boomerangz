package lifecycle

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
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
	Target   string   `json:"target"`
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
func (r Reference) bookmark(dataset string) string {
	return dataset + "#" + r.hold() + "-" + r.Metadata.Snapshot
}
func (r Reference) snapshot(dataset string) string { return dataset + "@" + r.Metadata.Name() }

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
		// A received proof describes another source's targets. It is cleanup
		// metadata, not authority to claim local holds or bookmarks.
		if p.Source != zfs.SourceLocal {
			continue
		}
		var r Reference
		if json.Unmarshal([]byte(p.Value), &r) != nil || r.Target == "" || r.GUID == 0 || r.Metadata.Lineage != lineage || !ValidID(r.Metadata.Snapshot) || r.Metadata.Created.IsZero() || r.property() != p.Name || seen[p.Name] {
			return nil, fmt.Errorf("invalid reference proof %s", p.Name)
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

// Protect records a recovery proof before placing the target hold. If interrupted
// between writes, the record remains reconstructable and a retry is idempotent.
func (s *Service) Protect(ctx context.Context, dataset, snapshot, target string) (Reference, error) {
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
	state, err := s.backend.InspectState(ctx, dataset, false)
	if err != nil {
		return Reference{}, err
	}
	if err := scopeReady(state, dataset); err != nil {
		return Reference{}, err
	}
	lineage, err := storedLineage(state, dataset)
	if err != nil {
		return Reference{}, err
	}
	owned, metadata, err := findOwned(state, dataset, snapshot, lineage)
	if err != nil {
		return Reference{}, err
	}
	r := Reference{Target: target, GUID: owned.GUID, Metadata: metadata}
	refs, err := referenceRecords(state, dataset, lineage)
	if err != nil {
		return Reference{}, err
	}
	exists := false
	for _, prior := range refs {
		if prior.property() == r.property() {
			if prior != r {
				return Reference{}, fmt.Errorf("conflicting reference proof")
			}
			exists = true
		}
	}
	if state.Received[dataset][r.property()] != "" {
		return Reference{}, fmt.Errorf("received reference proof requires explicit resolution")
	}
	if !exists && slices.Contains(state.Holds[snapshot], r.hold()) {
		return Reference{}, fmt.Errorf("unproven existing target hold")
	}
	if err := s.unchanged(ctx, dataset, false, state); err != nil {
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
	if !slices.Contains(state.Holds[snapshot], r.hold()) {
		if err := backend.Hold(ctx, r.hold(), snapshot); err != nil {
			return Reference{}, err
		}
	}
	return r, nil
}

// Checkpoint creates a versioned bookmark only after the caller supplies the
// destination snapshot GUID obtained by a successful receive verification.
// It deliberately retains the hold and previous checkpoints; the transfer layer
// releases obsolete references only once recovery no longer needs them.
func (s *Service) Checkpoint(ctx context.Context, dataset string, r Reference, verifiedGUID uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	backend, ok := s.backend.(referenceBackend)
	if !ok {
		return fmt.Errorf("reference operations unavailable")
	}
	if err := zfs.ValidateDataset(dataset); err != nil {
		return err
	}
	if verifiedGUID == 0 || verifiedGUID != r.GUID {
		return fmt.Errorf("destination snapshot GUID does not match source")
	}
	state, err := s.backend.InspectState(ctx, dataset, false)
	if err != nil {
		return err
	}
	if err := scopeReady(state, dataset); err != nil {
		return err
	}
	lineage, err := storedLineage(state, dataset)
	if err != nil {
		return err
	}
	refs, err := referenceRecords(state, dataset, lineage)
	if err != nil {
		return err
	}
	if !slices.Contains(refs, r) {
		return fmt.Errorf("reference proof is missing or changed")
	}
	owned, metadata, err := findOwned(state, dataset, r.snapshot(dataset), lineage)
	if err != nil {
		return err
	}
	if owned.GUID != r.GUID || metadata != r.Metadata || !slices.Contains(owned.Holds, r.hold()) {
		return fmt.Errorf("source GUID or protecting hold changed")
	}
	for _, o := range state.Objects {
		if o.Name == r.bookmark(dataset) {
			if o.Type != "bookmark" || o.GUID != r.GUID {
				return fmt.Errorf("conflicting bookmark")
			}
			return nil
		}
	}
	if err := s.unchanged(ctx, dataset, false, state); err != nil {
		return err
	}
	return backend.Bookmark(ctx, r.snapshot(dataset), r.bookmark(dataset))
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
	state, err := s.backend.InspectState(ctx, dataset, false)
	if err != nil {
		return err
	}
	if err := scopeReady(state, dataset); err != nil {
		return err
	}
	lineage, err := storedLineage(state, dataset)
	if err != nil {
		return err
	}
	refs, err := referenceRecords(state, dataset, lineage)
	if err != nil {
		return err
	}
	if !slices.Contains(refs, r) {
		return fmt.Errorf("reference proof is missing or changed")
	}
	hasBookmark := false
	for _, o := range state.Objects {
		if o.Name == r.bookmark(dataset) {
			if o.Type != "bookmark" || o.GUID != r.GUID {
				return fmt.Errorf("bookmark GUID changed")
			}
			hasBookmark = true
		}
	}
	hasHold := slices.Contains(state.Holds[r.snapshot(dataset)], r.hold())
	if hasHold {
		owned, metadata, err := findOwned(state, dataset, r.snapshot(dataset), lineage)
		if err != nil || owned.GUID != r.GUID || metadata != r.Metadata {
			return fmt.Errorf("held snapshot ownership changed")
		}
	}
	if err := s.unchanged(ctx, dataset, false, state); err != nil {
		return err
	}
	if hasBookmark {
		if err := backend.DestroyBookmark(ctx, r.bookmark(dataset)); err != nil {
			return err
		}
	}
	if hasHold {
		if err := backend.Release(ctx, r.hold(), r.snapshot(dataset)); err != nil {
			return err
		}
	}
	return backend.InheritProperty(ctx, dataset, r.property())
}
