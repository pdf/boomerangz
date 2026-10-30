package lifecycle

import (
	"context"
	"fmt"
	"reflect"
	"slices"

	"github.com/pdf/boomerangz/internal/zfs"
)

// DatasetLineage resolves explicit ownership metadata, not policy inheritance.
func DatasetLineage(state zfs.State, dataset string) (string, error) {
	return storedLineage(state, dataset)
}

// Snapshots returns a detached exact-dataset lifecycle inventory.
func Snapshots(state zfs.State, dataset string) []Snapshot {
	result := snapshotsIn(state, dataset)
	for i := range result {
		result[i].Properties = slices.Clone(result[i].Properties)
		result[i].Holds = slices.Clone(result[i].Holds)
		result[i].Clones = slices.Clone(result[i].Clones)
	}
	return result
}

// References returns locally proven target records for reconstruction.
func References(state zfs.State, dataset, lineage string) ([]Reference, error) {
	return referenceRecords(state, dataset, lineage)
}

// BookmarkName is the versioned cursor of the stream's own snapshot.
func (r Reference) BookmarkName() string { return r.bookmark() }

// SnapshotName is the stream's own source snapshot.
func (r Reference) SnapshotName() string { return r.snapshot() }

// PropertyName is the root property that records this proof.
func (r Reference) PropertyName() string { return r.property() }

// HoldName returns the deterministic target hold associated with this proof.
func (r Reference) HoldName() string { return r.hold() }

// ReleaseCompletedHold keeps the verified bookmark/record but releases its hold.
// The caller must have verified the destination and excluded active/resume work.
func (s *Service) ReleaseCompletedHold(ctx context.Context, dataset string, r Reference) error {
	unlock, err := s.lock(ctx, dataset)
	if err != nil {
		return err
	}
	defer unlock()
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
		return fmt.Errorf("reference proof changed")
	}
	var held []ReferenceSource
	for _, source := range r.sources() {
		snapshot, metadata, findErr := findOwned(state, source.Dataset, r.sourceSnapshot(source), lineage)
		if findErr != nil || snapshot.GUID != source.GUID || !reflect.DeepEqual(metadata, source.Metadata) {
			return fmt.Errorf("held snapshot proof changed")
		}
		found := false
		for _, object := range state.Objects {
			if object.Name == r.sourceBookmark(source) && object.Type == "bookmark" && object.GUID == source.GUID {
				found = true
			}
		}
		if !found {
			return fmt.Errorf("verified checkpoint is missing")
		}
		if slices.Contains(snapshot.Holds, r.hold()) {
			held = append(held, source)
		}
	}
	if len(held) == 0 {
		return nil
	}
	if err := s.unchanged(ctx, dataset, true, state); err != nil {
		return err
	}
	for _, source := range held {
		if err := backend.Release(ctx, r.hold(), r.sourceSnapshot(source)); err != nil {
			return err
		}
	}
	return nil
}
