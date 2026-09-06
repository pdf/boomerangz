package lifecycle

import (
	"testing"

	"github.com/pdf/boomerangz/internal/zfs"
)

func TestReferenceLifecycle(t *testing.T) {
	t.Parallel()
	b := backendWithSnapshots(t)
	addTestAuthority(b)
	s, _ := NewService(b, testInstallation)
	snapshot := b.state.Objects[1].Name
	r, err := s.Protect(t.Context(), "tank/data", snapshot, "local:backup/data")
	if err != nil {
		t.Fatal(err)
	}
	if len(b.writes) != 2 || len(b.state.Holds[snapshot]) != 1 {
		t.Fatal("missing protection")
	}
	if _, err := s.Protect(t.Context(), "tank/data", snapshot, r.Target); err != nil || len(b.writes) != 2 {
		t.Fatal("protection not idempotent", err)
	}
	if err := s.Checkpoint(t.Context(), "tank/data", r, r.GUID+1); err == nil || len(b.writes) != 2 {
		t.Fatal("checkpointed mismatched receive")
	}
	if err := s.Checkpoint(t.Context(), "tank/data", r, r.GUID); err != nil {
		t.Fatal(err)
	}
	if err := s.Checkpoint(t.Context(), "tank/data", r, r.GUID); err != nil || len(b.writes) != 3 {
		t.Fatal("checkpoint not idempotent", err)
	}
	b.state.ResumeTokens = map[string]string{"tank/data": "token"}
	if err := s.ReleaseReference(t.Context(), "tank/data", r); err == nil || len(b.writes) != 3 {
		t.Fatal("released resumable state")
	}
	b.state.ResumeTokens = nil
	if err := s.ReleaseReference(t.Context(), "tank/data", r); err != nil {
		t.Fatal(err)
	}
	if len(b.writes) != 6 || len(b.state.Holds[snapshot]) != 0 {
		t.Fatal("incomplete reference release")
	}
}

func TestRecursiveReferenceUsesOneRootProof(t *testing.T) {
	t.Parallel()
	b := backendWithSnapshots(t)
	addTestAuthority(b)
	rootSnapshot := b.state.Objects[1].Name
	var ownedMetadata Metadata
	// Use the exact root snapshot metadata so recursive members share the same
	// snapshot UUID and timestamp, as zfs snapshot -r -o produces in practice.
	for _, snapshot := range snapshotsIn(b.state, "tank/data") {
		if snapshot.Name == rootSnapshot {
			var err error
			ownedMetadata, err = Ownership(snapshot, testLineage)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	childSnapshot := "tank/data/child@" + ownedMetadata.Name()
	b.state.Objects = append(b.state.Objects,
		zfs.Object{Name: "tank/data/child", Type: "filesystem", GUID: 20},
		zfs.Object{Name: childSnapshot, Type: "snapshot", GUID: 21},
	)
	for key, value := range ownedMetadata.Properties() {
		b.state.Properties = append(b.state.Properties, zfs.Property{Dataset: childSnapshot, Name: key, Value: value, Source: zfs.SourceLocal})
	}
	s, _ := NewService(b, testInstallation)
	r, err := s.ProtectSet(t.Context(), "tank/data", []string{childSnapshot, rootSnapshot}, "local:backup/data")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Sources) != 2 || len(b.state.Holds[rootSnapshot]) != 1 || len(b.state.Holds[childSnapshot]) != 1 {
		t.Fatalf("recursive proof=%+v holds=%v", r, b.state.Holds)
	}
	verified := map[string]uint64{rootSnapshot: r.Sources[0].GUID, childSnapshot: 21}
	if err := s.CheckpointSet(t.Context(), "tank/data", r, verified); err != nil {
		t.Fatal(err)
	}
	if err := s.ReleaseCompletedHold(t.Context(), "tank/data", r); err != nil {
		t.Fatal(err)
	}
	if len(b.state.Holds[rootSnapshot])+len(b.state.Holds[childSnapshot]) != 0 {
		t.Fatal("recursive holds not released")
	}
}

func TestUnprovenReferencesAreNeverClaimed(t *testing.T) {
	t.Parallel()
	b := backendWithSnapshots(t)
	addTestAuthority(b)
	s, _ := NewService(b, testInstallation)
	snapshot := b.state.Objects[1].Name
	b.state.Holds = map[string][]string{snapshot: {SnapshotPrefix + TargetID("local:backup/data")}}
	if _, err := s.Protect(t.Context(), "tank/data", snapshot, "local:backup/data"); err == nil || len(b.writes) != 0 {
		t.Fatal("claimed name-only hold")
	}
}
