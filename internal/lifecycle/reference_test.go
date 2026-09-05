package lifecycle

import (
	"testing"

	"github.com/pdf/boomerangz/internal/zfs"
)

func TestReferenceLifecycle(t *testing.T) {
	t.Parallel()
	b := backendWithSnapshots(t)
	b.state.Properties = append(b.state.Properties, zfs.Property{Dataset: "tank/data", Name: LineageProperty, Value: testLineage, Source: zfs.SourceLocal})
	s, _ := NewService(b)
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

func TestUnprovenReferencesAreNeverClaimed(t *testing.T) {
	t.Parallel()
	b := backendWithSnapshots(t)
	b.state.Properties = append(b.state.Properties, zfs.Property{Dataset: "tank/data", Name: LineageProperty, Value: testLineage, Source: zfs.SourceLocal})
	s, _ := NewService(b)
	snapshot := b.state.Objects[1].Name
	b.state.Holds = map[string][]string{snapshot: {SnapshotPrefix + TargetID("local:backup/data")}}
	if _, err := s.Protect(t.Context(), "tank/data", snapshot, "local:backup/data"); err == nil || len(b.writes) != 0 {
		t.Fatal("claimed name-only hold")
	}
}
