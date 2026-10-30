package lifecycle

import (
	"encoding/json"
	"testing"
	"time"

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
	if err := s.Checkpoint(t.Context(), "tank/data", r, r.GUID()+1); err == nil || len(b.writes) != 2 {
		t.Fatal("checkpointed mismatched receive")
	}
	if err := s.Checkpoint(t.Context(), "tank/data", r, r.GUID()); err != nil {
		t.Fatal(err)
	}
	if err := s.Checkpoint(t.Context(), "tank/data", r, r.GUID()); err != nil || len(b.writes) != 3 {
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

func TestReplicationStreamUsesOneProofForItsSubtree(t *testing.T) {
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
	r, err := s.ProtectSet(t.Context(), "tank/data", "tank/data", []string{childSnapshot, rootSnapshot}, "local:backup/data")
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

// TestMemberStreamsKeepIndependentProofs covers the normal case under a root:
// members snapshotted at different instants, each with its own stream to the
// same target. One stream's proof must never stand in for, or release, another.
func TestMemberStreamsKeepIndependentProofs(t *testing.T) {
	t.Parallel()
	b := &memoryBackend{state: zfs.State{Objects: []zfs.Object{
		{Name: "tank/data", Type: "filesystem", GUID: 1},
		{Name: "tank/data/fast", Type: "filesystem", GUID: 2},
		{Name: "tank/data/slow", Type: "filesystem", GUID: 3},
	}}}
	s, err := NewService(b, testInstallation)
	if err != nil {
		t.Fatal(err)
	}
	effective := activeTestPolicy("tank/data")
	start := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	both, err := s.CreateSnapshot(t.Context(), "tank/data", []string{"tank/data/fast", "tank/data/slow"}, start, effective)
	if err != nil {
		t.Fatal(err)
	}
	later, err := s.CreateSnapshot(t.Context(), "tank/data", []string{"tank/data/fast"}, start.Add(time.Hour), effective)
	if err != nil {
		t.Fatal(err)
	}
	const target = "local:backup/data"
	fastOld, err := s.Protect(t.Context(), "tank/data", "tank/data/fast@"+both.Name(), target)
	if err != nil {
		t.Fatal(err)
	}
	slow, err := s.Protect(t.Context(), "tank/data", "tank/data/slow@"+both.Name(), target)
	if err != nil {
		t.Fatal(err)
	}
	// Same instant, same target, different streams: separate proofs.
	if fastOld.property() == slow.property() {
		t.Fatal("two members' proofs share one property")
	}
	fastNew, err := s.Protect(t.Context(), "tank/data", "tank/data/fast@"+later.Name(), target)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.ReleaseReference(t.Context(), "tank/data", fastOld); err != nil {
		t.Fatal(err)
	}
	state, _ := b.InspectState(t.Context(), "tank/data", true)
	refs, err := References(state, "tank/data", fastNew.Metadata.Lineage)
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 2 || !containsReference(refs, slow) || !containsReference(refs, fastNew) {
		t.Fatalf("releasing one member's old proof disturbed another: %+v", refs)
	}
	if holds := b.state.Holds["tank/data/slow@"+both.Name()]; len(holds) != 1 {
		t.Fatalf("slow lost its hold: %v", holds)
	}
	// One member, one source: the same shape as any other proof, with no
	// singular copy of the stream's snapshot to drift from it.
	encoded, err := json.Marshal(slow)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatal(err)
	}
	if _, singular := fields["guid"]; singular || len(slow.Sources) != 1 {
		t.Fatalf("serialized proof = %s", encoded)
	}
	if slow.GUID() != slow.Sources[0].GUID || slow.SnapshotName() != "tank/data/slow@"+both.Name() {
		t.Fatalf("stream accessors disagree with sources: %+v", slow)
	}
}

func TestStreamProofsStayInsideTheirStream(t *testing.T) {
	t.Parallel()
	b := &memoryBackend{state: zfs.State{Objects: []zfs.Object{
		{Name: "tank/data", Type: "filesystem", GUID: 1},
		{Name: "tank/data/a", Type: "filesystem", GUID: 2},
		{Name: "tank/data/b", Type: "filesystem", GUID: 3},
	}}}
	s, _ := NewService(b, testInstallation)
	metadata, err := s.CreateSnapshot(t.Context(), "tank/data", []string{"tank/data", "tank/data/a", "tank/data/b"}, time.Now(), activeTestPolicy("tank/data"))
	if err != nil {
		t.Fatal(err)
	}
	writes := len(b.writes)
	for name, test := range map[string]struct {
		stream    string
		snapshots []string
	}{
		"sibling in stream":    {"tank/data/a", []string{"tank/data/a@" + metadata.Name(), "tank/data/b@" + metadata.Name()}},
		"stream outside root":  {"tank/other", []string{"tank/data/a@" + metadata.Name()}},
		"missing own snapshot": {"tank/data", []string{"tank/data/a@" + metadata.Name()}},
	} {
		if _, err := s.ProtectSet(t.Context(), "tank/data", test.stream, test.snapshots, "local:backup/data"); err == nil {
			t.Errorf("%s: protected", name)
		}
	}
	if len(b.writes) != writes {
		t.Fatalf("refused protections wrote %v", b.writes[writes:])
	}
}
