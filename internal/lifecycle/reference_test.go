package lifecycle

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
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

func TestRootStreamInheritsItsSubtreeProof(t *testing.T) {
	t.Parallel()
	b := backendWithSnapshots(t)
	addTestAuthority(b)
	rootSnapshot := b.state.Objects[1].Name
	var metadata Metadata
	for _, snapshot := range snapshotsIn(b.state, "tank/data") {
		if snapshot.Name == rootSnapshot {
			var err error
			if metadata, err = Ownership(snapshot, testLineage); err != nil {
				t.Fatal(err)
			}
		}
	}
	childSnapshot := "tank/data/child@" + metadata.Name()
	b.state.Objects = append(b.state.Objects,
		zfs.Object{Name: "tank/data/child", Type: "filesystem", GUID: 20},
		zfs.Object{Name: childSnapshot, Type: "snapshot", GUID: 21},
	)
	for key, value := range metadata.Properties() {
		b.state.Properties = append(b.state.Properties, zfs.Property{Dataset: childSnapshot, Name: key, Value: value, Source: zfs.SourceLocal})
	}
	s, _ := NewService(b, testInstallation)
	const target = "local:backup/data"
	// A send -R stream proved and checkpointed the whole subtree.
	recursive, err := s.ProtectSet(t.Context(), "tank/data", "tank/data", []string{childSnapshot, rootSnapshot}, target)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CheckpointSet(t.Context(), "tank/data", recursive, map[string]uint64{rootSnapshot: recursive.Sources[0].GUID, childSnapshot: 21}); err != nil {
		t.Fatal(err)
	}
	// While the subtree's holds stand, the root's own stream must see the
	// child's too rather than placing it again.
	if _, err := s.ProtectSet(t.Context(), "tank/data", "tank/data", []string{rootSnapshot}, target); err != nil {
		t.Fatalf("a covered proof with its holds in place was refused: %v", err)
	}
	if err := s.ReleaseCompletedHold(t.Context(), "tank/data", recursive); err != nil {
		t.Fatal(err)
	}
	// After replicate=off the root's own stream asks for the root alone at
	// the same instant. The stored proof covers it and stands unchanged.
	own, err := s.ProtectSet(t.Context(), "tank/data", "tank/data", []string{rootSnapshot}, target)
	if err != nil {
		t.Fatalf("a covered proof was refused: %v", err)
	}
	if !reflect.DeepEqual(own, recursive) {
		t.Fatalf("proof = %+v, want the stored %+v", own, recursive)
	}
	// Only the root's snapshot was sent; the child's was verified before.
	if err := s.CheckpointSet(t.Context(), "tank/data", own, map[string]uint64{rootSnapshot: own.Sources[0].GUID}); err != nil {
		t.Fatalf("checkpoint of the inherited proof: %v", err)
	}
	if err := s.CheckpointSet(t.Context(), "tank/data", own, map[string]uint64{rootSnapshot: own.Sources[0].GUID + 1}); err == nil {
		t.Fatal("a mismatched verified GUID was accepted")
	}
}

func TestNarrowerProofDoesNotCoverAWiderStream(t *testing.T) {
	t.Parallel()
	b := backendWithSnapshots(t)
	addTestAuthority(b)
	rootSnapshot := b.state.Objects[1].Name
	var metadata Metadata
	for _, snapshot := range snapshotsIn(b.state, "tank/data") {
		if snapshot.Name == rootSnapshot {
			var err error
			if metadata, err = Ownership(snapshot, testLineage); err != nil {
				t.Fatal(err)
			}
		}
	}
	childSnapshot := "tank/data/child@" + metadata.Name()
	b.state.Objects = append(b.state.Objects,
		zfs.Object{Name: "tank/data/child", Type: "filesystem", GUID: 20},
		zfs.Object{Name: childSnapshot, Type: "snapshot", GUID: 21},
	)
	for key, value := range metadata.Properties() {
		b.state.Properties = append(b.state.Properties, zfs.Property{Dataset: childSnapshot, Name: key, Value: value, Source: zfs.SourceLocal})
	}
	s, _ := NewService(b, testInstallation)
	if _, err := s.ProtectSet(t.Context(), "tank/data", "tank/data", []string{rootSnapshot}, "local:backup/data"); err != nil {
		t.Fatal(err)
	}
	// The root alone was proved; nothing proves the child reached the target.
	if _, err := s.ProtectSet(t.Context(), "tank/data", "tank/data", []string{childSnapshot, rootSnapshot}, "local:backup/data"); err == nil {
		t.Fatal("a proof of the root alone was taken to cover its subtree")
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

// withChildren adds count child datasets of tank/data, each with a snapshot
// carrying metadata, and returns those snapshots' names.
func withChildren(b *memoryBackend, count int, metadata Metadata) []string {
	var names []string
	for i := range count {
		dataset := fmt.Sprintf("tank/data/child-%03d", i)
		snapshot := dataset + "@" + metadata.Name()
		b.state.Objects = append(b.state.Objects,
			zfs.Object{Name: dataset, Type: "filesystem", GUID: uint64(1000 + 2*i)},
			zfs.Object{Name: snapshot, Type: "snapshot", GUID: uint64(1001 + 2*i)},
		)
		for key, value := range metadata.Properties() {
			b.state.Properties = append(b.state.Properties, zfs.Property{Dataset: snapshot, Name: key, Value: value, Source: zfs.SourceLocal})
		}
		names = append(names, snapshot)
	}
	return names
}

func rootSnapshotMetadata(t *testing.T, b *memoryBackend) (string, Metadata) {
	t.Helper()
	root := b.state.Objects[1].Name
	for _, snapshot := range snapshotsIn(b.state, "tank/data") {
		if snapshot.Name == root {
			metadata, err := Ownership(snapshot, testLineage)
			if err != nil {
				t.Fatal(err)
			}
			return root, metadata
		}
	}
	t.Fatal("root snapshot missing")
	return "", Metadata{}
}

func TestReferenceProofStoresEachSourceOnlyByDatasetAndGUID(t *testing.T) {
	t.Parallel()
	b := backendWithSnapshots(t)
	addTestAuthority(b)
	root, metadata := rootSnapshotMetadata(t, b)
	snapshots := append(withChildren(b, 2, metadata), root)
	s, _ := NewService(b, testInstallation)
	r, err := s.ProtectSet(t.Context(), "tank/data", "tank/data", snapshots, "local:backup/data")
	if err != nil {
		t.Fatal(err)
	}
	var stored string
	for _, p := range b.state.Properties {
		if p.Dataset == "tank/data" && p.Name == r.PropertyName() {
			stored = p.Value
		}
	}
	var record struct {
		Metadata json.RawMessage              `json:"metadata"`
		Sources  []map[string]json.RawMessage `json:"sources"`
	}
	if err := json.Unmarshal([]byte(stored), &record); err != nil {
		t.Fatal(err)
	}
	if len(record.Metadata) == 0 || len(record.Sources) != 3 {
		t.Fatalf("stored proof = %s", stored)
	}
	// The instant is said once; every source is of it.
	for _, source := range record.Sources {
		if len(source) != 2 || source["dataset"] == nil || source["guid"] == nil {
			t.Fatalf("source carries more than its dataset and GUID: %s", stored)
		}
	}
	state, _ := b.InspectState(t.Context(), "tank/data", false)
	refs, err := References(state, "tank/data", testLineage)
	if err != nil || len(refs) != 1 || !reflect.DeepEqual(refs[0], r) {
		t.Fatalf("stored proof does not read back as written: %+v %v", refs, err)
	}
}

func TestProtectSetRefusesSourcesOfDifferentInstants(t *testing.T) {
	t.Parallel()
	b := backendWithSnapshots(t)
	addTestAuthority(b)
	root, metadata := rootSnapshotMetadata(t, b)
	other, err := NewMetadata(testLineage, metadata.Created.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	snapshots := append(withChildren(b, 1, other), root)
	s, _ := NewService(b, testInstallation)
	if _, err := s.ProtectSet(t.Context(), "tank/data", "tank/data", snapshots, "local:backup/data"); err == nil || !strings.Contains(err.Error(), "instant") {
		t.Fatalf("mixed-instant proof accepted: %v", err)
	}
	if len(b.writes) != 0 {
		t.Fatalf("refused proof wrote %v", b.writes)
	}
}

func TestProtectSetRefusesProofOverThePropertyLimit(t *testing.T) {
	t.Parallel()
	b := backendWithSnapshots(t)
	addTestAuthority(b)
	root, metadata := rootSnapshotMetadata(t, b)
	snapshots := append(withChildren(b, 200, metadata), root)
	s, _ := NewService(b, testInstallation)
	if _, err := s.ProtectSet(t.Context(), "tank/data", "tank/data", snapshots, "local:backup/data"); err == nil || !strings.Contains(err.Error(), "property limit") {
		t.Fatalf("oversized proof accepted: %v", err)
	}
	// Refused before the record or any hold, so nothing needs undoing.
	if len(b.writes) != 0 {
		t.Fatalf("refused proof wrote %v", b.writes)
	}
}
