package lifecycle

import (
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
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
	if _, err := s.Protect(t.Context(), "tank/data", snapshot, "local:backup/data"); err != nil || len(b.writes) != 2 {
		t.Fatal("protection not idempotent", err)
	}
	if err := s.Checkpoint(t.Context(), "tank/data", r, guidOf(b, snapshot)+1); err == nil || len(b.writes) != 2 {
		t.Fatal("checkpointed mismatched receive")
	}
	if err := s.Checkpoint(t.Context(), "tank/data", r, guidOf(b, snapshot)); err != nil {
		t.Fatal(err)
	}
	if err := s.Checkpoint(t.Context(), "tank/data", r, guidOf(b, snapshot)); err != nil || len(b.writes) != 3 {
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
	if !r.Recursive || len(sourcesOf(t, b, r)) != 2 || len(b.state.Holds[rootSnapshot]) != 1 || len(b.state.Holds[childSnapshot]) != 1 {
		t.Fatalf("recursive proof=%+v holds=%v", r, b.state.Holds)
	}
	verified := map[string]uint64{rootSnapshot: guidOf(b, rootSnapshot), childSnapshot: 21}
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
	if err := s.CheckpointSet(t.Context(), "tank/data", recursive, map[string]uint64{rootSnapshot: guidOf(b, rootSnapshot), childSnapshot: 21}); err != nil {
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
	if err := s.CheckpointSet(t.Context(), "tank/data", own, map[string]uint64{rootSnapshot: guidOf(b, rootSnapshot)}); err != nil {
		t.Fatalf("checkpoint of the inherited proof: %v", err)
	}
	if err := s.CheckpointSet(t.Context(), "tank/data", own, map[string]uint64{rootSnapshot: guidOf(b, rootSnapshot) + 1}); err == nil {
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
	if len(refs) != 2 || !slices.Contains(refs, slow) || !slices.Contains(refs, fastNew) {
		t.Fatalf("releasing one member's old proof disturbed another: %+v", refs)
	}
	if holds := b.state.Holds["tank/data/slow@"+both.Name()]; len(holds) != 1 {
		t.Fatalf("slow lost its hold: %v", holds)
	}
	// A member's own stream is not recursive: the endpoint is its snapshot
	// alone, even though the root's snapshot of the same instant is held too.
	if sources := sourcesOf(t, b, slow); slow.Recursive || len(sources) != 1 || slow.SnapshotName() != "tank/data/slow@"+both.Name() || sources[0].GUID != guidOf(b, slow.SnapshotName()) {
		t.Fatalf("stream accessors disagree with sources: %+v %+v", slow, sources)
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

// TestRecursiveProofDoesNotGrowWithItsSubtree pins the proof's stored form:
// the stream, the instant and whether it is recursive, and nothing per
// dataset. Which snapshots the endpoint consists of is read back from their
// holds, so a subtree of any size fits one ZFS property. 300 children is well
// past the 8191 bytes a record listing each source used to reach.
func TestRecursiveProofDoesNotGrowWithItsSubtree(t *testing.T) {
	t.Parallel()
	stored := map[int]string{}
	for _, children := range []int{2, 300} {
		b := backendWithSnapshots(t)
		addTestAuthority(b)
		root, metadata := rootSnapshotMetadata(t, b)
		snapshots := append(withChildren(b, children, metadata), root)
		s, _ := NewService(b, testInstallation)
		r, err := s.ProtectSet(t.Context(), "tank/data", "tank/data", snapshots, "local:backup/data")
		if err != nil {
			t.Fatalf("%d children: %v", children, err)
		}
		for _, p := range b.state.Properties {
			if p.Dataset == "tank/data" && p.Name == r.PropertyName() {
				stored[children] = p.Value
			}
		}
		var record map[string]json.RawMessage
		if err := json.Unmarshal([]byte(stored[children]), &record); err != nil {
			t.Fatal(err)
		}
		if len(record) != 2 || record["created"] == nil || string(record["recursive"]) != "true" {
			t.Fatalf("stored proof = %s", stored[children])
		}
		state, _ := b.InspectState(t.Context(), "tank/data", true)
		refs, err := References(state, "tank/data", testLineage)
		if err != nil || len(refs) != 1 || refs[0] != r {
			t.Fatalf("stored proof does not read back as written: %+v %v", refs, err)
		}
		sources, err := r.Sources(state)
		if err != nil || len(sources) != children+1 || sources[0].Dataset != "tank/data" {
			t.Fatalf("%d children: sources=%d err=%v", children, len(sources), err)
		}
	}
	if stored[2] == "" || len(stored[2]) != len(stored[300]) {
		t.Fatalf("proof grew with its subtree: %q vs %q", stored[2], stored[300])
	}
}

// A member stream's proof names the member relative to the root, so it can be
// read, and released, without listing the datasets under the root.
func TestMemberProofNamesItsStream(t *testing.T) {
	t.Parallel()
	b := &memoryBackend{state: zfs.State{Objects: []zfs.Object{
		{Name: "tank/data", Type: "filesystem", GUID: 1},
		{Name: "tank/data/a", Type: "filesystem", GUID: 2},
	}}}
	s, _ := NewService(b, testInstallation)
	metadata, err := s.CreateSnapshot(t.Context(), "tank/data", []string{"tank/data/a"}, time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC), activeTestPolicy("tank/data"))
	if err != nil {
		t.Fatal(err)
	}
	r, err := s.Protect(t.Context(), "tank/data", "tank/data/a@"+metadata.Name(), "local:backup/data")
	if err != nil {
		t.Fatal(err)
	}
	var value string
	for _, p := range b.state.Properties {
		if p.Name == r.PropertyName() {
			value = p.Value
		}
	}
	if want := `{"stream":"a","created":"2026-10-01T00:00:00Z"}`; value != want {
		t.Fatalf("stored proof = %s, want %s", value, want)
	}
	// The root's own state alone reads it back.
	state, _ := b.InspectState(t.Context(), "tank/data", false)
	refs, err := References(state, "tank/data", r.Metadata.Lineage)
	if err != nil || len(refs) != 1 || refs[0] != r {
		t.Fatalf("refs=%+v err=%v", refs, err)
	}
}

// A proof's key and value must agree, and nothing else may be in the value.
func TestReferenceRecordsRejectInconsistentProofs(t *testing.T) {
	t.Parallel()
	b := backendWithSnapshots(t)
	addTestAuthority(b)
	s, _ := NewService(b, testInstallation)
	r, err := s.Protect(t.Context(), "tank/data", b.state.Objects[1].Name, "local:backup/data")
	if err != nil {
		t.Fatal(err)
	}
	for name, value := range map[string]string{
		"another stream": `{"stream":"other","created":"2026-09-05T12:00:00Z"}`,
		"unknown field":  `{"created":"2026-09-05T12:00:00Z","sources":[]}`,
		"no instant":     `{}`,
		"trailing data":  `{"created":"2026-09-05T12:00:00Z"} {}`,
	} {
		state := zfs.State{Properties: []zfs.Property{{Dataset: "tank/data", Name: r.PropertyName(), Value: value, Source: zfs.SourceLocal}}}
		if refs, err := References(state, "tank/data", testLineage); err == nil {
			t.Errorf("%s: accepted %+v", name, refs)
		}
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

// A replica promoted by adoption keeps its source's proofs as hidden received
// values, and a target this installation shares with the source hashes to the
// same key. The received proof is the source's record: protection goes ahead.
func TestProtectIgnoresReceivedProofs(t *testing.T) {
	t.Parallel()
	b := backendWithSnapshots(t)
	addTestAuthority(b)
	s, _ := NewService(b, testInstallation)
	snapshot := b.state.Objects[1].Name
	prior, err := s.Protect(t.Context(), "tank/data", snapshot, "local:backup/data")
	if err != nil {
		t.Fatal(err)
	}
	// Only the received copy of the proof remains, as after a receive -x.
	if err := s.ReleaseReference(t.Context(), "tank/data", prior); err != nil {
		t.Fatal(err)
	}
	b.state.Received = map[string]map[string]string{"tank/data": {prior.PropertyName(): "source installation's proof"}}
	if _, err := s.Protect(t.Context(), "tank/data", snapshot, "local:backup/data"); err != nil {
		t.Fatalf("received proof blocked protection: %v", err)
	}
	if len(b.state.Holds[snapshot]) != 1 {
		t.Fatalf("holds = %v", b.state.Holds)
	}
}

// sourcesOf derives r's sources from everything the backend holds.
func sourcesOf(t *testing.T, b *memoryBackend, r Reference) []ReferenceSource {
	t.Helper()
	sources, err := r.Sources(b.state)
	if err != nil {
		t.Fatal(err)
	}
	return sources
}

func guidOf(b *memoryBackend, name string) uint64 {
	for _, object := range b.state.Objects {
		if object.Name == name {
			return object.GUID
		}
	}
	return 0
}
