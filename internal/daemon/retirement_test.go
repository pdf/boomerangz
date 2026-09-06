package daemon

import (
	"reflect"
	"testing"
	"time"

	"github.com/pdf/boomerangz/internal/lifecycle"
	"github.com/pdf/boomerangz/internal/zfs"
)

func replicaFixture(t *testing.T) (string, lifecycle.Metadata, zfs.State) {
	t.Helper()
	const lineage = "11111111-1111-4111-8111-111111111111"
	metadata := lifecycle.Metadata{Lineage: lineage, Snapshot: "22222222-2222-4222-8222-222222222222", Created: time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)}
	name := "backup/data@" + metadata.Name()
	state := zfs.State{
		Objects: []zfs.Object{{Name: "backup/data", Type: "filesystem", GUID: 1}, {Name: name, Type: "snapshot", GUID: 2}, {Name: "backup/data@foreign", Type: "snapshot", GUID: 3}},
		Properties: []zfs.Property{
			{Dataset: name, Name: lifecycle.LineageProperty, Value: lineage, Source: zfs.SourceLocal},
			{Dataset: name, Name: lifecycle.SnapshotProperty, Value: metadata.Snapshot, Source: zfs.SourceLocal},
			{Dataset: name, Name: lifecycle.CreatedProperty, Value: metadata.Created.Format(time.RFC3339Nano), Source: zfs.SourceLocal},
		},
		Holds: make(map[string][]string), Clones: make(map[string][]string), ResumeTokens: make(map[string]string),
	}
	return lineage, metadata, state
}

func TestOwnedReplicasPreserveForeignSnapshotsAndRejectDependencies(t *testing.T) {
	t.Parallel()
	lineage, _, state := replicaFixture(t)
	datasets, snapshots, err := ownedReplicas(state, lineage)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(datasets, []string{"backup/data"}) || len(snapshots) != 1 || snapshots[0] == "backup/data@foreign" {
		t.Fatalf("unexpected replicas: %v %v", datasets, snapshots)
	}
	state.Holds[snapshots[0]] = []string{"foreign-hold"}
	if _, _, err := ownedReplicas(state, lineage); err == nil {
		t.Fatal("dependent replica was accepted")
	}
}

func TestVerifyReplicaBindsFullMetadataAndGUID(t *testing.T) {
	t.Parallel()
	lineage, metadata, state := replicaFixture(t)
	name := state.Objects[1].Name
	if err := verifyReplica(state, name, lineage, 2, metadata); err != nil {
		t.Fatal(err)
	}
	changed := metadata
	changed.Snapshot = "33333333-3333-4333-8333-333333333333"
	if err := verifyReplica(state, name, lineage, 2, changed); err == nil {
		t.Fatal("mismatched full snapshot identity was accepted")
	}
}
