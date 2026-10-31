package daemon

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/pdf/boomerangz/internal/lifecycle"
	"github.com/pdf/boomerangz/internal/policy"
	"github.com/pdf/boomerangz/internal/zfs"
)

// replicationRootPool extends the report pool with a child beneath the report
// source and gives both one owned snapshot an hour old, as a replication
// root's single atomic snapshot would.
func replicationRootPool(t *testing.T, now time.Time) (*memoryZFS, []zfs.Property, string, string) {
	t.Helper()
	const child = reportSource + "/child"
	source, properties := newReportPool(t)
	source.inventory = append(source.inventory, zfs.Dataset{Name: child, Type: zfs.Filesystem, EncryptionRoot: "-"})
	source.states[child] = zfs.State{Objects: []zfs.Object{{Name: child, Type: "filesystem", GUID: 5, CreateTXG: 1}}}
	metadata, err := lifecycle.NewMetadata(reportLineage, now.Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if err := source.Snapshot(t.Context(), []string{reportSource, child}, metadata.Name(), metadata.Properties()); err != nil {
		t.Fatal(err)
	}
	properties = append(properties, zfs.Property{Dataset: reportSource, Name: policy.Namespace + "replicate", Value: "on", Source: zfs.SourceLocal})
	return source, properties, child, metadata.Name()
}

// main protected a replication root's pending snapshot by inspecting the root
// non-recursively and then searching that state for its children's snapshots,
// which a non-recursive inspection never contains. Only the root's snapshot was
// held, so the -R stream's base on every descendant could be destroyed before
// it was sent, and the transfer's own proof for the same instant then
// conflicted with the pending one.
func TestReplicationRootPendingProtectionHoldsTheSubtree(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 9, 6, 12, 30, 0, 0, time.UTC)
	source, _, child, name := replicationRootPool(t, now)
	runtime, _ := newReportRuntime(t, source, memoryStream{source: source, destination: source})
	if err := runtime.protectAndCoalesce(reportSource, reportSource+"@"+name, reportCanonical, true); err != nil {
		t.Fatal(err)
	}
	hold := lifecycle.SnapshotPrefix + lifecycle.TargetID(reportCanonical)
	for _, snapshot := range []string{reportSource + "@" + name, child + "@" + name} {
		if !slices.Contains(source.state(snapshot).Holds[snapshot], hold) {
			t.Errorf("%s is not held for the target", snapshot)
		}
	}
	state, err := source.InspectState(t.Context(), reportSource, false)
	if err != nil {
		t.Fatal(err)
	}
	references, err := lifecycle.References(state, reportSource, reportLineage)
	if err != nil {
		t.Fatal(err)
	}
	if len(references) != 1 || references[0].Dataset != reportSource || len(references[0].Sources) != 2 {
		t.Fatalf("the root stream's proof should cover its subtree: %+v", references)
	}
}

// main pruned a replication root by planning only the root's own snapshots.
// The -R snapshot lands on every descendant too, and nothing else ever pruned
// those, so they accumulated without bound.
func TestReplicationRootPrunesEveryDescendant(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 9, 6, 12, 30, 0, 0, time.UTC)
	source, properties, child, old := replicationRootPool(t, now)
	runtime, _ := newReportRuntime(t, source, memoryStream{source: source, destination: source})
	runtime.now = func() time.Time { return now }
	if err := runtime.management.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		runtime.management.Close()
		runtime.management.wait.Wait()
	})
	subscription := subscribe(t, runtime.status)
	// No targets: the run is about what gets snapshotted and pruned.
	properties = slices.DeleteFunc(properties, func(row zfs.Property) bool { return row.Name == policy.Namespace+"local" })
	properties = append(properties, zfs.Property{Dataset: reportSource, Name: policy.Namespace + "policy", Value: "1x5m", Source: zfs.SourceLocal})
	effective := policy.Resolve(zfs.Dataset{Name: reportSource, Type: zfs.Filesystem, EncryptionRoot: "-"}, nil, properties, nil)
	if !effective.Send.Replicate {
		t.Fatal("fixture: the root does not replicate")
	}
	members := []ScheduleMember{{Dataset: reportSource, Grid: effective.Grid}, {Dataset: child, Grid: effective.Grid}}
	if !runtime.enqueueSnapshot(Schedule{Dataset: reportSource, Policy: effective, Members: members, Due: []string{reportSource, child}}) {
		t.Fatal("snapshot job was not accepted")
	}
	runs := runsOf(t, subscription, "snapshot:"+reportSource, "prune:"+reportSource)
	if outcome := runs["snapshot:"+reportSource]; outcome[len(outcome)-1].State != "succeeded" {
		t.Fatalf("snapshot run = %+v", outcome)
	}
	prune := runs["prune:"+reportSource]
	if prune[len(prune)-1].State != "succeeded" {
		t.Fatalf("prune run = %+v", prune)
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	for _, dataset := range []string{reportSource, child} {
		for _, object := range source.state(dataset).Objects {
			if object.Name == dataset+"@"+old {
				t.Errorf("%s survived a 1x5m grid an hour after it was taken", object.Name)
			}
		}
		snapshots := slices.DeleteFunc(slices.Clone(source.state(dataset).Objects), func(object zfs.Object) bool {
			return object.Type != "snapshot" || !strings.Contains(object.Name, "@boomerangz-")
		})
		if len(snapshots) == 0 {
			t.Errorf("%s lost its newest snapshot", dataset)
		}
	}
}
