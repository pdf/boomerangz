package daemon

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/pdf/boomerangz/internal/discovery"
	"github.com/pdf/boomerangz/internal/policy"
	"github.com/pdf/boomerangz/internal/zfs"
)

// collisionReader serves discovery a fixed set of datasets and properties.
type collisionReader struct {
	datasets   []string
	properties []zfs.Property
}

func (r collisionReader) ListDatasets(context.Context) ([]zfs.Dataset, error) {
	var datasets []zfs.Dataset
	for _, name := range r.datasets {
		datasets = append(datasets, zfs.Dataset{Name: name, Type: zfs.Filesystem, EncryptionRoot: "-"})
	}
	return datasets, nil
}

func (r collisionReader) GetActivationProperties(context.Context) ([]zfs.Property, error) {
	return slices.DeleteFunc(slices.Clone(r.properties), func(row zfs.Property) bool { return row.Name != policy.Namespace+"enabled" }), nil
}

func (r collisionReader) GetStoredProperties(_ context.Context, names []string) ([]zfs.Property, error) {
	return slices.DeleteFunc(slices.Clone(r.properties), func(row zfs.Property) bool { return !slices.Contains(names, row.Dataset) }), nil
}

func TestBlockedTargetIsRefusedWithoutHoldingSnapshots(t *testing.T) {
	t.Parallel()
	source, properties := newReportPool(t)
	runtime, log := newReportRuntime(t, source, memoryStream{source: source, destination: source})
	// tank/other's replica would land on backup/data too, and neither root
	// has established one there, so both are blocked from that target.
	reader := collisionReader{
		datasets: []string{"tank", reportSource, "tank/other", "backup"},
		properties: append(slices.Clone(properties),
			zfs.Property{Dataset: "tank/other", Name: policy.Namespace + "enabled", Value: "on", Source: zfs.SourceLocal},
			zfs.Property{Dataset: "tank/other", Name: policy.Namespace + "local", Value: reportDestination, Source: zfs.SourceLocal},
		),
	}
	scanner, err := discovery.New(reader, discovery.Options{})
	if err != nil {
		t.Fatal(err)
	}
	generation, err := scanner.Scan(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	entry, _ := generation.Inspect(reportSource)
	// The root stays scheduled: snapshots and pruning do not depend on where
	// one of its replicas lands.
	if !isSchedulable(entry) || entry.ErrBlocked(reportDestination, false) == nil {
		t.Fatalf("fixture: schedulable=%t blocked=%+v errors=%q", isSchedulable(entry), entry.Blocked, entry.Policy.Errors)
	}
	runtime.mu.Lock()
	runtime.generation = generation
	runtime.mu.Unlock()

	snapshot := snapshotAt(t, source, 4)
	job := "local:" + reportSource + ":" + reportDestination
	if !runtime.enqueueLocal(reportSource, reportDestination, entry.Policy, []string{snapshot}) {
		t.Fatal("the blocked target was not reported")
	}
	states := log.waitRun(t, job, 0)
	if last := states[len(states)-1]; !strings.HasPrefix(last, "blocked") || !strings.Contains(last, "tank/other") {
		t.Fatalf("job states = %v, want blocked naming the other root", states)
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	if _, received := source.states[reportDestination]; received {
		t.Fatal("the blocked target received a replica")
	}
	if holds := source.state(reportSource).Holds[snapshot]; len(holds) != 0 {
		t.Fatalf("a snapshot was held for a target that cannot be sent to: %v", holds)
	}
	if _, pending := runtime.pending.Peek(reportSource, "local:"+reportDestination); pending {
		t.Fatal("a pending snapshot was recorded for the blocked target")
	}
}

// TestStatusClassifiesEveryDatasetAsDatasetListDoes publishes a generation
// holding an actor root, a blocked root, and datasets boomerangz does not
// manage, and requires the status view to list every one of them with the
// status `dataset list` gives it, and the blocked root with its blocked
// target.
func TestStatusClassifiesEveryDatasetAsDatasetListDoes(t *testing.T) {
	t.Parallel()
	_, properties := newReportPool(t)
	reader := collisionReader{
		datasets: []string{"tank", reportSource, "tank/other", "backup"},
		properties: append(slices.Clone(properties),
			zfs.Property{Dataset: "tank/other", Name: policy.Namespace + "enabled", Value: "on", Source: zfs.SourceLocal},
			zfs.Property{Dataset: "tank/other", Name: policy.Namespace + "local", Value: reportDestination, Source: zfs.SourceLocal},
		),
	}
	scanner, err := discovery.New(reader, discovery.Options{})
	if err != nil {
		t.Fatal(err)
	}
	generation, err := scanner.Scan(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	status := NewStatus(nil)
	runtime := &Runtime{status: status, generation: generation, known: map[string]bool{reportSource: true}, active: map[string]bool{reportSource: true}, recursive: map[string]bool{}}
	runtime.mu.Lock()
	runtime.publishRuntimeLocked()
	runtime.mu.Unlock()

	listed := map[string]DatasetStatus{}
	for _, dataset := range status.Snapshot().Datasets {
		listed[dataset.Name] = dataset
	}
	entries := generation.Entries()
	if len(listed) != len(entries) {
		t.Fatalf("status lists %d datasets, want every one of the generation's %d", len(listed), len(entries))
	}
	for _, entry := range entries {
		dataset, found := listed[entry.Dataset.Name]
		if !found || dataset.Status != entry.Status() {
			t.Errorf("%s: status %q (listed %t), want %q", entry.Dataset.Name, dataset.Status, found, entry.Status())
		}
		var blocked []string
		for _, target := range dataset.Blocked {
			blocked = append(blocked, target.Target)
		}
		var want []string
		for _, target := range entry.Blocked {
			want = append(want, target.Target)
		}
		if !slices.Equal(blocked, want) {
			t.Errorf("%s: blocked %q, want %q", entry.Dataset.Name, blocked, want)
		}
	}
	if !listed[reportSource].Active || listed["tank/other"].Active {
		t.Errorf("only the scheduled root is active: %+v", listed)
	}
	if listed["tank/other"].Status != discovery.StatusActive || len(listed["tank/other"].Blocked) == 0 || listed["tank"].Status != discovery.StatusInactive {
		t.Errorf("fixture did not produce an active blocked root and an inactive dataset: %+v", listed)
	}
}
