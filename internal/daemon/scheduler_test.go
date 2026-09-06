package daemon

import (
	"testing"
	"time"

	"github.com/pdf/boomerangz/internal/discovery"
	"github.com/pdf/boomerangz/internal/policy"
	"github.com/pdf/boomerangz/internal/zfs"
)

func scheduledEntry(t *testing.T, name, grid string) discovery.Entry {
	t.Helper()
	parsed, err := policy.ParseGrid(grid)
	if err != nil {
		t.Fatal(err)
	}
	return discovery.Entry{Dataset: zfs.Dataset{Name: name}, Inspected: true, Policy: policy.Effective{
		Enabled: true, Grid: parsed, Values: map[string]policy.Value{policy.Namespace + "enabled": {Value: "on", Dataset: name}},
	}}
}

func TestSchedulerPreservesDeadlineAndCoalescesCompletion(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	scheduler := NewScheduler()
	active, _, err := scheduler.Update([]discovery.Entry{scheduledEntry(t, "tank/a", "1x5m")}, now)
	if err != nil || len(active) != 1 {
		t.Fatal(err)
	}
	due, ok := scheduler.Next(t.Context())
	if !ok || due.Dataset != "tank/a" {
		t.Fatalf("unexpected due schedule: %#v", due)
	}
	completed := now.Add(17 * time.Minute)
	scheduler.Complete("tank/a", completed)
	want := completed.Add(5 * time.Minute)
	if got := scheduler.Entries()["tank/a"]; !got.Equal(want) {
		t.Fatalf("deadline=%s want=%s", got, want)
	}
	_, removed, err := scheduler.Update([]discovery.Entry{scheduledEntry(t, "tank/a", "1x5m")}, now.Add(time.Minute))
	if err != nil || len(removed) != 0 || !scheduler.Entries()["tank/a"].Equal(want) {
		t.Fatal("unchanged cadence reset its deadline")
	}
}

func TestSchedulerSuppressesCoveredAndInvalidEntries(t *testing.T) {
	t.Parallel()
	root := scheduledEntry(t, "tank/root", "1x5m")
	child := scheduledEntry(t, "tank/root/child", "1x5m")
	child.CoveredBy = root.Dataset.Name
	invalid := scheduledEntry(t, "tank/bad", "1x5m")
	invalid.Policy.Errors = []string{"bad"}
	scheduler := NewScheduler()
	active, _, err := scheduler.Update([]discovery.Entry{root, child, invalid}, time.Now())
	if err != nil || len(active) != 1 || active[0] != root.Dataset.Name {
		t.Fatalf("active=%v err=%v", active, err)
	}
}
