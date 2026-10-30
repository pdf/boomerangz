package daemon

import (
	"maps"
	"slices"
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
	return discovery.Entry{Dataset: zfs.Dataset{Name: name}, Inspected: true, Members: []string{name}, Policy: policy.Effective{
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
	scheduler.Complete("tank/a", due.Due, completed, nil)
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

// rootWithMembers returns a root and its members as discovery reports them:
// the root lists every member, and each member names the root.
func rootWithMembers(t *testing.T, root, rootGrid string, members map[string]string) []discovery.Entry {
	t.Helper()
	entry := scheduledEntry(t, root, rootGrid)
	entries := []discovery.Entry{entry}
	for _, name := range slices.Sorted(maps.Keys(members)) {
		member := scheduledEntry(t, name, members[name])
		member.CoveredBy, member.Member, member.Members = root, true, nil
		member.Policy.Values[policy.Namespace+"enabled"] = policy.Value{Value: "on", Dataset: root}
		entries[0].Members = append(entries[0].Members, name)
		entries = append(entries, member)
	}
	return entries
}

func TestSchedulerKeepsEachMembersOwnCadence(t *testing.T) {
	t.Parallel()
	start := time.Now().UTC()
	scheduler := NewScheduler()
	active, _, err := scheduler.Update(rootWithMembers(t, "tank/root", "1x5m", map[string]string{"tank/root/daily": "14x1d"}), start)
	if err != nil || !slices.Equal(active, []string{"tank/root"}) {
		t.Fatalf("active=%v err=%v", active, err)
	}
	first, ok := scheduler.Next(t.Context())
	if !ok || !slices.Equal(first.Due, []string{"tank/root", "tank/root/daily"}) {
		t.Fatalf("a new root is due for every member: %#v", first)
	}
	if len(first.Members) != 2 || first.Members[1].Grid.Cadence() != 24*time.Hour {
		t.Fatalf("members lost their own grids: %#v", first.Members)
	}
	scheduler.Complete("tank/root", first.Due, start, nil)
	item, _ := scheduler.entry("tank/root")
	if !item.earliest().Equal(start.Add(5 * time.Minute)) {
		t.Fatalf("root deadline = %s, want the finest member's", item.earliest())
	}
	// Collapsing to the finest cadence would snapshot the daily member every
	// five minutes; per-member deadlines leave it alone until its own is due.
	if due := item.schedule("tank/root", start.Add(6*time.Minute)).Due; !slices.Equal(due, []string{"tank/root"}) {
		t.Fatalf("due after six minutes = %q", due)
	}
	// A late fire is due for every member whose deadline has passed.
	if due := item.schedule("tank/root", start.Add(25*time.Hour)).Due; !slices.Equal(due, []string{"tank/root", "tank/root/daily"}) {
		t.Fatalf("due after a day = %q", due)
	}
	// Only the members that ran advance; one deferred by an existing snapshot
	// moves to the deadline that snapshot sets.
	later := start.Add(10 * time.Minute)
	deferred := start.Add(3 * time.Hour)
	scheduler.Complete("tank/root", []string{"tank/root"}, later, map[string]time.Time{"tank/root/daily": deferred})
	item, _ = scheduler.entry("tank/root")
	if !item.members["tank/root"].next.Equal(later.Add(5*time.Minute)) || !item.members["tank/root/daily"].next.Equal(deferred) {
		t.Fatalf("members = %#v", item.members)
	}
	// Retry postpones members already due and leaves later ones alone.
	retry := later.Add(time.Hour)
	scheduler.Retry("tank/root", retry)
	item, _ = scheduler.entry("tank/root")
	if !item.members["tank/root"].next.Equal(retry) || !item.members["tank/root/daily"].next.Equal(deferred) {
		t.Fatalf("after retry members = %#v", item.members)
	}
	// A member whose cadence is unchanged keeps its deadline across a
	// generation; one whose cadence changed starts again.
	if _, _, err := scheduler.Update(rootWithMembers(t, "tank/root", "1x5m", map[string]string{"tank/root/daily": "7x1h"}), later.Add(2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	item, _ = scheduler.entry("tank/root")
	if !item.members["tank/root"].next.Equal(retry) || !item.members["tank/root/daily"].next.Equal(later.Add(2*time.Hour)) {
		t.Fatalf("after update members = %#v", item.members)
	}
}

func TestSchedulerRunsReplicationRootOnItsOwnGrid(t *testing.T) {
	t.Parallel()
	start := time.Now().UTC()
	entries := rootWithMembers(t, "tank/tree", "1x1h", map[string]string{"tank/tree/fast": "1x5m"})
	entries[0].Policy.Send.Replicate = true
	scheduler := NewScheduler()
	if _, _, err := scheduler.Update(entries, start); err != nil {
		t.Fatal(err)
	}
	first, ok := scheduler.Next(t.Context())
	if !ok || !slices.Equal(first.Due, []string{"tank/tree", "tank/tree/fast"}) {
		t.Fatalf("due = %#v", first)
	}
	for _, member := range first.Members {
		if member.Grid.Cadence() != time.Hour {
			t.Fatalf("a -R member must use the root's grid: %#v", member)
		}
	}
	scheduler.Complete("tank/tree", first.Due, start, nil)
	item, _ := scheduler.entry("tank/tree")
	if !item.earliest().Equal(start.Add(time.Hour)) {
		t.Fatalf("the descendant's own grid leaked into a -R root: %s", item.earliest())
	}
	if due := item.schedule("tank/tree", start.Add(10*time.Minute)).Due; len(due) != 0 {
		t.Fatalf("due = %q", due)
	}
	forced, found := scheduler.Lookup("tank/tree")
	if !found || !slices.Equal(forced.Due, []string{"tank/tree", "tank/tree/fast"}) {
		t.Fatalf("forced = %#v", forced)
	}
}
