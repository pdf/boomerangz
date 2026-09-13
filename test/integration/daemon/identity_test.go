//go:build integration

package daemon_test

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/pdf/boomerangz/internal/daemon"
	"github.com/pdf/boomerangz/internal/zfs"
	"github.com/pdf/boomerangz/test/integration/internal/statuswait"
)

// createdSnapshot waits for dataset's snapshot job to report succeeded after
// from, and returns the snapshot it names and the cursor past it. retried
// names the outcomes the scenario lets a snapshot run end with first.
func createdSnapshot(t *testing.T, waiter *statuswait.Waiter, bound time.Duration, from statuswait.Cursor, dataset string, retried ...string) (string, statuswait.Cursor) {
	t.Helper()
	event, at := waiter.Outcome(t, bound, from, "snapshot:"+dataset, "succeeded", retried...)
	if !strings.HasPrefix(event.Snapshot, dataset+"@") {
		t.Fatalf("snapshot job for %s succeeded naming %q", dataset, event.Snapshot)
	}
	return event.Snapshot, at
}

// snapshotsCreated returns the snapshots dataset's snapshot jobs report
// creating in transitions, in order.
func snapshotsCreated(transitions []daemon.Event, dataset string) []string {
	var created []string
	for _, event := range transitions {
		if event.Job == "snapshot:"+dataset && event.State == "succeeded" {
			created = append(created, event.Snapshot)
		}
	}
	return created
}

// waitForCreation waits for a snapshot job of dataset to report creating
// snapshot at or after from, which is how a transition naming a snapshot is
// tied to the run that made it.
func waitForCreation(t *testing.T, waiter *statuswait.Waiter, bound time.Duration, from statuswait.Cursor, dataset, snapshot string) {
	t.Helper()
	if snapshot == "" {
		t.Fatalf("a transition that should name a snapshot of %s named none", dataset)
	}
	waiter.Next(t, "snapshot job for "+dataset+" to report creating "+snapshot, bound, from, func(event daemon.Event) bool {
		return event.Job == "snapshot:"+dataset && event.State == "succeeded" && event.Snapshot == snapshot
	})
}

// destruction classifies whether snapshot can be missing from a pool read
// taken just before a Settle, from the runs of jobs that destroy snapshots
// in the transitions up to its cursor.
type destruction int

const (
	notDestroyed destruction = iota
	mayBeDestroyed
	destroyed
)

func destructionOf(transitions []daemon.Event, snapshot string, jobs ...string) destruction {
	result := notDestroyed
	for _, job := range jobs {
		for _, run := range statuswait.Runs(transitions, job) {
			switch run.Effects() {
			case statuswait.Absent:
			case statuswait.Partial:
				result = max(result, mayBeDestroyed)
			case statuswait.Present:
				outcome, _ := run.Outcome()
				switch {
				case slices.Contains(outcome.Destroyed, snapshot):
					return destroyed
				case outcome.State != "succeeded" || outcome.DestroyedCount > len(outcome.Destroyed):
					// A run that ended any other way may have destroyed some
					// snapshots before it stopped, and a cut list may omit this
					// one.
					result = max(result, mayBeDestroyed)
				}
			}
		}
	}
	return result
}

// requireSnapshotAgreesWithEvents reads dataset once and checks snapshot
// against what the daemon's events say about it at that read: present unless
// a run that destroys snapshots may have destroyed it, and absent if one
// reported destroying it.
func requireSnapshotAgreesWithEvents(t *testing.T, direct *zfs.Direct, waiter *statuswait.Waiter, dataset, snapshot string) {
	t.Helper()
	state, err := direct.InspectState(t.Context(), dataset, false)
	if err != nil {
		t.Fatal(err)
	}
	settled := waiter.Settle(t, 30*time.Second)
	present := slices.ContainsFunc(state.Objects, func(object zfs.Object) bool { return object.Name == snapshot && object.Type == "snapshot" })
	switch destructionOf(waiter.Transitions()[:settled], snapshot, "prune:"+dataset, "retire:"+dataset) {
	case notDestroyed:
		if !present {
			t.Fatalf("%s is missing, and no run that destroys snapshots had started; transitions: %+v", snapshot, waiter.Transitions()[:settled])
		}
	case destroyed:
		if present {
			t.Fatalf("%s is present after a run reported destroying it", snapshot)
		}
	case mayBeDestroyed:
	}
}
