package daemon

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pdf/boomerangz/internal/config"
	"github.com/pdf/boomerangz/internal/daemonstate"
	"github.com/pdf/boomerangz/internal/lifecycle"
	"github.com/pdf/boomerangz/internal/policy"
	"github.com/pdf/boomerangz/internal/transfer"
	"github.com/pdf/boomerangz/internal/zfs"
)

// runOf receives updates until job reports an outcome, and returns every
// transition of job received on the way, in order.
func runOf(t *testing.T, subscription *Subscription, job string) []Event {
	t.Helper()
	var run []Event
	for {
		for _, event := range receive(t, subscription).Transitions {
			if event.Job != job {
				continue
			}
			run = append(run, event)
			if slices.Contains([]string{"succeeded", "failed", "blocked", "waiting-retry", "cancelled", "scheduled"}, event.State) {
				return run
			}
		}
	}
}

func stateOf(t *testing.T, run []Event, state string) Event {
	t.Helper()
	index := slices.IndexFunc(run, func(event Event) bool { return event.State == state })
	if index < 0 {
		t.Fatalf("run reported no %s: %+v", state, run)
	}
	return run[index]
}

func requireOneRun(t *testing.T, run []Event) {
	t.Helper()
	for _, event := range run {
		if event.RunID == 0 || event.RunID != run[0].RunID {
			t.Fatalf("run transitions do not share one run ID: %+v", run)
		}
	}
}

func TestRunIDIsRequiredAndSharedWithinARun(t *testing.T) {
	t.Parallel()
	status := NewStatus(time.Now)
	pool, err := newStatusPool("management", "management", 1, 4, status)
	if err != nil {
		t.Fatal(err)
	}
	subscription := subscribe(t, status)
	if err := pool.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	job := Job{ID: "prune:tank/a", Group: "tank/a", Scope: "tank/a", LockKey: "tank/a", StartState: "pruning", Run: func(context.Context) Outcome { return Outcome{} }}
	if added, err := pool.Submit(job); err == nil || added {
		t.Fatalf("a job without a run ID was accepted: added=%v err=%v", added, err)
	}
	var runs [][]Event
	for range 2 {
		job.RunID = nextRunID()
		if added, err := pool.Submit(job); err != nil || !added {
			t.Fatalf("submit added=%v err=%v", added, err)
		}
		run := runOf(t, subscription, job.ID)
		if states := []string{"pending-management", "pruning", "succeeded"}; !slices.Equal(statesOf(run), states) {
			t.Fatalf("run states = %v, want %v", statesOf(run), states)
		}
		requireOneRun(t, run)
		runs = append(runs, run)
	}
	if runs[0][0].RunID == runs[1][0].RunID {
		t.Fatalf("two runs of %s share run ID %d", job.ID, runs[0][0].RunID)
	}
}

func statesOf(run []Event) []string {
	states := make([]string, 0, len(run))
	for _, event := range run {
		states = append(states, event.State)
	}
	return states
}

func TestLocalTransferNamesWhatItSentAndWhereItLanded(t *testing.T) {
	t.Parallel()
	source, properties := newReportPool(t)
	runtime, _ := newReportRuntime(t, source, memoryStream{source: source, destination: source})
	subscription := subscribe(t, runtime.status)
	effective := policy.Resolve(zfs.Dataset{Name: reportSource, Type: zfs.Filesystem, EncryptionRoot: "-"}, nil, properties, nil)
	job := "local:" + reportSource + ":" + reportDestination
	newest := snapshotAt(t, source, 4)

	if !runtime.enqueueLocal(reportSource, reportDestination, effective, "") {
		t.Fatal("local job was not accepted")
	}
	run := runOf(t, subscription, job)
	requireOneRun(t, run)
	sending := stateOf(t, run, "sending")
	if sending.Snapshot != newest || sending.Mode != "full" || sending.Base != "" {
		t.Fatalf("full sending identity = %+v, want snapshot %s in full mode", sending.Identity, newest)
	}
	succeeded := stateOf(t, run, "succeeded")
	if succeeded.Snapshot != newest || succeeded.Destination != reportDestination {
		t.Fatalf("succeeded identity = %+v, want %s on %s", succeeded.Identity, newest, reportDestination)
	}

	// A newer snapshot is sent incrementally from the one already received.
	later := source.addSnapshot(t, 10)
	if !runtime.enqueueLocal(reportSource, reportDestination, effective, "") {
		t.Fatal("second local job was not accepted")
	}
	run = runOf(t, subscription, job)
	requireOneRun(t, run)
	if sending = stateOf(t, run, "sending"); sending.Snapshot != later || sending.Mode != "incremental-latest" || sending.Base != newest {
		t.Fatalf("incremental sending identity = %+v, want %s from %s", sending.Identity, later, newest)
	}
	if succeeded = stateOf(t, run, "succeeded"); succeeded.Snapshot != later {
		t.Fatalf("incremental succeeded identity = %+v, want %s", succeeded.Identity, later)
	}
}

type temporaryStreamError struct{}

func (temporaryStreamError) Error() string   { return "stream reset" }
func (temporaryStreamError) Temporary() bool { return true }

func TestTransferRetryNamesThePendingSnapshotItCarried(t *testing.T) {
	t.Parallel()
	source, properties := newReportPool(t)
	runtime, _ := newReportRuntime(t, source, memoryStream{source: source, destination: source, fail: temporaryStreamError{}})
	subscription := subscribe(t, runtime.status)
	effective := policy.Resolve(zfs.Dataset{Name: reportSource, Type: zfs.Filesystem, EncryptionRoot: "-"}, nil, properties, nil)
	job := "local:" + reportSource + ":" + reportDestination
	pending := snapshotAt(t, source, 2)
	if _, err := runtime.pending.Offer(reportSource, "local:"+reportDestination, transfer.PendingSnapshot{Name: pending, CreateTXG: 2}); err != nil {
		t.Fatal(err)
	}

	if !runtime.enqueueLocal(reportSource, reportDestination, effective, "") {
		t.Fatal("local job was not accepted")
	}
	run := runOf(t, subscription, job)
	retry := stateOf(t, run, "waiting-retry")
	if retry.Snapshot != pending {
		t.Fatalf("waiting-retry identity = %+v, want the carried pending snapshot %s", retry.Identity, pending)
	}
}

func TestOutcomeWithoutACarriedSnapshotNamesThePendingOne(t *testing.T) {
	t.Parallel()
	runtime := &Runtime{pending: &transfer.PendingSet{}}
	pending := reportSource + "@pending"
	if _, err := runtime.pending.Offer(reportSource, reportCanonical, transfer.PendingSnapshot{Name: pending, CreateTXG: 9}); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		outcome Outcome
		want    string
	}{
		{Outcome{State: "waiting-retry"}, pending},
		{Outcome{State: "blocked"}, pending},
		{Outcome{State: "blocked", Identity: daemonstate.Identity{Snapshot: reportSource + "@carried"}}, reportSource + "@carried"},
		{Outcome{State: "cancelled"}, pending},
		{Outcome{State: "succeeded"}, ""},
		{Outcome{State: "failed"}, ""},
	} {
		if got := runtime.withPending(reportSource, reportCanonical, test.outcome).Identity.Snapshot; got != test.want {
			t.Fatalf("withPending(%+v) snapshot = %q, want %q", test.outcome, got, test.want)
		}
	}
}

func TestRemoteTransferNamesTheSnapshotItsLastPassLanded(t *testing.T) {
	t.Parallel()
	runtime, _, source, destination, effective := remoteReportRuntime(t)
	subscription := subscribe(t, runtime.status)
	job := "remote:" + reportSource + ":" + reportRemote
	road, err := runtime.road(reportSource, reportRemote, effective)
	if err != nil {
		t.Fatal(err)
	}
	interrupted, err := transfer.NewRemote(source, destination, memoryStream{source: source, destination: destination, fail: errors.New("interrupted")}, reportInstallation)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := interrupted.Apply(t.Context(), road.request, nil); err == nil {
		t.Fatal("interrupted send succeeded")
	}
	destination.mu.Lock()
	destination.inventory = append(destination.inventory, zfs.Dataset{Name: reportDestination, Type: zfs.Filesystem, EncryptionRoot: "-"})
	destination.identities[reportDestination] = zfs.DatasetIdentity{Name: reportDestination, Type: zfs.Filesystem, GUID: 20, Pool: "backup", PoolGUID: 11}
	destination.states[reportDestination] = zfs.State{
		Objects:      []zfs.Object{{Name: reportDestination, Type: "filesystem", GUID: 20, CreateTXG: 20}},
		ResumeTokens: map[string]string{reportDestination: "1-resume-token"},
	}
	destination.mu.Unlock()
	later := source.addSnapshot(t, 10)

	if !runtime.enqueueRemote(reportSource, reportRemote, effective, "") {
		t.Fatal("remote job was not accepted")
	}
	run := runOf(t, subscription, job)
	requireOneRun(t, run)
	var modes []string
	for _, event := range run {
		if event.State == "sending" {
			modes = append(modes, event.Mode)
		}
	}
	if !slices.Equal(modes, []string{"resume", "incremental-latest"}) {
		t.Fatalf("sending modes = %v, want the resume and then the newer pass", modes)
	}
	if succeeded := stateOf(t, run, "succeeded"); succeeded.Snapshot != later || succeeded.Destination != reportDestination {
		t.Fatalf("succeeded identity = %+v, want %s on %s", succeeded.Identity, later, reportDestination)
	}
}

func TestScheduledSnapshotNamesTheSnapshotThatSetTheDeadline(t *testing.T) {
	t.Parallel()
	source, properties := newReportPool(t)
	runtime, _ := newReportRuntime(t, source, memoryStream{source: source, destination: source})
	runtime.now = func() time.Time { return time.Date(2026, 9, 6, 12, 30, 0, 0, time.UTC) }
	if err := runtime.management.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		runtime.management.Close()
		runtime.management.wait.Wait()
	})
	subscription := subscribe(t, runtime.status)
	properties = append(properties, zfs.Property{Dataset: reportSource, Name: policy.Namespace + "policy", Value: "1x1h", Source: zfs.SourceLocal})
	effective := policy.Resolve(zfs.Dataset{Name: reportSource, Type: zfs.Filesystem, EncryptionRoot: "-"}, nil, properties, nil)

	if !runtime.enqueueSnapshot(Schedule{Dataset: reportSource, Policy: effective}) {
		t.Fatal("snapshot job was not accepted")
	}
	run := runOf(t, subscription, "snapshot:"+reportSource)
	if want := snapshotAt(t, source, 4); stateOf(t, run, "scheduled").Snapshot != want {
		t.Fatalf("scheduled identity = %+v, want %s", run[len(run)-1].Identity, want)
	}
}

// snapshotAt names the report source's snapshot created at txg.
func snapshotAt(t *testing.T, source *memoryZFS, txg uint64) string {
	t.Helper()
	source.mu.Lock()
	defer source.mu.Unlock()
	for _, object := range source.state(reportSource).Objects {
		if object.Type == "snapshot" && object.CreateTXG == txg {
			return object.Name
		}
	}
	t.Fatalf("no snapshot at txg %d", txg)
	return ""
}

func TestMarkerActionNamesWhatReconciliationDid(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		plan lifecycle.InactivePlan
		want string
	}{
		{lifecycle.InactivePlan{Action: "set-inactive-marker", Applied: true}, "set"},
		{lifecycle.InactivePlan{Action: "clear-inactive-marker", Applied: true}, "cleared"},
		{lifecycle.InactivePlan{}, "none"},
		{lifecycle.InactivePlan{Action: "set-inactive-marker"}, "none"},
	} {
		if got := markerAction(test.plan); got != test.want {
			t.Fatalf("markerAction(%+v) = %q, want %q", test.plan, got, test.want)
		}
	}
}

func TestDestroyedIdentityNamesDestroyedSnapshotsUpToTheLimit(t *testing.T) {
	t.Parallel()
	plan := lifecycle.CleanPlan{Actions: []lifecycle.CleanAction{
		{Operation: "release", Object: "tank/a@one"},
		{Operation: "destroy-snapshot", Object: "tank/a@one"},
		{Operation: "inherit", Object: "tank/a", Property: lifecycle.InactiveProperty},
		{Operation: "destroy-snapshot", Object: "tank/a@two"},
	}}
	if got := destroyedSnapshots(plan); !slices.Equal(got.Destroyed, []string{"tank/a@one", "tank/a@two"}) || got.DestroyedCount != 2 {
		t.Fatalf("destroyed identity = %+v", got)
	}
	many := make([]string, daemonstate.DestroyedLimit+3)
	for index := range many {
		many[index] = "tank/a@" + strings.Repeat("x", index+1)
	}
	if got := daemonstate.DestroyedIdentity(many); len(got.Destroyed) != daemonstate.DestroyedLimit || got.DestroyedCount != len(many) || got.Destroyed[0] != many[0] {
		t.Fatalf("capped identity kept %d names of count %d", len(got.Destroyed), got.DestroyedCount)
	}
}

func TestWorkerStateLineCarriesTheIdentityATransitionSets(t *testing.T) {
	t.Parallel()
	var output bytes.Buffer
	status := NewStatus(time.Now)
	status.subscribeLog(slog.New(slog.NewJSONHandler(&output, nil)))
	status.record(Event{Kind: EventTransition, RunID: 7, Job: "local:tank/a:backup/a", State: "sending", Identity: daemonstate.Identity{Snapshot: "tank/a@two", Base: "tank/a@one", Mode: "incremental-latest"}})
	status.record(Event{Kind: EventTransition, RunID: 8, Job: "prune:tank/a", State: "succeeded", Identity: daemonstate.DestroyedIdentity([]string{"tank/a@old"})})
	status.record(Event{Kind: EventTransition, RunID: 9, Job: "prune:tank/b", State: "pruning"})
	status.flush(t.Context())
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != 3 {
		t.Fatalf("log = %q", output.String())
	}
	for index, want := range [][]string{
		{`"run_id":7`, `"snapshot":"tank/a@two"`, `"base":"tank/a@one"`, `"mode":"incremental-latest"`},
		{`"run_id":8`, `"destroyed":["tank/a@old"]`, `"destroyed_count":1`},
		{`"run_id":9`},
	} {
		for _, fragment := range want {
			if !strings.Contains(lines[index], fragment) {
				t.Fatalf("line %d = %s, missing %s", index, lines[index], fragment)
			}
		}
	}
	for _, key := range []string{`"snapshot"`, `"destroyed"`, `"marker"`, `"config_generation"`} {
		if strings.Contains(lines[2], key) {
			t.Fatalf("a transition that sets no identity logged %s: %s", key, lines[2])
		}
	}
}

// eventPool records the transitions a pool that is never started reports.
func eventPool(t *testing.T) (*Pool, func() []Event) {
	t.Helper()
	var mu sync.Mutex
	var recorded []Event
	pool, err := NewPool("transfer", 1, 4, func(event Event) {
		mu.Lock()
		defer mu.Unlock()
		recorded = append(recorded, event)
	})
	if err != nil {
		t.Fatal(err)
	}
	return pool, func() []Event {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(recorded)
	}
}

// A job that leaves its pool without running records cancelled from the pool,
// under its queue lock. It carries its run ID and names no snapshot: naming
// one would mean reading the pending set under the queue lock (chunk J).
func TestAJobLeavingItsPoolUnrunCarriesItsRunID(t *testing.T) {
	t.Parallel()
	cancelledOf := func(t *testing.T, recorded []Event, job Job) Event {
		t.Helper()
		index := slices.IndexFunc(recorded, func(event Event) bool { return event.Job == job.ID && event.State == "cancelled" })
		if index < 0 {
			t.Fatalf("%s recorded no cancelled: %+v", job.ID, recorded)
		}
		return recorded[index]
	}

	// Removed or discarded from its queue.
	pool, recorded := eventPool(t)
	removed, discarded := queueJob("tank/a", "tank/a", "tank/a"), queueJob("tank/b", "tank/b", "tank/b")
	for _, job := range []Job{removed, discarded} {
		if added, err := pool.Submit(job); err != nil || !added {
			t.Fatalf("submit %s: added=%v err=%v", job.ID, added, err)
		}
	}
	pool.RemoveScope("tank/a", "dataset deactivated")
	pool.DiscardPending("daemon shutting down")
	for _, job := range []Job{removed, discarded} {
		if got := cancelledOf(t, recorded(), job); got.RunID != job.RunID || got.Snapshot != "" {
			t.Fatalf("%s cancelled = %+v, want run ID %d and no snapshot", job.ID, got, job.RunID)
		}
	}

	// Popped by a worker whose pool has stopped.
	pool, recorded = eventPool(t)
	popped := queueJob("tank/c", "tank/c", "tank/c")
	if added, err := pool.Submit(popped); err != nil || !added {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := pool.Start(ctx); err != nil {
		t.Fatal(err)
	}
	pool.Close()
	pool.Wait()
	if got := cancelledOf(t, recorded(), popped); got.RunID != popped.RunID || got.Snapshot != "" {
		t.Fatalf("popped job's cancelled = %+v, want run ID %d and no snapshot", got, popped.RunID)
	}
}

func TestADeactivatedQueuedTransferNamesNoSnapshot(t *testing.T) {
	t.Parallel()
	source, properties := newReportPool(t)
	// No pool is started, so the transfer stays queued until it is removed.
	runtime, err := NewWithLocalStream(config.Defaults(), source, reportInstallation, slog.New(slog.NewTextHandler(io.Discard, nil)), memoryStream{source: source, destination: source})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runtime.local.Close)
	if err := runtime.gate.SetEnabled(reportSource, true); err != nil {
		t.Fatal(err)
	}
	subscription := subscribe(t, runtime.status)
	effective := policy.Resolve(zfs.Dataset{Name: reportSource, Type: zfs.Filesystem, EncryptionRoot: "-"}, nil, properties, nil)
	if _, err := runtime.pending.Offer(reportSource, "local:"+reportDestination, transfer.PendingSnapshot{Name: snapshotAt(t, source, 4), CreateTXG: 4}); err != nil {
		t.Fatal(err)
	}

	if !runtime.enqueueLocal(reportSource, reportDestination, effective, "") {
		t.Fatal("local job was not accepted")
	}
	runtime.local.RemoveScope(reportSource, "dataset deactivated")
	run := runOf(t, subscription, "local:"+reportSource+":"+reportDestination)
	requireOneRun(t, run)
	if cancelled := stateOf(t, run, "cancelled"); cancelled.Snapshot != "" {
		t.Fatalf("a transfer removed from its queue named %s; it never took a snapshot", cancelled.Snapshot)
	}
}
