package daemon

import (
	"context"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPoolsHaveIndependentCapacityAndDeduplicateRunningJobs(t *testing.T) {
	t.Parallel()
	first, _ := NewPool("management", 1, 1, nil)
	second, _ := NewPool("transfer", 1, 1, nil)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	_ = first.Start(ctx)
	_ = second.Start(ctx)
	blocked := make(chan struct{})
	started := make(chan struct{}, 2)
	job := func(id string) Job {
		return Job{ID: id, Group: id, Scope: id, Run: func(context.Context) Outcome {
			started <- struct{}{}
			<-blocked
			return Outcome{}
		}}
	}
	if added, err := first.Submit(job("same")); err != nil || !added {
		t.Fatal(err)
	}
	if added, err := second.Submit(job("same")); err != nil || !added {
		t.Fatal(err)
	}
	for range 2 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("independent pool did not start")
		}
	}
	if added, err := first.Submit(job("same")); err != nil || added {
		t.Fatalf("running duplicate accepted: %v %v", added, err)
	}
	close(blocked)
	first.Close()
	second.Close()
	first.Wait()
	second.Wait()
}

func TestPoolSerializesLockKeyAcrossWorkers(t *testing.T) {
	t.Parallel()
	pool, _ := NewPool("transfer", 2, 4, nil)
	_ = pool.Start(t.Context())
	var active atomic.Int32
	var overlap atomic.Bool
	var wait sync.WaitGroup
	wait.Add(2)
	for _, id := range []string{"a", "b"} {
		job := queueJob(id, id, id)
		job.LockKey = "one-target"
		job.Run = func(context.Context) Outcome {
			if active.Add(1) != 1 {
				overlap.Store(true)
			}
			time.Sleep(10 * time.Millisecond)
			active.Add(-1)
			wait.Done()
			return Outcome{}
		}
		_, _ = pool.Submit(job)
	}
	wait.Wait()
	pool.Close()
	pool.Wait()
	if overlap.Load() {
		t.Fatal("same target overlapped")
	}
}

func TestHierarchyLocksAllowSiblingsAndBlockAncestors(t *testing.T) {
	t.Parallel()
	var locks keyLocks
	releaseA, err := locks.acquireScope(t.Context(), "local:backup/root", "backup/root/a")
	if err != nil {
		t.Fatal(err)
	}
	releaseB, err := locks.acquireScope(t.Context(), "local:backup/root", "backup/root/b")
	if err != nil {
		releaseA()
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if _, err := locks.acquireScope(ctx, "local:backup/root", "backup/root"); err == nil {
		releaseB()
		releaseA()
		t.Fatal("ancestor lock overlapped active descendants")
	}
	releaseB()
	releaseA()
	releaseRoot, err := locks.acquireScope(t.Context(), "local:backup/root", "backup/root")
	if err != nil {
		t.Fatal(err)
	}
	releaseRoot()
}

func TestHierarchyLocksDoNotStarveWaitingAncestor(t *testing.T) {
	t.Parallel()
	var locks keyLocks
	key := "local:backup/root"
	releaseRight, err := locks.acquireScope(t.Context(), key, "backup/root/right")
	if err != nil {
		t.Fatal(err)
	}
	type acquired struct {
		name    string
		release func()
	}
	started := make(chan acquired, 2)
	acquire := func(name, scope string) {
		release, acquireErr := locks.acquireScope(t.Context(), key, scope)
		if acquireErr != nil {
			t.Errorf("acquire %s: %v", name, acquireErr)
			return
		}
		started <- acquired{name: name, release: release}
	}
	go acquire("root", "backup/root")
	deadline := time.Now().Add(time.Second)
	for {
		locks.mu.Lock()
		queued := len(locks.waiters[key]) == 1
		locks.mu.Unlock()
		if queued {
			break
		}
		if time.Now().After(deadline) {
			releaseRight()
			t.Fatal("ancestor did not begin waiting")
		}
		time.Sleep(time.Millisecond)
	}
	go acquire("left", "backup/root/left")
	select {
	case got := <-started:
		got.release()
		releaseRight()
		t.Fatalf("%s bypassed the waiting ancestor", got.name)
	case <-time.After(20 * time.Millisecond):
	}
	releaseRight()
	root := <-started
	if root.name != "root" {
		root.release()
		t.Fatalf("%s acquired before the waiting ancestor", root.name)
	}
	select {
	case got := <-started:
		got.release()
		root.release()
		t.Fatalf("%s overlapped the active ancestor", got.name)
	case <-time.After(20 * time.Millisecond):
	}
	root.release()
	left := <-started
	if left.name != "left" {
		left.release()
		t.Fatalf("unexpected final acquirer %s", left.name)
	}
	left.release()
}

func TestPoolResizeDoesNotCancelRunningJob(t *testing.T) {
	t.Parallel()
	pool, err := NewPool("transfer", 1, 4, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	if err := pool.Start(ctx); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan error, 1)
	_, err = pool.Submit(Job{ID: "active", Group: "active", Scope: "active", Run: func(ctx context.Context) Outcome {
		close(started)
		<-release
		finished <- ctx.Err()
		return Outcome{}
	}})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	if err := pool.Resize(2); err != nil {
		t.Fatal(err)
	}
	if err := pool.Resize(1); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-finished; err != nil {
		t.Fatalf("resize cancelled active job: %v", err)
	}
	pool.Close()
	pool.Wait()
}

func TestPoolSilentOutcomeLeavesTheReportedStateStanding(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var states []string
	pool, _ := NewPool("transfer", 1, 2, func(event Event) {
		mu.Lock()
		defer mu.Unlock()
		states = append(states, event.State+":"+event.Reason)
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	_ = pool.Start(ctx)
	after := make(chan Outcome, 1)
	job := Job{ID: "remote:tank/data:home", Group: "tank/data", Scope: "tank/data", StartState: "probing",
		Run:   func(context.Context) Outcome { return Outcome{State: "waiting-retry", Reason: "offline", Silent: true} },
		After: func(outcome Outcome) { after <- outcome }}
	if added, err := pool.Submit(job); err != nil || !added {
		t.Fatal(err)
	}
	select {
	case outcome := <-after:
		if outcome.State != "waiting-retry" || !outcome.Silent {
			t.Fatalf("silent outcome was not delivered to After: %+v", outcome)
		}
	case <-time.After(time.Second):
		t.Fatal("job did not finish")
	}
	pool.Close()
	pool.Wait()
	mu.Lock()
	defer mu.Unlock()
	// An attempt that changed nothing reports no terminal transition, so the
	// status store keeps whatever the last real attempt left there.
	want := []string{"pending-transfer:", "probing:"}
	if len(states) != len(want) {
		t.Fatalf("recorded %v, want %v", states, want)
	}
	for index, state := range want {
		if states[index] != state {
			t.Fatalf("recorded %v, want %v", states, want)
		}
	}
}

func TestPoolRecordsPendingBeforeAJobPoppedImmediatelyStarts(t *testing.T) {
	t.Parallel()
	status := NewStatus(time.Now)
	pool, err := newStatusPool("transfer", "local_transfer", 4, 64, status)
	if err != nil {
		t.Fatal(err)
	}
	subscription := subscribe(t, status)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	if err := pool.Start(ctx); err != nil {
		t.Fatal(err)
	}
	const jobs = 50
	for index := range jobs {
		id := "job-" + strconv.Itoa(index)
		if added, err := pool.Submit(Job{ID: id, Group: id, Scope: id, StartState: "probing", Run: func(context.Context) Outcome { return Outcome{} }}); err != nil || !added {
			t.Fatalf("submit %s: added=%v err=%v", id, added, err)
		}
	}
	transitions, _ := collect(t, subscription, 3*jobs)
	states := map[string][]string{}
	for _, event := range transitions {
		states[event.Job] = append(states[event.Job], event.State)
	}
	for job, got := range states {
		if want := []string{"pending-transfer", "probing", "succeeded"}; !slices.Equal(got, want) {
			t.Fatalf("%s recorded %v, want %v", job, got, want)
		}
	}
	pool.Close()
	pool.Wait()
}

// recordingPool is a pool whose reporter records each transition as
// job:state:reason.
func recordingPool(t *testing.T) (*Pool, func() []string) {
	t.Helper()
	var mu sync.Mutex
	var recorded []string
	pool, err := NewPool("transfer", 1, 4, func(event Event) {
		mu.Lock()
		defer mu.Unlock()
		recorded = append(recorded, event.Job+":"+event.State+":"+event.Reason)
	})
	if err != nil {
		t.Fatal(err)
	}
	return pool, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(recorded)
	}
}

func TestPoolRecordsCancelledForJobsDroppedFromItsQueue(t *testing.T) {
	t.Parallel()
	pool, recorded := recordingPool(t)
	dropped := 0
	for _, id := range []string{"a", "b", "c"} {
		job := queueJob(id, id, id)
		job.Drop = func() { dropped++ }
		if added, err := pool.Submit(job); err != nil || !added {
			t.Fatalf("submit %s: added=%v err=%v", id, added, err)
		}
	}
	if removed := pool.RemoveScope("b", "dataset deactivated"); removed != 1 {
		t.Fatalf("removed %d", removed)
	}
	if removed := pool.DiscardPending("daemon shutting down"); removed != 2 {
		t.Fatalf("discarded %d", removed)
	}
	got := recorded()
	if want := []string{"a:pending-transfer:", "b:pending-transfer:", "c:pending-transfer:", "b:cancelled:dataset deactivated"}; !slices.Equal(got[:4], want) {
		t.Fatalf("recorded %v, want %v first", got, want)
	}
	// DiscardAll takes groups in map order, so only the set is fixed.
	discarded := slices.Sorted(slices.Values(got[4:]))
	if want := []string{"a:cancelled:daemon shutting down", "c:cancelled:daemon shutting down"}; !slices.Equal(discarded, want) {
		t.Fatalf("discard recorded %v, want %v", discarded, want)
	}
	if dropped != 3 {
		t.Fatalf("dropped %d jobs, want 3", dropped)
	}
	// A dropped job is no longer known, so it can be submitted again.
	if added, err := pool.Submit(queueJob("b", "b", "b")); err != nil || !added {
		t.Fatalf("resubmit: added=%v err=%v", added, err)
	}
}

func TestPoolRecordsCancelledForAPoppedJobItDoesNotStart(t *testing.T) {
	t.Parallel()
	pool, recorded := recordingPool(t)
	ran := false
	job := queueJob("a", "a", "a")
	job.Run = func(context.Context) Outcome {
		ran = true
		return Outcome{}
	}
	if added, err := pool.Submit(job); err != nil || !added {
		t.Fatal(err)
	}
	// A worker whose pool is already stopped pops the job and must not run
	// it, and must not leave it reported as pending either.
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := pool.Start(ctx); err != nil {
		t.Fatal(err)
	}
	pool.Close()
	pool.Wait()
	want := []string{"a:pending-transfer:", "a:cancelled:worker pool stopped before the job started"}
	if got := recorded(); ran || !slices.Equal(got, want) {
		t.Fatalf("ran=%t recorded %v, want %v", ran, got, want)
	}
}
