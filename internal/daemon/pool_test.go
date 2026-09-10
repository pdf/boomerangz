package daemon

import (
	"context"
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
