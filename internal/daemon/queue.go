// Package daemon coordinates scheduling and bounded worker execution.
package daemon

import (
	"context"
	"fmt"
	"slices"
	"sync"

	"github.com/pdf/boomerangz/internal/daemonstate"
)

// Outcome is the stable result of one worker attempt.
type Outcome struct {
	State  string
	Reason string
}

// Job is typed daemon work. ID deduplicates equivalent queued work, Group is
// the fairness key, Scope is removed on deactivation, and LockKey serializes
// operations that must never overlap.
type Job struct {
	ID         string
	Group      string
	Scope      string
	LockKey    string
	StartState string
	Run        func(context.Context) Outcome
	After      func(Outcome)
	Drop       func()
}

// QueueSnapshot is retained as the daemon package's public queue view.
type QueueSnapshot = daemonstate.QueueSnapshot

// FairQueue is a bounded, deduplicating round-robin queue. Each group gets one
// dequeue opportunity before another job from a busy group can run.
type FairQueue struct {
	mu       sync.Mutex
	capacity int
	count    int
	groups   map[string][]Job
	order    []string
	ids      map[string]bool
	notify   chan struct{}
	closed   bool
}

// NewFairQueue creates an in-memory queue with a strict pending-job bound.
func NewFairQueue(capacity int) (*FairQueue, error) {
	if capacity < 1 {
		return nil, fmt.Errorf("queue capacity must be positive")
	}
	return &FairQueue{capacity: capacity, groups: make(map[string][]Job), ids: make(map[string]bool), notify: make(chan struct{})}, nil
}

func (q *FairQueue) signal() {
	close(q.notify)
	q.notify = make(chan struct{})
}

// Offer adds a job, returning false when its ID is already pending.
func (q *FairQueue) Offer(job Job) (bool, error) {
	if job.ID == "" || job.Group == "" || job.Scope == "" || job.Run == nil {
		return false, fmt.Errorf("complete queue job metadata is required")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return false, fmt.Errorf("queue is closed")
	}
	if q.ids[job.ID] {
		return false, nil
	}
	if q.count >= q.capacity {
		return false, fmt.Errorf("queue capacity %d reached", q.capacity)
	}
	if len(q.groups[job.Group]) == 0 {
		q.order = append(q.order, job.Group)
	}
	q.groups[job.Group] = append(q.groups[job.Group], job)
	q.ids[job.ID] = true
	q.count++
	q.signal()
	return true, nil
}

// Pop waits for the next fair job. A closed queue drains before returning false.
func (q *FairQueue) Pop(ctx context.Context) (Job, bool) {
	for {
		q.mu.Lock()
		if q.count > 0 {
			group := q.order[0]
			jobs := q.groups[group]
			job := jobs[0]
			jobs = jobs[1:]
			q.order = q.order[1:]
			if len(jobs) == 0 {
				delete(q.groups, group)
			} else {
				q.groups[group] = jobs
				q.order = append(q.order, group)
			}
			delete(q.ids, job.ID)
			q.count--
			q.mu.Unlock()
			return job, true
		}
		if q.closed {
			q.mu.Unlock()
			return Job{}, false
		}
		notify := q.notify
		q.mu.Unlock()
		select {
		case <-ctx.Done():
			return Job{}, false
		case <-notify:
		}
	}
}

// RemoveScope discards all queued work for one deactivated scheduling root.
func (q *FairQueue) RemoveScope(scope string) int {
	q.mu.Lock()
	var dropped []Job
	for group, jobs := range q.groups {
		kept := jobs[:0]
		for _, job := range jobs {
			if job.Scope == scope {
				dropped = append(dropped, job)
				delete(q.ids, job.ID)
				q.count--
			} else {
				kept = append(kept, job)
			}
		}
		if len(kept) == 0 {
			delete(q.groups, group)
			q.order = slices.DeleteFunc(q.order, func(candidate string) bool { return candidate == group })
		} else {
			q.groups[group] = kept
		}
	}
	if len(dropped) > 0 {
		q.signal()
	}
	q.mu.Unlock()
	for _, job := range dropped {
		if job.Drop != nil {
			job.Drop()
		}
	}
	return len(dropped)
}

// DiscardAll removes every job that has not started.
func (q *FairQueue) DiscardAll() int {
	q.mu.Lock()
	dropped := make([]Job, 0, q.count)
	for _, jobs := range q.groups {
		dropped = append(dropped, jobs...)
	}
	q.groups = make(map[string][]Job)
	q.order = nil
	q.ids = make(map[string]bool)
	q.count = 0
	if len(dropped) > 0 {
		q.signal()
	}
	q.mu.Unlock()
	for _, job := range dropped {
		if job.Drop != nil {
			job.Drop()
		}
	}
	return len(dropped)
}

// Close stops new submissions and allows already queued work to drain.
func (q *FairQueue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.closed {
		q.closed = true
		q.signal()
	}
}

// Snapshot returns a detached queue view.
func (q *FairQueue) Snapshot() QueueSnapshot {
	q.mu.Lock()
	defer q.mu.Unlock()
	result := QueueSnapshot{Capacity: q.capacity, Pending: q.count}
	groups := make(map[string][]Job, len(q.groups))
	for group, jobs := range q.groups {
		groups[group] = slices.Clone(jobs)
	}
	order := slices.Clone(q.order)
	for len(order) > 0 {
		group := order[0]
		order = order[1:]
		jobs := groups[group]
		result.IDs = append(result.IDs, jobs[0].ID)
		if len(jobs) > 1 {
			groups[group] = jobs[1:]
			order = append(order, group)
		}
	}
	return result
}

// Position returns the one-based fair dequeue position of a pending job.
func (q *FairQueue) Position(id string) int {
	return slices.Index(q.Snapshot().IDs, id) + 1
}
