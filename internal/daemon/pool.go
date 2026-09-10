package daemon

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/pdf/boomerangz/internal/daemonstate"
)

// Event is retained as the daemon package's public operational value.
type Event = daemonstate.Event

// Reporter receives short, serially constructed status values.
type Reporter func(Event)

type keyLocks struct {
	mu      sync.Mutex
	active  map[string]map[string]int
	waiters map[string][]*lockWaiter
	changed chan struct{}
}

type lockWaiter struct {
	scope string
}

func (k *keyLocks) acquire(ctx context.Context, key string) (func(), error) {
	return k.acquireScope(ctx, key, "")
}

func overlappingLockScope(a, b string) bool {
	return a == "" || b == "" || a == b || strings.HasPrefix(a, b+"/") || strings.HasPrefix(b, a+"/")
}

func (k *keyLocks) signalLocked() {
	if k.changed != nil {
		close(k.changed)
	}
	k.changed = make(chan struct{})
}

func (k *keyLocks) removeWaiterLocked(key string, waiter *lockWaiter) {
	waiters := k.waiters[key]
	for index, candidate := range waiters {
		if candidate != waiter {
			continue
		}
		waiters = append(waiters[:index], waiters[index+1:]...)
		if len(waiters) == 0 {
			delete(k.waiters, key)
		} else {
			k.waiters[key] = waiters
		}
		return
	}
}

func (k *keyLocks) acquireScope(ctx context.Context, key, scope string) (func(), error) {
	if key == "" {
		return func() {}, nil
	}
	waiter := &lockWaiter{scope: scope}
	k.mu.Lock()
	if k.waiters == nil {
		k.waiters = make(map[string][]*lockWaiter)
	}
	k.waiters[key] = append(k.waiters[key], waiter)
	k.mu.Unlock()
	for {
		k.mu.Lock()
		if err := ctx.Err(); err != nil {
			k.removeWaiterLocked(key, waiter)
			k.signalLocked()
			k.mu.Unlock()
			return nil, err
		}
		blocked := false
		for active := range k.active[key] {
			if overlappingLockScope(active, scope) {
				blocked = true
				break
			}
		}
		if !blocked {
			for _, earlier := range k.waiters[key] {
				if earlier == waiter {
					break
				}
				if overlappingLockScope(earlier.scope, scope) {
					blocked = true
					break
				}
			}
		}
		if !blocked {
			if k.active == nil {
				k.active = make(map[string]map[string]int)
			}
			if k.active[key] == nil {
				k.active[key] = make(map[string]int)
			}
			k.removeWaiterLocked(key, waiter)
			k.active[key][scope]++
			k.signalLocked()
			k.mu.Unlock()
			return func() {
				k.mu.Lock()
				k.active[key][scope]--
				if k.active[key][scope] == 0 {
					delete(k.active[key], scope)
				}
				if len(k.active[key]) == 0 {
					delete(k.active, key)
				}
				k.signalLocked()
				k.mu.Unlock()
			}, nil
		}
		if k.changed == nil {
			k.changed = make(chan struct{})
		}
		changed := k.changed
		k.mu.Unlock()
		select {
		case <-ctx.Done():
			continue
		case <-changed:
		}
	}
}

// Pool runs one independently bounded class of jobs.
type Pool struct {
	name    string
	workers int
	queue   *FairQueue
	report  Reporter
	locks   *keyLocks
	mu      sync.Mutex
	known   map[string]bool
	started bool
	ctx     context.Context
	stops   []context.CancelFunc
	wait    sync.WaitGroup
}

// NewPool constructs a worker pool; Start launches exactly workers goroutines.
func NewPool(name string, workers, queueCapacity int, report Reporter) (*Pool, error) {
	if name == "" || workers < 1 {
		return nil, fmt.Errorf("pool name and positive worker count are required")
	}
	queue, err := NewFairQueue(queueCapacity)
	if err != nil {
		return nil, err
	}
	return &Pool{name: name, workers: workers, queue: queue, report: report, locks: &keyLocks{}, known: make(map[string]bool)}, nil
}

// Start begins worker execution once.
func (p *Pool) Start(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.started {
		return fmt.Errorf("pool already started")
	}
	p.started = true
	p.ctx = ctx
	for range p.workers {
		p.startWorkerLocked()
	}
	return nil
}

func (p *Pool) startWorkerLocked() {
	workerCtx, stop := context.WithCancel(p.ctx)
	p.stops = append(p.stops, stop)
	p.wait.Add(1)
	go p.worker(p.ctx, workerCtx)
}

// Resize changes the number of workers. Retiring workers finish an active job
// before exiting; newly added workers begin consuming queued work immediately.
func (p *Pool) Resize(workers int) error {
	if workers < 1 {
		return fmt.Errorf("positive worker count is required")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.started {
		p.workers = workers
		return nil
	}
	for len(p.stops) < workers {
		p.startWorkerLocked()
	}
	for len(p.stops) > workers {
		last := len(p.stops) - 1
		p.stops[last]()
		p.stops = p.stops[:last]
	}
	p.workers = workers
	return nil
}

// Submit deduplicates queued and running jobs by ID.
func (p *Pool) Submit(job Job) (bool, error) {
	p.mu.Lock()
	if p.known[job.ID] {
		p.mu.Unlock()
		return false, nil
	}
	p.known[job.ID] = true
	originalDrop := job.Drop
	job.Drop = func() {
		if originalDrop != nil {
			originalDrop()
		}
		p.complete(job.ID)
	}
	p.mu.Unlock()
	added, err := p.queue.Offer(job)
	if err != nil || !added {
		p.complete(job.ID)
	}
	if added {
		p.emit(job, "pending-"+p.name, "")
	}
	return added, err
}

func (p *Pool) complete(id string) {
	p.mu.Lock()
	delete(p.known, id)
	p.mu.Unlock()
}

func (p *Pool) emit(job Job, state, reason string) {
	if p.report != nil {
		p.report(Event{Pool: p.name, Job: job.ID, Scope: job.Scope, Target: job.LockKey, State: state, Reason: reason, At: time.Now().UTC(), Pending: p.queue.Snapshot().Pending, Position: p.queue.Position(job.ID)})
	}
}

func (p *Pool) worker(runCtx, workerCtx context.Context) {
	defer p.wait.Done()
	for {
		job, ok := p.queue.Pop(workerCtx)
		if !ok {
			return
		}
		if err := runCtx.Err(); err != nil {
			if job.Drop != nil {
				job.Drop()
			}
			continue
		}
		release, err := p.locks.acquireScope(runCtx, job.LockKey, job.LockScope)
		if err != nil {
			outcome := Outcome{State: "failed", Reason: err.Error()}
			p.emit(job, outcome.State, outcome.Reason)
			p.complete(job.ID)
			if job.After != nil {
				job.After(outcome)
			}
			continue
		}
		state := job.StartState
		if state == "" {
			state = "running"
		}
		p.emit(job, state, "")
		outcome := job.Run(runCtx)
		release()
		if outcome.State == "" {
			outcome.State = "succeeded"
		}
		p.emit(job, outcome.State, outcome.Reason)
		p.complete(job.ID)
		if job.After != nil {
			job.After(outcome)
		}
		if workerCtx.Err() != nil {
			return
		}
	}
}

// RemoveScope removes work that has not started for a deactivated root.
func (p *Pool) RemoveScope(scope string) int { return p.queue.RemoveScope(scope) }

// DiscardPending removes all work that has not started.
func (p *Pool) DiscardPending() int { return p.queue.DiscardAll() }

// Close stops submissions and drains queued work unless the worker context is cancelled.
func (p *Pool) Close() { p.queue.Close() }

// Wait waits until every worker has exited.
func (p *Pool) Wait() { p.wait.Wait() }

// Snapshot returns the current pending queue view.
func (p *Pool) Snapshot() QueueSnapshot { return p.queue.Snapshot() }
