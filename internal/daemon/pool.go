package daemon

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Event is a structured worker-state transition suitable for logs and the
// future control-plane status cache.
type Event struct {
	Pool     string    `json:"pool"`
	Job      string    `json:"job"`
	Scope    string    `json:"scope"`
	Target   string    `json:"target,omitempty"`
	State    string    `json:"state"`
	Reason   string    `json:"reason,omitempty"`
	At       time.Time `json:"at"`
	Pending  int       `json:"pending"`
	Position int       `json:"queue_position,omitempty"`
}

// Reporter receives short, serially constructed status values.
type Reporter func(Event)

type keyLocks struct {
	mu    sync.Mutex
	locks map[string]chan struct{}
}

func (k *keyLocks) acquire(ctx context.Context, key string) (func(), error) {
	if key == "" {
		return func() {}, nil
	}
	k.mu.Lock()
	if k.locks == nil {
		k.locks = make(map[string]chan struct{})
	}
	lock := k.locks[key]
	if lock == nil {
		lock = make(chan struct{}, 1)
		lock <- struct{}{}
		k.locks[key] = lock
	}
	k.mu.Unlock()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-lock:
		return func() { lock <- struct{}{} }, nil
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
	for range p.workers {
		p.wait.Add(1)
		go p.worker(ctx)
	}
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

func (p *Pool) worker(ctx context.Context) {
	defer p.wait.Done()
	for {
		job, ok := p.queue.Pop(ctx)
		if !ok {
			return
		}
		if err := ctx.Err(); err != nil {
			if job.Drop != nil {
				job.Drop()
			}
			continue
		}
		release, err := p.locks.acquire(ctx, job.LockKey)
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
		outcome := job.Run(ctx)
		release()
		if outcome.State == "" {
			outcome.State = "succeeded"
		}
		p.emit(job, outcome.State, outcome.Reason)
		p.complete(job.ID)
		if job.After != nil {
			job.After(outcome)
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
