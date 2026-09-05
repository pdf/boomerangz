package lifecycle

import (
	"context"
	"fmt"
	"sync"

	"github.com/pdf/boomerangz/internal/zfs"
)

// WorkKind distinguishes short operations from cancellable streams.
type WorkKind uint8

// Supported work kinds use separate cancellation behavior on deactivation.
const (
	Management WorkKind = iota
	Transfer
)

// Gate is the lifecycle boundary for a future scheduler's queues. A disabled
// scope rejects new tickets and cancels queued tickets and running transfers.
// Running management tickets remain registered until their caller finishes.
// It stores no recovery data: holds, bookmarks and tokens remain untouched.
type Gate struct {
	mu     sync.Mutex
	scopes map[string]*scope
}

type scope struct {
	enabled bool
	tickets map[*Ticket]bool
	changed chan struct{}
}

// Ticket holds a job's cancellable context and its queue/running state.
// Every successful Queue must be paired with Finish, including cancelled work.
type Ticket struct {
	gate     *Gate
	scope    *scope
	kind     WorkKind
	ctx      context.Context
	cancel   context.CancelFunc
	running  bool
	finished bool
}

// GateStatus is an in-memory view; disabled scopes retain their ZFS recovery state.
type GateStatus struct {
	Enabled    bool `json:"enabled"`
	Queued     int  `json:"queued"`
	Management int  `json:"management"`
	Transfers  int  `json:"transfers"`
}

func (g *Gate) get(dataset string) *scope {
	if g.scopes == nil {
		g.scopes = make(map[string]*scope)
	}
	s := g.scopes[dataset]
	if s == nil {
		s = &scope{tickets: make(map[*Ticket]bool), changed: make(chan struct{})}
		g.scopes[dataset] = s
	}
	return s
}

func signal(s *scope) { close(s.changed); s.changed = make(chan struct{}) }

// SetEnabled applies an activation transition for one exact scheduling scope.
// Replication descendants must be scheduled under their root, not separately.
func (g *Gate) SetEnabled(dataset string, enabled bool) error {
	if err := zfs.ValidateDataset(dataset); err != nil {
		return err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	s := g.get(dataset)
	s.enabled = enabled
	if !enabled {
		for ticket := range s.tickets {
			if !ticket.running || ticket.kind == Transfer {
				ticket.cancel()
			}
		}
	}
	signal(s)
	return nil
}

// Queue registers work before it enters a worker queue. The worker must call
// Start after dequeueing; a cancelled queued ticket can never start later.
func (g *Gate) Queue(ctx context.Context, dataset string, kind WorkKind) (*Ticket, error) {
	if err := zfs.ValidateDataset(dataset); err != nil {
		return nil, err
	}
	if kind != Management && kind != Transfer {
		return nil, fmt.Errorf("invalid work kind")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	s := g.get(dataset)
	if !s.enabled {
		return nil, fmt.Errorf("dataset is disabled")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	jobCtx, cancel := context.WithCancel(ctx)
	ticket := &Ticket{gate: g, scope: s, kind: kind, ctx: jobCtx, cancel: cancel}
	s.tickets[ticket] = true
	signal(s)
	return ticket, nil
}

// Context is cancelled on deactivation for transfers and work not yet started.
func (t *Ticket) Context() context.Context { return t.ctx }

// Start atomically checks activation and transitions a ticket out of the queue.
func (t *Ticket) Start() error {
	t.gate.mu.Lock()
	defer t.gate.mu.Unlock()
	if t.finished || t.running {
		return fmt.Errorf("ticket is finished or already running")
	}
	if err := t.ctx.Err(); err != nil {
		return err
	}
	if !t.scope.enabled {
		return fmt.Errorf("dataset is disabled")
	}
	t.running = true
	signal(t.scope)
	return nil
}

// Finish releases a ticket after work and result reconstruction have finished.
func (t *Ticket) Finish() {
	t.gate.mu.Lock()
	defer t.gate.mu.Unlock()
	if t.finished {
		return
	}
	t.finished = true
	t.cancel()
	delete(t.scope.tickets, t)
	signal(t.scope)
}

// Status counts only runnable queued work, but includes cancelled transfers
// until they have actually exited and called Finish.
func (g *Gate) Status(dataset string) GateStatus {
	g.mu.Lock()
	defer g.mu.Unlock()
	s := g.get(dataset)
	status := GateStatus{Enabled: s.enabled}
	for t := range s.tickets {
		if !t.running {
			if t.ctx.Err() == nil {
				status.Queued++
			}
		} else if t.kind == Management {
			status.Management++
		} else {
			status.Transfers++
		}
	}
	return status
}

// WaitQuiescent waits for running work to finish on an already disabled scope.
// It does not enable, disable, or mutate datasets and observes cancellation.
func (g *Gate) WaitQuiescent(ctx context.Context, dataset string) error {
	if err := zfs.ValidateDataset(dataset); err != nil {
		return err
	}
	for {
		g.mu.Lock()
		s := g.get(dataset)
		if s.enabled {
			g.mu.Unlock()
			return fmt.Errorf("scope is still enabled")
		}
		running := false
		for t := range s.tickets {
			if t.running {
				running = true
				break
			}
		}
		changed := s.changed
		g.mu.Unlock()
		if !running {
			return ctx.Err()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}
