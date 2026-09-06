package transfer

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/pdf/boomerangz/internal/zfs"
)

// PendingSnapshot is the newest eligible work coalesced for one target.
type PendingSnapshot struct {
	Name      string `json:"name"`
	CreateTXG uint64 `json:"create_txg"`
}

// PendingSet coalesces disconnected work independently per source-target pair.
type PendingSet struct {
	mu   sync.Mutex
	jobs map[string]PendingSnapshot
}

func pendingKey(source, target string) (string, error) {
	if err := zfs.ValidateDataset(source); err != nil {
		return "", err
	}
	if target == "" || strings.ContainsAny(target, "\x00\r\n") {
		return "", fmt.Errorf("canonical target is required")
	}
	return source + "\x00" + target, nil
}

// Offer retains only the newest eligible snapshot for a pair.
func (p *PendingSet) Offer(source, target string, snapshot PendingSnapshot) (bool, error) {
	key, err := pendingKey(source, target)
	if err != nil {
		return false, err
	}
	if snapshot.CreateTXG == 0 || !strings.HasPrefix(snapshot.Name, source+"@") {
		return false, fmt.Errorf("pending snapshot must be on the source with a creation transaction")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.jobs == nil {
		p.jobs = make(map[string]PendingSnapshot)
	}
	current, exists := p.jobs[key]
	if exists && current.CreateTXG >= snapshot.CreateTXG {
		return false, nil
	}
	p.jobs[key] = snapshot
	return true, nil
}

// Peek returns detached pending work without removing it.
func (p *PendingSet) Peek(source, target string) (PendingSnapshot, bool) {
	key, err := pendingKey(source, target)
	if err != nil {
		return PendingSnapshot{}, false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	snapshot, exists := p.jobs[key]
	return snapshot, exists
}

// Complete removes work only if it is still the exact item just verified.
func (p *PendingSet) Complete(source, target string, snapshot PendingSnapshot) bool {
	key, err := pendingKey(source, target)
	if err != nil {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if current, exists := p.jobs[key]; !exists || current != snapshot {
		return false
	}
	delete(p.jobs, key)
	return true
}

// RetryPolicy controls bounded exponential roadwarrior retries with symmetric
// jitter. Random is supplied by the caller as a value in [0,1].
type RetryPolicy struct {
	Initial    time.Duration
	Maximum    time.Duration
	Multiplier float64
	Jitter     float64
}

// DefaultRetryPolicy avoids synchronized reconnect storms while bounding wait.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{Initial: 5 * time.Second, Maximum: 5 * time.Minute, Multiplier: 2, Jitter: 0.2}
}

func (p RetryPolicy) validate() error {
	if p.Initial <= 0 || p.Maximum < p.Initial || p.Multiplier < 1 || p.Jitter < 0 || p.Jitter > 1 {
		return fmt.Errorf("invalid retry policy")
	}
	return nil
}

// Delay returns the delay after the given consecutive failure count.
func (p RetryPolicy) Delay(failures int, random float64) (time.Duration, error) {
	if err := p.validate(); err != nil {
		return 0, err
	}
	if failures < 1 || random < 0 || random > 1 || math.IsNaN(random) {
		return 0, fmt.Errorf("invalid retry state")
	}
	base := float64(p.Initial) * math.Pow(p.Multiplier, float64(failures-1))
	if base > float64(p.Maximum) || math.IsInf(base, 1) {
		base = float64(p.Maximum)
	}
	factor := 1 - p.Jitter + 2*p.Jitter*random
	delay := time.Duration(base * factor)
	if delay > p.Maximum {
		delay = p.Maximum
	}
	return delay, nil
}

type transferApplier interface {
	Apply(context.Context, Request, func(zfs.Progress)) (Result, error)
}

// RecoveryOutcome describes one explicit or due remote reconciliation attempt.
type RecoveryOutcome struct {
	Status    string    `json:"status"`
	NotBefore time.Time `json:"not_before,omitempty"`
	Results   []Result  `json:"results,omitempty"`
}

// Roadwarrior reconstructs resume work on demand and coalesces newly due work.
// Scheduling and worker ownership remain outside this recovery component.
type Roadwarrior struct {
	engine  transferApplier
	request Request
	pending *PendingSet
	retry   RetryPolicy
	now     func() time.Time
	random  func() float64

	mu        sync.Mutex
	failures  int
	notBefore time.Time
}

// NewRoadwarrior constructs a per-source-target recovery coordinator.
func NewRoadwarrior(engine transferApplier, request Request, pending *PendingSet, retry RetryPolicy, now func() time.Time, random func() float64) (*Roadwarrior, error) {
	if engine == nil {
		return nil, fmt.Errorf("transfer engine is required")
	}
	if _, err := pendingKey(request.Source, canonicalTarget(request)); err != nil {
		return nil, err
	}
	if pending == nil {
		pending = &PendingSet{}
	}
	if err := retry.validate(); err != nil {
		return nil, err
	}
	if now == nil || random == nil {
		return nil, fmt.Errorf("retry clock and jitter source are required")
	}
	return &Roadwarrior{engine: engine, request: request, pending: pending, retry: retry, now: now, random: random}, nil
}

// Offer coalesces a newly eligible source snapshot while the target is offline.
func (r *Roadwarrior) Offer(snapshot PendingSnapshot) (bool, error) {
	return r.pending.Offer(r.request.Source, canonicalTarget(r.request), snapshot)
}

// Reconcile resumes durable state first, then sends the newest coalesced or
// currently eligible snapshot. Only temporary transport failures enter retry.
func (r *Roadwarrior) Reconcile(ctx context.Context, report func(zfs.Progress)) (RecoveryOutcome, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	if now.Before(r.notBefore) {
		return RecoveryOutcome{Status: "waiting-retry", NotBefore: r.notBefore}, nil
	}
	request := r.request
	pending, hasPending := r.pending.Peek(request.Source, canonicalTarget(request))
	if hasPending {
		request.Snapshot = pending.Name
	}
	outcome := RecoveryOutcome{Status: "probing"}
	for attempts := 0; attempts < 2; attempts++ {
		result, err := r.engine.Apply(ctx, request, report)
		outcome.Results = append(outcome.Results, result)
		if err != nil {
			var temporary interface{ Temporary() bool }
			if !errors.As(err, &temporary) || !temporary.Temporary() {
				outcome.Status = "blocked"
				return outcome, err
			}
			r.failures++
			delay, delayErr := r.retry.Delay(r.failures, r.random())
			if delayErr != nil {
				return outcome, delayErr
			}
			r.notBefore = now.Add(delay)
			outcome.Status, outcome.NotBefore = "waiting-retry", r.notBefore
			return outcome, err
		}
		r.failures, r.notBefore = 0, time.Time{}
		if result.Plan.Mode != "resume" {
			if hasPending && result.Verified && result.Plan.Snapshot == pending.Name {
				r.pending.Complete(request.Source, canonicalTarget(request), pending)
			}
			outcome.Status = "succeeded"
			return outcome, nil
		}
		// The resume token pins its older exact source snapshot. A second JIT
		// planning pass now sends the newest coalesced/currently eligible state.
	}
	return outcome, fmt.Errorf("remote remained resumable after successful resume")
}
