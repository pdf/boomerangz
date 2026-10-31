package transfer

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
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

// CoalesceResult reports whether a newer item replaced queued work and whether
// the displaced snapshot is not currently selected by an in-flight attempt.
type CoalesceResult struct {
	Changed           bool
	Superseded        PendingSnapshot
	ReleaseSuperseded bool
}

// PendingSet coalesces disconnected work independently per stream-target pair.
// A stream is the dataset whose snapshots it sends: a root, or one of its
// members, each of which keeps its own pending endpoint.
type PendingSet struct {
	mu     sync.Mutex
	jobs   map[string]PendingSnapshot
	active map[string]PendingSnapshot
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
	result, err := p.Coalesce(source, target, snapshot)
	return result.Changed, err
}

// Coalesce retains the newest eligible snapshot and identifies an older
// non-active item whose durable recovery reference can be released.
func (p *PendingSet) Coalesce(source, target string, snapshot PendingSnapshot) (CoalesceResult, error) {
	key, err := pendingKey(source, target)
	if err != nil {
		return CoalesceResult{}, err
	}
	if snapshot.CreateTXG == 0 || !strings.HasPrefix(snapshot.Name, source+"@") {
		return CoalesceResult{}, fmt.Errorf("pending snapshot must be on the source with a creation transaction")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.jobs == nil {
		p.jobs = make(map[string]PendingSnapshot)
	}
	current, exists := p.jobs[key]
	if exists && current.CreateTXG >= snapshot.CreateTXG {
		return CoalesceResult{}, nil
	}
	p.jobs[key] = snapshot
	result := CoalesceResult{Changed: true}
	if exists {
		result.Superseded = current
		result.ReleaseSuperseded = p.active[key] != current
	}
	return result, nil
}

// Begin reserves the current coalesced snapshot for an attempt. A newer Offer
// may replace the queued item while this exact snapshot remains protected.
func (p *PendingSet) Begin(source, target string) (PendingSnapshot, bool) {
	key, err := pendingKey(source, target)
	if err != nil {
		return PendingSnapshot{}, false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	snapshot, exists := p.jobs[key]
	if !exists {
		return PendingSnapshot{}, false
	}
	if p.active == nil {
		p.active = make(map[string]PendingSnapshot)
	}
	p.active[key] = snapshot
	return snapshot, true
}

// End releases an attempt reservation and removes the queued item only when
// that exact snapshot completed; a newer coalesced item remains pending.
func (p *PendingSet) End(source, target string, snapshot PendingSnapshot, complete bool) {
	key, err := pendingKey(source, target)
	if err != nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.active[key] == snapshot {
		delete(p.active, key)
	}
	if complete && p.jobs[key] == snapshot {
		delete(p.jobs, key)
	}
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

// PeekScope returns pending work for root's own stream, or failing that for the
// first of its members' streams that has any, so a root's job can name what it
// carries.
func (p *PendingSet) PeekScope(root, target string) (PendingSnapshot, bool) {
	if snapshot, found := p.Peek(root, target); found {
		return snapshot, true
	}
	if _, err := pendingKey(root, target); err != nil {
		return PendingSnapshot{}, false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	var keys []string
	for key := range p.jobs {
		source, keyTarget, _ := strings.Cut(key, "\x00")
		if keyTarget == target && strings.HasPrefix(source, root+"/") {
			keys = append(keys, key)
		}
	}
	if len(keys) == 0 {
		return PendingSnapshot{}, false
	}
	slices.Sort(keys)
	return p.jobs[keys[0]], true
}

// Reservation is the pending endpoint one stream's attempt carries.
type Reservation struct {
	Stream   string
	Snapshot PendingSnapshot
}

// BeginStreams reserves each of the request's streams' coalesced pending work
// and returns the request with those endpoints pinned. Finish ends every
// reservation, completing each stream that verified the exact endpoint it
// carried, whether or not the other streams did.
func (p *PendingSet) BeginStreams(request Request, target string) (Request, []Reservation, error) {
	streams, err := request.Streams()
	if err != nil {
		return request, nil, err
	}
	request.Members = slices.Clone(request.Members)
	var reserved []Reservation
	for _, stream := range streams {
		snapshot, found := p.Begin(stream.Dataset, target)
		if !found {
			continue
		}
		reserved = append(reserved, Reservation{Stream: stream.Dataset, Snapshot: snapshot})
		if len(request.Members) == 0 {
			request.Snapshot = snapshot.Name
			continue
		}
		for i := range request.Members {
			if request.Members[i].Dataset == stream.Dataset {
				request.Members[i].Snapshot = snapshot.Name
			}
		}
	}
	return request, reserved, nil
}

// FinishStreams ends reservations made by BeginStreams against result.
func (p *PendingSet) FinishStreams(target string, reserved []Reservation, result Result) {
	verified := make(map[string]string)
	for _, stream := range StreamResults(result) {
		if stream.Verified {
			verified[stream.Plan.Source] = stream.Plan.Snapshot
		}
	}
	for _, reservation := range reserved {
		snapshot, ok := verified[reservation.Stream]
		p.End(reservation.Stream, target, reservation.Snapshot, ok && snapshot == reservation.Snapshot.Name)
	}
}

// StreamResults returns one result per stream attempted.
func StreamResults(result Result) []Result {
	if len(result.Streams) > 0 {
		return result.Streams
	}
	return []Result{result}
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
	Apply(context.Context, Request, Reporter) (Result, error)
}

// RecoveryOutcome describes one explicit or due remote reconciliation attempt.
type RecoveryOutcome struct {
	Status string `json:"status"`
	// Pending is the pending snapshot the attempt carried, if any.
	Pending   string    `json:"pending,omitempty"`
	Reason    string    `json:"reason,omitempty"`
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
	reason    string
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

// NotBefore reports the deadline the current backoff sets, zero when no
// failure is outstanding. A caller that owns scheduling uses it to leave the
// coordinator alone until an attempt would do something.
func (r *Roadwarrior) NotBefore() time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.notBefore
}

// Offer coalesces a newly eligible snapshot, of the root or of one of its
// members, while the target is offline.
func (r *Roadwarrior) Offer(snapshot PendingSnapshot) (bool, error) {
	return r.pending.Offer(datasetOf(snapshot.Name), canonicalTarget(r.request), snapshot)
}

// Reconcile resumes durable state first, then sends the newest coalesced or
// currently eligible snapshot. Only temporary transport failures enter retry.
func (r *Roadwarrior) Reconcile(ctx context.Context, report Reporter) (RecoveryOutcome, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	if now.Before(r.notBefore) {
		// An attempt made before the backoff deadline reports the failure that
		// set it, so a caller that shows this outcome as the job's latest state
		// never shows a retry with no cause.
		outcome := RecoveryOutcome{Status: "waiting-retry", Reason: r.reason, NotBefore: r.notBefore}
		if pending, found := r.pending.PeekScope(r.request.Source, canonicalTarget(r.request)); found {
			outcome.Pending = pending.Name
		}
		return outcome, nil
	}
	target := canonicalTarget(r.request)
	request, reserved, err := r.pending.BeginStreams(r.request, target)
	if err != nil {
		return RecoveryOutcome{Status: "blocked"}, err
	}
	var last Result
	defer func() { r.pending.FinishStreams(target, reserved, last) }()
	outcome := RecoveryOutcome{Status: "probing"}
	if len(reserved) > 0 {
		outcome.Pending = reserved[0].Snapshot.Name
	}
	for attempts := 0; attempts < 2; attempts++ {
		result, err := r.engine.Apply(ctx, request, report)
		last = result
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
			r.notBefore, r.reason = now.Add(delay), err.Error()
			outcome.Status, outcome.NotBefore, outcome.Reason = "waiting-retry", r.notBefore, r.reason
			return outcome, err
		}
		r.failures, r.notBefore, r.reason = 0, time.Time{}, ""
		if result.Plan.Mode != "resume" {
			outcome.Status = "succeeded"
			return outcome, nil
		}
		// The resume token pins its older exact source snapshot. A second JIT
		// planning pass now sends the newest coalesced/currently eligible state.
	}
	return outcome, fmt.Errorf("remote remained resumable after successful resume")
}
