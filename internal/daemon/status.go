package daemon

import (
	"context"
	"errors"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pdf/boomerangz/internal/daemonstate"
	"github.com/pdf/boomerangz/internal/transfer"
)

const (
	// transitionBound is the capacity of a subscriber's transition channel,
	// and the most undelivered transitions its forwarder takes from it. A
	// subscriber that refuses a transition has therefore fallen behind.
	transitionBound = 256
	// stateUpdateInterval limits updates that carry no transition, which are
	// progress samples and view changes, to one per interval per subscriber.
	// Transitions are never delayed by it.
	stateUpdateInterval = 250 * time.Millisecond
	// statusInputBuffer absorbs bursts from producers. A full buffer blocks a
	// producer only for as long as the owner's in-memory work takes.
	statusInputBuffer = 1024
	// progressBuffer holds samples the owner has not yet read. A sample that
	// finds it full is dropped: only the newest sample per job matters, and
	// replication never waits on status for one.
	progressBuffer = 64
	// logRetryInterval re-offers deliveries the log refused when no later
	// change arrives to carry them.
	logRetryInterval = 50 * time.Millisecond
)

// ErrSubscriberOverflow ends a subscription whose consumer fell more than
// transitionBound transitions behind. Its sequence is broken, so it is ended
// rather than continued with a gap.
var ErrSubscriberOverflow = errors.New("status subscriber fell behind")

// Update is one delivery to a status subscriber.
type Update = daemonstate.Update

// EventKind says whether an Event is a transition or a progress sample.
type EventKind = daemonstate.EventKind

// Event kinds; see daemonstate.
const (
	EventTransition = daemonstate.EventTransition
	EventProgress   = daemonstate.EventProgress
)

// Status owns the daemon's status: job states, the newest progress per
// running job, the runtime's dataset view, snapshot deadlines, queue views,
// and the subscribers. One goroutine owns that memory.
//
// Transitions, views, and requests share one input channel, so a read made
// after a change was sent observes it and one producer's messages stay in
// order. Progress samples arrive on a second channel that the owner reads only
// when the first is empty. A sample carries the count of the sending
// transition it was taken under, so it needs no order against transitions.
//
// The owner holds no lock and references no other component, so producers may
// send while holding their own locks. It never waits on a consumer: every send
// to a forwarder is non-blocking.
type Status struct {
	input    chan statusMessage
	progress chan Event
	now      func() time.Time
	peak     atomic.Int64
}

type statusMessage struct {
	event     *Event        // a transition
	queue     *queueView    // a pool's queue view, sent under the queue lock
	deadlines *deadlineView // scheduler deadlines, sent under the scheduler lock
	runtime   *runtimeView  // the runtime's dataset view, sent under the runtime lock
	subscribe *subscribeRequest
	snapshot  chan ControlSnapshot
	flush     chan struct{}
}

type queueView struct {
	name     string
	snapshot QueueSnapshot
}

type deadlineView struct {
	entries map[string]time.Time
}

type runtimeView struct {
	generation       uint64
	entries          int // datasets the discovery generation holds
	configGeneration uint64
	datasets         []DatasetStatus // sorted; NextSnapshot is filled by the owner
}

type subscribeRequest struct {
	log   bool // the daemon log, which marks gaps instead of ending
	reply chan *forwarder
}

// NewStatus starts a status owner. It serves for the life of the process: the
// control plane reads it after the runtime it describes has stopped.
func NewStatus(now func() time.Time) *Status {
	if now == nil {
		now = time.Now
	}
	s := &Status{input: make(chan statusMessage, statusInputBuffer), progress: make(chan Event, progressBuffer), now: now}
	go newStatusOwner(s).run()
	return s
}

// record sends a transition, or offers a progress sample without waiting. A
// nil Status discards it.
func (s *Status) record(event Event) {
	if s == nil {
		return
	}
	if event.Kind == EventProgress {
		select {
		case s.progress <- event:
		default:
		}
		return
	}
	s.input <- statusMessage{event: &event}
}

// publishQueue sends a queue view, and the pending transition for the job an
// offer added, as one message. Callers hold the queue lock.
func (s *Status) publishQueue(name string, snapshot QueueSnapshot, pending *Event) {
	if s == nil {
		return
	}
	s.input <- statusMessage{queue: &queueView{name: name, snapshot: snapshot}, event: pending}
}

// publishDeadlines sends detached scheduler deadlines. Callers hold the
// scheduler lock.
func (s *Status) publishDeadlines(entries map[string]time.Time) {
	if s == nil {
		return
	}
	s.input <- statusMessage{deadlines: &deadlineView{entries: entries}}
}

// publishRuntime sends the runtime's dataset view. Callers hold the runtime
// lock.
func (s *Status) publishRuntime(view runtimeView) {
	if s == nil {
		return
	}
	s.input <- statusMessage{runtime: &view}
}

// Snapshot returns a detached copy of the current state.
func (s *Status) Snapshot() ControlSnapshot {
	reply := make(chan ControlSnapshot, 1)
	s.input <- statusMessage{snapshot: reply}
	result := <-reply
	result.Observed = s.now().UTC()
	return result
}

// BacklogPeak reports the most undelivered transitions any subscriber has
// held at once since the owner started.
func (s *Status) BacklogPeak() int {
	if s == nil {
		return 0
	}
	return int(s.peak.Load())
}

func (s *Status) observeBacklog(held int) {
	for {
		current := s.peak.Load()
		if int64(held) <= current || s.peak.CompareAndSwap(current, int64(held)) {
			return
		}
	}
}

// Subscribe registers a subscriber. The first Update carries the full state
// at registration and no transitions; each later Update carries the
// transitions since the previous one, in order, and the state as of the last
// of them. The channel is closed when the subscription ends; Err then reports
// why, and is ErrSubscriberOverflow when the subscriber fell behind.
// Cancelling ctx ends it.
func (s *Status) Subscribe(ctx context.Context) (*Subscription, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f := s.register(false)
	stop := context.AfterFunc(ctx, func() { f.end(context.Cause(ctx)) })
	go func() {
		<-f.exited
		stop()
	}()
	go f.run()
	return &Subscription{forwarder: f}, nil
}

// subscribeLog registers the daemon log as a subscriber. It writes every
// transition as a "worker state" line and every newly applied discovery
// generation as a "discovery complete" line, on its own goroutine, and it
// never ends: what it could not take is written as one "status log dropped
// transitions" line in its place.
func (s *Status) subscribeLog(logger *slog.Logger) {
	f := s.register(true)
	go f.runLog()
	go func() {
		for batch := range f.batches {
			for _, delivered := range batch {
				switch {
				case delivered.gap.count > 0:
					logger.Error("status log dropped transitions", "count", delivered.gap.count, "first", delivered.gap.first, "last", delivered.gap.last)
				case delivered.flush != nil:
					close(delivered.flush)
				case delivered.discovery != nil:
					logger.Info("discovery complete", "generation", delivered.discovery.generation, "datasets", delivered.discovery.datasets)
				default:
					logTransition(logger, delivered.event)
				}
			}
		}
	}()
}

// flush waits until the daemon log writer has taken every transition sent
// before it, or ctx ends.
func (s *Status) flush(ctx context.Context) {
	if s == nil {
		return
	}
	flushed := make(chan struct{})
	select {
	case s.input <- statusMessage{flush: flushed}:
	case <-ctx.Done():
		return
	}
	select {
	case <-flushed:
	case <-ctx.Done():
	}
}

func (s *Status) register(log bool) *forwarder {
	reply := make(chan *forwarder, 1)
	s.input <- statusMessage{subscribe: &subscribeRequest{log: log, reply: reply}}
	return <-reply
}

func logTransition(logger *slog.Logger, event Event) {
	args := []any{"pool", event.Pool, "job", event.Job, "scope", event.Scope, "target", event.Target, "state", event.State, "reason", event.Reason, "pending", event.Pending}
	switch event.State {
	case "failed":
		logger.Error("worker state", args...)
	case "blocked", "waiting-retry":
		logger.Warn("worker state", args...)
	default:
		logger.Info("worker state", args...)
	}
}

// Subscription is one live status subscription.
type Subscription struct {
	forwarder *forwarder
}

// Updates delivers the subscription's updates and is closed when it ends.
func (u *Subscription) Updates() <-chan Update { return u.forwarder.updates }

// Err reports why the subscription ended, or nil while it is live.
func (u *Subscription) Err() error {
	u.forwarder.mu.Lock()
	defer u.forwarder.mu.Unlock()
	return u.forwarder.err
}

// statusOwner is the memory only the owner goroutine touches.
type statusOwner struct {
	status      *Status
	revision    uint64
	jobs        map[string]Event
	jobQueues   map[string]string
	queues      map[string]QueueSnapshot
	deadlines   map[string]time.Time
	runtime     runtimeView
	subscribers map[*forwarder]*subscriber
	current     *ControlSnapshot // immutable once built; nil when stale
}

// subscriber is the owner's record of one forwarder.
type subscriber struct {
	// refused holds, in order, deliveries the log could not take: gaps
	// counting transitions, and flushes waiting to be offered again. Any other
	// subscriber ends on a refusal, so its record stays empty.
	refused []delivery
}

func newStatusOwner(s *Status) *statusOwner {
	return &statusOwner{
		status:      s,
		jobs:        make(map[string]Event),
		jobQueues:   make(map[string]string),
		queues:      make(map[string]QueueSnapshot),
		deadlines:   make(map[string]time.Time),
		subscribers: make(map[*forwarder]*subscriber),
	}
}

func (o *statusOwner) run() {
	var retry *time.Ticker
	for {
		var retries <-chan time.Time
		if o.hasRefused() {
			if retry == nil {
				retry = time.NewTicker(logRetryInterval)
			}
			retries = retry.C
		} else if retry != nil {
			retry.Stop()
			retry = nil
		}
		// Samples are read only when nothing on the input channel is waiting.
		select {
		case m := <-o.status.input:
			o.handle(m)
			continue
		default:
		}
		select {
		case m := <-o.status.input:
			o.handle(m)
		case sample := <-o.status.progress:
			if o.applyProgress(sample) {
				o.fanOut(nil, nil)
			}
		case <-retries:
			o.fanOut(nil, nil)
		}
	}
}

func (o *statusOwner) handle(m statusMessage) {
	switch {
	case m.subscribe != nil:
		f := newForwarder(o.status, m.subscribe.log)
		if !f.log {
			f.state <- o.state()
		}
		o.subscribers[f] = &subscriber{}
		m.subscribe.reply <- f
	case m.snapshot != nil:
		m.snapshot <- cloneSnapshot(o.state())
	case m.flush != nil:
		logged := false
		for f, record := range o.subscribers {
			if f.log {
				record.refused = append(record.refused, delivery{flush: m.flush})
				logged = true
			}
		}
		if !logged {
			close(m.flush)
		}
		o.fanOut(nil, nil)
	default:
		if transition, discovery, changed := o.apply(m); changed {
			o.fanOut(transition, discovery)
		}
	}
}

// apply updates owned state from one input message. It returns the transition
// to deliver, if the message carried one, the discovery generation to log, if
// the message applied a new one, and whether the state changed.
func (o *statusOwner) apply(m statusMessage) (*Event, *discoveryRecord, bool) {
	changed := false
	if m.queue != nil {
		o.queues[m.queue.name] = m.queue.snapshot
		changed = true
	}
	if m.deadlines != nil {
		o.deadlines = m.deadlines.entries
		changed = true
	}
	var discovery *discoveryRecord
	if m.runtime != nil {
		if m.runtime.generation != 0 && m.runtime.generation != o.runtime.generation {
			discovery = &discoveryRecord{generation: m.runtime.generation, datasets: m.runtime.entries, at: o.status.now().UTC()}
		}
		o.runtime = *m.runtime
		changed = true
	}
	var transition *Event
	if m.event != nil && m.event.Kind == EventTransition {
		event := *m.event
		if m.queue != nil {
			o.jobQueues[event.Job] = m.queue.name
		}
		o.stamp(&event)
		// A transition replaces the row, so leaving sending clears progress.
		o.jobs[event.Job] = event
		transition = &event
		changed = true
	}
	if changed {
		o.revision++
		o.current = nil
	}
	return transition, discovery, changed
}

// applyProgress keeps a sample only while its job is sending, and only under
// the sending transition it was taken under: a first pass's sample read after
// a resume's second sending must not overwrite the new stream's progress.
func (o *statusOwner) applyProgress(sample Event) bool {
	row, found := o.jobs[sample.Job]
	if sample.Kind != EventProgress || !found || row.State != string(transfer.PhaseSending) || row.Send != sample.Send {
		return false
	}
	row.Bytes, row.TotalBytes, row.BytesPerSecond, row.ETA, row.TotalKnown = sample.Bytes, sample.TotalBytes, sample.BytesPerSecond, sample.ETA, sample.TotalKnown
	o.jobs[sample.Job] = row
	o.revision++
	o.current = nil
	return true
}

// stamp sets a job event's queue fields from the newest view of its queue.
func (o *statusOwner) stamp(event *Event) {
	name, found := o.jobQueues[event.Job]
	if !found {
		return
	}
	queue := o.queues[name]
	event.Pending = queue.Pending
	event.Position = slices.Index(queue.IDs, event.Job) + 1
}

// state returns the current snapshot, shared and never modified once built.
func (o *statusOwner) state() *ControlSnapshot {
	if o.current != nil {
		return o.current
	}
	result := &ControlSnapshot{Revision: o.revision, Generation: o.runtime.generation, ConfigGeneration: o.runtime.configGeneration, Queues: maps.Clone(o.queues)}
	if result.Queues == nil {
		result.Queues = map[string]QueueSnapshot{}
	}
	for _, dataset := range o.runtime.datasets {
		dataset.NextSnapshot = o.deadlines[dataset.Name]
		result.Datasets = append(result.Datasets, dataset)
	}
	result.Jobs = make([]Event, 0, len(o.jobs))
	for _, event := range o.jobs {
		o.stamp(&event)
		result.Jobs = append(result.Jobs, event)
	}
	slices.SortFunc(result.Jobs, func(a, b Event) int { return strings.Compare(a.Job, b.Job) })
	o.current = result
	return result
}

func (o *statusOwner) hasRefused() bool {
	for _, record := range o.subscribers {
		if len(record.refused) > 0 {
			return true
		}
	}
	return false
}

// fanOut delivers a change to every subscriber without blocking. A transition
// is sent before the state it produced, and carries that state's revision. A
// subscriber that refuses a transition has fallen behind: the log records a
// gap in its place, and any other subscription ends. A newly applied discovery
// generation is delivered to the log alone, which writes it as a record of its
// own; every other subscriber sees it as state.
func (o *statusOwner) fanOut(transition *Event, discovery *discoveryRecord) {
	for f, record := range o.subscribers {
		select {
		case <-f.exited:
			delete(o.subscribers, f)
			continue
		default:
		}
		if f.log {
			if discovery != nil {
				o.deliverLog(f, record, &delivery{discovery: discovery})
			}
			var next *delivery
			if transition != nil {
				next = &delivery{event: *transition, revision: o.revision}
			}
			o.deliverLog(f, record, next)
			continue
		}
		if transition != nil {
			select {
			case f.transitions <- delivery{event: *transition, revision: o.revision}:
			default:
				f.overflow()
				delete(o.subscribers, f)
				continue
			}
		}
		// The state channel holds one value and only the owner sends to it,
		// so once an unread state is taken back the send cannot block.
		select {
		case <-f.state:
		default:
		}
		f.state <- o.state()
	}
}

// deliverLog offers the log what it refused earlier, in order, then the next
// delivery, a transition or a discovery generation. Nothing is offered past a
// refusal, so the log's order holds, and a flush is closed only by the writer
// once it has taken what preceded it. A refused transition or discovery
// generation is counted in a gap, so what the log holds back stays bounded.
func (o *statusOwner) deliverLog(f *forwarder, record *subscriber, next *delivery) {
	for len(record.refused) > 0 {
		select {
		case f.transitions <- record.refused[0]:
			record.refused = record.refused[1:]
			continue
		default:
		}
		break
	}
	if next == nil {
		return
	}
	if len(record.refused) == 0 {
		select {
		case f.transitions <- *next:
			return
		default:
		}
	}
	last := len(record.refused) - 1
	if last < 0 || record.refused[last].gap.count == 0 {
		record.refused = append(record.refused, delivery{})
		last++
	}
	at := next.event.At
	if next.discovery != nil {
		at = next.discovery.at
	}
	record.refused[last].gap.add(at)
}

// gap counts transitions and discovery generations the daemon log could not
// take, and the span of their times.
type gap struct {
	count       int
	first, last time.Time
}

func (g *gap) add(at time.Time) {
	if g.count == 0 || at.Before(g.first) {
		g.first = at
	}
	if g.count == 0 || at.After(g.last) {
		g.last = at
	}
	g.count++
}

// delivery is one entry on a forwarder's transition channel: a transition
// and the revision it produced, or, for the log only, a discovery generation,
// a gap, or a flush.
type delivery struct {
	event     Event
	revision  uint64
	discovery *discoveryRecord
	gap       gap
	flush     chan struct{}
}

// discoveryRecord is a discovery generation the runtime has applied, as the
// daemon log writes it.
type discoveryRecord struct {
	generation uint64
	datasets   int
	at         time.Time
}

// forwarder decouples the owner from one consumer's pace.
type forwarder struct {
	status       *Status
	log          bool
	transitions  chan delivery         // capacity transitionBound
	state        chan *ControlSnapshot // capacity one, conflated by the owner
	updates      chan Update
	batches      chan []delivery
	overflowed   chan struct{} // closed by the owner when a transition is refused
	ended        chan struct{} // closed when the subscriber ends the subscription
	exited       chan struct{} // closed when the forwarder has stopped
	overflowOnce sync.Once
	endOnce      sync.Once

	mu    sync.Mutex
	cause error
	err   error
}

func newForwarder(s *Status, log bool) *forwarder {
	f := &forwarder{status: s, log: log, transitions: make(chan delivery, transitionBound), overflowed: make(chan struct{}), ended: make(chan struct{}), exited: make(chan struct{})}
	if log {
		f.batches = make(chan []delivery)
	} else {
		f.state = make(chan *ControlSnapshot, 1)
		f.updates = make(chan Update)
	}
	return f
}

func (f *forwarder) overflow() { f.overflowOnce.Do(func() { close(f.overflowed) }) }

func (f *forwarder) end(cause error) {
	f.endOnce.Do(func() {
		f.mu.Lock()
		f.cause = cause
		f.mu.Unlock()
		close(f.ended)
	})
}

func (f *forwarder) finish(err error) {
	f.mu.Lock()
	f.err = err
	f.mu.Unlock()
	close(f.updates)
	close(f.exited)
}

// run serves a subscription. It prefers transitions to state, and offers an
// update only once its state includes every transition the update carries.
func (f *forwarder) run() {
	var (
		pending  []Event
		covered  uint64 // revision the newest pending transition produced
		state    *ControlSnapshot
		dirty    bool
		lastSent time.Time
		timer    *time.Timer
		throttle <-chan time.Time
		outgoing *Update // built for the current pending and state
	)
	stopTimer := func() {
		if timer != nil {
			timer.Stop()
			timer, throttle = nil, nil
		}
	}
	defer stopTimer()
	take := func(d delivery) {
		pending = append(pending, d.event)
		covered, outgoing = d.revision, nil
		f.status.observeBacklog(len(pending) + len(f.transitions))
	}
	for {
		// Holding a full bound, stop taking transitions: the channel fills,
		// and the owner's refused send ends the subscription.
		var transitions <-chan delivery
		if len(pending) < transitionBound {
			transitions = f.transitions
			select {
			case d := <-transitions:
				take(d)
				continue
			default:
			}
		}
		var updates chan<- Update
		switch {
		case state == nil:
		case len(pending) > 0 && state.Revision >= covered:
			updates = f.updates
		case len(pending) == 0 && dirty:
			if wait := stateUpdateInterval - time.Since(lastSent); wait <= 0 {
				updates = f.updates
			} else if throttle == nil {
				timer = time.NewTimer(wait)
				throttle = timer.C
			}
		}
		var update Update
		if updates != nil {
			if outgoing == nil {
				built := Update{Transitions: pending, State: cloneSnapshot(state)}
				built.State.Observed = f.status.now().UTC()
				outgoing = &built
			}
			update = *outgoing
		}
		select {
		case d := <-transitions:
			take(d)
		case next := <-f.state:
			state, dirty, outgoing = next, true, nil
			// The owner sent every transition this state includes before the
			// state itself, so they are already in the channel.
			for len(pending) < transitionBound {
				select {
				case d := <-f.transitions:
					take(d)
					continue
				default:
				}
				break
			}
		case updates <- update:
			pending, dirty, outgoing = nil, false, nil
			lastSent = time.Now()
			stopTimer()
		case <-throttle:
			timer, throttle = nil, nil
		case <-f.overflowed:
			f.finish(ErrSubscriberOverflow)
			return
		case <-f.ended:
			f.mu.Lock()
			cause := f.cause
			f.mu.Unlock()
			f.finish(cause)
			return
		}
	}
}

// runLog serves the daemon log writer, which never ends. It hands deliveries
// over in the owner's order, in batches of at most transitionBound.
func (f *forwarder) runLog() {
	var batch []delivery
	for {
		var transitions <-chan delivery
		if len(batch) < transitionBound {
			transitions = f.transitions
		}
		var batches chan<- []delivery
		if len(batch) > 0 {
			batches = f.batches
		}
		select {
		case d := <-transitions:
			batch = append(batch, d)
			f.status.observeBacklog(len(batch) + len(f.transitions))
		case batches <- batch:
			batch = nil
		}
	}
}

func cloneSnapshot(snapshot *ControlSnapshot) ControlSnapshot {
	if snapshot == nil {
		return ControlSnapshot{}
	}
	result := *snapshot
	result.Datasets = slices.Clone(snapshot.Datasets)
	result.Jobs = slices.Clone(snapshot.Jobs)
	result.Queues = maps.Clone(snapshot.Queues)
	for name, queue := range result.Queues {
		queue.IDs = slices.Clone(queue.IDs)
		result.Queues[name] = queue
	}
	return result
}
