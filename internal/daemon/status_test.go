package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"
)

func transition(job, state, reason string) Event {
	return Event{Kind: EventTransition, Job: job, State: state, Reason: reason, At: time.Now().UTC()}
}

func sending(job string, send uint64) Event {
	event := transition(job, "sending", "")
	event.Send = send
	return event
}

func progress(job string, send, bytes uint64) Event {
	return Event{Kind: EventProgress, Job: job, Send: send, Bytes: bytes, At: time.Now().UTC()}
}

// ownedStatus is a status owner the test goroutine drives in place of the
// owner goroutine, so the order of its work is the test's.
func ownedStatus(t *testing.T) (*statusOwner, *Subscription) {
	t.Helper()
	owner := newStatusOwner(&Status{now: time.Now})
	reply := make(chan *forwarder, 1)
	owner.handle(statusMessage{subscribe: &subscribeRequest{reply: reply}})
	f := <-reply
	stop := context.AfterFunc(t.Context(), func() { f.end(t.Context().Err()) })
	t.Cleanup(func() { stop() })
	return owner, &Subscription{forwarder: f}
}

func (o *statusOwner) record(event Event) {
	if event.Kind == EventProgress {
		if o.applyProgress(event) {
			o.fanOut(nil, nil)
		}
		return
	}
	o.handle(statusMessage{event: &event})
}

// receive returns the next update, failing the test if none arrives.
func receive(t *testing.T, subscription *Subscription) Update {
	t.Helper()
	select {
	case update, ok := <-subscription.Updates():
		if !ok {
			t.Fatalf("subscription ended: %v", subscription.Err())
		}
		return update
	case <-time.After(5 * time.Second):
		t.Fatal("no status update arrived")
	}
	return Update{}
}

// collect receives updates until it holds count transitions.
func collect(t *testing.T, subscription *Subscription, count int) ([]Event, Update) {
	t.Helper()
	var transitions []Event
	var last Update
	for len(transitions) < count {
		last = receive(t, subscription)
		transitions = append(transitions, last.Transitions...)
	}
	if len(transitions) != count {
		t.Fatalf("received %d transitions, want %d: %+v", len(transitions), count, transitions)
	}
	return transitions, last
}

// drain discards updates until the subscription ends.
func drain(subscription *Subscription) {
	for update := range subscription.Updates() {
		_ = update
	}
}

func subscribe(t *testing.T, status *Status) *Subscription {
	t.Helper()
	subscription, err := status.Subscribe(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if first := receive(t, subscription); len(first.Transitions) != 0 {
		t.Fatalf("first update carried transitions: %+v", first.Transitions)
	}
	return subscription
}

func TestStatusDeliversTransitionsInOrder(t *testing.T) {
	t.Parallel()
	status := NewStatus(time.Now)
	subscription := subscribe(t, status)
	var sent []string
	for index := range 100 {
		job := "job-" + strconv.Itoa(index%3)
		reason := strconv.Itoa(index)
		// Two identical consecutive transitions are two events, not one.
		status.record(transition(job, "waiting-retry", reason))
		status.record(transition(job, "waiting-retry", reason))
		sent = append(sent, job+"/"+reason, job+"/"+reason)
	}
	transitions, _ := collect(t, subscription, len(sent))
	var got []string
	for _, event := range transitions {
		got = append(got, event.Job+"/"+event.Reason)
	}
	if !slices.Equal(got, sent) {
		t.Fatalf("delivered %v, want %v", got, sent)
	}
}

func TestStatusConsumerThatNeverReadsDoesNotBlockProducers(t *testing.T) {
	t.Parallel()
	status := NewStatus(time.Now)
	stalled, err := status.Subscribe(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for index := range 20 * (transitionBound + statusInputBuffer) {
			status.record(transition("job", "running", strconv.Itoa(index)))
		}
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("a subscriber that never reads blocked the producer")
	}
	if jobs := status.Snapshot().Jobs; len(jobs) != 1 {
		t.Fatalf("status = %+v", jobs)
	}
	drain(stalled)
	if !errors.Is(stalled.Err(), ErrSubscriberOverflow) {
		t.Fatalf("stalled subscription err = %v, want overflow", stalled.Err())
	}
}

func TestStatusOverflowEndsOnlyTheSubscriberThatFellBehind(t *testing.T) {
	t.Parallel()
	status := NewStatus(time.Now)
	stalled, err := status.Subscribe(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	reading := subscribe(t, status)
	total := 0
	// Batches below the bound, each drained by the reading subscriber before
	// the next, overflow only the subscriber that never reads.
	for range 8 {
		for range transitionBound / 2 {
			status.record(transition("job", "running", strconv.Itoa(total)))
			total++
		}
		collect(t, reading, transitionBound/2)
	}
	drain(stalled)
	if !errors.Is(stalled.Err(), ErrSubscriberOverflow) {
		t.Fatalf("stalled subscription err = %v, want overflow", stalled.Err())
	}
	status.record(transition("job", "succeeded", ""))
	if transitions, _ := collect(t, reading, 1); transitions[0].State != "succeeded" || reading.Err() != nil {
		t.Fatalf("reading subscription after overflow: %+v err=%v", transitions, reading.Err())
	}
}

func TestStatusSubscriptionEndsWithItsContext(t *testing.T) {
	t.Parallel()
	status := NewStatus(time.Now)
	ctx, cancel := context.WithCancel(t.Context())
	subscription, err := status.Subscribe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	drain(subscription)
	if !errors.Is(subscription.Err(), context.Canceled) {
		t.Fatalf("err = %v, want cancellation", subscription.Err())
	}
	if _, err := status.Subscribe(ctx); err == nil {
		t.Fatal("subscribed with a cancelled context")
	}
}

func TestStatusConflatesProgressWithoutDisplacingTransitions(t *testing.T) {
	t.Parallel()
	owner, subscription := ownedStatus(t)
	owner.record(sending("a", 1))
	owner.record(sending("b", 2))
	// The forwarder has not run while the samples arrive. Queued, they would
	// have spent the transition bound many times over.
	for sample := range uint64(4 * transitionBound) {
		owner.record(progress("a", 1, sample+1))
		owner.record(progress("b", 2, 2*(sample+1)))
	}
	owner.record(transition("c", "running", ""))
	go subscription.forwarder.run()
	transitions, last := collect(t, subscription, 3)
	var jobs []string
	for _, event := range transitions {
		jobs = append(jobs, event.Job)
	}
	if !slices.Equal(jobs, []string{"a", "b", "c"}) {
		t.Fatalf("transitions = %v", jobs)
	}
	bytes := map[string]uint64{}
	for _, job := range last.State.Jobs {
		bytes[job.Job] = job.Bytes
	}
	if bytes["a"] != 4*transitionBound || bytes["b"] != 8*transitionBound {
		t.Fatalf("held progress = %v, want the newest sample per job", bytes)
	}
	if err := subscription.Err(); err != nil {
		t.Fatalf("progress overflowed a subscription: %v", err)
	}
}

func TestStatusDiscardsProgressThatDoesNotBelongToTheCurrentSend(t *testing.T) {
	t.Parallel()
	owner := newStatusOwner(&Status{now: time.Now})
	bytes := func() uint64 {
		t.Helper()
		jobs := owner.state().Jobs
		if len(jobs) != 1 {
			t.Fatalf("status = %+v", jobs)
		}
		return jobs[0].Bytes
	}
	if owner.applyProgress(progress("unknown", 1, 1)) {
		t.Fatal("kept a sample for a job with no transition")
	}
	owner.record(transition("job", "probing", ""))
	if owner.applyProgress(progress("job", 1, 2)) {
		t.Fatal("kept a sample for a job that is not sending")
	}
	owner.record(sending("job", 1))
	if !owner.applyProgress(progress("job", 1, 3)) || bytes() != 3 {
		t.Fatalf("sample for the current send was not kept: bytes=%d", bytes())
	}
	// A resume starts a second stream. A first-pass sample read after it
	// must not overwrite the second stream's progress.
	owner.record(transition("job", "verifying", ""))
	owner.record(sending("job", 2))
	if owner.applyProgress(progress("job", 1, 9)) || bytes() != 0 {
		t.Fatalf("kept a sample from the previous send: bytes=%d", bytes())
	}
	if !owner.applyProgress(progress("job", 2, 4)) || bytes() != 4 {
		t.Fatalf("sample for the second send was not kept: bytes=%d", bytes())
	}
	// Leaving sending clears progress, and a late sample does not restore it.
	owner.record(transition("job", "verifying", ""))
	if owner.applyProgress(progress("job", 2, 5)) || bytes() != 0 {
		t.Fatalf("kept a sample after the job left sending: bytes=%d", bytes())
	}
}

func TestStatusProgressDoesNotWaitForTheOwner(t *testing.T) {
	t.Parallel()
	status := &Status{input: make(chan statusMessage), progress: make(chan Event, progressBuffer), now: time.Now}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for sample := range uint64(10 * progressBuffer) {
			status.record(progress("job", 1, sample))
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a sample waited for an owner that is not reading")
	}
}

func TestStatusRejectsAnEventWithoutAKind(t *testing.T) {
	t.Parallel()
	status := NewStatus(time.Now)
	status.record(Event{Job: "job", State: "running"})
	if snapshot := status.Snapshot(); len(snapshot.Jobs) != 0 || snapshot.Revision != 0 {
		t.Fatalf("status = %+v", snapshot)
	}
}

func TestStatusFirstUpdateAgreesWithLaterTransitions(t *testing.T) {
	t.Parallel()
	status := NewStatus(time.Now)
	status.record(transition("a", "pending-management", ""))
	status.record(transition("b", "running", ""))
	status.publishQueue("management", QueueSnapshot{Capacity: 4, Pending: 1, IDs: []string{"a"}}, nil)
	subscription, err := status.Subscribe(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	first := receive(t, subscription)
	if len(first.Transitions) != 0 {
		t.Fatalf("first update transitions = %+v", first.Transitions)
	}
	states := map[string]string{}
	for _, job := range first.State.Jobs {
		states[job.Job] = job.State
	}
	if want := map[string]string{"a": "pending-management", "b": "running"}; !mapsEqual(states, want) {
		t.Fatalf("first state = %v, want %v", states, want)
	}
	status.record(transition("b", "succeeded", ""))
	status.record(transition("a", "snapshotting", ""))
	status.record(transition("c", "running", ""))
	transitions, last := collect(t, subscription, 3)
	for _, event := range transitions {
		states[event.Job] = event.State
	}
	final := map[string]string{}
	for _, job := range last.State.Jobs {
		final[job.Job] = job.State
	}
	if !mapsEqual(states, final) {
		t.Fatalf("first state plus transitions = %v, final state = %v", states, final)
	}
	if last.State.Revision <= first.State.Revision {
		t.Fatalf("revision did not advance: %d then %d", first.State.Revision, last.State.Revision)
	}
}

func mapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for key, value := range a {
		if other, found := b[key]; !found || other != value {
			return false
		}
	}
	return true
}

func TestStatusStampsQueueFieldsFromTheNewestView(t *testing.T) {
	t.Parallel()
	status := NewStatus(time.Now)
	pending := transition("b", "pending-management", "")
	status.publishQueue("management", QueueSnapshot{Capacity: 4, Pending: 2, IDs: []string{"a", "b"}}, &pending)
	jobs := status.Snapshot().Jobs
	if len(jobs) != 1 || jobs[0].Pending != 2 || jobs[0].Position != 2 {
		t.Fatalf("queued status = %+v", jobs)
	}
	status.publishQueue("management", QueueSnapshot{Capacity: 4, Pending: 1, IDs: []string{"b"}}, nil)
	jobs = status.Snapshot().Jobs
	if len(jobs) != 1 || jobs[0].Pending != 1 || jobs[0].Position != 1 {
		t.Fatalf("status after a pop = %+v", jobs)
	}
}

func TestStatusReadsObserveEarlierWrites(t *testing.T) {
	t.Parallel()
	status := NewStatus(time.Now)
	for generation := range uint64(100) {
		status.publishRuntime(runtimeView{configGeneration: generation})
		if got := status.Snapshot().ConfigGeneration; got != generation {
			t.Fatalf("configuration generation = %d, want %d", got, generation)
		}
	}
}

func TestStatusThrottlesUpdatesWithoutTransitions(t *testing.T) {
	t.Parallel()
	status := NewStatus(time.Now)
	subscription := subscribe(t, status)
	status.record(transition("job", "running", ""))
	collect(t, subscription, 1)
	started := time.Now()
	for generation := range uint64(50) {
		status.publishRuntime(runtimeView{generation: generation + 1})
	}
	for {
		if update := receive(t, subscription); update.State.Generation == 50 {
			break
		}
	}
	if elapsed := time.Since(started); elapsed < stateUpdateInterval/2 {
		t.Fatalf("an update without a transition arrived after %v, inside the %v limit", elapsed, stateUpdateInterval)
	}
	// A transition is not held back by the limit.
	started = time.Now()
	status.record(transition("job", "succeeded", ""))
	collect(t, subscription, 1)
	if elapsed := time.Since(started); elapsed >= stateUpdateInterval {
		t.Fatalf("a transition waited %v", elapsed)
	}
}

// lineLog records the messages and attributes a logger writes, and can hold
// its writer.
type lineLog struct {
	mu    sync.Mutex
	lines []map[string]string
	hold  chan struct{}
}

func (*lineLog) Enabled(context.Context, slog.Level) bool { return true }
func (l *lineLog) WithAttrs([]slog.Attr) slog.Handler     { return l }
func (l *lineLog) WithGroup(string) slog.Handler          { return l }
func (l *lineLog) Handle(_ context.Context, record slog.Record) error {
	if l.hold != nil {
		<-l.hold
	}
	line := map[string]string{"msg": record.Message, "level": record.Level.String()}
	record.Attrs(func(attr slog.Attr) bool {
		line[attr.Key] = attr.Value.String()
		return true
	})
	l.mu.Lock()
	l.lines = append(l.lines, line)
	l.mu.Unlock()
	return nil
}

func (l *lineLog) snapshot() []map[string]string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.lines)
}

func TestStatusLogWritesEveryTransitionInOwnerOrder(t *testing.T) {
	t.Parallel()
	log := &lineLog{}
	status := NewStatus(time.Now)
	status.subscribeLog(slog.New(log))
	subscription := subscribe(t, status)
	var producers sync.WaitGroup
	for producer := range 4 {
		producers.Go(func() {
			for index := range 40 {
				job := fmt.Sprintf("job-%d", producer)
				status.record(transition(job, "running", strconv.Itoa(index)))
				status.record(progress(job, 1, uint64(index)))
			}
		})
	}
	producers.Wait()
	status.record(transition("job-0", "failed", "last"))
	transitions, _ := collect(t, subscription, 161)
	status.flush(t.Context())
	var logged, delivered []string
	for _, line := range log.snapshot() {
		if line["msg"] != "worker state" {
			t.Fatalf("unexpected log line %v", line)
		}
		logged = append(logged, line["job"]+"/"+line["state"]+"/"+line["reason"])
	}
	for _, event := range transitions {
		delivered = append(delivered, event.Job+"/"+event.State+"/"+event.Reason)
	}
	if !slices.Equal(logged, delivered) {
		t.Fatalf("log order %v differs from delivery order %v", logged, delivered)
	}
	if last := log.snapshot()[160]; last["level"] != "ERROR" {
		t.Fatalf("failed transition logged at %s", last["level"])
	}
}

func TestStatusLogMarksAGapWhenItFallsBehind(t *testing.T) {
	t.Parallel()
	log := &lineLog{hold: make(chan struct{})}
	status := NewStatus(time.Now)
	status.subscribeLog(slog.New(log))
	total := 4 * (transitionBound + statusInputBuffer)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for index := range total {
			status.record(transition("job", "running", strconv.Itoa(index)))
		}
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("a stalled log writer blocked the producer")
	}
	close(log.hold)
	status.flush(t.Context())
	// Everything before the flush has been taken, so the log keeps up again.
	status.record(transition("job", "succeeded", "after"))
	status.flush(t.Context())
	lines := log.snapshot()
	if gaps := checkLogSequence(t, lines[:len(lines)-1], total); gaps == 0 {
		t.Fatal("a stalled log writer wrote no gap line")
	}
	if last := lines[len(lines)-1]; last["msg"] != "worker state" || last["reason"] != "after" {
		t.Fatalf("log did not resume after the gap: last line %v", last)
	}
}

// checkLogSequence requires lines to account for entries 0 through total-1 in
// order, each either written or counted in the gap line that stands in its
// place. Entry i is a transition with reason i or a discovery generation i+1.
// It returns the number of gap lines.
func checkLogSequence(t *testing.T, lines []map[string]string, total int) int {
	t.Helper()
	next, gaps := 0, 0
	for index, line := range lines {
		switch line["msg"] {
		case "status log dropped transitions":
			count, err := strconv.Atoi(line["count"])
			if err != nil || count == 0 || line["level"] != "ERROR" || line["first"] == "" || line["last"] == "" {
				t.Fatalf("gap line %d: %v", index, line)
			}
			gaps++
			next += count
		case "worker state":
			if reason, err := strconv.Atoi(line["reason"]); err != nil || reason != next {
				t.Fatalf("line %d is transition %q, want %d", index, line["reason"], next)
			}
			next++
		case "discovery complete":
			if generation, err := strconv.Atoi(line["generation"]); err != nil || generation != next+1 || line["level"] != "INFO" {
				t.Fatalf("line %d is discovery generation %q, want %d", index, line["generation"], next+1)
			}
			next++
		default:
			t.Fatalf("unexpected line %d: %v", index, line)
		}
	}
	if next != total {
		t.Fatalf("log accounts for %d transitions, want %d", next, total)
	}
	return gaps
}

func TestStatusFlushWaitsForTheLogWriter(t *testing.T) {
	t.Parallel()
	log := &lineLog{hold: make(chan struct{})}
	status := NewStatus(time.Now)
	status.subscribeLog(slog.New(log))
	for index := range 2 * transitionBound {
		status.record(transition("job", "running", strconv.Itoa(index)))
	}
	flushed := make(chan struct{})
	go func() {
		defer close(flushed)
		status.flush(t.Context())
	}()
	select {
	case <-flushed:
		t.Fatal("flush returned while the log writer was held")
	case <-time.After(3 * logRetryInterval):
	}
	close(log.hold)
	select {
	case <-flushed:
	case <-time.After(5 * time.Second):
		t.Fatal("flush did not return once the log writer was released")
	}
	checkLogSequence(t, log.snapshot(), 2*transitionBound)
}

func TestStatusLogWritesEachNewDiscoveryGeneration(t *testing.T) {
	t.Parallel()
	log := &lineLog{}
	status := NewStatus(time.Now)
	status.subscribeLog(slog.New(log))
	// A runtime with no generation yet writes nothing.
	status.publishRuntime(runtimeView{configGeneration: 1})
	status.record(transition("job", "running", "before"))
	status.publishRuntime(runtimeView{generation: 1, entries: 3, configGeneration: 1})
	// The same generation published again, by a configuration reload or an
	// activation change, is not a new discovery.
	status.publishRuntime(runtimeView{generation: 1, entries: 3, configGeneration: 2})
	status.record(transition("job", "succeeded", "after"))
	status.publishRuntime(runtimeView{generation: 2, entries: 4, configGeneration: 2})
	status.flush(t.Context())
	var got []string
	for _, line := range log.snapshot() {
		switch line["msg"] {
		case "worker state":
			got = append(got, "worker state/"+line["reason"])
		case "discovery complete":
			got = append(got, "discovery complete/"+line["generation"]+"/"+line["datasets"]+"/"+line["level"])
		default:
			t.Fatalf("unexpected log line %v", line)
		}
	}
	want := []string{"worker state/before", "discovery complete/1/3/INFO", "worker state/after", "discovery complete/2/4/INFO"}
	if !slices.Equal(got, want) {
		t.Fatalf("log = %v, want %v", got, want)
	}
}

func TestStatusLogCountsDiscoveryGenerationsInAGap(t *testing.T) {
	t.Parallel()
	log := &lineLog{hold: make(chan struct{})}
	status := NewStatus(time.Now)
	status.subscribeLog(slog.New(log))
	total := 4 * (transitionBound + statusInputBuffer)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for index := range total {
			if index%10 == 0 {
				status.publishRuntime(runtimeView{generation: uint64(index + 1)})
				continue
			}
			status.record(transition("job", "running", strconv.Itoa(index)))
		}
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("a stalled log writer blocked the producer")
	}
	close(log.hold)
	status.flush(t.Context())
	if gaps := checkLogSequence(t, log.snapshot(), total); gaps == 0 {
		t.Fatal("a stalled log writer wrote no gap line")
	}
}
