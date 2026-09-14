package statuswait

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pdf/boomerangz/internal/daemonstate"
)

type scriptedSubscription struct {
	updates chan daemonstate.Update
	mu      sync.Mutex
	err     error
}

func (s *scriptedSubscription) Updates() <-chan daemonstate.Update { return s.updates }

func (s *scriptedSubscription) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

func (s *scriptedSubscription) end(err error) {
	s.mu.Lock()
	s.err = err
	s.mu.Unlock()
	close(s.updates)
}

type scriptedSource struct {
	subscription *scriptedSubscription
	mu           sync.Mutex
	revision     uint64 // the owner's current revision, which ControlStatus reports
}

func (s *scriptedSource) ControlStatus() daemonstate.ControlSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return daemonstate.ControlSnapshot{Revision: s.revision}
}

func (s *scriptedSource) record(transitions int) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.revision += uint64(transitions)
	return s.revision
}

func newScriptedSource() *scriptedSource {
	return &scriptedSource{subscription: &scriptedSubscription{updates: make(chan daemonstate.Update)}}
}

func (s *scriptedSource) SubscribeStatus(ctx context.Context) (daemonstate.Subscription, error) {
	go func() {
		<-ctx.Done()
		defer func() { _ = recover() }() // already ended by the test
		s.subscription.end(ctx.Err())
	}()
	return s.subscription, nil
}

// offer delivers events as recorded by the owner, with the state they
// produced.
func (s *scriptedSource) offer(events ...daemonstate.Event) error {
	return s.deliver(daemonstate.Update{Transitions: events, State: daemonstate.ControlSnapshot{Revision: s.record(len(events))}})
}

func (s *scriptedSource) deliver(update daemonstate.Update) error {
	select {
	case s.subscription.updates <- update:
		return nil
	case <-time.After(5 * time.Second):
		return errors.New("waiter did not take an update")
	}
}

// send delivers events and returns once the waiter has recorded them.
func (s *scriptedSource) send(t *testing.T, w *Waiter, events ...daemonstate.Event) {
	t.Helper()
	before := w.Mark()
	if err := s.offer(events...); err != nil {
		t.Fatal(err)
	}
	w.Until(t, "the update to be recorded", 5*time.Second, 0, func(transitions []daemonstate.Event) (bool, error) {
		return len(transitions) >= int(before)+len(events), nil
	})
}

// fatalRecorder stands in for the running test so a wait's failure can be
// observed instead of failing the test that provoked it.
type fatalRecorder struct {
	testing.TB
	mu      sync.Mutex
	message string
}

func (r *fatalRecorder) Helper() {}

func (r *fatalRecorder) Fatalf(format string, args ...any) {
	r.mu.Lock()
	r.message = fmt.Sprintf(format, args...)
	r.mu.Unlock()
	runtime.Goexit()
}

// failure runs wait on its own goroutine, as testing does, and returns the
// message it failed with, or "" when it returned.
func failure(t *testing.T, wait func(testing.TB)) string {
	t.Helper()
	recorder := &fatalRecorder{TB: t}
	done := make(chan struct{})
	go func() {
		defer close(done)
		wait(recorder)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("wait neither returned nor failed")
	}
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	return recorder.message
}

func transition(job, state, reason string) daemonstate.Event {
	return daemonstate.Event{Kind: daemonstate.EventTransition, Pool: "management", Job: job, State: state, Reason: reason, At: time.Now().UTC()}
}

func TestNextFindsOccurrencesInOrder(t *testing.T) {
	source := newScriptedSource()
	w := New(t, source)
	source.send(t, w, transition("snapshot:a", "snapshotting", ""), transition("snapshot:a", "succeeded", ""))

	// The second occurrence is delivered only once the wait for it has begun,
	// so the wait is woken by the delivery rather than finding it recorded.
	firstFound := make(chan struct{})
	offered := make(chan error, 1)
	go func() {
		<-firstFound
		time.Sleep(20 * time.Millisecond) // a fixture: let the second wait block first
		offered <- source.offer(transition("snapshot:a", "scheduled", "existing owned snapshot sets the next deadline"), transition("snapshot:a", "succeeded", "second"))
	}()
	var (
		second daemonstate.Event
		at     Cursor
	)
	message := failure(t, func(tb testing.TB) {
		_, after := w.Next(tb, "first success", time.Second, 0, Job("snapshot:a", "succeeded"))
		close(firstFound)
		second, at = w.Next(tb, "second success", 5*time.Second, after, Job("snapshot:a", "succeeded"))
	})
	if err := <-offered; err != nil {
		t.Fatal(err)
	}
	if message != "" {
		t.Fatal(message)
	}
	if second.Reason != "second" || at != 4 {
		t.Fatalf("second occurrence = %+v at %d, want the fourth transition", second, at)
	}
	if mark := w.Mark(); mark != 4 {
		t.Fatalf("mark = %d, want 4", mark)
	}
}

func TestUntilListsTransitionsOnExpiry(t *testing.T) {
	source := newScriptedSource()
	w := New(t, source)
	source.send(t, w, transition("snapshot:a", "failed", "zfs snapshot: dataset is busy"))
	mark := w.Mark()
	source.send(t, w, transition("prune:a", "pruning", ""))

	message := failure(t, func(tb testing.TB) {
		w.Next(tb, "prune success", 50*time.Millisecond, mark, Job("prune:a", "succeeded"))
	})
	if !strings.Contains(message, "timed out after 50ms waiting for prune success") {
		t.Fatalf("failure did not name the wait: %q", message)
	}
	if !strings.Contains(message, "prune:a pool=management state=pruning") {
		t.Fatalf("failure did not list the transitions received: %q", message)
	}
	if strings.Contains(message, "snapshot:a") {
		t.Fatalf("failure listed transitions from before its cursor: %q", message)
	}
}

func TestUntilFailsWhenTheSubscriptionEnds(t *testing.T) {
	source := newScriptedSource()
	w := New(t, source)
	source.send(t, w, transition("remote:a:home", "probing", ""))
	overflow := errors.New("status subscriber fell behind")
	source.subscription.end(overflow)

	message := failure(t, func(tb testing.TB) {
		w.Next(tb, "remote success", time.Hour, 0, Job("remote:a:home", "succeeded"))
	})
	if !strings.Contains(message, "status subscription ended: status subscriber fell behind") || !strings.Contains(message, "state=probing") {
		t.Fatalf("failure = %q, want the subscription error and the transitions received", message)
	}
}

func TestUntilFailsAtOnceOnAConditionError(t *testing.T) {
	source := newScriptedSource()
	w := New(t, source)
	source.send(t, w, transition("local:a:b", "blocked", "destination is not writable"))

	message := failure(t, func(tb testing.TB) {
		w.Until(tb, "local success", time.Hour, 0, func(transitions []daemonstate.Event) (bool, error) {
			for _, event := range transitions {
				if event.State == "blocked" {
					return false, fmt.Errorf("%s ended in %s", event.Job, event.State)
				}
			}
			return false, nil
		})
	})
	if !strings.Contains(message, "local:a:b ended in blocked") || !strings.Contains(message, `reason="destination is not writable"`) {
		t.Fatalf("failure = %q, want the condition error and the transitions received", message)
	}
}

func TestUpdatesWithoutTransitionsDoNotAdvanceTheCursor(t *testing.T) {
	source := newScriptedSource()
	w := New(t, source)
	select {
	case source.subscription.updates <- daemonstate.Update{State: daemonstate.ControlSnapshot{Revision: 7}}:
	case <-time.After(5 * time.Second):
		t.Fatal("waiter did not take the initial state")
	}
	source.send(t, w, transition("snapshot:a", "succeeded", ""))
	if mark := w.Mark(); mark != 1 {
		t.Fatalf("mark = %d, want only the transition counted", mark)
	}
}

func TestOutcomeSkipsRunningStatesAndNamedRetries(t *testing.T) {
	source := newScriptedSource()
	w := New(t, source)
	source.send(t, w,
		transition("local:a:b", "pending-transfer", ""),
		transition("local:a:b", "planning", ""),
		transition("local:a:b", "waiting-retry", "waiting for destination ancestor b"),
		transition("local:c:b", "blocked", "another job's outcome"),
		transition("local:a:b", "sending", ""),
		transition("local:a:b", "verifying", ""),
		transition("local:a:b", "succeeded", ""),
	)
	event, at := w.Outcome(t, time.Second, 0, "local:a:b", "succeeded", "waiting-retry")
	if event.State != "succeeded" || at != 7 {
		t.Fatalf("outcome = %+v at %d, want the success at 7", event, at)
	}

	message := failure(t, func(tb testing.TB) {
		w.Outcome(tb, time.Hour, 0, "local:a:b", "succeeded")
	})
	if !strings.Contains(message, `local:a:b ended waiting-retry: waiting for destination ancestor b`) {
		t.Fatalf("failure = %q, want the unlisted retry to end the wait", message)
	}
}

func TestOutcomeTreatsAReasonedProbingAsAnOutcome(t *testing.T) {
	source := newScriptedSource()
	w := New(t, source)
	source.send(t, w, transition("remote:a:home", "probing", ""), transition("remote:a:home", "probing", "remote remained resumable after successful resume"))

	message := failure(t, func(tb testing.TB) {
		w.Outcome(tb, time.Hour, 0, "remote:a:home", "succeeded", "waiting-retry")
	})
	if !strings.Contains(message, "remote:a:home ended probing: remote remained resumable") {
		t.Fatalf("failure = %q, want the reasoned probing to end the wait", message)
	}
}

func TestSettleWaitsForEveryTransitionRecordedBeforeIt(t *testing.T) {
	source := newScriptedSource()
	w := New(t, source)
	source.send(t, w, transition("snapshot:a", "snapshotting", ""))

	// The owner has recorded two more transitions that the waiter has not yet
	// received, so settling now must wait for them rather than returning the
	// cursor the waiter holds.
	recorded := source.record(2)
	settled := make(chan Cursor, 1)
	message := failure(t, func(tb testing.TB) {
		go func() {
			<-time.After(20 * time.Millisecond) // a fixture: let Settle block first
			_ = source.deliver(daemonstate.Update{
				Transitions: []daemonstate.Event{transition("snapshot:a", "succeeded", ""), transition("prune:a", "pending-management", "")},
				State:       daemonstate.ControlSnapshot{Revision: recorded},
			})
		}()
		settled <- w.Settle(tb, 5*time.Second)
	})
	if message != "" {
		t.Fatal(message)
	}
	if at := <-settled; at != 3 {
		t.Fatalf("settled at %d, want past all three recorded transitions", at)
	}

	// A state-only update at the current revision settles without transitions.
	if err := source.deliver(daemonstate.Update{State: daemonstate.ControlSnapshot{Revision: recorded}}); err != nil {
		t.Fatal(err)
	}
	if at := w.Settle(t, 5*time.Second); at != 3 {
		t.Fatalf("settled at %d with nothing new recorded, want 3", at)
	}
}

func TestRunsGroupTransitionsAndClassifyTheirEffects(t *testing.T) {
	event := func(run uint64, job, state, reason string) daemonstate.Event {
		e := transition(job, state, reason)
		e.RunID = run
		return e
	}
	transitions := []daemonstate.Event{
		event(1, "prune:a", "pending-management", ""),
		event(2, "prune:a", "pending-management", ""),
		event(1, "prune:a", "pruning", ""),
		event(3, "snapshot:a", "snapshotting", ""),
		event(1, "prune:a", "succeeded", ""),
		event(4, "remote:a:home", "pending-transfer", ""),
		event(4, "remote:a:home", "probing", ""),
		event(5, "remote:a:home", "probing", ""),
		event(5, "remote:a:home", "probing", "remote remained resumable after successful resume"),
	}
	transitions = append(transitions,
		event(6, "prune:a", "pending-management", ""),
		event(6, "prune:a", "cancelled", "dataset deactivated"),
	)
	prunes := Runs(transitions, "prune:a")
	if len(prunes) != 3 || len(prunes[0]) != 3 || len(prunes[1]) != 1 {
		t.Fatalf("prune runs = %+v", prunes)
	}
	if got := prunes[2].Effects(); got != Absent {
		t.Fatalf("a run dropped from its queue = %v, want Absent", got)
	}
	if got := prunes[0].Effects(); got != Present {
		t.Fatalf("a run with an outcome = %v, want Present", got)
	}
	if outcome, ended := prunes[0].Outcome(); !ended || outcome.State != "succeeded" {
		t.Fatalf("outcome = %+v ended=%v", outcome, ended)
	}
	if got := prunes[1].Effects(); got != Absent || prunes[1].Started() {
		t.Fatalf("a run still pending = %v, want Absent", got)
	}
	remotes := Runs(transitions, "remote:a:home")
	if got := remotes[0].Effects(); got != Partial {
		t.Fatalf("a started run without an outcome = %v, want Partial", got)
	}
	if got := remotes[1].Effects(); got != Present {
		t.Fatalf("a run ending in a reasoned probing = %v, want Present", got)
	}
}

func TestChangedClosesOnTheNextDelivery(t *testing.T) {
	source := newScriptedSource()
	w := New(t, source)
	changed := w.Changed()
	select {
	case <-changed:
		t.Fatal("changed closed before any delivery")
	default:
	}
	source.send(t, w, transition("snapshot:a", "succeeded", ""))
	select {
	case <-changed:
	default:
		t.Fatal("changed did not close on a delivery")
	}
}
