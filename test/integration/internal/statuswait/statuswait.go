// Package statuswait waits on what a daemon reports rather than sampling its
// status.
//
// A Waiter waits on an in-process daemon's status subscription. It subscribes
// when it is created, so it observes every transition recorded from then on:
// create it before starting the work it observes. Transitions a subscription
// missed before it existed are gone, and a wait for one fails on its bound as
// though the daemon never did the work.
//
// A Log waits on a daemon subprocess's log, which carries every transition
// from process start, so it can be attached before the process starts and
// needs no subscription.
package statuswait

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/pdf/boomerangz/internal/daemonstate"
)

// Source is what a Waiter subscribes to; *daemon.Runtime satisfies it.
type Source interface {
	SubscribeStatus(context.Context) (daemonstate.Subscription, error)
	ControlStatus() daemonstate.ControlSnapshot
}

// Cursor is a position in a recorded sequence: the number of entries before
// it. A Waiter's sequence is the transitions it received; a Log's is the
// lines it decoded.
type Cursor int

// Waiter records every transition its subscription delivers, in order, and
// wakes waiters on each delivery.
type Waiter struct {
	source   Source
	seq      *sequence[daemonstate.Event]
	revision uint64 // of the newest state received; guarded by seq.mu
}

const subscriptionSource = "status subscription"

// New subscribes to source for the life of the test.
func New(t testing.TB, source Source) *Waiter {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	subscription, err := source.SubscribeStatus(ctx)
	if err != nil {
		cancel()
		t.Fatalf("subscribe to daemon status: %v", err)
	}
	w := &Waiter{source: source, seq: newSequence[daemonstate.Event]()}
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		w.drain(subscription)
	}()
	t.Cleanup(func() {
		cancel()
		<-drained
	})
	return w
}

// drain reads the subscription until it ends, so the waiter never falls
// behind the daemon between waits.
func (w *Waiter) drain(subscription daemonstate.Subscription) {
	for update := range subscription.Updates() {
		w.seq.update(func() {
			w.seq.items = append(w.seq.items, update.Transitions...)
			w.revision = max(w.revision, update.State.Revision)
		})
	}
	w.seq.update(func() { w.seq.ended, w.seq.err = true, subscription.Err() })
}

// Mark returns the position after every transition received so far. A
// transition recorded before Mark was called can still arrive after it; use
// Settle where that matters.
func (w *Waiter) Mark() Cursor { return w.seq.mark() }

// Settle returns a cursor past every transition the daemon recorded before
// Settle was called, so every transition after it was recorded later. A test
// reads the pool, then settles, to learn which work began after its read.
//
// It relies on the status owner answering a status request in order behind
// every transition sent before it, and on each update carrying the
// transitions its state includes. The forwarder breaks the second only while
// it holds its full transition bound undelivered, which the waiter's drain
// prevents.
func (w *Waiter) Settle(t testing.TB, bound time.Duration) Cursor {
	t.Helper()
	revision := w.source.ControlStatus().Revision
	var at Cursor
	w.seq.wait(t, fmt.Sprintf("status revision %d to be delivered", revision), bound, 0, subscriptionSource, func(transitions []daemonstate.Event) (bool, error) {
		at = Cursor(len(transitions))
		return w.revision >= revision, nil
	}, describeTransitions)
	return at
}

// Changed returns a channel closed on the next delivery or when the
// subscription ends, for a test waiting on the waiter and something else at
// once. Take it before reading what it guards, so a delivery in between is
// not missed.
func (w *Waiter) Changed() <-chan struct{} { return w.seq.next() }

// Transitions returns a copy of every transition received so far.
func (w *Waiter) Transitions() []daemonstate.Event { return w.seq.clone() }

// Condition inspects the transitions received from a wait's cursor. It
// reports whether the wait is satisfied, or an error that fails it at once.
type Condition func(transitions []daemonstate.Event) (bool, error)

// Until waits up to bound for condition to hold over the transitions received
// from from, rechecking on every delivery. It fails the test when condition
// returns an error, when the subscription ends, and when bound expires; each
// failure lists the transitions received from from.
func (w *Waiter) Until(t testing.TB, description string, bound time.Duration, from Cursor, condition Condition) {
	t.Helper()
	w.seq.wait(t, description, bound, from, subscriptionSource, condition, describeTransitions)
}

// Next waits up to bound for the first transition after from that match
// accepts, and returns it with the cursor just past it, from which a later
// wait finds the next occurrence.
func (w *Waiter) Next(t testing.TB, description string, bound time.Duration, from Cursor, match func(daemonstate.Event) bool) (daemonstate.Event, Cursor) {
	t.Helper()
	var (
		found daemonstate.Event
		at    Cursor
	)
	w.Until(t, description, bound, from, func(transitions []daemonstate.Event) (bool, error) {
		for index, event := range transitions {
			if match(event) {
				found, at = event, from+Cursor(index)+1
				return true, nil
			}
		}
		return false, nil
	})
	return found, at
}

// Outcome waits up to bound for job to report want after from, and returns
// that transition with the cursor just past it. want is usually an outcome,
// and may be a state the job reports while running. Other running states are
// skipped, as are the outcomes in retried, which the caller names because the
// daemon runs the job again after them. Any other outcome fails the wait at
// once: the job ended some way the behaviour does not allow.
func (w *Waiter) Outcome(t testing.TB, bound time.Duration, from Cursor, job, want string, retried ...string) (daemonstate.Event, Cursor) {
	t.Helper()
	var (
		found daemonstate.Event
		at    Cursor
	)
	w.Until(t, outcomeDescription(job, want, retried), bound, from, func(transitions []daemonstate.Event) (bool, error) {
		for index, event := range transitions {
			matched, err := outcomeOf(event, job, want, retried)
			if err != nil {
				return false, err
			}
			if matched {
				found, at = event, from+Cursor(index)+1
				return true, nil
			}
		}
		return false, nil
	})
	return found, at
}

// runningStates are reported between a job being queued and its outcome, and
// never as an outcome: the pool's pending state, every job's start state, and
// the transfer phases.
var runningStates = []string{"running", "snapshotting", "pruning", "reconciling", "retiring", "planning", "sending", "verifying"}

// Running reports whether a transition marks a job queued or running rather
// than ended. probing is a remote job's start state, and also the outcome of
// a run that failed before it reached another state, which carries the error
// as its reason.
func Running(event daemonstate.Event) bool {
	switch {
	case strings.HasPrefix(event.State, "pending-"):
		return true
	case event.State == "probing":
		return event.Reason == ""
	default:
		return slices.Contains(runningStates, event.State)
	}
}

// Run is one run of a job: every transition carrying its run ID, in order.
type Run []daemonstate.Event

// Runs groups job's transitions by run, in the order each run first appears.
func Runs(transitions []daemonstate.Event, job string) []Run {
	var runs []Run
	index := make(map[uint64]int)
	for _, event := range transitions {
		if event.Job != job {
			continue
		}
		at, found := index[event.RunID]
		if !found {
			at = len(runs)
			index[event.RunID] = at
			runs = append(runs, nil)
		}
		runs[at] = append(runs[at], event)
	}
	return runs
}

// Started reports whether the run had begun its work: a pool records a run's
// start state before running it, so a run with none has done nothing, even
// if it reports an outcome, as a job dropped from its queue does.
func (r Run) Started() bool {
	return slices.ContainsFunc(r, func(event daemonstate.Event) bool {
		return Running(event) && !strings.HasPrefix(event.State, "pending-")
	})
}

// Outcome returns the run's outcome, if it has reported one.
func (r Run) Outcome() (daemonstate.Event, bool) {
	if len(r) == 0 || Running(r[len(r)-1]) {
		return daemonstate.Event{}, false
	}
	return r[len(r)-1], true
}

// Effects classifies a run against a pool read taken just before a Settle
// that returned the cursor the transitions end at.
type Effects int

const (
	// Absent means the run had not started, so none of its effects are in the
	// read.
	Absent Effects = iota
	// Partial means the run had started without reporting an outcome, so any of
	// its effects may or may not be in the read.
	Partial
	// Present means the run had reported its outcome, so all of its effects are
	// in the read.
	Present
)

// Effects classifies r, holding the transitions up to a Settle's cursor. A
// run dropped from its queue before starting reports cancelled without a
// start state, and did nothing.
func (r Run) Effects() Effects {
	_, ended := r.Outcome()
	switch {
	case !r.Started():
		return Absent
	case ended:
		return Present
	default:
		return Partial
	}
}

// Job matches a transition of job into state.
func Job(job, state string) func(daemonstate.Event) bool {
	return func(event daemonstate.Event) bool { return event.Job == job && event.State == state }
}

// Count returns how many transitions in transitions match.
func Count(transitions []daemonstate.Event, match func(daemonstate.Event) bool) int {
	count := 0
	for _, event := range transitions {
		if match(event) {
			count++
		}
	}
	return count
}

func describeTransitions(transitions []daemonstate.Event, from Cursor) string {
	return describe(transitions[min(int(from), len(transitions)):])
}

func describe(transitions []daemonstate.Event) string {
	if len(transitions) == 0 {
		return "no transitions received"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%d transitions received:", len(transitions))
	for _, event := range transitions {
		b.WriteString("\n  ")
		describeEvent(&b, event)
	}
	return b.String()
}
