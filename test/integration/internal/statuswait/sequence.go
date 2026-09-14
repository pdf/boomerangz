package statuswait

import (
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pdf/boomerangz/internal/daemonstate"
)

// sequence is an ordered record that one writer appends to and waits recheck
// on every change: broadcast-and-recheck, since the condition is one the test
// process owns. A Waiter and a Log each keep one.
type sequence[T any] struct {
	mu      sync.Mutex
	items   []T
	ended   bool
	err     error
	changed chan struct{} // closed and replaced on every change
}

func newSequence[T any]() *sequence[T] {
	return &sequence[T]{changed: make(chan struct{})}
}

// update applies change with the lock held and wakes every wait.
func (s *sequence[T]) update(change func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	change()
	close(s.changed)
	s.changed = make(chan struct{})
}

func (s *sequence[T]) mark() Cursor {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Cursor(len(s.items))
}

func (s *sequence[T]) clone() []T {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.items)
}

func (s *sequence[T]) next() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.changed
}

// wait is the loop behind every wait. condition is called with the lock held
// and the items from from. It fails the test when condition returns an error,
// when the sequence ends, naming what ended as source, and when bound expires;
// each failure reports describe, also called with the lock held.
func (s *sequence[T]) wait(t testing.TB, description string, bound time.Duration, from Cursor, source string, condition func([]T) (bool, error), describe func(items []T, from Cursor) string) {
	t.Helper()
	timer := time.NewTimer(bound)
	defer timer.Stop()
	for {
		s.mu.Lock()
		done, err := condition(s.items[min(int(from), len(s.items)):])
		ended, endErr, changed := s.ended, s.err, s.changed
		var report string
		if err != nil || (!done && ended) {
			report = describe(s.items, from)
		}
		s.mu.Unlock()
		switch {
		case err != nil:
			t.Fatalf("waiting for %s: %v\n%s", description, err, report)
		case done:
			return
		case ended:
			t.Fatalf("waiting for %s: %s ended: %v\n%s", description, source, endErr, report)
		}
		select {
		case <-changed:
		case <-timer.C:
			s.mu.Lock()
			report = describe(s.items, from)
			s.mu.Unlock()
			t.Fatalf("timed out after %s waiting for %s\n%s", bound, description, report)
		}
	}
}

// outcomeOf applies Outcome's rule to one transition: it reports whether the
// transition is job reporting want, and fails on any outcome of job that is
// neither want nor in retried.
func outcomeOf(event daemonstate.Event, job, want string, retried []string) (bool, error) {
	switch {
	case event.Job != job:
	case event.State == want:
		return true, nil
	case Running(event) || slices.Contains(retried, event.State):
	default:
		return false, fmt.Errorf("%s ended %s: %s", job, event.State, event.Reason)
	}
	return false, nil
}

// outcomeDescription names an Outcome wait.
func outcomeDescription(job, want string, retried []string) string {
	description := job + " to report " + want
	if len(retried) > 0 {
		description += " after any of " + strings.Join(retried, ", ")
	}
	return description
}

// endedOf reports whether a transition is an outcome of job other than the
// ones in retried.
func endedOf(event daemonstate.Event, job string, retried []string) bool {
	return event.Job == job && !Running(event) && !slices.Contains(retried, event.State)
}

func describeEvent(b *strings.Builder, event daemonstate.Event) {
	fmt.Fprintf(b, "%s %s pool=%s state=%s", event.At.Format(time.RFC3339Nano), event.Job, event.Pool, event.State)
	if event.Target != "" {
		fmt.Fprintf(b, " target=%s", event.Target)
	}
	if event.Snapshot != "" {
		fmt.Fprintf(b, " snapshot=%s", event.Snapshot)
	}
	if event.Reason != "" {
		fmt.Fprintf(b, " reason=%q", event.Reason)
	}
}
