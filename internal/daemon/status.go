package daemon

import (
	"context"
	"slices"
	"strings"
	"sync"
)

// StatusStore retains the latest stable transition for each daemon job.
type StatusStore struct {
	mu       sync.Mutex
	events   map[string]Event
	revision uint64
	changed  chan struct{}
}

// Record updates one job without exposing mutable internal state.
func (s *StatusStore) Record(event Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.events == nil {
		s.events = make(map[string]Event)
	}
	s.events[event.Job] = event
	s.revision++
	if s.changed != nil {
		close(s.changed)
	}
	s.changed = make(chan struct{})
}

// Snapshot returns deterministic latest-job status for the control API.
func (s *StatusStore) Snapshot() []Event {
	_, result := s.SnapshotRevision()
	return result
}

// SnapshotRevision returns one coherent revision and its detached events.
func (s *StatusStore) SnapshotRevision() (uint64, []Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]Event, 0, len(s.events))
	for _, event := range s.events {
		result = append(result, event)
	}
	slices.SortFunc(result, func(a, b Event) int { return strings.Compare(a.Job, b.Job) })
	return s.revision, result
}

// Wait blocks until a revision newer than after is available.
func (s *StatusStore) Wait(ctx context.Context, after uint64) error {
	for {
		s.mu.Lock()
		if s.revision > after {
			s.mu.Unlock()
			return nil
		}
		if s.changed == nil {
			s.changed = make(chan struct{})
		}
		changed := s.changed
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}
