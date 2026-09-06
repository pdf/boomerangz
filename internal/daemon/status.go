package daemon

import (
	"slices"
	"strings"
	"sync"
)

// StatusStore retains the latest stable transition for each daemon job.
type StatusStore struct {
	mu     sync.Mutex
	events map[string]Event
}

// Record updates one job without exposing mutable internal state.
func (s *StatusStore) Record(event Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.events == nil {
		s.events = make(map[string]Event)
	}
	s.events[event.Job] = event
}

// Snapshot returns deterministic latest-job status for the future control API.
func (s *StatusStore) Snapshot() []Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]Event, 0, len(s.events))
	for _, event := range s.events {
		result = append(result, event)
	}
	slices.SortFunc(result, func(a, b Event) int { return strings.Compare(a.Job, b.Job) })
	return result
}
