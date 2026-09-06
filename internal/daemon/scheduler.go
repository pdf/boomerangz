package daemon

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/pdf/boomerangz/internal/discovery"
	"github.com/pdf/boomerangz/internal/lifecycle"
	"github.com/pdf/boomerangz/internal/policy"
)

// Schedule is a detached due snapshot root and the generation policy it uses.
type Schedule struct {
	Dataset  string
	Policy   policy.Effective
	Deadline time.Time
	Force    bool
}

// Lookup returns the current policy for an active scheduled root.
func (s *Scheduler) Lookup(dataset string) (Schedule, bool) {
	item, exists := s.entry(dataset)
	return Schedule{Dataset: dataset, Policy: item.policy.Clone(), Deadline: item.next}, exists
}

type deadline struct {
	policy  policy.Effective
	cadence time.Duration
	next    time.Time
	pending bool
}

// Scheduler maintains per-root snapshot deadlines independently of discovery.
type Scheduler struct {
	mu      sync.Mutex
	entries map[string]deadline
	wake    chan struct{}
}

// NewScheduler creates an empty deadline set.
func NewScheduler() *Scheduler {
	return &Scheduler{entries: make(map[string]deadline), wake: make(chan struct{})}
}

func (s *Scheduler) signal() {
	close(s.wake)
	s.wake = make(chan struct{})
}

// Update atomically replaces schedulable roots from a complete generation.
// Existing deadlines survive policy generations when cadence is unchanged.
func (s *Scheduler) Update(entries []discovery.Entry, now time.Time) (active, removed []string, err error) {
	if now.IsZero() {
		return nil, nil, fmt.Errorf("scheduler update time is required")
	}
	next := make(map[string]deadline)
	for _, entry := range entries {
		if entry.CoveredBy != "" || !entry.Inspected || !entry.Policy.Enabled || !entry.Policy.Valid() {
			continue
		}
		if lifecycle.ActiveRoot(entry.Policy, entry.Dataset.Name) != nil {
			continue
		}
		cadence := entry.Policy.Grid.Cadence()
		if cadence <= 0 {
			continue
		}
		item := deadline{policy: entry.Policy.Clone(), cadence: cadence, next: now.UTC()}
		if previous, exists := s.entry(entry.Dataset.Name); exists && previous.cadence == cadence {
			item.next, item.pending = previous.next, previous.pending
		}
		next[entry.Dataset.Name] = item
		active = append(active, entry.Dataset.Name)
	}
	s.mu.Lock()
	for name := range s.entries {
		if _, exists := next[name]; !exists {
			removed = append(removed, name)
		}
	}
	s.entries = next
	s.signal()
	s.mu.Unlock()
	slices.Sort(active)
	slices.Sort(removed)
	return active, removed, nil
}

func (s *Scheduler) entry(name string) (deadline, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	item, exists := s.entries[name]
	return item, exists
}

// Complete advances a root from actual completion time. Missed periods are
// coalesced and are never replayed as backfill snapshots.
func (s *Scheduler) Complete(dataset string, completed time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	item, exists := s.entries[dataset]
	if !exists {
		return
	}
	item.pending = false
	item.next = completed.UTC().Add(item.cadence)
	s.entries[dataset] = item
	s.signal()
}

// Retry makes a failed or dropped root eligible on the next scheduler pass.
func (s *Scheduler) Retry(dataset string, notBefore time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	item, exists := s.entries[dataset]
	if !exists {
		return
	}
	item.pending = false
	item.next = notBefore.UTC()
	s.entries[dataset] = item
	s.signal()
}

// Next waits for the earliest deadline and marks it pending until Complete or Retry.
func (s *Scheduler) Next(ctx context.Context) (Schedule, bool) {
	for {
		s.mu.Lock()
		var selected string
		var earliest time.Time
		for name, item := range s.entries {
			if item.pending || (!earliest.IsZero() && !item.next.Before(earliest)) {
				continue
			}
			selected, earliest = name, item.next
		}
		wake := s.wake
		if selected == "" {
			s.mu.Unlock()
			select {
			case <-ctx.Done():
				return Schedule{}, false
			case <-wake:
			}
			continue
		}
		wait := time.Until(earliest)
		if wait <= 0 {
			item := s.entries[selected]
			item.pending = true
			s.entries[selected] = item
			s.mu.Unlock()
			return Schedule{Dataset: selected, Policy: item.policy.Clone(), Deadline: earliest}, true
		}
		s.mu.Unlock()
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return Schedule{}, false
		case <-wake:
			if !timer.Stop() {
				<-timer.C
			}
		case <-timer.C:
		}
	}
}

// Entries returns current deadlines for status and tests.
func (s *Scheduler) Entries() map[string]time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make(map[string]time.Time, len(s.entries))
	for name, item := range s.entries {
		result[name] = item.next
	}
	return maps.Clone(result)
}
