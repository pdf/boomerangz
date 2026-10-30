package daemon

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/pdf/boomerangz/internal/discovery"
	"github.com/pdf/boomerangz/internal/lifecycle"
	"github.com/pdf/boomerangz/internal/policy"
)

// ScheduleMember is one member of a root, with the grid it is pruned against.
type ScheduleMember struct {
	Dataset string
	Grid    policy.Grid
}

// Retention pairs the member with its grid for lifecycle pruning.
func (m ScheduleMember) Retention() lifecycle.Retention {
	return lifecycle.Retention{Dataset: m.Dataset, Grid: m.Grid}
}

// Schedule is a detached due snapshot root. Members lists every member the root
// snapshots, root first; Due names the members whose own deadline has passed, or
// every member when the schedule is forced.
type Schedule struct {
	Dataset  string
	Policy   policy.Effective
	Members  []ScheduleMember
	Due      []string
	Deadline time.Time
	Force    bool
}

// Lookup returns the current policy for an active scheduled root, with every
// member due: it serves forced snapshots.
func (s *Scheduler) Lookup(dataset string) (Schedule, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	item, exists := s.entries[dataset]
	if !exists {
		return Schedule{}, false
	}
	schedule := item.schedule(dataset, time.Time{})
	schedule.Due = item.all()
	return schedule, true
}

// memberDeadline is one member's own cadence within a root. Members keep
// separate cadences because collapsing grids across members multiplies snapshot
// and transfer counts: a root ticking at its finest member's cadence would give
// a 14x1d member a snapshot and a send every five minutes.
type memberDeadline struct {
	grid    policy.Grid
	cadence time.Duration
	next    time.Time
}

// deadline is one root. Under replicate=on the one -R snapshot covers the whole
// subtree on the root's grid, so the root is its only timed member and every
// member is due whenever it fires.
type deadline struct {
	policy    policy.Effective
	members   map[string]memberDeadline
	order     []string
	replicate []ScheduleMember
	pending   bool
}

func (d deadline) earliest() time.Time {
	var earliest time.Time
	for _, member := range d.members {
		if earliest.IsZero() || member.next.Before(earliest) {
			earliest = member.next
		}
	}
	return earliest
}

func (d deadline) all() []string {
	if d.replicate != nil {
		names := make([]string, 0, len(d.replicate))
		for _, member := range d.replicate {
			names = append(names, member.Dataset)
		}
		return names
	}
	return slices.Clone(d.order)
}

// schedule describes the root with the members due by now. A zero now selects
// none, for callers that set Due themselves.
func (d deadline) schedule(dataset string, now time.Time) Schedule {
	schedule := Schedule{Dataset: dataset, Policy: d.policy.Clone(), Deadline: d.earliest()}
	if d.replicate != nil {
		schedule.Members = slices.Clone(d.replicate)
		if !now.IsZero() && !now.Before(schedule.Deadline) {
			schedule.Due = d.all()
		}
		return schedule
	}
	for _, name := range d.order {
		member := d.members[name]
		schedule.Members = append(schedule.Members, ScheduleMember{Dataset: name, Grid: member.grid})
		if !now.IsZero() && !now.Before(member.next) {
			schedule.Due = append(schedule.Due, name)
		}
	}
	return schedule
}

// Scheduler maintains per-root snapshot deadlines independently of discovery.
type Scheduler struct {
	mu      sync.Mutex
	entries map[string]deadline
	wake    chan struct{}
	status  *Status
}

// NewScheduler creates an empty deadline set.
func NewScheduler() *Scheduler {
	return &Scheduler{entries: make(map[string]deadline), wake: make(chan struct{})}
}

// signal wakes Next and publishes the changed deadlines. Callers hold the lock,
// which orders the published views.
func (s *Scheduler) signal() {
	close(s.wake)
	s.wake = make(chan struct{})
	s.status.publishDeadlines(s.entriesLocked())
}

func isSchedulable(entry discovery.Entry) bool {
	if entry.CoveredBy != "" || !entry.Inspected || !entry.Policy.Enabled || !entry.Policy.Valid() {
		return false
	}
	return lifecycle.ActiveRoot(entry.Policy, entry.Dataset.Name) == nil
}

// Update atomically replaces schedulable roots from a complete generation, whose
// entries must include each root's members. A member's deadline survives policy
// generations while its cadence is unchanged.
func (s *Scheduler) Update(entries []discovery.Entry, now time.Time) (active, removed []string, err error) {
	if now.IsZero() {
		return nil, nil, fmt.Errorf("scheduler update time is required")
	}
	byName := make(map[string]discovery.Entry, len(entries))
	for _, entry := range entries {
		byName[entry.Dataset.Name] = entry
	}
	next := make(map[string]deadline)
	for _, entry := range entries {
		if !isSchedulable(entry) {
			continue
		}
		root := entry.Dataset.Name
		previous, existed := s.entry(root)
		item := deadline{policy: entry.Policy.Clone(), members: make(map[string]memberDeadline)}
		timed := entry.Members
		if entry.Policy.Send.Replicate {
			for _, name := range entry.Members {
				item.replicate = append(item.replicate, ScheduleMember{Dataset: name, Grid: entry.Policy.Grid})
			}
			timed = []string{root}
		}
		for _, name := range timed {
			member, found := byName[name]
			if !found {
				continue
			}
			cadence := member.Policy.Grid.Cadence()
			if cadence <= 0 {
				continue
			}
			state := memberDeadline{grid: member.Policy.Grid, cadence: cadence, next: now.UTC()}
			if old, kept := previous.members[name]; existed && kept && old.cadence == cadence {
				state.next = old.next
			}
			item.members[name] = state
			item.order = append(item.order, name)
		}
		if len(item.order) == 0 {
			continue
		}
		item.pending = existed && previous.pending
		next[root] = item
		active = append(active, root)
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

// Complete settles a fired root: each due member advances from the actual
// completion time, so missed periods are coalesced and never replayed as
// backfill, and each deferred member moves to the deadline its newest existing
// snapshot already sets.
func (s *Scheduler) Complete(dataset string, due []string, completed time.Time, deferred map[string]time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	item, exists := s.entries[dataset]
	if !exists {
		return
	}
	item.pending = false
	if item.replicate != nil && len(due) > 0 {
		// The -R root is the only timed member and advances for all of them.
		due = []string{dataset}
	}
	for _, name := range due {
		if member, found := item.members[name]; found {
			member.next = completed.UTC().Add(member.cadence)
			item.members[name] = member
		}
	}
	for name, at := range deferred {
		if member, found := item.members[name]; found {
			member.next = at.UTC()
			item.members[name] = member
		}
	}
	s.entries[dataset] = item
	s.signal()
}

// Retry makes a failed or dropped root eligible again no earlier than notBefore.
// Members already due move to notBefore; members due later keep their deadline.
func (s *Scheduler) Retry(dataset string, notBefore time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	item, exists := s.entries[dataset]
	if !exists {
		return
	}
	item.pending = false
	for name, member := range item.members {
		if member.next.Before(notBefore) {
			member.next = notBefore.UTC()
			item.members[name] = member
		}
	}
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
			next := item.earliest()
			if item.pending || (!earliest.IsZero() && !next.Before(earliest)) {
				continue
			}
			selected, earliest = name, next
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
			// A late fire is due for every member whose deadline has passed
			// by now, not only the one that set the earliest deadline.
			fired := time.Now()
			if fired.Before(earliest) {
				fired = earliest
			}
			return item.schedule(selected, fired), true
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
	return s.entriesLocked()
}

func (s *Scheduler) entriesLocked() map[string]time.Time {
	result := make(map[string]time.Time, len(s.entries))
	for name, item := range s.entries {
		result[name] = item.earliest()
	}
	return result
}
