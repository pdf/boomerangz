// Package discovery builds and atomically publishes immutable policy inventories.
package discovery

import (
	"context"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pdf/boomerangz/internal/policy"
	"github.com/pdf/boomerangz/internal/zfs"
)

// Reader is the read-only subset of the ZFS executor required for discovery.
type Reader interface {
	ListDatasets(context.Context) ([]zfs.Dataset, error)
	GetActivationProperties(context.Context) ([]zfs.Property, error)
	GetStoredProperties(context.Context, []string) ([]zfs.Property, error)
}

// LifecycleReader is implemented by readers that can sparsely expose durable
// source-root authority. Keeping it optional preserves compatibility with
// read-only remote endpoints and focused discovery test readers.
type LifecycleReader interface {
	GetLifecycleProperties(context.Context) ([]zfs.Property, error)
}

// Entry is a detached dataset inspection. Uninspected policies lack local
// overrides other than activation; require Inspected before using other fields.
type Entry struct {
	Dataset   zfs.Dataset      `json:"dataset"`
	Parent    string           `json:"parent,omitempty"`
	Inspected bool             `json:"inspected"`
	CoveredBy string           `json:"covered_by,omitempty"`
	Policy    policy.Effective `json:"policy"`
	Stored    []zfs.Property   `json:"stored,omitempty"`
}

func (e Entry) clone() Entry {
	e.Policy = e.Policy.Clone()
	e.Stored = slices.Clone(e.Stored)
	return e
}

// Generation is immutable. All exported inspection methods return detached copies.
type Generation struct {
	id      uint64
	created time.Time
	names   []string
	entries map[string]Entry
}

// ID increases only after a complete scan is published.
func (g *Generation) ID() uint64 { return g.id }

// Created returns the completion time of the scan.
func (g *Generation) Created() time.Time { return g.created }

// Entries returns the parent-before-child inventory.
func (g *Generation) Entries() []Entry {
	entries := make([]Entry, 0, len(g.names))
	for _, name := range g.names {
		entries = append(entries, g.entries[name].clone())
	}
	return entries
}

// Inspect returns one detached dataset record.
func (g *Generation) Inspect(name string) (Entry, bool) {
	entry, exists := g.entries[name]
	return entry.clone(), exists
}

// Changed returns datasets whose inspection changed, including removed datasets.
func (g *Generation) Changed(previous *Generation) []string {
	changed := make(map[string]bool)
	for name, entry := range g.entries {
		if previous == nil || !reflect.DeepEqual(entry, previous.entries[name]) {
			changed[name] = true
		}
	}
	if previous != nil {
		for name := range previous.entries {
			if _, exists := g.entries[name]; !exists {
				changed[name] = true
			}
		}
	}
	return slices.Sorted(maps.Keys(changed))
}

// Options bounds property arguments and defines known remote names.
type Options struct {
	BatchSize        int
	MaxArgumentBytes int
	Remotes          []string
}

// Scanner serializes discovery and exposes the last complete generation to readers.
type Scanner struct {
	reader   Reader
	options  Options
	remotes  map[string]struct{}
	interval time.Duration
	mu       sync.Mutex
	current  atomic.Pointer[Generation]
	requests chan struct{}
	changed  chan struct{}
}

// New creates a scanner with bounded defaults. Reader must not be nil.
func New(reader Reader, options Options) (*Scanner, error) {
	if reader == nil {
		return nil, fmt.Errorf("discovery reader is required")
	}
	if options.BatchSize == 0 {
		options.BatchSize = 128
	}
	if options.MaxArgumentBytes == 0 {
		options.MaxArgumentBytes = 32 * 1024
	}
	if options.BatchSize < 1 || options.MaxArgumentBytes < 1 {
		return nil, fmt.Errorf("discovery batch limits must be positive")
	}
	s := &Scanner{reader: reader, options: options, remotes: make(map[string]struct{}), requests: make(chan struct{}, 1), changed: make(chan struct{}, 1)}
	for _, remote := range options.Remotes {
		s.remotes[remote] = struct{}{}
	}
	return s, nil
}

// Reconfigure atomically updates the periodic interval and known remote names.
// It wakes Run so the new configuration is reflected without waiting for the
// previous interval to expire.
func (s *Scanner) Reconfigure(interval time.Duration, remotes []string) error {
	if interval <= 0 {
		return fmt.Errorf("reconciliation interval must be positive")
	}
	next := make(map[string]struct{}, len(remotes))
	for _, remote := range remotes {
		next[remote] = struct{}{}
	}
	s.mu.Lock()
	s.interval = interval
	s.remotes = next
	s.mu.Unlock()
	select {
	case s.changed <- struct{}{}:
	default:
	}
	return nil
}

// Current returns the last complete generation, or nil before the first success.
func (s *Scanner) Current() *Generation { return s.current.Load() }

// Request coalesces reconciliation hints into a single pending scan.
func (s *Scanner) Request() {
	select {
	case s.requests <- struct{}{}:
	default:
	}
}

// Run performs startup, periodic, and requested scans until cancellation. retained
// supplies datasets involved in pending or resumable work. report runs serially.
func (s *Scanner) Run(ctx context.Context, interval time.Duration, retained func() []string, report func(*Generation, error)) error {
	if interval <= 0 {
		return fmt.Errorf("reconciliation interval must be positive")
	}
	s.mu.Lock()
	s.interval = interval
	s.mu.Unlock()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		var names []string
		if retained != nil {
			names = retained()
		}
		generation, err := s.Scan(ctx, names)
		if report != nil {
			report(generation, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		case <-s.requests:
		case <-s.changed:
			s.mu.Lock()
			interval = s.interval
			s.mu.Unlock()
			ticker.Reset(interval)
		}
	}
}

// Scan builds a complete generation. retained also permits explicit inspection of
// inactive datasets. Failure leaves Current unchanged and returns no generation.
func (s *Scanner) Scan(ctx context.Context, retained []string) (*Generation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	datasets, err := s.reader.ListDatasets(ctx)
	if err != nil {
		return nil, err
	}
	names, inventory, err := indexDatasets(datasets)
	if err != nil {
		return nil, err
	}
	activation, err := s.reader.GetActivationProperties(ctx)
	if err != nil {
		return nil, err
	}
	if err := validateRows(activation, inventory, nil, true); err != nil {
		return nil, err
	}
	stored := group(activation)
	entries := resolve(names, inventory, stored, nil, s.remotes)
	inspect := make(map[string]bool)
	if lifecycleReader, ok := s.reader.(LifecycleReader); ok {
		rows, lifecycleErr := lifecycleReader.GetLifecycleProperties(ctx)
		if lifecycleErr != nil {
			return nil, lifecycleErr
		}
		if lifecycleErr = validateRows(rows, inventory, nil, false); lifecycleErr != nil {
			return nil, lifecycleErr
		}
		for _, row := range rows {
			if row.Source == zfs.SourceLocal {
				addAncestors(inspect, row.Dataset)
			}
			stored[row.Dataset] = append(stored[row.Dataset], row)
		}
		entries = resolve(names, inventory, stored, nil, s.remotes)
	}
	for _, name := range names {
		entry := entries[name]
		// Invalid activation must be inspectable, even though it cannot enable work.
		if entry.Policy.Enabled || !entry.Policy.Valid() {
			addAncestors(inspect, name)
		}
	}
	for _, name := range retained {
		if _, exists := inventory[name]; !exists {
			return nil, fmt.Errorf("inspection dataset %q is absent from inventory", name)
		}
		addAncestors(inspect, name)
	}
	fetched := make(map[string]bool)
	for {
		var pending []string
		for _, name := range names {
			if inspect[name] && !fetched[name] {
				pending = append(pending, name)
			}
		}
		if len(pending) == 0 {
			break
		}
		if err := s.fetch(ctx, pending, inventory, stored, fetched, activation); err != nil {
			return nil, err
		}
		entries = resolve(names, inventory, stored, fetched, s.remotes)
		covered := make(map[string]bool)
		for _, name := range names {
			entry := entries[name]
			if covered[entry.Parent] {
				inspect[name] = true
				covered[name] = true
			}
			if entry.Policy.Enabled && entry.Policy.Valid() && entry.Policy.Send.Replicate && fetched[name] {
				covered[name] = true
			}
		}
	}
	markReplication(names, entries)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	id := uint64(1)
	if previous := s.Current(); previous != nil {
		id = previous.id + 1
	}
	generation := &Generation{id: id, created: time.Now(), names: names, entries: entries}
	s.current.Store(generation)
	return generation, nil
}

func (s *Scanner) fetch(ctx context.Context, pending []string, inventory map[string]zfs.Dataset, stored map[string][]zfs.Property, fetched map[string]bool, activation []zfs.Property) error {
	for len(pending) > 0 {
		count, bytes := 0, 0
		for count < len(pending) && count < s.options.BatchSize && bytes+len(pending[count])+1 <= s.options.MaxArgumentBytes {
			bytes += len(pending[count]) + 1
			count++
		}
		if count == 0 {
			return fmt.Errorf("dataset %q exceeds argument budget", pending[0])
		}
		batch := pending[:count]
		rows, err := s.reader.GetStoredProperties(ctx, batch)
		if err != nil {
			return err
		}
		allowed := make(map[string]bool)
		for _, name := range batch {
			allowed[name] = true
		}
		if err := validateRows(rows, inventory, allowed, false); err != nil {
			return err
		}
		// Detect activation edits or vanished datasets between the sparse and
		// detailed reads. OpenZFS cannot provide an atomic multi-command snapshot.
		var before, after []zfs.Property
		for _, row := range activation {
			if allowed[row.Dataset] {
				before = append(before, row)
			}
		}
		for _, row := range rows {
			if row.Name == policy.Namespace+"enabled" {
				after = append(after, row)
			}
		}
		sortRows(before)
		sortRows(after)
		if !slices.Equal(before, after) {
			return fmt.Errorf("activation changed during discovery; retry scan")
		}
		grouped := group(rows)
		for _, name := range batch {
			stored[name] = grouped[name]
			fetched[name] = true
		}
		pending = pending[count:]
	}
	return nil
}

func indexDatasets(datasets []zfs.Dataset) ([]string, map[string]zfs.Dataset, error) {
	inventory := make(map[string]zfs.Dataset)
	for _, dataset := range datasets {
		if err := zfs.ValidateDataset(dataset.Name); err != nil {
			return nil, nil, err
		}
		if dataset.Type != zfs.Filesystem && dataset.Type != zfs.Volume {
			return nil, nil, fmt.Errorf("invalid dataset type %q", dataset.Type)
		}
		if _, exists := inventory[dataset.Name]; exists {
			return nil, nil, fmt.Errorf("duplicate dataset %q", dataset.Name)
		}
		inventory[dataset.Name] = dataset
	}
	for name := range inventory {
		if parent := parentName(name); parent != "" {
			if _, exists := inventory[parent]; !exists {
				return nil, nil, fmt.Errorf("missing parent %q", parent)
			}
		}
	}
	return slices.Sorted(maps.Keys(inventory)), inventory, nil
}

func validateRows(rows []zfs.Property, inventory map[string]zfs.Dataset, allowed map[string]bool, activation bool) error {
	seen := make(map[string]bool)
	for _, row := range rows {
		if _, exists := inventory[row.Dataset]; !exists {
			return fmt.Errorf("property references unknown dataset %q", row.Dataset)
		}
		if allowed != nil && !allowed[row.Dataset] {
			return fmt.Errorf("property outside requested batch: %q", row.Dataset)
		}
		if !strings.HasPrefix(row.Name, policy.Namespace) || (activation && row.Name != policy.Namespace+"enabled") {
			return fmt.Errorf("unexpected property %q", row.Name)
		}
		if row.Source != zfs.SourceLocal && row.Source != zfs.SourceReceived {
			return fmt.Errorf("unexpected property source %q", row.Source)
		}
		key := row.Dataset + "\x00" + row.Name + "\x00" + string(row.Source)
		if seen[key] {
			return fmt.Errorf("duplicate stored property %q on %q", row.Name, row.Dataset)
		}
		seen[key] = true
	}
	return nil
}

func sortRows(rows []zfs.Property) {
	slices.SortFunc(rows, func(a, b zfs.Property) int {
		return strings.Compare(a.Dataset+"\x00"+a.Name+"\x00"+string(a.Source)+"\x00"+a.Value, b.Dataset+"\x00"+b.Name+"\x00"+string(b.Source)+"\x00"+b.Value)
	})
}

func group(rows []zfs.Property) map[string][]zfs.Property {
	grouped := make(map[string][]zfs.Property)
	for _, row := range rows {
		grouped[row.Dataset] = append(grouped[row.Dataset], row)
	}
	for _, rows := range grouped {
		sortRows(rows)
	}
	return grouped
}

func parentName(name string) string {
	if index := strings.LastIndexByte(name, '/'); index >= 0 {
		return name[:index]
	}
	return ""
}

func addAncestors(set map[string]bool, name string) {
	for name != "" {
		set[name] = true
		name = parentName(name)
	}
}

func resolve(names []string, inventory map[string]zfs.Dataset, stored map[string][]zfs.Property, fetched map[string]bool, remotes map[string]struct{}) map[string]Entry {
	entries := make(map[string]Entry)
	for _, name := range names {
		parent := parentName(name)
		var inherited *policy.Effective
		if parent != "" {
			entry := entries[parent]
			inherited = &entry.Policy
		}
		entries[name] = Entry{Dataset: inventory[name], Parent: parent, Inspected: fetched[name], Stored: slices.Clone(stored[name]), Policy: policy.Resolve(inventory[name], inherited, stored[name], remotes)}
	}
	return entries
}

func markReplication(names []string, entries map[string]Entry) {
	for _, name := range names {
		entry := entries[name]
		parent := entries[entry.Parent]
		root := parent.CoveredBy
		if root == "" && parent.Inspected && parent.Policy.Enabled && parent.Policy.Valid() && parent.Policy.Send.Replicate {
			root = entry.Parent
		}
		if root != "" {
			entry.CoveredBy = root
			rootPolicy := entries[root].Policy
			keys := make(map[string]bool)
			for key := range entry.Policy.Values {
				keys[key] = true
			}
			for key := range rootPolicy.Values {
				keys[key] = true
			}
			for _, key := range slices.Sorted(maps.Keys(keys)) {
				if entry.Policy.Values[key].Value != rootPolicy.Values[key].Value {
					entry.Policy.Warnings = append(entry.Policy.Warnings, "replication root "+root+" governs differing "+strings.TrimPrefix(key, policy.Namespace))
				}
			}
			entries[name] = entry
		}
	}
	// A plaintext replication root may contain encrypted descendants. Validate
	// the entire covered scope after identifying roots so invalid roots do not
	// accidentally turn descendants into independent jobs.
	checked := make(map[string]bool)
	for _, name := range names {
		entry := entries[name]
		if entry.CoveredBy != "" && !checked[entry.CoveredBy] && entry.Dataset.EncryptionRoot != "" && entry.Dataset.EncryptionRoot != "-" {
			root := entries[entry.CoveredBy]
			root.Policy = root.Policy.ForReplicationScope(root.Dataset.EncryptionRoot != "" && root.Dataset.EncryptionRoot != "-", name)
			entries[entry.CoveredBy] = root
			checked[entry.CoveredBy] = true
		}
	}
}
