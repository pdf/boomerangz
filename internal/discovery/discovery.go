// Package discovery builds and atomically publishes immutable policy inventories.
package discovery

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/url"
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
//
// A dataset is an actor root when it is enabled and no ancestor is. Every other
// dataset in a root's subtree names that root in CoveredBy, whether it is a
// member or excluded, so nothing inside a root ever acts independently. A
// root's Members lists the datasets it snapshots and sends, root first and
// parents before children.
type Entry struct {
	Dataset   zfs.Dataset      `json:"dataset"`
	Parent    string           `json:"parent,omitempty"`
	Inspected bool             `json:"inspected"`
	CoveredBy string           `json:"covered_by,omitempty"`
	Member    bool             `json:"member,omitempty"`
	Members   []string         `json:"members,omitempty"`
	Policy    policy.Effective `json:"policy"`
	Stored    []zfs.Property   `json:"stored,omitempty"`
	// Blocked lists the targets an actor root may not send to, because its
	// replica there would overlap another replica or an actor root. Only
	// those targets stop: snapshots, pruning and every other target go on.
	Blocked []BlockedTarget `json:"blocked,omitempty"`
	// Replica is set on a dataset that is, or lies within, a replica
	// boomerangz receives into, rather than a source. It describes where the
	// dataset came from, not what it does: one that is also an actor root or
	// member is still that.
	Replica Replica `json:"replica,omitzero"`
}

// Replica says what marks a dataset as a replica. Any of its fields may be
// empty, but not all of them.
type Replica struct {
	// Source is the actor root on this host whose local destination places
	// its replica here.
	Source string `json:"source,omitempty"`
	// Destination is the local destination root or configured replication
	// root the replica lies beneath.
	Destination string `json:"destination,omitempty"`
	// Lineage is the source root's lineage, received with a stream that
	// carried properties.
	Lineage string `json:"lineage,omitempty"`
}

// BlockedTarget is one target an actor root may not send to, and why.
// Target is a local destination root or, with Remote, a remote's name.
type BlockedTarget struct {
	Target string `json:"target"`
	Remote bool   `json:"remote,omitempty"`
	Reason string `json:"reason"`
}

func (e Entry) clone() Entry {
	e.Policy = e.Policy.Clone()
	e.Stored = slices.Clone(e.Stored)
	e.Members = slices.Clone(e.Members)
	e.Blocked = slices.Clone(e.Blocked)
	return e
}

// ErrBlocked reports why the root may not send to target, a local destination
// root or, with remote, a remote's name; nil when it may.
func (e Entry) ErrBlocked(target string, remote bool) error {
	for _, blocked := range e.Blocked {
		if blocked.Target == target && blocked.Remote == remote {
			return fmt.Errorf("%s may not send to %s: %s", e.Dataset.Name, target, blocked.Reason)
		}
	}
	return nil
}

// ErrCovered reports that a dataset sits inside another actor root's subtree
// and so cannot be operated on as a root of its own.
func (e Entry) ErrCovered() error {
	if e.CoveredBy == "" {
		return nil
	}
	if e.Member {
		return fmt.Errorf("%s is a member of actor root %s; operate on the root", e.Dataset.Name, e.CoveredBy)
	}
	return fmt.Errorf("%s is excluded from actor root %s and cannot act independently", e.Dataset.Name, e.CoveredBy)
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
	// Destinations says where each configured remote receives. A remote
	// missing from it is still a valid name, but its replicas cannot be
	// checked for overlap with others.
	Destinations map[string]RemoteDestination
	// ReplicationRoots are the datasets this host receives remote sources'
	// replicas beneath. They only mark replicas, so a scanner that has no use
	// for Entry.Replica may leave them unset.
	ReplicationRoots []string
}

// RemoteDestination is where a configured remote receives: the namespace of
// datasets it writes into - one host's pools, named by transport and address -
// and its configured root there. Remotes that share a namespace write into one
// dataset tree, so their replicas can overlap.
type RemoteDestination struct {
	Namespace string
	Root      string
}

// RemoteDestinationOf derives a remote's destination from its canonical
// target: a URL whose path is the configured root. The user it connects as
// does not change which datasets it reaches, so it is not part of the
// namespace.
func RemoteDestinationOf(canonical string) (RemoteDestination, error) {
	parsed, err := url.Parse(canonical)
	if err != nil {
		return RemoteDestination{}, err
	}
	root := strings.TrimPrefix(parsed.Path, "/")
	if parsed.Scheme == "" || parsed.Host == "" || zfs.ValidateDataset(root) != nil {
		return RemoteDestination{}, fmt.Errorf("canonical target %q does not name a transport, address and root", canonical)
	}
	return RemoteDestination{Namespace: parsed.Scheme + "://" + strings.ToLower(parsed.Host), Root: root}, nil
}

// Scanner serializes discovery and exposes the last complete generation to readers.
type Scanner struct {
	reader       Reader
	options      Options
	remotes      map[string]struct{}
	destinations map[string]RemoteDestination
	interval     time.Duration
	mu           sync.Mutex
	current      atomic.Pointer[Generation]
	requests     chan struct{}
	changed      chan struct{}
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
	s.destinations = maps.Clone(options.Destinations)
	return s, nil
}

// Reconfigure atomically updates the periodic interval, the known remote names
// and where they receive. It wakes Run so the new configuration is reflected
// without waiting for the previous interval to expire.
func (s *Scanner) Reconfigure(interval time.Duration, remotes []string, destinations map[string]RemoteDestination) error {
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
	s.destinations = maps.Clone(destinations)
	s.mu.Unlock()
	select {
	case s.changed <- struct{}{}:
	default:
	}
	return nil
}

// SetDestinations replaces where each remote receives and requests a scan, so
// replicas are revalidated at once; the interval and remote set are unchanged.
func (s *Scanner) SetDestinations(destinations map[string]RemoteDestination) {
	s.mu.Lock()
	s.destinations = maps.Clone(destinations)
	s.mu.Unlock()
	s.Request()
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
			// A root owns its whole subtree, valid or not, so all of it is
			// inspected: members need their own grids and send options, and
			// excluded datasets keep pruning against their own grids.
			if entry.Policy.Enabled {
				covered[name] = true
			}
		}
	}
	markRoots(names, entries)
	replicas, roots := plannedReplicas(names, entries, s.destinations)
	markCollisions(entries, replicas, roots)
	markReplicas(names, entries, replicas, s.options.ReplicationRoots)
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

// markRoots assigns every dataset inside an actor root's subtree to that root.
//
// Activation inherits, so an enabled dataset with an enabled ancestor is a
// member of the root above it, never a peer; and an intervening enabled=off
// excludes only the datasets it covers without severing deeper enabled ones
// from the root. An invalid root still owns its subtree: letting a policy error
// turn its descendants into roots would fork lineages that fixing the error
// could not merge back.
//
// Under replicate=on the one -R stream carries the whole subtree, so every
// descendant is a member whatever it sets and its boomerangz properties are
// ignored. Otherwise members are the enabled, valid descendants, each sending
// with its own options; the receive path mapping and the wire format belong to
// the root, because every member's replica is placed relative to the root's.
func markRoots(names []string, entries map[string]Entry) {
	within := make(map[string]string, len(names))
	members := make(map[string][]string)
	for _, name := range names {
		entry := entries[name]
		root := within[entry.Parent]
		if root == "" && entry.Policy.Enabled {
			root = name
		}
		within[name] = root
		if root == "" {
			continue
		}
		if root == name {
			members[root] = append(members[root], name)
			continue
		}
		entry.CoveredBy = root
		rootEntry := entries[root]
		switch {
		case rootEntry.Policy.Send.Replicate:
			entry.Member = true
			if !entry.Policy.Enabled {
				entry.Policy.Warnings = append(entry.Policy.Warnings, "replication root "+root+" sends this dataset although it is disabled here; replicate=on cannot exclude descendants")
			}
			keys := make(map[string]bool)
			for key := range entry.Policy.Values {
				keys[key] = true
			}
			for key := range rootEntry.Policy.Values {
				keys[key] = true
			}
			delete(keys, policy.Namespace+"enabled")
			for _, key := range slices.Sorted(maps.Keys(keys)) {
				if entry.Policy.Values[key].Value != rootEntry.Policy.Values[key].Value {
					entry.Policy.Warnings = append(entry.Policy.Warnings, "replication root "+root+" governs differing "+strings.TrimPrefix(key, policy.Namespace))
				}
			}
		case entry.Policy.Enabled && entry.Inspected && entry.Policy.Valid():
			entry.Member = true
			if entry.Policy.Send.Replicate {
				entry.Policy.Send.Replicate = false
				entry.Policy.Warnings = append(entry.Policy.Warnings, "actor root "+root+" sends members individually; replicate is ignored here")
			}
			if entry.Policy.Discard != rootEntry.Policy.Discard {
				entry.Policy.Discard = rootEntry.Policy.Discard
				entry.Policy.Warnings = append(entry.Policy.Warnings, "actor root "+root+" sets the receive path mapping; discard is ignored here")
			}
		case entry.Policy.Enabled:
			rootEntry.Policy.Warnings = append(rootEntry.Policy.Warnings, "member "+name+" is excluded while its policy is invalid")
			entries[root] = rootEntry
		}
		if entry.Member {
			members[root] = append(members[root], name)
		}
		entries[name] = entry
	}
	for root, set := range members {
		entry := entries[root]
		entry.Members = set
		entries[root] = entry
	}
	// A plaintext replication root may contain encrypted descendants, and its
	// one stream must then be raw throughout. Validate the whole covered scope
	// once roots are known so an invalid root cannot release its descendants.
	checked := make(map[string]bool)
	for _, name := range names {
		entry := entries[name]
		if !entry.Member || checked[entry.CoveredBy] || entry.Dataset.EncryptionRoot == "" || entry.Dataset.EncryptionRoot == "-" {
			continue
		}
		root := entries[entry.CoveredBy]
		if !root.Policy.Send.Replicate {
			continue
		}
		root.Policy = root.Policy.ForReplicationScope(root.Dataset.EncryptionRoot != "" && root.Dataset.EncryptionRoot != "-", name)
		entries[entry.CoveredBy] = root
		checked[entry.CoveredBy] = true
	}
}

// replica is where one actor root's replica for one target lands.
type replica struct {
	root      string
	target    string
	name      string
	remote    bool
	namespace string
	mapped    string
	// established is set when the root's stored binding for this target
	// already proves a replica at mapped.
	established bool
}

func overlaps(a, b string) bool {
	return a == b || strings.HasPrefix(a, b+"/") || strings.HasPrefix(b, a+"/")
}

// plannedReplicas lists where each actor root's replicas land, for every local
// destination and every remote whose destination is known, with the actor
// roots themselves.
func plannedReplicas(names []string, entries map[string]Entry, remotes map[string]RemoteDestination) ([]replica, []string) {
	var replicas []replica
	var roots []string
	for _, name := range names {
		root := entries[name]
		if root.CoveredBy != "" || !root.Policy.Enabled {
			continue
		}
		roots = append(roots, name)
		local, remote := slices.Clone(root.Policy.Local), slices.Clone(root.Policy.Remote)
		if !root.Policy.Send.Replicate {
			for _, member := range root.Members {
				if member == name {
					continue
				}
				local = append(local, entries[member].Policy.Local...)
				remote = append(remote, entries[member].Policy.Remote...)
			}
		}
		bound := storedBindings(root)
		discard := zfs.ReceiveDiscard(root.Policy.Discard)
		slices.Sort(local)
		for _, target := range slices.Compact(local) {
			if mapped, err := zfs.MapReceiveDataset(name, target, discard); err == nil {
				established := slices.ContainsFunc(bound, func(b storedBinding) bool {
					return b.Transport == "local" && b.DestinationRoot == target && b.MappedDataset == mapped
				})
				replicas = append(replicas, replica{root: name, target: "local destination " + target, name: target, namespace: "local", mapped: mapped, established: established})
			}
		}
		slices.Sort(remote)
		for _, target := range slices.Compact(remote) {
			destination, known := remotes[target]
			if !known {
				continue
			}
			if mapped, err := zfs.MapReceiveDataset(name, destination.Root, discard); err == nil {
				established := slices.ContainsFunc(bound, func(b storedBinding) bool {
					return b.Transport != "local" && b.DestinationRoot == destination.Root && b.MappedDataset == mapped
				})
				replicas = append(replicas, replica{root: name, target: "remote " + target, name: target, remote: true, namespace: destination.Namespace, mapped: mapped, established: established})
			}
		}
	}
	return replicas, roots
}

// markCollisions blocks each actor root's target whose replica would overlap
// another replica in the same destination namespace, or would land inside an
// actor root on this host.
//
// A root owns its replica's whole subtree: its members are received beneath
// it, and a reseed destroys it recursively. Two replicas where one contains
// the other - two roots, or one root's two targets - are therefore never
// safe: the first sign would be a lineage or GUID failure after a stream had
// already run, and a reseed of one would destroy the other. A local replica
// inside a root's source is no safer: it would inherit that root's activation
// and be snapshotted as one of its members.
//
// Only the target is blocked, never the root: its snapshots, pruning and other
// targets have nothing to do with where one replica lands. A replica the root
// has already established there, by a stored binding, keeps its target and the
// newcomer that would overlap it is blocked, so a mistake in new configuration
// cannot stop a working replica. Without that evidence, or with it on both
// sides, both are blocked.
func markCollisions(entries map[string]Entry, replicas []replica, roots []string) {
	block := func(r replica, reason string) {
		entry := entries[r.root]
		if !slices.ContainsFunc(entry.Blocked, func(b BlockedTarget) bool { return b.Target == r.name && b.Remote == r.remote }) {
			entry.Blocked = append(entry.Blocked, BlockedTarget{Target: r.name, Remote: r.remote, Reason: reason})
		}
		entries[r.root] = entry
	}
	describe := func(r, other replica) string {
		return fmt.Sprintf("%s places this root's replica at %s, which overlaps %s's replica %s from %s", r.target, r.mapped, other.root, other.mapped, other.target)
	}
	for i, a := range replicas {
		for _, b := range replicas[i+1:] {
			if a.namespace != b.namespace || !overlaps(a.mapped, b.mapped) {
				continue
			}
			if !b.established || a.established {
				block(b, describe(b, a))
			}
			if !a.established || b.established {
				block(a, describe(a, b))
			}
		}
		if a.namespace != "local" {
			continue
		}
		for _, root := range roots {
			if overlaps(a.mapped, root) {
				block(a, fmt.Sprintf("%s places this root's replica at %s, which overlaps actor root %s on this host", a.target, a.mapped, root))
			}
		}
	}
}

// markReplicas marks the datasets that are replicas boomerangz receives into:
// one where an actor root on this host places its replica on a local
// destination, one carrying a source root's received lineage, and one beneath
// a root this host receives remote sources' replicas into. Everything beneath
// a replica is part of it.
//
// A received lineage is only present when the stream carried properties, and
// a remote source's replica is only recognised by its replication root when
// this host's configuration names it; the other marks need neither.
func markReplicas(names []string, entries map[string]Entry, replicas []replica, replicationRoots []string) {
	for _, name := range names {
		entry := entries[name]
		marked := entries[entry.Parent].Replica
		// A blocked target receives nothing, so what lies there is not its
		// replica.
		if marked.Source == "" {
			for _, r := range replicas {
				if r.namespace == "local" && r.mapped == name && entries[r.root].ErrBlocked(r.name, false) == nil {
					marked.Source, marked.Destination = r.root, r.name
					break
				}
			}
		}
		if lineage := receivedLineage(entry); lineage != "" {
			marked.Lineage = lineage
		}
		if marked.Destination == "" {
			for _, root := range replicationRoots {
				if strings.HasPrefix(name, root+"/") {
					marked.Destination = root
					break
				}
			}
		}
		entry.Replica = marked
		entries[name] = entry
	}
}

// receivedLineage is the lineage a dataset received from its source root. A
// dataset owned here is a source, whatever it once received.
func receivedLineage(entry Entry) string {
	var lineage string
	for _, row := range entry.Stored {
		if row.Dataset != entry.Dataset.Name {
			continue
		}
		switch {
		case row.Name == policy.StateNamespace+"owner" && row.Source == zfs.SourceLocal:
			return ""
		case row.Name == policy.StateNamespace+"lineage" && row.Source == zfs.SourceReceived:
			lineage = row.Value
		}
	}
	return lineage
}

// storedBinding is the part of a root's stored target binding that says where
// a replica was established.
type storedBinding struct {
	Transport       string `json:"transport"`
	DestinationRoot string `json:"destination_root"`
	MappedDataset   string `json:"mapped_dataset"`
}

// storedBindings reads the target bindings stored locally on root. A binding
// that does not parse proves nothing and is skipped.
func storedBindings(root Entry) []storedBinding {
	var bindings []storedBinding
	for _, row := range root.Stored {
		if row.Dataset != root.Dataset.Name || row.Source != zfs.SourceLocal || !strings.HasPrefix(row.Name, policy.StateNamespace+"target:") {
			continue
		}
		var binding storedBinding
		if json.Unmarshal([]byte(row.Value), &binding) == nil && binding.MappedDataset != "" {
			bindings = append(bindings, binding)
		}
	}
	return bindings
}
