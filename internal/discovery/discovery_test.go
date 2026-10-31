package discovery

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pdf/boomerangz/internal/lifecycle"
	"github.com/pdf/boomerangz/internal/policy"
	"github.com/pdf/boomerangz/internal/zfs"
)

type fakeReader struct {
	datasets         []zfs.Dataset
	properties       []zfs.Property
	batches          [][]string
	failBatch        int
	activationError  error
	changeActivation bool
}

type lifecycleFakeReader struct {
	*fakeReader
	lifecycle []zfs.Property
}

func (f *lifecycleFakeReader) GetLifecycleProperties(context.Context) ([]zfs.Property, error) {
	return slices.Clone(f.lifecycle), nil
}

func (f *fakeReader) ListDatasets(context.Context) ([]zfs.Dataset, error) {
	return slices.Clone(f.datasets), nil
}
func (f *fakeReader) GetActivationProperties(context.Context) ([]zfs.Property, error) {
	var rows []zfs.Property
	for _, p := range f.properties {
		if p.Name == policy.Namespace+"enabled" {
			rows = append(rows, p)
		}
	}
	return rows, f.activationError
}
func (f *fakeReader) GetStoredProperties(_ context.Context, names []string) ([]zfs.Property, error) {
	f.batches = append(f.batches, slices.Clone(names))
	if f.failBatch == len(f.batches) {
		return nil, errors.New("injected batch failure")
	}
	var rows []zfs.Property
	for _, p := range f.properties {
		if slices.Contains(names, p.Dataset) {
			if f.changeActivation && p.Name == policy.Namespace+"enabled" {
				continue
			}
			rows = append(rows, p)
		}
	}
	return rows, nil
}

func fixture() *fakeReader {
	f := &fakeReader{}
	for _, name := range []string{"tank/tree/off", "backup", "tank/other", "tank/tree", "tank", "tank/bad"} {
		f.datasets = append(f.datasets, zfs.Dataset{Name: name, Type: zfs.Filesystem, EncryptionRoot: "-"})
	}
	f.properties = []zfs.Property{
		{Dataset: "tank/tree", Name: policy.Namespace + "enabled", Value: "on", Source: zfs.SourceLocal},
		{Dataset: "tank/tree", Name: policy.Namespace + "replicate", Value: "on", Source: zfs.SourceLocal},
		{Dataset: "tank/tree/off", Name: policy.Namespace + "enabled", Value: "off", Source: zfs.SourceLocal},
		{Dataset: "tank/bad", Name: policy.Namespace + "enabled", Value: "on", Source: zfs.SourceLocal},
		{Dataset: "tank/bad", Name: policy.Namespace + "remote", Value: "missing", Source: zfs.SourceLocal},
		{Dataset: "backup", Name: policy.Namespace + "enabled", Value: "on", Source: zfs.SourceReceived},
	}
	return f
}

func TestSparseScanAndIsolation(t *testing.T) {
	t.Parallel()
	f := fixture()
	s, err := New(f, Options{BatchSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	g, err := s.Scan(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	var fetched []string
	for _, batch := range f.batches {
		if len(batch) > 2 {
			t.Fatal("unbounded batch")
		}
		fetched = append(fetched, batch...)
	}
	slices.Sort(fetched)
	if !reflect.DeepEqual(fetched, []string{"tank", "tank/bad", "tank/tree", "tank/tree/off"}) {
		t.Fatalf("unexpected inspection: %v", fetched)
	}
	child, _ := g.Inspect("tank/tree/off")
	if !child.Inspected || child.CoveredBy != "tank/tree" || child.Policy.Enabled || len(child.Policy.Warnings) == 0 {
		t.Fatalf("bad recursive coverage: %#v", child)
	}
	bad, _ := g.Inspect("tank/bad")
	root, _ := g.Inspect("tank/tree")
	if bad.Policy.Valid() || !root.Policy.Valid() {
		t.Fatal("policy error was not isolated")
	}
	backup, _ := g.Inspect("backup")
	if backup.Inspected || backup.Policy.Enabled {
		t.Fatal("received backup activated")
	}
	root.Policy.Values[policy.Namespace+"enabled"] = policy.Value{Value: "off"}
	root.Stored[0].Value = "bad"
	again, _ := g.Inspect("tank/tree")
	if again.Policy.Values[policy.Namespace+"enabled"].Value != "on" || again.Stored[0].Value == "bad" {
		t.Fatal("generation was mutable")
	}
	next, err := s.Scan(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if next.ID() != 2 || len(next.Changed(g)) != 0 {
		t.Fatalf("unchanged scan diff: %v", next.Changed(g))
	}
}

func TestFailurePreservesGeneration(t *testing.T) {
	t.Parallel()
	f := fixture()
	s, _ := New(f, Options{BatchSize: 1})
	previous, err := s.Scan(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	f.failBatch = len(f.batches) + 2
	if _, err := s.Scan(t.Context(), nil); err == nil || s.Current() != previous {
		t.Fatal("partial batch scan published")
	}
	f.failBatch = 0
	f.changeActivation = true
	if _, err := s.Scan(t.Context(), nil); err == nil || s.Current() != previous {
		t.Fatal("inconsistent activation published")
	}
	f.changeActivation = false
	f.activationError = errors.New("query failure")
	if _, err := s.Scan(t.Context(), nil); err == nil || s.Current() != previous {
		t.Fatal("failed query published")
	}
}

func TestInactiveInspectionAndDeactivation(t *testing.T) {
	t.Parallel()
	f := fixture()
	s, _ := New(f, Options{})
	g, err := s.Scan(t.Context(), []string{"backup"})
	if err != nil {
		t.Fatal(err)
	}
	b, _ := g.Inspect("backup")
	if !b.Inspected || b.Policy.Enabled || len(b.Stored) != 1 {
		t.Fatal("missing inactive inspection")
	}
	f.properties[0].Value = "off"
	next, err := s.Scan(t.Context(), []string{"tank/tree"})
	if err != nil {
		t.Fatal(err)
	}
	entry, _ := next.Inspect("tank/tree")
	if entry.Policy.Enabled || !slices.Contains(next.Changed(g), "tank/tree") {
		t.Fatal("disable not reflected")
	}
	if _, err := s.Scan(t.Context(), []string{"missing"}); err == nil {
		t.Fatal("accepted absent retained dataset")
	}
}

func TestSparseLifecycleRootsAreReconstructedAfterRestart(t *testing.T) {
	t.Parallel()
	base := fixture()
	reader := &lifecycleFakeReader{fakeReader: base, lifecycle: []zfs.Property{
		{Dataset: "tank/other", Name: policy.StateNamespace + "owner", Value: "11111111-1111-4111-8111-111111111111", Source: zfs.SourceLocal},
		{Dataset: "tank/other", Name: policy.StateNamespace + "lineage", Value: "22222222-2222-4222-8222-222222222222", Source: zfs.SourceLocal},
		{Dataset: "tank/other", Name: policy.StateNamespace + "inactive", Value: "marker", Source: zfs.SourceLocal},
	}}
	reader.properties = append(reader.properties, reader.lifecycle...)
	scanner, err := New(reader, Options{})
	if err != nil {
		t.Fatal(err)
	}
	generation, err := scanner.Scan(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	entry, exists := generation.Inspect("tank/other")
	if !exists || !entry.Inspected {
		t.Fatal("inactive lifecycle root was not inspected")
	}
	for _, property := range reader.lifecycle {
		if !slices.Contains(entry.Stored, property) {
			t.Fatalf("lifecycle property missing: %#v", property)
		}
	}
}

func TestConcurrentScansAndReaders(t *testing.T) {
	t.Parallel()
	s, _ := New(fixture(), Options{})
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 5 {
				g, err := s.Scan(t.Context(), nil)
				if err != nil {
					t.Error(err)
					return
				}
				entries := g.Entries()
				entries[0].Policy.Values["mutated"] = policy.Value{}
				_ = s.Current().Entries()
			}
		})
	}
	wg.Wait()
	if s.Current().ID() != 40 {
		t.Fatal("scans were not serialized")
	}
}

func TestRequestCoalescingAndCancellation(t *testing.T) {
	t.Parallel()
	s, _ := New(fixture(), Options{})
	for range 100 {
		s.Request()
	}
	if len(s.requests) != 1 {
		t.Fatal("requests not coalesced")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := s.Scan(ctx, nil); !errors.Is(err, context.Canceled) || s.Current() != nil {
		t.Fatal("canceled scan published")
	}
}

func TestRunStartupAndRequestedScan(t *testing.T) {
	t.Parallel()
	s, _ := New(fixture(), Options{})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	count := 0
	err := s.Run(ctx, time.Hour, nil, func(g *Generation, err error) {
		if err != nil || g == nil {
			t.Errorf("scan failed: %v", err)
			cancel()
			return
		}
		count++
		if count == 1 {
			for range 10 {
				s.Request()
			}
		} else {
			cancel()
		}
	})
	if !errors.Is(err, context.Canceled) || count != 2 {
		t.Fatalf("run: %d scans, %v", count, err)
	}
}

func TestInspectionBoundsAndMalformedInventory(t *testing.T) {
	t.Parallel()
	f := fixture()
	s, _ := New(f, Options{MaxArgumentBytes: 16})
	if _, err := s.Scan(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	for _, batch := range f.batches {
		bytes := 0
		for _, name := range batch {
			bytes += len(name) + 1
		}
		if bytes > 16 {
			t.Fatal("argument byte limit exceeded")
		}
	}
	f.datasets = append(f.datasets, f.datasets[0])
	previous := s.Current()
	if _, err := s.Scan(t.Context(), nil); err == nil || s.Current() != previous {
		t.Fatal("duplicate inventory published")
	}
	s, _ = New(fixture(), Options{MaxArgumentBytes: 1})
	if _, err := s.Scan(t.Context(), nil); err == nil {
		t.Fatal("oversize dataset accepted")
	}
}

func TestEncryptedReplicationDescendant(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                           string
		rows                           []zfs.Property
		valid, raw, requested, implied bool
	}{
		{name: "automatic", valid: true, raw: true},
		{name: "explicit root off", rows: []zfs.Property{{Dataset: "tank/tree", Name: policy.Namespace + "raw", Value: "off", Source: zfs.SourceLocal}}},
		{name: "inherited off", rows: []zfs.Property{{Dataset: "tank", Name: policy.Namespace + "raw", Value: "off", Source: zfs.SourceLocal}}},
		{name: "received off ignored", rows: []zfs.Property{{Dataset: "tank/tree", Name: policy.Namespace + "raw", Value: "off", Source: zfs.SourceReceived}}, valid: true, raw: true},
		{name: "incompatible override", rows: []zfs.Property{{Dataset: "tank/tree", Name: policy.Namespace + "set_prop:encryption", Value: "off", Source: zfs.SourceLocal}}, raw: true},
		{name: "explicit raw incompatible override", rows: []zfs.Property{{Dataset: "tank/tree", Name: policy.Namespace + "raw", Value: "on", Source: zfs.SourceLocal}, {Dataset: "tank/tree", Name: policy.Namespace + "ignore_prop:encryption", Value: "on", Source: zfs.SourceLocal}}, raw: true, requested: true},
		{name: "implied flags", rows: []zfs.Property{{Dataset: "tank/tree", Name: policy.Namespace + "compressed", Value: "off", Source: zfs.SourceLocal}}, valid: true, raw: true, implied: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := fixture()
			f.datasets[0].EncryptionRoot = "tank/tree/off"
			f.properties = append(f.properties, tc.rows...)
			s, _ := New(f, Options{})
			g, err := s.Scan(t.Context(), nil)
			if err != nil {
				t.Fatal(err)
			}
			root, _ := g.Inspect("tank/tree")
			child, _ := g.Inspect("tank/tree/off")
			if root.Policy.Valid() != tc.valid || root.Policy.Send.Raw != tc.raw || root.Policy.Requested.Raw != tc.requested || child.CoveredBy != "tank/tree" {
				t.Fatalf("unexpected scope policy: %#v; coverage %q", root.Policy, child.CoveredBy)
			}
			if tc.raw && !tc.requested && len(root.Policy.Warnings) == 0 {
				t.Fatal("automatic selection not explained")
			}
			if tc.implied && (root.Policy.Requested.Compressed || !root.Policy.Send.Compressed || !root.Policy.Send.EmbeddedData || len(root.Policy.Warnings) < 2) {
				t.Fatal("implied flags not exposed")
			}
		})
	}
}

func rootFixture(t *testing.T, datasets []string, properties ...zfs.Property) (*fakeReader, *Generation) {
	t.Helper()
	f := &fakeReader{}
	for _, name := range datasets {
		f.datasets = append(f.datasets, zfs.Dataset{Name: name, Type: zfs.Filesystem, EncryptionRoot: "-"})
	}
	f.properties = properties
	s, err := New(f, Options{BatchSize: 8, Remotes: []string{"offsite"}})
	if err != nil {
		t.Fatal(err)
	}
	g, err := s.Scan(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	return f, g
}

func local(dataset, key, value string) zfs.Property {
	return zfs.Property{Dataset: dataset, Name: policy.Namespace + key, Value: value, Source: zfs.SourceLocal}
}

func TestRootOwnsSparseSubtree(t *testing.T) {
	t.Parallel()
	_, g := rootFixture(t,
		[]string{"tank", "tank/root", "tank/root/first", "tank/root/first/second", "tank/root/first/other", "tank/root/kept"},
		local("tank/root", "enabled", "on"),
		local("tank/root", "local", "backup"),
		local("tank/root/first", "enabled", "off"),
		local("tank/root/first/second", "enabled", "on"),
	)
	root, _ := g.Inspect("tank/root")
	if root.CoveredBy != "" || root.Member {
		t.Fatalf("root is covered: %#v", root)
	}
	if want := []string{"tank/root", "tank/root/first/second", "tank/root/kept"}; !slices.Equal(root.Members, want) {
		t.Fatalf("members = %q, want %q", root.Members, want)
	}
	for name, member := range map[string]bool{
		"tank/root/first":        false,
		"tank/root/first/other":  false,
		"tank/root/first/second": true,
		"tank/root/kept":         true,
	} {
		entry, _ := g.Inspect(name)
		// Coverage is ownership: every dataset inside the root names it,
		// member or not, so none can be scheduled or operated on alone.
		if entry.CoveredBy != "tank/root" || entry.Member != member || len(entry.Members) != 0 {
			t.Errorf("%s: covered by %q, member %t, members %q", name, entry.CoveredBy, entry.Member, entry.Members)
		}
		if err := entry.ErrCovered(); err == nil {
			t.Errorf("%s: not refused as an independent root", name)
		}
		if !entry.Inspected {
			t.Errorf("%s: a root's subtree must be inspected", name)
		}
	}
	// A locally enabled dataset beneath an enabled one is a member, not the
	// independent peer root it would have been when activation alone decided.
	second, _ := g.Inspect("tank/root/first/second")
	if second.Policy.Values[policy.Namespace+"enabled"].Dataset != "tank/root/first/second" {
		t.Fatal("fixture: second should hold its own activation")
	}
}

func TestMembersSendWithTheirOwnOptionsUnderTheRootsMapping(t *testing.T) {
	t.Parallel()
	_, g := rootFixture(t,
		[]string{"tank", "tank/root", "tank/root/raw", "tank/root/tree"},
		local("tank/root", "enabled", "on"),
		local("tank/root", "local", "backup"),
		local("tank/root", "discard", "first"),
		local("tank/root/raw", "raw", "on"),
		local("tank/root/raw", "remote", "offsite"),
		local("tank/root/tree", "replicate", "on"),
		local("tank/root/tree", "discard", "all"),
	)
	raw, _ := g.Inspect("tank/root/raw")
	if !raw.Member || !raw.Policy.Send.Raw || !slices.Contains(raw.Policy.Remote, "offsite") || len(raw.Policy.Warnings) != 0 {
		t.Fatalf("member options were not honoured: %#v", raw.Policy)
	}
	tree, _ := g.Inspect("tank/root/tree")
	if !tree.Member || tree.Policy.Send.Replicate || !tree.Policy.Requested.Replicate {
		t.Fatalf("a member's replicate must be ignored, not honoured: %#v", tree.Policy)
	}
	if tree.Policy.Discard != policy.DiscardFirst {
		t.Fatalf("a member's discard must follow the root: %v", tree.Policy.Discard)
	}
	if len(tree.Policy.Warnings) != 2 {
		t.Fatalf("expected replicate and discard warnings, got %q", tree.Policy.Warnings)
	}
}

func TestInvalidPoliciesDoNotReleaseOwnership(t *testing.T) {
	t.Parallel()
	_, g := rootFixture(t,
		[]string{"tank", "tank/root", "tank/root/broken", "tank/root/broken/below", "tank/bad", "tank/bad/child"},
		local("tank/root", "enabled", "on"),
		local("tank/root", "local", "backup"),
		local("tank/root/broken", "remote", "missing"),
		local("tank/bad", "enabled", "on"),
		local("tank/bad", "remote", "missing"),
		local("tank/bad/child", "enabled", "on"),
		local("tank/bad/child", "remote", "offsite"),
	)
	root, _ := g.Inspect("tank/root")
	broken, _ := g.Inspect("tank/root/broken")
	if broken.Member || broken.CoveredBy != "tank/root" || slices.Contains(root.Members, "tank/root/broken") {
		t.Fatalf("an invalid member was kept: %#v", broken)
	}
	if !slices.ContainsFunc(root.Policy.Warnings, func(w string) bool { return strings.Contains(w, "tank/root/broken") }) {
		t.Fatalf("the root does not say which member it dropped: %q", root.Policy.Warnings)
	}
	// broken inherits its invalid remote into below, so it is excluded too.
	below, _ := g.Inspect("tank/root/broken/below")
	if below.Member || below.CoveredBy != "tank/root" {
		t.Fatalf("below: %#v", below)
	}
	// An invalid root keeps its subtree; its valid child does not become a
	// root that fixing the parent would then have to absorb.
	child, _ := g.Inspect("tank/bad/child")
	if child.CoveredBy != "tank/bad" || !child.Member {
		t.Fatalf("an invalid root released its child: %#v", child)
	}
}

func TestReplicationRootCarriesDisabledDescendants(t *testing.T) {
	t.Parallel()
	_, g := rootFixture(t,
		[]string{"tank", "tank/tree", "tank/tree/off", "tank/tree/off/deep"},
		local("tank/tree", "enabled", "on"),
		local("tank/tree", "replicate", "on"),
		local("tank/tree", "local", "backup"),
		local("tank/tree/off", "enabled", "off"),
	)
	root, _ := g.Inspect("tank/tree")
	if want := []string{"tank/tree", "tank/tree/off", "tank/tree/off/deep"}; !slices.Equal(root.Members, want) {
		t.Fatalf("members = %q, want %q", root.Members, want)
	}
	off, _ := g.Inspect("tank/tree/off")
	if !off.Member || len(off.Policy.Warnings) != 1 || !strings.Contains(off.Policy.Warnings[0], "although it is disabled here") {
		t.Fatalf("an excluded descendant under -R needs its own warning, not a governs-differing one: %q", off.Policy.Warnings)
	}
}

func collisionScan(t *testing.T, destinations map[string]RemoteDestination, datasets []string, properties ...zfs.Property) *Generation {
	t.Helper()
	f := &fakeReader{properties: properties}
	for _, name := range datasets {
		f.datasets = append(f.datasets, zfs.Dataset{Name: name, Type: zfs.Filesystem, EncryptionRoot: "-"})
	}
	s, err := New(f, Options{BatchSize: 8, Remotes: []string{"near", "far", "unknown"}, Destinations: destinations})
	if err != nil {
		t.Fatal(err)
	}
	g, err := s.Scan(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	return g
}

// requireCollision requires root to stay valid with exactly target blocked, for
// a reason naming other.
func requireCollision(t *testing.T, g *Generation, root, target string, remote bool, other string) {
	t.Helper()
	entry, _ := g.Inspect(root)
	if !entry.Policy.Valid() {
		t.Errorf("%s should stay valid, with only its target blocked: %q", root, entry.Policy.Errors)
	}
	err := entry.ErrBlocked(target, remote)
	if err == nil || !strings.Contains(err.Error(), other) {
		t.Errorf("%s should be blocked from %s, naming %s: %v (blocked %+v)", root, target, other, err, entry.Blocked)
	}
}

// requireUnblocked requires root to be valid with no target blocked.
func requireUnblocked(t *testing.T, g *Generation, root string) {
	t.Helper()
	if entry, _ := g.Inspect(root); !entry.Policy.Valid() || len(entry.Blocked) != 0 {
		t.Errorf("%s should be valid and unblocked: errors %q, blocked %+v", root, entry.Policy.Errors, entry.Blocked)
	}
}

func TestCollidingReplicasBlockTheirTargets(t *testing.T) {
	t.Parallel()
	datasets := []string{"tank", "tank/a", "tank/a/home", "tank/a/home/docs", "tank/b", "tank/b/home", "tank/c", "tank/c/data", "tank/x", "tank/x/data", "backup"}
	enabled := func(root string, extra ...string) []zfs.Property {
		rows := []zfs.Property{local(root, "enabled", "on")}
		for i := 0; i+1 < len(extra); i += 2 {
			rows = append(rows, local(root, extra[i], extra[i+1]))
		}
		return rows
	}
	t.Run("equal leaves under discard=all", func(t *testing.T) {
		t.Parallel()
		var rows []zfs.Property
		rows = append(rows, enabled("tank/a/home", "local", "backup", "discard", "all")...)
		rows = append(rows, enabled("tank/b/home", "local", "backup", "discard", "all")...)
		g := collisionScan(t, nil, datasets, rows...)
		requireCollision(t, g, "tank/a/home", "backup", false, "tank/b/home")
		requireCollision(t, g, "tank/b/home", "backup", false, "tank/a/home")
	})
	t.Run("one replica inside another", func(t *testing.T) {
		t.Parallel()
		// tank/c/data maps to backup/c/data under discard=first; tank/x/data
		// to backup/c/data/data under discard=all from backup/c/data.
		var rows []zfs.Property
		rows = append(rows, enabled("tank/c/data", "local", "backup", "discard", "first")...)
		rows = append(rows, enabled("tank/x/data", "local", "backup/c/data", "discard", "all")...)
		g := collisionScan(t, nil, datasets, rows...)
		requireCollision(t, g, "tank/c/data", "backup", false, "tank/x/data")
		requireCollision(t, g, "tank/x/data", "backup/c/data", false, "tank/c/data")
	})
	t.Run("disjoint siblings", func(t *testing.T) {
		t.Parallel()
		var rows []zfs.Property
		rows = append(rows, enabled("tank/a/home", "local", "backup", "discard", "first")...)
		rows = append(rows, enabled("tank/b/home", "local", "backup", "discard", "first")...)
		g := collisionScan(t, nil, datasets, rows...)
		requireUnblocked(t, g, "tank/a/home")
		requireUnblocked(t, g, "tank/b/home")
	})
	t.Run("one root's own targets", func(t *testing.T) {
		t.Parallel()
		// backup/home and backup/home/home: reseeding the first would
		// destroy the second.
		g := collisionScan(t, nil, datasets, enabled("tank/a/home", "local", "backup,backup/home", "discard", "all")...)
		requireCollision(t, g, "tank/a/home", "backup", false, "backup/home/home")
		requireCollision(t, g, "tank/a/home", "backup/home", false, "backup/home")
	})
	t.Run("replica inside an actor root", func(t *testing.T) {
		t.Parallel()
		// Received replicas would inherit tank/a's activation and be
		// snapshotted as its members.
		var rows []zfs.Property
		rows = append(rows, enabled("tank/a")...)
		rows = append(rows, enabled("tank/c/data", "local", "tank/a/home", "discard", "all")...)
		g := collisionScan(t, nil, datasets, rows...)
		requireCollision(t, g, "tank/c/data", "tank/a/home", false, "actor root tank/a")
		requireUnblocked(t, g, "tank/a")
	})
	t.Run("a member's target", func(t *testing.T) {
		t.Parallel()
		// Only a member sends to backup, but its replica lives under its
		// root's, so the roots collide.
		var rows []zfs.Property
		rows = append(rows, enabled("tank/a/home", "discard", "all")...)
		rows = append(rows, local("tank/a/home/docs", "local", "backup"))
		rows = append(rows, enabled("tank/b/home", "local", "backup", "discard", "all")...)
		g := collisionScan(t, nil, datasets, rows...)
		requireCollision(t, g, "tank/a/home", "backup", false, "tank/b/home")
		requireCollision(t, g, "tank/b/home", "backup", false, "tank/a/home")
	})
	t.Run("remotes sharing a namespace", func(t *testing.T) {
		t.Parallel()
		destinations := map[string]RemoteDestination{
			"near": {Namespace: "ssh://host:22", Root: "pool/backup"},
			"far":  {Namespace: "ssh://host:22", Root: "pool/backup"},
		}
		var rows []zfs.Property
		rows = append(rows, enabled("tank/a/home", "remote", "near", "discard", "all")...)
		rows = append(rows, enabled("tank/b/home", "remote", "far", "discard", "all")...)
		g := collisionScan(t, destinations, datasets, rows...)
		requireCollision(t, g, "tank/a/home", "near", true, "remote far")
		requireCollision(t, g, "tank/b/home", "far", true, "remote near")
	})
	t.Run("remotes in different namespaces", func(t *testing.T) {
		t.Parallel()
		destinations := map[string]RemoteDestination{
			"near": {Namespace: "ssh://host:22", Root: "pool/backup"},
			"far":  {Namespace: "native://other:9000", Root: "pool/backup"},
		}
		var rows []zfs.Property
		rows = append(rows, enabled("tank/a/home", "remote", "near", "discard", "all")...)
		rows = append(rows, enabled("tank/b/home", "remote", "far", "discard", "all")...)
		// A local target at the same path is a different namespace too.
		rows = append(rows, enabled("tank/c/data", "local", "pool/backup", "discard", "all")...)
		g := collisionScan(t, destinations, datasets, rows...)
		requireUnblocked(t, g, "tank/a/home")
		requireUnblocked(t, g, "tank/b/home")
	})
	// bound records a stored binding for root's replica on a local target,
	// keyed as transfer keys it: by the hash of the target's canonical form.
	bound := func(root, target string) zfs.Property {
		return local(root, "state:target:"+lifecycle.TargetID("local:"+target), `{"pool_guid":1,"anchor_guid":2}`)
	}
	t.Run("an established replica keeps its target", func(t *testing.T) {
		t.Parallel()
		// tank/a/home has been replicating to backup/home; tank/b/home is
		// new configuration that would land on the same dataset.
		var rows []zfs.Property
		rows = append(rows, enabled("tank/a/home", "local", "backup", "discard", "all")...)
		rows = append(rows, bound("tank/a/home", "backup"))
		rows = append(rows, enabled("tank/b/home", "local", "backup", "discard", "all")...)
		g := collisionScan(t, nil, datasets, rows...)
		requireUnblocked(t, g, "tank/a/home")
		requireCollision(t, g, "tank/b/home", "backup", false, "tank/a/home")
	})
	t.Run("a binding elsewhere proves nothing here", func(t *testing.T) {
		t.Parallel()
		// The stored binding is for a different target, so neither side is
		// established at backup/home.
		var rows []zfs.Property
		rows = append(rows, enabled("tank/a/home", "local", "backup", "discard", "all")...)
		rows = append(rows, bound("tank/a/home", "elsewhere"))
		rows = append(rows, enabled("tank/b/home", "local", "backup", "discard", "all")...)
		g := collisionScan(t, nil, datasets, rows...)
		requireCollision(t, g, "tank/a/home", "backup", false, "tank/b/home")
		requireCollision(t, g, "tank/b/home", "backup", false, "tank/a/home")
	})
	t.Run("both established", func(t *testing.T) {
		t.Parallel()
		var rows []zfs.Property
		rows = append(rows, enabled("tank/a/home", "local", "backup", "discard", "all")...)
		rows = append(rows, bound("tank/a/home", "backup"))
		rows = append(rows, enabled("tank/b/home", "local", "backup", "discard", "all")...)
		rows = append(rows, bound("tank/b/home", "backup"))
		g := collisionScan(t, nil, datasets, rows...)
		requireCollision(t, g, "tank/a/home", "backup", false, "tank/b/home")
		requireCollision(t, g, "tank/b/home", "backup", false, "tank/a/home")
	})
	t.Run("a remote's binding is found by its canonical target", func(t *testing.T) {
		t.Parallel()
		const canonical = "ssh://replicator@host:22/pool/backup"
		destinations := map[string]RemoteDestination{"near": {Namespace: "ssh://host:22", Root: "pool/backup", Canonical: canonical}}
		var rows []zfs.Property
		rows = append(rows, enabled("tank/a/home", "remote", "near", "discard", "all")...)
		rows = append(rows, local("tank/a/home", "state:target:"+lifecycle.TargetID(canonical), `{"pool_guid":1,"anchor_guid":2}`))
		rows = append(rows, enabled("tank/b/home", "remote", "near", "discard", "all")...)
		g := collisionScan(t, destinations, datasets, rows...)
		requireUnblocked(t, g, "tank/a/home")
		requireCollision(t, g, "tank/b/home", "near", true, "tank/a/home")
	})
	t.Run("only the colliding target is blocked", func(t *testing.T) {
		t.Parallel()
		destinations := map[string]RemoteDestination{"near": {Namespace: "ssh://host:22", Root: "pool/backup"}}
		var rows []zfs.Property
		rows = append(rows, enabled("tank/a/home", "local", "backup", "remote", "near", "discard", "all")...)
		rows = append(rows, enabled("tank/b/home", "local", "backup", "discard", "all")...)
		g := collisionScan(t, destinations, datasets, rows...)
		requireCollision(t, g, "tank/a/home", "backup", false, "tank/b/home")
		if entry, _ := g.Inspect("tank/a/home"); entry.ErrBlocked("near", true) != nil {
			t.Errorf("the remote target was blocked by a collision on another: %+v", entry.Blocked)
		}
	})
}

func TestRemoteDestinationOfCanonicalTargets(t *testing.T) {
	t.Parallel()
	for canonical, want := range map[string]RemoteDestination{
		"ssh://replicator@Backup.Example.net:22/pool/data": {Namespace: "ssh://backup.example.net:22", Root: "pool/data", Canonical: "ssh://replicator@Backup.Example.net:22/pool/data"},
		"native://10.0.0.5:9443/tank/replicas":             {Namespace: "native://10.0.0.5:9443", Root: "tank/replicas", Canonical: "native://10.0.0.5:9443/tank/replicas"},
	} {
		got, err := RemoteDestinationOf(canonical)
		if err != nil || got != want {
			t.Errorf("%s: got %+v err %v, want %+v", canonical, got, err, want)
		}
	}
	for _, bad := range []string{"", "ssh:///pool", "ssh://host:22/", "pool/data"} {
		if _, err := RemoteDestinationOf(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}

func TestRemoteOnThisHostIsCheckedAgainstLocalDatasets(t *testing.T) {
	t.Parallel()
	datasets := []string{"tank", "tank/data", "tank/x", "tank/x/data", "backup", "backup/near"}
	properties := []zfs.Property{
		local("tank/data", "enabled", "on"),
		local("tank/data", "remote", "near"),
		local("tank/data", "discard", "first"),
		local("tank/x/data", "enabled", "on"),
		local("tank/x/data", "local", "backup/near"),
		local("tank/x/data", "discard", "all"),
	}
	// Both map to backup/near/data: tank/data on the remote, tank/x/data on
	// this host. Only a remote known to be this host shares the namespace.
	elsewhere := map[string]RemoteDestination{"near": {Namespace: "ssh://backup.example.net:22", Root: "backup/near"}}
	requireUnblocked(t, collisionScan(t, elsewhere, datasets, properties...), "tank/data")
	here := map[string]RemoteDestination{"near": {Namespace: "local", Root: "backup/near"}}
	g := collisionScan(t, here, datasets, properties...)
	requireCollision(t, g, "tank/data", "near", true, "tank/x/data")
	requireCollision(t, g, "tank/x/data", "backup/near", false, "tank/data")
}

func TestReplicasAreMarked(t *testing.T) {
	t.Parallel()
	const lineage = "22222222-2222-4222-8222-222222222222"
	received := func(dataset, key, value string) zfs.Property {
		return zfs.Property{Dataset: dataset, Name: policy.StateNamespace + key, Value: value, Source: zfs.SourceReceived}
	}
	f := &fakeReader{properties: []zfs.Property{
		// tank/home sends itself to backup and its member docs to archive,
		// both mapped under discard=first, so docs lands beneath the root's
		// place on archive even though the root does not send there.
		local("tank/home", "enabled", "on"),
		local("tank/home", "local", "backup"),
		local("tank/home", "discard", "first"),
		local("tank/home/docs", "local", "archive"),
		// tank/a and tank/b would both land on blocked/x, so nothing there
		// is either one's replica.
		local("tank/a/x", "enabled", "on"),
		local("tank/a/x", "local", "blocked"),
		local("tank/a/x", "discard", "all"),
		local("tank/b/x", "enabled", "on"),
		local("tank/b/x", "local", "blocked"),
		local("tank/b/x", "discard", "all"),
	}}
	for _, name := range []string{
		"tank", "tank/home", "tank/home/docs", "tank/a", "tank/a/x", "tank/b", "tank/b/x",
		"backup", "backup/home", "backup/home/docs", "archive", "archive/home", "archive/home/docs",
		"blocked", "blocked/x",
		"pool", "pool/recv", "pool/recv/host", "pool/recv/host/data",
		"pool/copy", "pool/copy/child", "pool/promoted", "pool/plain",
	} {
		f.datasets = append(f.datasets, zfs.Dataset{Name: name, Type: zfs.Filesystem, EncryptionRoot: "-"})
	}
	reader := &lifecycleFakeReader{fakeReader: f, lifecycle: []zfs.Property{
		received("pool/copy", "lineage", lineage),
		received("pool/copy", "owner", "11111111-1111-4111-8111-111111111111"),
		// Owned here since, so a source whatever it once received.
		received("pool/promoted", "lineage", lineage),
		{Dataset: "pool/promoted", Name: policy.StateNamespace + "owner", Value: "33333333-3333-4333-8333-333333333333", Source: zfs.SourceLocal},
	}}
	reader.properties = append(reader.properties, reader.lifecycle...)
	s, err := New(reader, Options{ReplicationRoots: []string{"pool/recv"}})
	if err != nil {
		t.Fatal(err)
	}
	g, err := s.Scan(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]Replica{
		"backup/home":         {Source: "tank/home", Destination: "backup"},
		"backup/home/docs":    {Source: "tank/home", Destination: "backup"},
		"archive/home":        {Source: "tank/home", Destination: "archive"},
		"archive/home/docs":   {Source: "tank/home", Destination: "archive"},
		"pool/recv/host":      {Destination: "pool/recv"},
		"pool/recv/host/data": {Destination: "pool/recv"},
		"pool/copy":           {Lineage: lineage},
		"pool/copy/child":     {Lineage: lineage},
		// Not replicas: destination and replication roots themselves, a
		// blocked target, the sources, a dataset owned here, and one
		// nothing marks.
		"backup":        {},
		"archive":       {},
		"blocked/x":     {},
		"pool/recv":     {},
		"tank/home":     {},
		"tank/a/x":      {},
		"pool/promoted": {},
		"pool/plain":    {},
	} {
		if entry, _ := g.Inspect(name); entry.Replica != want {
			t.Errorf("%s: replica %+v, want %+v", name, entry.Replica, want)
		}
	}
}
