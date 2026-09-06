package discovery

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

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
