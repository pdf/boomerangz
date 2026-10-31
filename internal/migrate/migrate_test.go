package migrate

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/pdf/boomerangz/internal/lifecycle"
	"github.com/pdf/boomerangz/internal/policy"
	"github.com/pdf/boomerangz/internal/transfer"
	"github.com/pdf/boomerangz/internal/zfs"
)

// The testdata fixtures are format v1 as v1 wrote it: v1-root.json is the
// state of a root that main's lifecycle service built, with a completed and a
// pending recursive proof for one target and a member stream's proof for
// another, and v1-bindings.json is main's encoding of a promoted and a
// bootstrapping binding for those targets. Neither is written by hand.

const (
	localTarget  = "local:backup"
	remoteTarget = "ssh://backup@host:22/pool/archive"
)

// memoryBackend is a pool holding one tree. hook, when set, runs before every
// inspection, with the number of inspections so far.
type memoryBackend struct {
	state zfs.State
	reads int
	sets  []string
	hook  func(*memoryBackend, int)
}

func (b *memoryBackend) InspectState(_ context.Context, dataset string, recursive bool) (zfs.State, error) {
	b.reads++
	if b.hook != nil {
		b.hook(b, b.reads)
	}
	in := func(name string) bool {
		owner, _, _ := strings.Cut(name, "@")
		owner, _, _ = strings.Cut(owner, "#")
		return owner == dataset || (recursive && strings.HasPrefix(owner, dataset+"/"))
	}
	var state zfs.State
	for _, o := range b.state.Objects {
		if in(o.Name) {
			state.Objects = append(state.Objects, o)
		}
	}
	for _, p := range b.state.Properties {
		if in(p.Dataset) {
			state.Properties = append(state.Properties, p)
		}
	}
	state.Holds = map[string][]string{}
	for name, tags := range b.state.Holds {
		if in(name) {
			state.Holds[name] = slices.Clone(tags)
		}
	}
	return state, nil
}

func (b *memoryBackend) SetProperties(_ context.Context, object string, values map[string]string) error {
	for _, key := range slices.Sorted(maps.Keys(values)) {
		b.sets = append(b.sets, key)
		b.state.Properties = slices.DeleteFunc(b.state.Properties, func(p zfs.Property) bool {
			return p.Dataset == object && p.Name == key && p.Source == zfs.SourceLocal
		})
		b.state.Properties = append(b.state.Properties, zfs.Property{Dataset: object, Name: key, Value: values[key], Source: zfs.SourceLocal})
	}
	return nil
}

func (b *memoryBackend) ListDatasets(context.Context) ([]zfs.Dataset, error) {
	var datasets []zfs.Dataset
	for _, o := range b.state.Objects {
		if o.Type == "filesystem" {
			datasets = append(datasets, zfs.Dataset{Name: o.Name, Type: zfs.Filesystem})
		}
	}
	return datasets, nil
}

func (b *memoryBackend) GetStoredProperties(_ context.Context, names []string) ([]zfs.Property, error) {
	var rows []zfs.Property
	for _, p := range b.state.Properties {
		if slices.Contains(names, p.Dataset) && strings.HasPrefix(p.Name, policy.Namespace) {
			rows = append(rows, p)
		}
	}
	return rows, nil
}

func (b *memoryBackend) value(property string) string { return local(b.state, "tank/data", property) }

// v1Root is the v1 fixture with both bindings, a suspension marker, and the
// state a replica of some other root received, which is no format of its own.
func v1Root(t *testing.T) *memoryBackend {
	t.Helper()
	b := &memoryBackend{}
	data, err := os.ReadFile("testdata/v1-root.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &b.state); err != nil {
		t.Fatal(err)
	}
	var bindings map[string]string
	data, err = os.ReadFile("testdata/v1-bindings.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &bindings); err != nil {
		t.Fatal(err)
	}
	for _, name := range slices.Sorted(maps.Keys(bindings)) {
		b.state.Properties = append(b.state.Properties, zfs.Property{Dataset: "tank/data", Name: name, Value: bindings[name], Source: zfs.SourceLocal})
	}
	b.state.Properties = append(b.state.Properties,
		zfs.Property{Dataset: "tank/data", Name: lifecycle.TargetStatePrefix + lifecycle.TargetID(remoteTarget) + ":suspended", Value: "unverified", Source: zfs.SourceLocal},
		zfs.Property{Dataset: "tank/data", Name: lifecycle.ReferencePrefix + "received", Value: "not a proof", Source: zfs.SourceReceived})
	return b
}

// identities answers a binding's identity checks for the fixture's targets.
type identities map[string]zfs.DatasetIdentity

func (i identities) ListDatasets(context.Context) ([]zfs.Dataset, error) { return nil, nil }
func (i identities) InspectState(context.Context, string, bool) (zfs.State, error) {
	return zfs.State{}, nil
}
func (i identities) InspectDatasetIdentity(_ context.Context, name string) (zfs.DatasetIdentity, error) {
	identity, ok := i[name]
	if !ok {
		return zfs.DatasetIdentity{}, errors.New("no such dataset")
	}
	return identity, nil
}

func TestMigratesAV1Root(t *testing.T) {
	t.Parallel()
	b := v1Root(t)
	owner, lineage := b.value(lifecycle.OwnerProperty), b.value(lifecycle.LineageProperty)
	received := slices.DeleteFunc(slices.Clone(b.state.Properties), func(p zfs.Property) bool { return p.Source == zfs.SourceLocal })
	// Control arm: this build refuses the root as it stands.
	if _, err := lifecycle.RootAuthority(b.state, "tank/data", owner); err == nil || !strings.Contains(err.Error(), "boomerangz migrate") {
		t.Fatalf("v1 root accepted: %v", err)
	}

	preview, err := Preview(t.Context(), b, "tank/data")
	if err != nil || preview.Status != StatusPending || preview.From != 1 || preview.To != 2 || len(preview.Blockers) != 0 {
		t.Fatalf("preview=%+v err=%v", preview, err)
	}
	// Three proofs, two bindings, then the marker, and nothing else.
	if len(preview.Actions) != 6 || preview.Actions[5] != (Action{Property: lifecycle.VersionProperty, Value: "2"}) || len(b.sets) != 0 {
		t.Fatalf("actions=%+v sets=%v", preview.Actions, b.sets)
	}
	applied, err := Apply(t.Context(), b, "tank/data")
	if err != nil || applied.Status != StatusMigrated || applied.Applied != 6 {
		t.Fatalf("applied=%+v err=%v", applied, err)
	}

	if got, err := lifecycle.RootAuthority(b.state, "tank/data", owner); err != nil || got != lineage {
		t.Fatalf("migrated root refused: lineage=%s err=%v", got, err)
	}
	// Each proof derives from ZFS exactly the endpoint v1 listed.
	refs, err := lifecycle.References(b.state, "tank/data", lineage)
	if err != nil || len(refs) != 3 {
		t.Fatalf("refs=%+v err=%v", refs, err)
	}
	type endpoint struct {
		target    string
		recursive bool
		guids     []uint64
	}
	want := []endpoint{
		{lifecycle.TargetID(localTarget), true, []uint64{103, 104, 105}},
		{lifecycle.TargetID(localTarget), true, []uint64{106, 107, 108}},
		{lifecycle.TargetID(remoteTarget), false, []uint64{107}},
	}
	var got []endpoint
	for _, r := range refs {
		sources, err := r.Sources(b.state)
		if err != nil {
			t.Fatal(err)
		}
		e := endpoint{target: r.TargetID, recursive: r.Recursive}
		for _, s := range sources {
			e.guids = append(e.guids, s.GUID)
		}
		got = append(got, e)
	}
	compare := func(a, b endpoint) int {
		if c := strings.Compare(a.target, b.target); c != 0 {
			return c
		}
		return int(a.guids[0]) - int(b.guids[0])
	}
	slices.SortFunc(got, compare)
	slices.SortFunc(want, compare)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("endpoints %+v, want %+v", got, want)
	}
	// Each binding verifies against its destination as this build reads it.
	for _, test := range []struct {
		request transfer.Request
		anchor  zfs.DatasetIdentity
	}{
		{transfer.Request{Source: "tank/data", DestinationRoot: "backup", Policy: policy.Effective{Discard: policy.DiscardOff}}, zfs.DatasetIdentity{Name: "backup", Type: zfs.Filesystem, GUID: 501, Pool: "backup", PoolGUID: 500}},
		{transfer.Request{Source: "tank/data", DestinationRoot: "pool/archive", Policy: policy.Effective{Discard: policy.DiscardFirst}, Transport: "ssh", RemoteName: "archive", CanonicalTarget: remoteTarget}, zfs.DatasetIdentity{Name: "pool/archive", Type: zfs.Filesystem, GUID: 601, Pool: "pool", PoolGUID: 600}},
	} {
		if _, err := transfer.VerifyTargetBinding(t.Context(), identities{test.anchor.Name: test.anchor}, b.state, test.request); err != nil {
			t.Errorf("binding for %s: %v", test.request.DestinationRoot, err)
		}
	}
	if b.value(lifecycle.TargetStatePrefix+lifecycle.TargetID(remoteTarget)+":suspended") != "unverified" {
		t.Error("suspension marker changed")
	}
	if now := slices.DeleteFunc(slices.Clone(b.state.Properties), func(p zfs.Property) bool { return p.Source == zfs.SourceLocal }); !reflect.DeepEqual(now, received) {
		t.Errorf("received state changed: %v, was %v", now, received)
	}

	again, err := Preview(t.Context(), b, "tank/data")
	if err != nil || again.Status != StatusCurrent || len(again.Actions) != 0 {
		t.Fatalf("second preview=%+v err=%v", again, err)
	}
}

// A run that stops part way leaves the root unmarked with some records
// rewritten, and running again finishes it without touching those.
func TestInterruptedMigrationConverges(t *testing.T) {
	t.Parallel()
	whole := v1Root(t)
	if _, err := Apply(t.Context(), whole, "tank/data"); err != nil {
		t.Fatal(err)
	}
	for stop := 1; stop < 6; stop++ {
		b := v1Root(t)
		plan, err := Preview(t.Context(), b, "tank/data")
		if err != nil {
			t.Fatal(err)
		}
		for _, action := range plan.Actions[:stop] {
			if err := b.SetProperties(t.Context(), "tank/data", map[string]string{action.Property: action.Value}); err != nil {
				t.Fatal(err)
			}
		}
		resumed, err := Preview(t.Context(), b, "tank/data")
		if err != nil || resumed.Status != StatusPending || !reflect.DeepEqual(resumed.Actions, plan.Actions[stop:]) {
			t.Fatalf("after %d writes: resumed=%+v err=%v", stop, resumed, err)
		}
		if _, err := Apply(t.Context(), b, "tank/data"); err != nil {
			t.Fatal(err)
		}
		sorted := func(s zfs.State) []zfs.Property {
			return slices.SortedFunc(slices.Values(s.Properties), func(a, b zfs.Property) int {
				return strings.Compare(a.Dataset+a.Name+string(a.Source), b.Dataset+b.Name+string(b.Source))
			})
		}
		if !reflect.DeepEqual(sorted(b.state), sorted(whole.state)) {
			t.Fatalf("after %d writes the migration ended elsewhere", stop)
		}
	}
}

func TestMigrationRefusesWhatItCannotCarryOver(t *testing.T) {
	t.Parallel()
	proof := func(b *memoryBackend) *zfs.Property {
		for i, p := range b.state.Properties {
			if strings.HasPrefix(p.Name, lifecycle.ReferencePrefix) && p.Source == zfs.SourceLocal {
				return &b.state.Properties[i]
			}
		}
		t.Fatal("fixture has no proof")
		return nil
	}
	for name, change := range map[string]func(*memoryBackend){
		"snapshot GUID changed": func(b *memoryBackend) {
			for i, o := range b.state.Objects {
				if o.Type == "snapshot" {
					b.state.Objects[i].GUID += 1000
				}
			}
		},
		"another lineage": func(b *memoryBackend) {
			p := proof(b)
			var record map[string]any
			_ = json.Unmarshal([]byte(p.Value), &record)
			record["metadata"].(map[string]any)["lineage"] = "33333333-3333-4333-8333-333333333333"
			value, _ := json.Marshal(record)
			p.Value = string(value)
		},
		"unreadable proof": func(b *memoryBackend) { proof(b).Value = `{"target":` },
		"unreadable binding": func(b *memoryBackend) {
			b.state.Properties = append(b.state.Properties, zfs.Property{Dataset: "tank/data", Name: lifecycle.TargetStatePrefix + lifecycle.TargetID("local:other"), Value: `{"version":1}`, Source: zfs.SourceLocal})
		},
		"newer format": func(b *memoryBackend) {
			b.state.Properties = append(b.state.Properties, zfs.Property{Dataset: "tank/data", Name: lifecycle.VersionProperty, Value: "3", Source: zfs.SourceLocal})
		},
	} {
		t.Run(name, func(t *testing.T) {
			b := v1Root(t)
			change(b)
			plan, err := Preview(t.Context(), b, "tank/data")
			if err != nil || plan.Status != StatusBlocked || len(plan.Blockers) == 0 {
				t.Fatalf("plan=%+v err=%v", plan, err)
			}
			if _, err := Apply(t.Context(), b, "tank/data"); err == nil || len(b.sets) != 0 {
				t.Fatalf("applied a blocked migration: err=%v sets=%v", err, b.sets)
			}
		})
	}
}

// Apply writes a property only while it still holds what the plan read.
func TestMigrationStopsWhenStateChangesUnderIt(t *testing.T) {
	t.Parallel()
	b := v1Root(t)
	plan, err := Preview(t.Context(), b, "tank/data")
	if err != nil {
		t.Fatal(err)
	}
	target := plan.Actions[1].Property
	b.reads = 0
	b.hook = func(b *memoryBackend, reads int) {
		// After both plans and the first action, before checking the second.
		if reads == 5 {
			_ = b.SetProperties(t.Context(), "tank/data", map[string]string{target: "changed"})
			b.sets = b.sets[:len(b.sets)-1]
		}
	}
	applied, err := Apply(t.Context(), b, "tank/data")
	if err == nil || applied.Applied != 1 || len(b.sets) != 1 || b.value(lifecycle.VersionProperty) != "" {
		t.Fatalf("applied=%+v err=%v sets=%v", applied, err, b.sets)
	}
}

func TestMigrationLeavesOtherDatasetsAlone(t *testing.T) {
	t.Parallel()
	b := v1Root(t)
	// A member, and a dataset boomerangz never managed, have no format.
	for _, dataset := range []string{"tank/data/a", "tank/elsewhere"} {
		b.state.Objects = append(b.state.Objects, zfs.Object{Name: dataset, Type: "filesystem", GUID: 900})
		plan, err := Preview(t.Context(), b, dataset)
		if err != nil || plan.Status != StatusUnmanaged || len(plan.Actions) != 0 {
			t.Fatalf("%s: plan=%+v err=%v", dataset, plan, err)
		}
	}
	// A root with no records needs only its marker.
	b.state.Objects = append(b.state.Objects, zfs.Object{Name: "tank/bare", Type: "filesystem", GUID: 901})
	b.state.Properties = append(b.state.Properties,
		zfs.Property{Dataset: "tank/bare", Name: lifecycle.OwnerProperty, Value: b.value(lifecycle.OwnerProperty), Source: zfs.SourceLocal},
		zfs.Property{Dataset: "tank/bare", Name: lifecycle.LineageProperty, Value: "44444444-4444-4444-8444-444444444444", Source: zfs.SourceLocal})
	plan, err := Preview(t.Context(), b, "tank/bare")
	if err != nil || plan.Status != StatusPending || !reflect.DeepEqual(plan.Actions, []Action{{Property: lifecycle.VersionProperty, Value: "2"}}) {
		t.Fatalf("bare root: plan=%+v err=%v", plan, err)
	}
	roots, err := Roots(t.Context(), b)
	if err != nil || !slices.Equal(roots, []string{"tank/bare", "tank/data"}) {
		t.Fatalf("roots=%v err=%v", roots, err)
	}
}
