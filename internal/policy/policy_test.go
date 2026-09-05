package policy

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/pdf/boomerangz/internal/zfs"
)

func TestGrid(t *testing.T) {
	t.Parallel()
	g, err := ParseGrid(" 12x5m, 24x1h,14x1d ")
	if err != nil {
		t.Fatal(err)
	}
	if g.String() != DefaultGrid || g.Cadence() != 5*time.Minute || g.Horizon() != 361*time.Hour {
		t.Fatalf("unexpected grid: %#v", g)
	}
	buckets := g.Buckets()
	buckets[0].Count = 0
	if g.Buckets()[0].Count != 12 {
		t.Fatal("mutable grid")
	}
	for _, invalid := range []string{"", "1x1s", "1x1.5h", "0x1m", "1x0m", "-1x1m", "1x1m,", "1x1h,2x60m", "1x1w,1x1d", "999999999999x1m", "1x9999999999999999999w", "2147483647x1w", "1x1525028h,1x1525029h"} {
		if _, err := ParseGrid(invalid); err == nil {
			t.Errorf("accepted %q", invalid)
		}
	}
}

func property(dataset, name, value string, source zfs.PropertySource) zfs.Property {
	return zfs.Property{Dataset: dataset, Name: Namespace + name, Value: value, Source: source}
}

func TestLocalOnlyInheritance(t *testing.T) {
	t.Parallel()
	parent := Resolve(zfs.Dataset{Name: "tank", EncryptionRoot: "-"}, nil, []zfs.Property{
		property("tank", "enabled", "on", zfs.SourceLocal),
		property("tank", "policy", "3x1h", zfs.SourceLocal),
		property("tank", "remote", "hostile", zfs.SourceReceived),
		property("tank", "state:lineage", "uuid", zfs.SourceLocal),
	}, nil)
	child := Resolve(zfs.Dataset{Name: "tank/child", EncryptionRoot: "tank/child"}, &parent, []zfs.Property{
		property("tank/child", "enabled", "off", zfs.SourceReceived),
		property("tank/child", "policy", "bad", zfs.SourceReceived),
		property("tank/child", "set_prop:org.boomerangz:enabled", "on", zfs.SourceReceived),
	}, nil)
	if !child.Enabled || !child.Valid() || child.Grid.String() != "3x1h" || !child.Requested.Raw || parent.Requested.Raw {
		t.Fatalf("bad inheritance: %#v", child)
	}
	if child.Values[Namespace+"policy"].Dataset != "tank" {
		t.Fatal("lost provenance")
	}
	if _, exists := child.Values[StateNamespace+"lineage"]; exists {
		t.Fatal("state entered policy")
	}
	child.Values[Namespace+"policy"] = Value{Value: "changed"}
	if parent.Values[Namespace+"policy"].Value != "3x1h" {
		t.Fatal("mutated parent")
	}
	disabled := Resolve(zfs.Dataset{Name: "tank/child"}, &parent, []zfs.Property{property("tank/child", "enabled", "off", zfs.SourceLocal)}, nil)
	if disabled.Enabled {
		t.Fatal("local off did not mask ancestor")
	}
	received := Resolve(zfs.Dataset{Name: "backup"}, nil, []zfs.Property{property("backup", "enabled", "on", zfs.SourceReceived)}, nil)
	if received.Enabled {
		t.Fatal("received activation enabled backup")
	}
	promoted := Resolve(zfs.Dataset{Name: "backup"}, nil, []zfs.Property{
		property("backup", "enabled", "on", zfs.SourceLocal), property("backup", "remote", "hostile", zfs.SourceReceived),
		property("backup", "policy", "bad", zfs.SourceReceived),
	}, nil)
	if !promoted.Enabled || !promoted.Valid() || len(promoted.Remote) != 0 || promoted.Grid.String() != DefaultGrid {
		t.Fatalf("promotion trusted received policy: %#v", promoted)
	}
}

func TestPolicyValidation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		values    map[string]string
		encrypted bool
		valid     bool
		want      string
	}{
		{"unknown remote", map[string]string{"remote": "unknown"}, false, false, "unknown remote"},
		{"empty target", map[string]string{"local": "tank/a,"}, false, false, "empty entry"},
		{"argument injection", map[string]string{"local": "-bad"}, false, false, "invalid ZFS"},
		{"toggle", map[string]string{"compressed": "yes"}, false, false, "on or off"},
		{"mapping", map[string]string{"discard": "both"}, false, false, "none, first, or all"},
		{"reserved set", map[string]string{"set_prop:org.boomerangz:enabled": "on"}, false, false, "reserved"},
		{"reserved ignore", map[string]string{"ignore_prop:org.boomerangz:state:lineage": "off"}, false, false, "reserved"},
		{"raw encryption", map[string]string{"set_prop:encryption": "off"}, true, false, "raw encrypted"},
		{"recursive encryption", map[string]string{"replicate": "on", "raw": "off"}, true, false, "requires raw"},
		{"keylocation", map[string]string{"set_prop:keylocation": "prompt"}, true, true, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dataset := zfs.Dataset{Name: "tank", EncryptionRoot: "-"}
			if tc.encrypted {
				dataset.EncryptionRoot = "tank"
			}
			var rows []zfs.Property
			for key, value := range tc.values {
				rows = append(rows, property("tank", key, value, zfs.SourceLocal))
			}
			p := Resolve(dataset, nil, rows, nil)
			if p.Valid() != tc.valid || !strings.Contains(strings.Join(p.Errors, " "), tc.want) {
				t.Fatalf("errors = %v", p.Errors)
			}
		})
	}
}

func TestReceiveConflictsAndRawFlags(t *testing.T) {
	t.Parallel()
	p := Resolve(zfs.Dataset{Name: "tank", EncryptionRoot: "-"}, nil, []zfs.Property{
		property("tank", "raw", "on", zfs.SourceLocal), property("tank", "compressed", "off", zfs.SourceLocal),
		property("tank", "replicate", "on", zfs.SourceLocal),
		property("tank", "set_prop:compression", "zstd", zfs.SourceLocal), property("tank", "ignore_prop:compression", "on", zfs.SourceLocal),
		property("tank", "ignore_prop:mountpoint", "on", zfs.SourceLocal), property("tank", "ignore_prop:readonly", "off", zfs.SourceLocal),
	}, nil)
	if !p.Valid() || len(p.Warnings) != 2 || p.Requested.Compressed || !p.Send.Compressed || !p.Send.EmbeddedData || !p.Send.Props || p.Requested.Props {
		t.Fatalf("unexpected policy: %#v", p)
	}
	if !reflect.DeepEqual(p.IgnoreProperties, []string{"mountpoint"}) || p.SetProperties["compression"] != "zstd" {
		t.Fatalf("receive conflict unresolved: %#v", p)
	}
}

func TestTargets(t *testing.T) {
	t.Parallel()
	got, err := ParseTargets(" a, b,a ")
	if err != nil || !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatalf("%v, %v", got, err)
	}
}

func TestDiscardInheritance(t *testing.T) {
	t.Parallel()
	for _, choice := range []Discard{DiscardNone, DiscardFirst, DiscardAll} {
		parent := Resolve(zfs.Dataset{Name: "tank"}, nil, []zfs.Property{property("tank", "discard", string(choice), zfs.SourceLocal)}, nil)
		child := Resolve(zfs.Dataset{Name: "tank/child"}, &parent, nil, nil)
		if !parent.Valid() || !child.Valid() || child.Discard != choice {
			t.Fatalf("choice %s did not inherit: %#v", choice, child)
		}
		reset := Resolve(zfs.Dataset{Name: "tank/child"}, &parent, []zfs.Property{property("tank/child", "discard", "none", zfs.SourceLocal)}, nil)
		if !reset.Valid() || reset.Discard != DiscardNone {
			t.Fatal("none did not override inherited discard")
		}
	}
	defaults := Resolve(zfs.Dataset{Name: "tank"}, nil, nil, nil)
	if defaults.Discard != DiscardNone {
		t.Fatal("incorrect default")
	}
}

func FuzzResolve(f *testing.F) {
	f.Add("discard", "first", true)
	f.Add("set_prop:org.boomerangz:enabled", "on", true)
	f.Add("enabled", "on", false)
	f.Fuzz(func(t *testing.T, key, value string, local bool) {
		source := zfs.SourceReceived
		if local {
			source = zfs.SourceLocal
		}
		p := Resolve(zfs.Dataset{Name: "tank"}, nil, []zfs.Property{property("tank", key, value, source)}, nil)
		if !local && (p.Enabled || len(p.SetProperties) > 0 || len(p.Remote) > 0 || !p.Valid()) {
			t.Fatal("received configuration affected policy")
		}
		for key := range p.SetProperties {
			if strings.HasPrefix(key, Namespace) {
				t.Fatal("reserved receive property accepted")
			}
		}
	})
}

func FuzzGrid(f *testing.F) {
	for _, seed := range []string{DefaultGrid, "", "1x1w", "999999999999x1h"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, value string) {
		grid, err := ParseGrid(value)
		if err != nil {
			return
		}
		if grid.Cadence() <= 0 || grid.Horizon() < grid.Cadence() {
			t.Fatal("invalid duration")
		}
		roundtrip, err := ParseGrid(grid.String())
		if err != nil || !reflect.DeepEqual(grid, roundtrip) {
			t.Fatal("roundtrip failed")
		}
	})
}

func FuzzTargets(f *testing.F) {
	f.Add("a,b,a")
	f.Add(",")
	f.Fuzz(func(t *testing.T, value string) {
		targets, err := ParseTargets(value)
		if err != nil {
			return
		}
		seen := map[string]bool{}
		for _, target := range targets {
			if target == "" || seen[target] {
				t.Fatal("invalid target list")
			}
			seen[target] = true
		}
	})
}
