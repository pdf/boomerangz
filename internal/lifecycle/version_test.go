package lifecycle

import (
	"strings"
	"testing"
	"time"

	"github.com/pdf/boomerangz/internal/zfs"
)

// A root's state is read only in the format it says it is in. A root from
// before the marker existed is the first format, and must be migrated.
func TestRootAuthorityReadsOnlyTheCurrentFormat(t *testing.T) {
	t.Parallel()
	const owner = "11111111-1111-4111-8111-111111111111"
	const lineage = "22222222-2222-4222-8222-222222222222"
	for name, test := range map[string]struct {
		version  []zfs.Property
		received string
		want     string
	}{
		"current":           {version: []zfs.Property{{Value: "2", Source: zfs.SourceLocal}}},
		"unmarked":          {want: "format v1, and this build reads v2; stop the daemon and run boomerangz migrate tank/data"},
		"only received":     {received: "2", want: "format v1"},
		"older":             {version: []zfs.Property{{Value: "1", Source: zfs.SourceLocal}}, want: "format v1"},
		"newer":             {version: []zfs.Property{{Value: "3", Source: zfs.SourceLocal}}, want: "newer than the v2 this build reads; upgrade boomerangz"},
		"leading zero":      {version: []zfs.Property{{Value: "02", Source: zfs.SourceLocal}}, want: "invalid"},
		"zero":              {version: []zfs.Property{{Value: "0", Source: zfs.SourceLocal}}, want: "invalid"},
		"not a number":      {version: []zfs.Property{{Value: "v2", Source: zfs.SourceLocal}}, want: "invalid"},
		"duplicate":         {version: []zfs.Property{{Value: "2", Source: zfs.SourceLocal}, {Value: "2", Source: zfs.SourceLocal}}, want: "invalid"},
		"received and ours": {version: []zfs.Property{{Value: "2", Source: zfs.SourceLocal}}, received: "1"},
	} {
		t.Run(name, func(t *testing.T) {
			state := zfs.State{Properties: []zfs.Property{
				{Dataset: "tank/data", Name: OwnerProperty, Value: owner, Source: zfs.SourceLocal},
				{Dataset: "tank/data", Name: LineageProperty, Value: lineage, Source: zfs.SourceLocal},
			}}
			for _, row := range test.version {
				row.Dataset, row.Name = "tank/data", VersionProperty
				state.Properties = append(state.Properties, row)
			}
			if test.received != "" {
				state.Properties = append(state.Properties, zfs.Property{Dataset: "tank/data", Name: VersionProperty, Value: test.received, Source: zfs.SourceReceived})
			}
			got, err := RootAuthority(state, "tank/data", owner)
			if test.want == "" {
				if err != nil || got != lineage {
					t.Fatalf("lineage=%s err=%v", got, err)
				}
			} else if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("err=%v, want %q", err, test.want)
			}
		})
	}
}

// Authority is written with the format its state is written in, whether a
// root is new or adopted.
func TestAuthorityIsWrittenWithTheCurrentFormat(t *testing.T) {
	t.Parallel()
	version := func(b *memoryBackend) string {
		for _, p := range b.state.Properties {
			if p.Dataset == "tank/data" && p.Name == VersionProperty && p.Source == zfs.SourceLocal {
				return p.Value
			}
		}
		return ""
	}
	t.Run("new root", func(t *testing.T) {
		b := &memoryBackend{state: zfs.State{Objects: []zfs.Object{{Name: "tank/data", Type: "filesystem", GUID: 1}}}}
		s, _ := NewService(b, testInstallation)
		if _, err := s.CreateSnapshot(t.Context(), "tank/data", []string{"tank/data"}, time.Now(), activeTestPolicy("tank/data")); err != nil {
			t.Fatal(err)
		}
		if got := version(b); got != "2" {
			t.Fatalf("version=%q", got)
		}
	})
	t.Run("adopted root", func(t *testing.T) {
		b := backendWithSnapshots(t)
		s, _ := NewService(b, testInstallation)
		if _, err := s.AdoptDataset(t.Context(), "tank/data", activeTestPolicy("tank/data")); err != nil {
			t.Fatal(err)
		}
		if got := version(b); got != "2" {
			t.Fatalf("version=%q", got)
		}
	})
	// An unmarked root keeping records of its own is in the first format, and
	// adoption would vouch for them as the current one.
	t.Run("unmigrated records", func(t *testing.T) {
		b := backendWithSnapshots(t)
		b.state.Properties = append(b.state.Properties, zfs.Property{Dataset: "tank/data", Name: TargetStatePrefix + TargetID("local:backup"), Value: `{"version":1}`, Source: zfs.SourceLocal})
		s, _ := NewService(b, testInstallation)
		b.writes = nil
		if _, err := s.AdoptDataset(t.Context(), "tank/data", activeTestPolicy("tank/data")); err == nil || !strings.Contains(err.Error(), "boomerangz migrate") || len(b.writes) != 0 {
			t.Fatalf("adopted unmigrated records: err=%v writes=%v", err, b.writes)
		}
	})
}
