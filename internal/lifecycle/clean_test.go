package lifecycle

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/pdf/boomerangz/internal/policy"
	"github.com/pdf/boomerangz/internal/zfs"
)

type testCleanSafety struct {
	offline bool
	busy    bool
}

func (s testCleanSafety) Quiescent(context.Context, []string) error {
	if s.busy {
		return fmt.Errorf("active job")
	}
	return nil
}
func (s testCleanSafety) CheckTarget(context.Context, string, string) error {
	if s.offline {
		return fmt.Errorf("target inaccessible")
	}
	return nil
}

func TestCleanPreviewApply(t *testing.T) {
	t.Parallel()
	for _, destroy := range []bool{false, true} {
		b := backendWithSnapshots(t)
		addTestAuthority(b)
		b.state.Properties = append(b.state.Properties, zfs.Property{Dataset: "tank/data", Name: policy.Namespace + "enabled", Value: "on", Source: zfs.SourceLocal})
		s, _ := NewService(b, testInstallation)
		r, err := s.Protect(t.Context(), "tank/data", b.state.Objects[1].Name, "local:backup/data")
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Checkpoint(t.Context(), "tank/data", r, r.GUID); err != nil {
			t.Fatal(err)
		}
		b.writes = nil
		options := CleanOptions{DestroyOwnedSnapshots: destroy}
		preview, err := s.Clean(t.Context(), "tank/data", options, false, testCleanSafety{})
		if err != nil || len(preview.Blockers) != 0 || len(b.writes) != 0 {
			t.Fatalf("preview: %v %v", preview, err)
		}
		applied, err := s.Clean(t.Context(), "tank/data", options, true, testCleanSafety{})
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(preview.Actions, applied.Actions) || applied.Applied != len(preview.Actions) {
			t.Fatal("preview/apply mismatch")
		}
		if len(b.state.Properties) != 0 || len(b.state.Holds[r.snapshot()]) != 0 {
			t.Fatal("retained effective metadata or hold")
		}
		want := 3
		if destroy {
			want = 0
		}
		if len(snapshotsIn(b.state, "tank/data")) != want {
			t.Fatal("incorrect snapshot preservation")
		}
	}
}

func TestCleanBlockers(t *testing.T) {
	t.Parallel()
	for _, reason := range []string{"busy", "resume", "hold", "bookmark", "ancestor", "offline", "hidden-lineage", "snapshot-lineage"} {
		t.Run(reason, func(t *testing.T) {
			b := backendWithSnapshots(t)
			addTestAuthority(b)
			s, _ := NewService(b, testInstallation)
			safety := testCleanSafety{}
			switch reason {
			case "busy":
				safety.busy = true
			case "resume":
				b.state.ResumeTokens = map[string]string{"tank/data": "token"}
			case "hold":
				b.state.Holds = map[string][]string{b.state.Objects[1].Name: {SnapshotPrefix + "unproven"}}
			case "bookmark":
				b.state.Objects = append(b.state.Objects, zfs.Object{Name: "tank/data#boomerangz-unproven", Type: "bookmark", GUID: 10})
			case "ancestor":
				b.state.Properties = append(b.state.Properties, zfs.Property{Dataset: "tank", Name: policy.Namespace + "enabled", Value: "on", Source: zfs.SourceLocal})
			case "offline":
				if _, err := s.Protect(t.Context(), "tank/data", b.state.Objects[1].Name, "ssh:offline"); err != nil {
					t.Fatal(err)
				}
				b.writes = nil
				safety.offline = true
			case "hidden-lineage":
				b.state.Received = map[string]map[string]string{"tank/data": {LineageProperty: "conflicting"}}
			case "snapshot-lineage":
				for i, p := range b.state.Properties {
					if p.Dataset == b.state.Objects[1].Name && p.Name == LineageProperty {
						b.state.Properties[i].Value = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
					}
				}
			}
			plan, err := s.Clean(t.Context(), "tank/data", CleanOptions{}, true, safety)
			if err == nil || len(plan.Blockers) == 0 || len(b.writes) != 0 {
				t.Fatalf("unsafe clean: %v %v", plan, err)
			}
		})
	}
}

func TestCleanPreservesNonNamespaceSettings(t *testing.T) {
	t.Parallel()
	b := backendWithSnapshots(t)
	b.state.Properties = append(b.state.Properties, zfs.Property{Dataset: "tank/data", Name: "compression", Value: "zstd", Source: zfs.SourceLocal})
	s, _ := NewService(b, testInstallation)
	if _, err := s.Clean(t.Context(), "tank/data", CleanOptions{}, true, testCleanSafety{}); err != nil {
		t.Fatal(err)
	}
	if len(b.state.Properties) != 1 || b.state.Properties[0].Name != "compression" {
		t.Fatal("changed nonnamespace property")
	}
}

func TestCleanDoesNotTrustReceivedReferences(t *testing.T) {
	t.Parallel()
	b := backendWithSnapshots(t)
	key := ReferencePrefix + "foreign"
	b.state.Properties = append(b.state.Properties, zfs.Property{Dataset: "tank/data", Name: key, Value: "received proof", Source: zfs.SourceReceived})
	b.state.Received = map[string]map[string]string{"tank/data": {key: "received proof"}}
	s, _ := NewService(b, testInstallation)
	if _, err := s.Clean(t.Context(), "tank/data", CleanOptions{}, true, testCleanSafety{}); err != nil {
		t.Fatal(err)
	}
	if len(b.state.Properties) != 0 || b.state.Received["tank/data"][key] != "received proof" {
		t.Fatal("unexpected received-property handling")
	}
}

func TestCleanStopsOnUnexpectedMutation(t *testing.T) {
	t.Parallel()
	b := backendWithSnapshots(t)
	addTestAuthority(b)
	b.beforeRead = func(b *memoryBackend) {
		if b.reads == 4 {
			b.state.Objects[0].GUID++
		}
	}
	s, _ := NewService(b, testInstallation)
	plan, err := s.Clean(t.Context(), "tank/data", CleanOptions{}, true, testCleanSafety{})
	if err == nil || plan.Applied != 1 || len(b.writes) != 1 {
		t.Fatalf("continued after replacement: %v %v", plan, err)
	}
}
