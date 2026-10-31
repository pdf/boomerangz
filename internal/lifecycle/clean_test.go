package lifecycle

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"strings"
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
		if err := s.Checkpoint(t.Context(), "tank/data", r, guidOf(b, r.SnapshotName())); err != nil {
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

func TestCleanTransitionNamesWhatDiffered(t *testing.T) {
	t.Parallel()
	property := zfs.Property{Dataset: "tank/data@a", Name: "org.boomerangz:state:snapshot", Value: "2026", Source: zfs.SourceLocal}
	before := zfs.State{Objects: []zfs.Object{{Name: "tank/data@a", Type: "snapshot", GUID: 1}}, Properties: []zfs.Property{property}}
	action := CleanAction{Operation: "inherit", Object: "tank/data@a", Property: property.Name}
	after := zfs.State{Objects: before.Objects}
	if difference := cleanTransition(before, after, action); difference != "" {
		t.Fatalf("the expected transition was reported as %q", difference)
	}
	stray := zfs.Property{Dataset: "tank/data@a", Name: "org.boomerangz:state:lineage", Value: "x", Source: zfs.SourceLocal}
	after.Properties = []zfs.Property{stray}
	if difference := cleanTransition(before, after, action); !strings.Contains(difference, "state:lineage") || !strings.Contains(difference, "appeared") {
		t.Fatalf("difference = %q, want the property that appeared", difference)
	}
}

// Forgetting a target removes that target's state and nothing else, without
// checking the target: it is for a target that can no longer be checked.
func TestCleanForgetsOnlyTheNamedTarget(t *testing.T) {
	t.Parallel()
	const gone, kept = "ssh://gone/backup", "local:backup/data"
	setup := func(t *testing.T) (*memoryBackend, *Service) {
		t.Helper()
		b := backendWithSnapshots(t)
		addTestAuthority(b)
		b.state.Properties = append(b.state.Properties, zfs.Property{Dataset: "tank/data", Name: policy.Namespace + "enabled", Value: "on", Source: zfs.SourceLocal})
		s, _ := NewService(b, testInstallation)
		for _, target := range []string{gone, kept} {
			r, err := s.Protect(t.Context(), "tank/data", b.state.Objects[1].Name, target)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Checkpoint(t.Context(), "tank/data", r, guidOf(b, r.SnapshotName())); err != nil {
				t.Fatal(err)
			}
			b.state.Properties = append(b.state.Properties, zfs.Property{Dataset: "tank/data", Name: TargetStatePrefix + TargetID(target), Value: "{}", Source: zfs.SourceLocal})
		}
		b.state.Properties = append(b.state.Properties,
			zfs.Property{Dataset: "tank/data", Name: TargetStatePrefix + TargetID(gone) + ":suspended", Value: "unverified", Source: zfs.SourceLocal},
			// The sending installation's binding for the same target is not this root's.
			zfs.Property{Dataset: "tank/data", Name: TargetStatePrefix + TargetID(gone), Value: "{}", Source: zfs.SourceReceived})
		b.writes = nil
		return b, s
	}
	mentions := func(b *memoryBackend, id string) []string {
		var found []string
		for _, p := range b.state.Properties {
			if p.Source == zfs.SourceLocal && strings.Contains(p.Name, id) {
				found = append(found, p.Name)
			}
		}
		for _, o := range b.state.Objects {
			if strings.Contains(o.Name, id) {
				found = append(found, o.Name)
			}
		}
		for snapshot, tags := range b.state.Holds {
			for _, tag := range tags {
				if strings.Contains(tag, id) {
					found = append(found, snapshot+" "+tag)
				}
			}
		}
		return found
	}

	t.Run("forgets", func(t *testing.T) {
		b, s := setup(t)
		// Control arm: an ordinary clean cannot get past the gone target.
		if plan, _ := s.Clean(t.Context(), "tank/data", CleanOptions{}, false, testCleanSafety{offline: true}); len(plan.Blockers) == 0 {
			t.Fatal("an unreachable target did not block clean")
		}
		keptBefore := mentions(b, TargetID(kept))
		if len(mentions(b, TargetID(gone))) < 4 || len(keptBefore) < 4 {
			t.Fatalf("fixture: gone=%v kept=%v", mentions(b, TargetID(gone)), keptBefore)
		}
		snapshots := len(snapshotsIn(b.state, "tank/data"))
		options := CleanOptions{ForgetTargets: []string{TargetID(gone)}}
		preview, err := s.Clean(t.Context(), "tank/data", options, false, testCleanSafety{offline: true})
		if err != nil || len(preview.Blockers) != 0 || len(b.writes) != 0 {
			t.Fatalf("preview=%+v err=%v writes=%v", preview, err, b.writes)
		}
		// One inherit per local property: the received binding gets none.
		if !slices.EqualFunc(preview.Actions, slices.CompactFunc(slices.Clone(preview.Actions), func(a, b CleanAction) bool { return a == b }), func(a, b CleanAction) bool { return a == b }) {
			t.Fatalf("repeated actions: %+v", preview.Actions)
		}
		applied, err := s.Clean(t.Context(), "tank/data", options, true, testCleanSafety{offline: true})
		if err != nil || applied.Applied != len(preview.Actions) {
			t.Fatalf("applied=%+v err=%v", applied, err)
		}
		if left := mentions(b, TargetID(gone)); len(left) != 0 {
			t.Fatalf("forgotten target's state remains: %v", left)
		}
		if after := mentions(b, TargetID(kept)); !reflect.DeepEqual(after, keptBefore) {
			t.Fatalf("other target's state changed: %v, was %v", after, keptBefore)
		}
		if lineage, err := storedLineage(b.state, "tank/data"); err != nil || lineage == "" || len(snapshotsIn(b.state, "tank/data")) != snapshots {
			t.Fatalf("ownership or snapshots changed: lineage=%q err=%v", lineage, err)
		}
		if b.state.Received != nil && b.state.Received["tank/data"] != nil {
			t.Fatal("received state was touched")
		}
	})
	t.Run("refuses", func(t *testing.T) {
		for name, test := range map[string]struct {
			options CleanOptions
			prepare func(*memoryBackend)
			blocked bool
		}{
			"unknown target": {options: CleanOptions{ForgetTargets: []string{TargetID("local:elsewhere")}}, blocked: true},
			"unproven hold": {options: CleanOptions{ForgetTargets: []string{TargetID(gone)}}, blocked: true, prepare: func(b *memoryBackend) {
				b.state.Holds[b.state.Objects[2].Name] = append(b.state.Holds[b.state.Objects[2].Name], SnapshotPrefix+TargetID(gone))
			}},
			"busy":      {options: CleanOptions{ForgetTargets: []string{TargetID(gone)}}, blocked: true, prepare: func(b *memoryBackend) { b.state.ResumeTokens = map[string]string{"tank/data": "token"} }},
			"recursive": {options: CleanOptions{Recursive: true, ForgetTargets: []string{TargetID(gone)}}},
			"destroy":   {options: CleanOptions{DestroyOwnedSnapshots: true, ForgetTargets: []string{TargetID(gone)}}},
			"not an ID": {options: CleanOptions{ForgetTargets: []string{gone}}},
		} {
			t.Run(name, func(t *testing.T) {
				b, s := setup(t)
				if test.prepare != nil {
					test.prepare(b)
				}
				plan, err := s.Clean(t.Context(), "tank/data", test.options, true, testCleanSafety{offline: true})
				if err == nil || len(b.writes) != 0 || test.blocked != (len(plan.Blockers) > 0) {
					t.Fatalf("plan=%+v err=%v writes=%v", plan, err, b.writes)
				}
			})
		}
	})
}
