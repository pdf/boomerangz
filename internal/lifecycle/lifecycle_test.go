package lifecycle

import (
	"reflect"
	"testing"
	"time"

	"github.com/pdf/boomerangz/internal/policy"
	"github.com/pdf/boomerangz/internal/zfs"
)

const testLineage = "12345678-1234-4234-8234-123456789abc"
const testInstallation = "abcdefab-cdef-4abc-8def-abcdefabcdef"

func owned(t *testing.T, created time.Time) Snapshot {
	t.Helper()
	m, err := NewMetadata(testLineage, created)
	if err != nil {
		t.Fatal(err)
	}
	s := Snapshot{Name: "tank/data@" + m.Name(), GUID: 1}
	for name, value := range m.Properties() {
		s.Properties = append(s.Properties, zfs.Property{Dataset: s.Name, Name: name, Value: value, Source: zfs.SourceLocal})
	}
	return s
}

func TestOwnership(t *testing.T) {
	t.Parallel()
	s := owned(t, time.Date(2026, 9, 5, 12, 0, 0, 123456789, time.UTC))
	m, err := Ownership(s, testLineage)
	if err != nil || m.Lineage != testLineage {
		t.Fatalf("ownership: %v", err)
	}
	for _, mutate := range []func(*Snapshot){
		func(s *Snapshot) { s.Name = "tank/data@boomerangz-fake" },
		func(s *Snapshot) { s.GUID = 0 },
		func(s *Snapshot) { s.Properties = nil },
		func(s *Snapshot) { s.Properties[0].Dataset = "tank/data" },
		func(s *Snapshot) { s.Properties[0].Source = "inherited" },
		func(s *Snapshot) { s.Properties = append(s.Properties, s.Properties[0]) },
	} {
		modified := s
		modified.Properties = append([]zfs.Property(nil), s.Properties...)
		mutate(&modified)
		if _, err := Ownership(modified, testLineage); err == nil {
			t.Fatalf("accepted invalid ownership: %#v", modified)
		}
	}
	for i := range s.Properties {
		s.Properties[i].Source = zfs.SourceReceived
	}
	if _, err := Ownership(s, testLineage); err != nil {
		t.Fatalf("rejected received internal ownership: %v", err)
	}
}

func TestAdoptRejectsAmbiguousLineages(t *testing.T) {
	t.Parallel()
	s := owned(t, time.Now())
	if got, err := Adopt([]Snapshot{s}); err != nil || got != testLineage {
		t.Fatalf("adoption: %s %v", got, err)
	}
	other := owned(t, time.Now().Add(time.Hour))
	for i, p := range other.Properties {
		if p.Name == LineageProperty {
			other.Properties[i].Value = "87654321-1234-4234-8234-123456789abc"
		}
	}
	if _, err := Adopt([]Snapshot{s, other}); err == nil {
		t.Fatal("ambiguous adoption accepted")
	}
	if _, err := Adopt(nil); err == nil {
		t.Fatal("adopted without proof")
	}
}

func TestPruneBoundariesAndExemptions(t *testing.T) {
	t.Parallel()
	grid, err := policy.ParseGrid("2x5m,1x1h")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	ages := []time.Duration{0, 4 * time.Minute, 5 * time.Minute, 9 * time.Minute, 10 * time.Minute, 69 * time.Minute, 70 * time.Minute, 80 * time.Minute, 90 * time.Minute, 100 * time.Minute}
	var snapshots []Snapshot
	for _, age := range ages {
		snapshots = append(snapshots, owned(t, now.Add(-age)))
	}
	snapshots[7].Holds = []string{"foreign-hold"}
	snapshots[8].Clones = []string{"tank/clone"}
	snapshots[9].ResumeRequired = true
	snapshots = append(snapshots, Snapshot{Name: "tank/data@foreign", GUID: 99})
	plan, err := PlanPrune("tank/data", testLineage, grid, snapshots)
	if err != nil {
		t.Fatal(err)
	}
	wantDestroy := map[string]bool{snapshots[0].Name: true, snapshots[2].Name: true, snapshots[4].Name: true, snapshots[6].Name: true}
	for _, d := range plan {
		if d.Destroy != wantDestroy[d.Snapshot] {
			t.Errorf("unexpected decision: %#v", d)
		}
	}
	// Input order never changes retention choices or output order.
	for i, j := 0, len(snapshots)-1; i < j; i, j = i+1, j-1 {
		snapshots[i], snapshots[j] = snapshots[j], snapshots[i]
	}
	again, err := PlanPrune("tank/data", testLineage, grid, snapshots)
	if err != nil || !reflect.DeepEqual(plan, again) {
		t.Fatal("unstable pruning preview")
	}
	reordered, err := policy.ParseGrid("1x1h,1x5m,1x5m")
	if err != nil {
		t.Fatal(err)
	}
	again, err = PlanPrune("tank/data", testLineage, reordered, snapshots)
	if err != nil || !reflect.DeepEqual(plan, again) {
		t.Fatal("written tier order changed pruning decisions")
	}
}

func TestSnapshotCadence(t *testing.T) {
	t.Parallel()
	grid, _ := policy.ParseGrid("1x5m")
	now := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		age time.Duration
		due bool
	}{{-time.Hour, false}, {5 * time.Minute, false}, {5*time.Minute + time.Nanosecond, true}, {24 * time.Hour, true}} {
		s := owned(t, now.Add(-tc.age))
		due, err := SnapshotDue(now, "tank/data", testLineage, grid, []Snapshot{s})
		if err != nil || due != tc.due {
			t.Fatalf("age %s: due=%v err=%v", tc.age, due, err)
		}
	}
	due, err := SnapshotDue(now, "tank/data", testLineage, grid, nil)
	if err != nil || !due {
		t.Fatal("empty history is not due")
	}
}

func TestPruneFixedMonthYearBoundaries(t *testing.T) {
	t.Parallel()
	grid, err := policy.ParseGrid("1x1y,1x1mo")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2028, 3, 31, 12, 0, 0, 0, time.UTC)
	ages := []int{0, 29, 30, 394, 395}
	snapshots := make([]Snapshot, len(ages))
	want := map[string]bool{}
	for i, days := range ages {
		snapshots[i] = owned(t, now.Add(-time.Duration(days)*24*time.Hour))
		want[snapshots[i].Name] = days == 0 || days == 30 || days == 395
	}
	decisions, err := PlanPrune("tank/data", testLineage, grid, snapshots)
	if err != nil {
		t.Fatal(err)
	}
	for _, decision := range decisions {
		if decision.Destroy != want[decision.Snapshot] {
			t.Fatalf("incorrect fixed-duration boundary: %v", decision)
		}
	}
}

func FuzzOwnership(f *testing.F) {
	f.Add("tank/data@boomerangz-fake", testLineage, "2026-09-05T12:00:00Z")
	f.Fuzz(func(t *testing.T, name, id, created string) {
		s := Snapshot{Name: name, GUID: 1, Properties: []zfs.Property{{Dataset: name, Name: LineageProperty, Value: testLineage, Source: zfs.SourceLocal}, {Dataset: name, Name: SnapshotProperty, Value: id, Source: zfs.SourceLocal}, {Dataset: name, Name: CreatedProperty, Value: created, Source: zfs.SourceLocal}}}
		m, err := Ownership(s, testLineage)
		if err == nil && (!ValidID(m.Snapshot) || m.Lineage != testLineage) {
			t.Fatal("invalid ownership accepted")
		}
	})
}
