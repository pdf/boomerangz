package lifecycle

import (
	"slices"
	"testing"
	"time"

	"github.com/pdf/boomerangz/internal/zfs"
)

func TestInactiveGraceLifecycle(t *testing.T) {
	t.Parallel()
	b := backendWithSnapshots(t)
	addTestAuthority(b)
	s, err := NewService(b, testInstallation)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	preview, err := s.ReconcileInactive(t.Context(), "tank/data", false, now, 24*time.Hour, false)
	if err != nil || preview.Action != "set-inactive-marker" || preview.Applied || preview.Deadline == nil || !preview.Deadline.Equal(now.Add(24*time.Hour)) || len(b.writes) != 0 {
		t.Fatalf("preview=%+v err=%v writes=%v", preview, err, b.writes)
	}
	applied, err := s.ReconcileInactive(t.Context(), "tank/data", false, now, 24*time.Hour, true)
	if err != nil || !applied.Applied {
		t.Fatalf("apply=%+v err=%v", applied, err)
	}
	due, err := s.ReconcileInactive(t.Context(), "tank/data", false, now.Add(24*time.Hour), 24*time.Hour, false)
	if err != nil || !due.Due || due.Action != "" {
		t.Fatalf("due=%+v err=%v", due, err)
	}
	cleared, err := s.ReconcileInactive(t.Context(), "tank/data", true, now.Add(25*time.Hour), 24*time.Hour, true)
	if err != nil || !cleared.Applied {
		t.Fatalf("clear=%+v err=%v", cleared, err)
	}
	status, err := s.ReconcileInactive(t.Context(), "tank/data", true, now.Add(25*time.Hour), 24*time.Hour, false)
	if err != nil || status.Marker != nil || status.Action != "" {
		t.Fatalf("active status=%+v err=%v", status, err)
	}
}

func TestRetirePreservesPolicyAndForeignSnapshots(t *testing.T) {
	t.Parallel()
	b := backendWithSnapshots(t)
	addTestAuthority(b)
	b.state.Properties = append(b.state.Properties, zfs.Property{Dataset: "tank/data", Name: "org.boomerangz:enabled", Value: "off", Source: zfs.SourceLocal})
	b.state.Objects = append(b.state.Objects, zfs.Object{Name: "tank/data@foreign", Type: "snapshot", GUID: 99})
	s, _ := NewService(b, testInstallation)
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	if _, err := s.ReconcileInactive(t.Context(), "tank/data", false, now, 24*time.Hour, true); err != nil {
		t.Fatal(err)
	}
	b.writes = nil
	waiting, err := s.Retire(t.Context(), "tank/data", false, now.Add(23*time.Hour), 24*time.Hour, false, testCleanSafety{})
	if err != nil || waiting.Eligible || len(waiting.Clean.Actions) != 0 {
		t.Fatalf("waiting=%+v err=%v", waiting, err)
	}
	preview, err := s.Retire(t.Context(), "tank/data", false, now.Add(24*time.Hour), 24*time.Hour, false, testCleanSafety{})
	if err != nil || !preview.Eligible || len(preview.Clean.Blockers) != 0 || len(b.writes) != 0 {
		t.Fatalf("preview=%+v err=%v writes=%v", preview, err, b.writes)
	}
	for _, action := range preview.Clean.Actions {
		if action.Operation == "inherit" && action.Property == "org.boomerangz:enabled" {
			t.Fatal("retirement planned removal of public policy")
		}
	}
	applied, err := s.Retire(t.Context(), "tank/data", false, now.Add(24*time.Hour), 24*time.Hour, true, testCleanSafety{})
	if err != nil || applied.Clean.Applied != len(preview.Clean.Actions) {
		t.Fatalf("applied=%+v err=%v", applied, err)
	}
	if !slices.ContainsFunc(b.state.Properties, func(property zfs.Property) bool {
		return property.Dataset == "tank/data" && property.Name == "org.boomerangz:enabled" && property.Value == "off"
	}) {
		t.Fatal("retirement removed the inactive public policy")
	}
	if !slices.ContainsFunc(b.state.Objects, func(object zfs.Object) bool { return object.Name == "tank/data@foreign" }) || len(snapshotsIn(b.state, "tank/data")) != 1 {
		t.Fatal("retirement did not preserve exactly the foreign snapshot")
	}
}

func TestInactiveGraceZeroDisablesAutomaticRetirement(t *testing.T) {
	t.Parallel()
	b := backendWithSnapshots(t)
	addTestAuthority(b)
	s, _ := NewService(b, testInstallation)
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	if _, err := s.ReconcileInactive(t.Context(), "tank/data", false, now, 0, true); err != nil {
		t.Fatal(err)
	}
	status, err := s.ReconcileInactive(t.Context(), "tank/data", false, now.Add(365*24*time.Hour), 0, false)
	if err != nil || status.Automatic || status.Due || status.Deadline != nil {
		t.Fatalf("status=%+v err=%v", status, err)
	}
}

func TestInactiveMarkerRejectsChangedAuthority(t *testing.T) {
	t.Parallel()
	b := backendWithSnapshots(t)
	addTestAuthority(b)
	s, _ := NewService(b, testInstallation)
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	if _, err := s.ReconcileInactive(t.Context(), "tank/data", false, now, time.Hour, true); err != nil {
		t.Fatal(err)
	}
	b.state.Objects[0].GUID++
	if _, err := s.ReconcileInactive(t.Context(), "tank/data", false, now.Add(time.Hour), time.Hour, false); err == nil {
		t.Fatal("accepted inactive marker after dataset replacement")
	}
	b.state.Objects[0].GUID--
	b.state.Received = map[string]map[string]string{"tank/data": {InactiveProperty: "foreign"}}
	if _, err := s.ReconcileInactive(t.Context(), "tank/data", false, now.Add(time.Hour), time.Hour, false); err == nil {
		t.Fatal("accepted received inactive marker")
	}
}

func TestInactiveMarkerRejectsTrailingJSON(t *testing.T) {
	t.Parallel()
	b := backendWithSnapshots(t)
	addTestAuthority(b)
	b.state.Properties = append(b.state.Properties, zfs.Property{
		Dataset: "tank/data",
		Name:    InactiveProperty,
		Value:   `{"version":1,"since":"2026-09-06T12:00:00Z","dataset_guid":1,"lineage":"00000000-0000-4000-8000-000000000001","owner":"00000000-0000-4000-8000-000000000002"} true`,
		Source:  zfs.SourceLocal,
	})
	s, _ := NewService(b, testInstallation)
	if _, err := s.ReconcileInactive(t.Context(), "tank/data", false, time.Date(2026, 9, 6, 13, 0, 0, 0, time.UTC), time.Hour, false); err == nil {
		t.Fatal("accepted trailing inactive marker data")
	}
}

func TestInactiveMarkerRevalidatesBeforeWrite(t *testing.T) {
	t.Parallel()
	b := backendWithSnapshots(t)
	addTestAuthority(b)
	b.beforeRead = func(backend *memoryBackend) {
		if backend.reads == 2 {
			backend.state.Objects[0] = zfs.Object{Name: "tank/data", Type: "filesystem", GUID: 999}
		}
	}
	s, _ := NewService(b, testInstallation)
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	if _, err := s.ReconcileInactive(t.Context(), "tank/data", false, now, 24*time.Hour, true); err == nil || len(b.writes) != 0 {
		t.Fatalf("changed source was mutated: err=%v writes=%v", err, b.writes)
	}
}
