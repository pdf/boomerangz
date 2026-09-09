package transfer

import (
	"slices"
	"strings"
	"testing"

	"github.com/pdf/boomerangz/internal/zfs"
)

func TestReseedDestroysPartialReplicaAndReleasesRecoveryState(t *testing.T) {
	t.Parallel()
	backend, request := newLocalBackend(t)
	engine, err := NewLocal(backend, localTestStream{backend: backend, fail: true}, fixtureInstallation)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Apply(t.Context(), request, nil); err == nil {
		t.Fatal("injected initial transfer failure succeeded")
	}
	backend.destExists = true
	backend.inventory = append(backend.inventory, zfs.Dataset{Name: "backup/data", Type: zfs.Filesystem})
	backend.destination = zfs.State{
		Objects:      []zfs.Object{{Name: "backup/data", Type: "filesystem", GUID: 20, CreateTXG: 20}},
		ResumeTokens: map[string]string{"backup/data": "resume-token"},
	}
	service, err := NewReseedService(backend, backend, fixtureInstallation)
	if err != nil {
		t.Fatal(err)
	}
	preview, err := service.Plan(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if !preview.DestinationExists || len(preview.ResumeDatasets) != 1 || len(preview.References) != 1 || preview.Applied {
		t.Fatalf("preview=%+v", preview)
	}
	result, err := service.Apply(t.Context(), request)
	if err != nil {
		t.Fatalf("apply=%+v err=%v writes=%v", result, err, backend.writes)
	}
	retainedHold := false
	for _, holds := range backend.source.Holds {
		retainedHold = retainedHold || len(holds) > 0
	}
	if !result.Applied || backend.destExists || retainedHold {
		t.Fatalf("result=%+v destination=%v holds=%v", result, backend.destExists, backend.source.Holds)
	}
	for _, property := range backend.source.Properties {
		if strings.HasPrefix(property.Name, targetBindingPrefix) || strings.HasPrefix(property.Name, "org.boomerangz:state:reference:") {
			t.Fatalf("reseed retained source target state: %+v", property)
		}
	}
	writes := strings.Join(backend.writes, "\n")
	if !strings.Contains(writes, "abort backup/data\ndestroy-dataset backup/data") {
		t.Fatalf("partial receive was not abandoned before destruction: %v", backend.writes)
	}
}

func TestReseedRejectsChangedDestinationPool(t *testing.T) {
	t.Parallel()
	backend, request := newLocalBackend(t)
	plan, err := Build(request, View{Inventory: backend.inventory, Source: backend.source, DestinationIdentity: zfs.DatasetIdentity{Name: "backup", Type: zfs.Filesystem, GUID: 10, Pool: "backup", PoolGUID: 11}}, fixtureInstallation)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := encodeBinding(plan.TargetBinding)
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.SetProperties(t.Context(), request.Source, map[string]string{targetBindingProperty(plan.TargetBinding.CanonicalTarget): encoded}); err != nil {
		t.Fatal(err)
	}
	backend.identity = &zfs.DatasetIdentity{Name: "backup", Type: zfs.Filesystem, GUID: 99, Pool: "backup", PoolGUID: 100}
	service, _ := NewReseedService(backend, backend, fixtureInstallation)
	if _, err := service.Plan(t.Context(), request); err == nil || !strings.Contains(err.Error(), "identity differs") {
		t.Fatalf("changed pool accepted: %v", err)
	}
}

func TestReseedAllowsExplicitCleanupBeforeBindingWasStored(t *testing.T) {
	t.Parallel()
	backend, request := newLocalBackend(t)
	backend.destExists = true
	backend.inventory = append(backend.inventory, zfs.Dataset{Name: "backup/data", Type: zfs.Filesystem})
	backend.destination = zfs.State{Objects: []zfs.Object{{Name: "backup/data", Type: "filesystem", GUID: 20, CreateTXG: 20}}}
	service, _ := NewReseedService(backend, backend, fixtureInstallation)
	preview, err := service.Plan(t.Context(), request)
	if err != nil || preview.BindingStored || preview.Binding.Anchor != "backup/data" || preview.ReceiveAnchor != "backup" || !preview.DestinationExists {
		t.Fatalf("unbound preview=%+v err=%v", preview, err)
	}
	result, err := service.Apply(t.Context(), request)
	if err != nil || !result.Applied || backend.destExists {
		t.Fatalf("unbound apply=%+v err=%v", result, err)
	}
	if !slices.Equal(backend.checks["backup"], []string{"canmount", "create", "destroy", "mount", "receive:append", "userprop"}) {
		t.Fatalf("fresh receive permissions=%v", backend.checks["backup"])
	}
}
