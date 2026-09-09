package daemon

import (
	"context"
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/pdf/boomerangz/internal/config"
	"github.com/pdf/boomerangz/internal/lifecycle"
	"github.com/pdf/boomerangz/internal/policy"
	"github.com/pdf/boomerangz/internal/transfer"
	"github.com/pdf/boomerangz/internal/zfs"
)

type targetBackend struct {
	datasets   []zfs.Dataset
	identities map[string]zfs.DatasetIdentity
	states     map[string]zfs.State
	destroyed  []string
}

func (b *targetBackend) ListDatasets(context.Context) ([]zfs.Dataset, error) {
	return slices.Clone(b.datasets), nil
}
func (b *targetBackend) InspectDatasetIdentity(_ context.Context, dataset string) (zfs.DatasetIdentity, error) {
	return b.identities[dataset], nil
}
func (*targetBackend) GetActivationProperties(context.Context) ([]zfs.Property, error) {
	return nil, nil
}
func (*targetBackend) GetStoredProperties(context.Context, []string) ([]zfs.Property, error) {
	return nil, nil
}
func (b *targetBackend) InspectState(_ context.Context, dataset string, _ bool) (zfs.State, error) {
	return b.states[dataset], nil
}
func (*targetBackend) SetProperties(context.Context, string, map[string]string) error { return nil }
func (*targetBackend) CreateReceiveParent(context.Context, string) error              { return nil }
func (b *targetBackend) InheritProperty(_ context.Context, object, property string) error {
	state := b.states[object]
	state.Properties = slices.DeleteFunc(state.Properties, func(row zfs.Property) bool {
		return row.Dataset == object && row.Name == property
	})
	b.states[object] = state
	return nil
}
func (*targetBackend) Snapshot(context.Context, string, string, bool, map[string]string) error {
	return nil
}
func (b *targetBackend) DestroySnapshot(_ context.Context, snapshot string) error {
	dataset, _, _ := strings.Cut(snapshot, "@")
	state := b.states[dataset]
	state.Objects = slices.DeleteFunc(state.Objects, func(object zfs.Object) bool { return object.Name == snapshot })
	state.Properties = slices.DeleteFunc(state.Properties, func(row zfs.Property) bool { return row.Dataset == snapshot })
	b.states[dataset] = state
	b.destroyed = append(b.destroyed, snapshot)
	return nil
}
func (*targetBackend) Bookmark(context.Context, string, string) error { return nil }
func (*targetBackend) DestroyBookmark(context.Context, string) error  { return nil }
func (*targetBackend) Hold(context.Context, string, string) error     { return nil }
func (*targetBackend) Release(context.Context, string, string) error  { return nil }
func (*targetBackend) EstimateSend(context.Context, zfs.SendOptions) (zfs.Estimate, error) {
	return zfs.Estimate{}, nil
}

func TestRetireReferenceDestroysOnlyProvenReplicaAndClearsLineage(t *testing.T) {
	t.Parallel()
	const installation = "11111111-1111-4111-8111-111111111111"
	const lineage = "22222222-2222-4222-8222-222222222222"
	metadata := lifecycle.Metadata{Lineage: lineage, Snapshot: "33333333-3333-4333-8333-333333333333", Created: time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)}
	canonical := "local:backup/data"
	binding := transfer.TargetBinding{Version: 1, Transport: "local", CanonicalTarget: canonical, DestinationRoot: "backup/data", MappedDataset: "backup/data", Pool: "backup", PoolGUID: 90, Anchor: "backup/data", AnchorGUID: 91}
	encoded, err := json.Marshal(binding)
	if err != nil {
		t.Fatal(err)
	}
	bindingProperty := policy.StateNamespace + "target:" + lifecycle.TargetID(canonical)
	sourceState := zfs.State{Objects: []zfs.Object{{Name: "tank/data", Type: "filesystem", GUID: 10}}, Properties: []zfs.Property{{Dataset: "tank/data", Name: bindingProperty, Value: string(encoded), Source: zfs.SourceLocal}}}
	owned := "backup/data@" + metadata.Name()
	backend := &targetBackend{
		datasets:   []zfs.Dataset{{Name: "backup/data", Type: zfs.Filesystem}},
		identities: map[string]zfs.DatasetIdentity{"backup/data": {Name: "backup/data", Type: zfs.Filesystem, GUID: 91, Pool: "backup", PoolGUID: 90}},
		states: map[string]zfs.State{"backup/data": {
			Objects: []zfs.Object{{Name: "backup/data", Type: "filesystem", GUID: 91}, {Name: owned, Type: "snapshot", GUID: 20}, {Name: "backup/data@foreign", Type: "snapshot", GUID: 21}},
			Properties: []zfs.Property{
				{Dataset: "backup/data", Name: lifecycle.LineageProperty, Value: lineage, Source: zfs.SourceLocal},
				{Dataset: owned, Name: lifecycle.LineageProperty, Value: lineage, Source: zfs.SourceLocal},
				{Dataset: owned, Name: lifecycle.SnapshotProperty, Value: metadata.Snapshot, Source: zfs.SourceLocal},
				{Dataset: owned, Name: lifecycle.CreatedProperty, Value: metadata.Created.Format(time.RFC3339Nano), Source: zfs.SourceLocal},
			},
		}},
	}
	runtime := &Runtime{backend: backend, installation: installation, config: config.Defaults()}
	effective := policy.Effective{Local: []string{"backup/data"}, Discard: policy.DiscardOff}
	reference := lifecycle.Reference{Target: canonical, Dataset: "tank/data", GUID: 20, Metadata: metadata}
	if err := runtime.retireReference(t.Context(), "tank/data", effective, sourceState, lineage, reference); err != nil {
		t.Fatal(err)
	}
	state := backend.states["backup/data"]
	if !reflect.DeepEqual(backend.destroyed, []string{owned}) || slices.ContainsFunc(state.Objects, func(object zfs.Object) bool { return object.Name == owned }) || !slices.ContainsFunc(state.Objects, func(object zfs.Object) bool { return object.Name == "backup/data@foreign" }) {
		t.Fatalf("unexpected target retirement: destroyed=%v state=%#v", backend.destroyed, state)
	}
	if slices.ContainsFunc(state.Properties, func(row zfs.Property) bool {
		return row.Dataset == "backup/data" && row.Name == lifecycle.LineageProperty
	}) {
		t.Fatal("retired destination lineage remained local")
	}
}
