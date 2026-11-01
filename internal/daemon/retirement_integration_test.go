package daemon

import (
	"context"
	"encoding/json"
	"net"
	"os/exec"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/pdf/boomerangz/internal/config"
	"github.com/pdf/boomerangz/internal/lifecycle"
	"github.com/pdf/boomerangz/internal/policy"
	replicationrpc "github.com/pdf/boomerangz/internal/replication/rpc"
	"github.com/pdf/boomerangz/internal/zfs"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
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
func (*targetBackend) CheckPermissions(context.Context, string, []string) error { return nil }
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
func (*targetBackend) Snapshot(context.Context, []string, string, map[string]string) error {
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

// retirementFixture is a target at canonical holding one replica the source
// proves it owns, beside a foreign snapshot retirement must leave alone.
func retirementFixture(t *testing.T, canonical string) (*targetBackend, zfs.State, string, lifecycle.Reference) {
	t.Helper()
	const lineage = "22222222-2222-4222-8222-222222222222"
	metadata := lifecycle.Metadata{Lineage: lineage, Snapshot: "33333333-3333-4333-8333-333333333333", Created: time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)}
	encoded, err := json.Marshal(map[string]any{"pool_guid": 90, "mapped": "backup/data", "anchor_guid": 91})
	if err != nil {
		t.Fatal(err)
	}
	bindingProperty := policy.StateNamespace + "target:" + lifecycle.TargetID(canonical)
	sourceState := zfs.State{Objects: []zfs.Object{{Name: "tank/data", Type: "filesystem", GUID: 10}}, Properties: []zfs.Property{{Dataset: "tank/data", Name: bindingProperty, Value: string(encoded), Source: zfs.SourceLocal}, {Dataset: "tank/data", Name: lifecycle.VersionProperty, Value: "2", Source: zfs.SourceLocal}}}
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
			},
		}},
	}
	reference := lifecycle.Reference{TargetID: lifecycle.TargetID(canonical), Dataset: "tank/data", Metadata: metadata}
	// The source snapshot was pruned once checkpointed; its bookmark remains.
	sourceState.Objects = append(sourceState.Objects, zfs.Object{Name: reference.BookmarkName(), Type: "bookmark", GUID: 20})
	return backend, sourceState, lineage, reference
}

func checkRetired(t *testing.T, backend *targetBackend, reference lifecycle.Reference) {
	t.Helper()
	owned := "backup/data@" + reference.Metadata.Name()
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

func TestRetireReferenceDestroysOnlyProvenReplicaAndClearsLineage(t *testing.T) {
	t.Parallel()
	backend, sourceState, lineage, reference := retirementFixture(t, "local:backup/data")
	runtime := &Runtime{backend: backend, installation: "11111111-1111-4111-8111-111111111111", config: config.Defaults()}
	effective := policy.Effective{Local: []string{"backup/data"}, Discard: policy.DiscardOff}
	if err := runtime.retireReference(t.Context(), "tank/data", effective, sourceState, lineage, reference); err != nil {
		t.Fatal(err)
	}
	checkRetired(t, backend, reference)
}

// rpcRemote reaches its target through the real remote replication service,
// so a remote job exercises exactly the operations the protocol carries.
type rpcRemote struct {
	canonical string
	client    *replicationrpc.Client
}

func (r *rpcRemote) CanonicalTarget() string { return r.canonical }
func (*rpcRemote) Transport() string         { return "ssh" }
func (r *rpcRemote) Open(context.Context) (openedRemote, error) {
	return openedRemote{mode: "rpc", executor: r.client.Executor(), close: func() error { return nil }}, nil
}

func newRPCRemote(t *testing.T, canonical string, backend zfs.Executor, root string) *rpcRemote {
	t.Helper()
	service, err := replicationrpc.NewServerWithReceiver(backend, root, func(ctx context.Context, _ []string) *exec.Cmd {
		return exec.CommandContext(ctx, "false")
	})
	if err != nil {
		t.Fatal(err)
	}
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	replicationrpc.RegisterRemoteServiceServer(server, service)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	connection, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	client, err := replicationrpc.NewClient(replicationrpc.NewRemoteServiceClient(connection), nil)
	if err != nil {
		t.Fatal(err)
	}
	return &rpcRemote{canonical: canonical, client: client}
}

func TestRetireReferenceDestroysProvenReplicaOnARemoteTarget(t *testing.T) {
	t.Parallel()
	const canonical = "ssh://offsite.example/backup/data"
	backend, sourceState, lineage, reference := retirementFixture(t, canonical)
	cfg := config.Defaults()
	cfg.Remotes = map[string]config.RemoteConfig{"offsite": {Transport: "ssh", Root: "backup/data"}}
	runtime := &Runtime{backend: &targetBackend{}, installation: "11111111-1111-4111-8111-111111111111", config: cfg, remotes: map[string]remoteClient{"offsite": newRPCRemote(t, canonical, backend, "backup/data")}}
	effective := policy.Effective{Remote: []string{"offsite"}, Discard: policy.DiscardOff}
	if err := runtime.retireReference(t.Context(), "tank/data", effective, sourceState, lineage, reference); err != nil {
		t.Fatal(err)
	}
	checkRetired(t, backend, reference)
}
