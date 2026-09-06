package transfer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/pdf/boomerangz/internal/lifecycle"
	"github.com/pdf/boomerangz/internal/zfs"
)

type localBackend struct {
	zfs.Executor
	inventory   []zfs.Dataset
	source      zfs.State
	destination zfs.State
	destExists  bool
	writes      []string
}

func cloneState(state zfs.State) (zfs.State, error) {
	var cloned zfs.State
	data, err := json.Marshal(state)
	if err == nil {
		err = json.Unmarshal(data, &cloned)
	}
	return cloned, err
}

func (b *localBackend) ListDatasets(context.Context) ([]zfs.Dataset, error) {
	return slices.Clone(b.inventory), nil
}

func (b *localBackend) InspectState(_ context.Context, dataset string, _ bool) (zfs.State, error) {
	if dataset == "tank/data" {
		return cloneState(b.source)
	}
	if dataset == "backup/data" && b.destExists {
		return cloneState(b.destination)
	}
	return zfs.State{}, fmt.Errorf("dataset absent")
}

func (b *localBackend) InspectDatasetIdentity(_ context.Context, dataset string) (zfs.DatasetIdentity, error) {
	switch dataset {
	case "backup":
		return zfs.DatasetIdentity{Name: dataset, Type: zfs.Filesystem, GUID: 10, Pool: "backup", PoolGUID: 11}, nil
	case "backup/data":
		if b.destExists {
			return zfs.DatasetIdentity{Name: dataset, Type: zfs.Filesystem, GUID: 20, Pool: "backup", PoolGUID: 11}, nil
		}
	}
	return zfs.DatasetIdentity{}, fmt.Errorf("identity absent")
}

func (b *localBackend) GetStoredProperties(_ context.Context, _ []string) ([]zfs.Property, error) {
	return nil, nil
}

func (b *localBackend) SetProperties(_ context.Context, object string, values map[string]string) error {
	state := &b.source
	if strings.HasPrefix(object, "backup/data") {
		state = &b.destination
	}
	for key, value := range values {
		state.Properties = slices.DeleteFunc(state.Properties, func(row zfs.Property) bool {
			return row.Dataset == object && row.Name == key && row.Source == zfs.SourceLocal
		})
		state.Properties = append(state.Properties, zfs.Property{Dataset: object, Name: key, Value: value, Source: zfs.SourceLocal})
		b.writes = append(b.writes, "set "+object+" "+key)
	}
	return nil
}

func (b *localBackend) InheritProperty(_ context.Context, object, key string) error {
	b.destination.Properties = slices.DeleteFunc(b.destination.Properties, func(row zfs.Property) bool { return row.Dataset == object && row.Name == key })
	b.writes = append(b.writes, "inherit "+object+" "+key)
	return nil
}

func (b *localBackend) Hold(_ context.Context, tag, snapshot string) error {
	if b.source.Holds == nil {
		b.source.Holds = make(map[string][]string)
	}
	b.source.Holds[snapshot] = append(b.source.Holds[snapshot], tag)
	b.writes = append(b.writes, "hold "+snapshot)
	return nil
}

func (b *localBackend) Release(_ context.Context, tag, snapshot string) error {
	b.source.Holds[snapshot] = slices.Delete(b.source.Holds[snapshot], slices.Index(b.source.Holds[snapshot], tag), slices.Index(b.source.Holds[snapshot], tag)+1)
	b.writes = append(b.writes, "release "+snapshot)
	return nil
}

func (b *localBackend) Bookmark(_ context.Context, snapshot, bookmark string) error {
	for _, object := range b.source.Objects {
		if object.Name == snapshot {
			b.source.Objects = append(b.source.Objects, zfs.Object{Name: bookmark, Type: "bookmark", GUID: object.GUID, CreateTXG: object.CreateTXG})
			b.writes = append(b.writes, "bookmark "+bookmark)
			return nil
		}
	}
	return fmt.Errorf("snapshot absent")
}

func (b *localBackend) DestroyBookmark(_ context.Context, bookmark string) error {
	b.source.Objects = slices.DeleteFunc(b.source.Objects, func(object zfs.Object) bool { return object.Name == bookmark })
	b.writes = append(b.writes, "destroy "+bookmark)
	return nil
}

func (b *localBackend) EstimateSend(context.Context, zfs.SendOptions) (zfs.Estimate, error) {
	return zfs.Estimate{Bytes: 123}, nil
}

type localTestStream struct {
	backend *localBackend
	fail    bool
}

type remoteTestStream struct {
	source      *localBackend
	destination *localBackend
}

func (s remoteTestStream) Run(_ context.Context, send zfs.SendOptions, receive zfs.ReceiveOptions, estimate zfs.Estimate, _ func(zfs.Progress)) (zfs.Progress, error) {
	var endpoint zfs.Object
	for _, object := range s.source.source.Objects {
		if object.Name == send.Snapshot {
			endpoint = object
		}
	}
	component := strings.TrimPrefix(endpoint.Name, "tank/data@")
	s.destination.destExists = true
	s.destination.inventory = append(s.destination.inventory, zfs.Dataset{Name: "backup/data", Type: zfs.Filesystem, EncryptionRoot: "-"})
	s.destination.destination = zfs.State{Objects: []zfs.Object{
		{Name: receive.Root, Type: "filesystem", GUID: 20, CreateTXG: 20},
		{Name: receive.Root + "@" + component, Type: "snapshot", GUID: endpoint.GUID, CreateTXG: 21},
	}}
	return zfs.Progress{Bytes: estimate.Bytes}, nil
}

func (s localTestStream) Run(_ context.Context, send zfs.SendOptions, receive zfs.ReceiveOptions, estimate zfs.Estimate, _ func(zfs.Progress)) (zfs.Progress, error) {
	s.backend.writes = append(s.backend.writes, "stream")
	if s.fail {
		return zfs.Progress{}, errors.New("injected receiver failure")
	}
	var endpoint zfs.Object
	for _, object := range s.backend.source.Objects {
		selected := object.Name == send.Snapshot
		if send.ResumeToken != "" && object.Type == "snapshot" && len(s.backend.source.Holds[object.Name]) > 0 {
			selected = true
		}
		if selected && object.CreateTXG > endpoint.CreateTXG {
			endpoint = object
		}
	}
	component := strings.TrimPrefix(endpoint.Name, "tank/data@")
	s.backend.destExists = true
	s.backend.inventory = append(s.backend.inventory, zfs.Dataset{Name: "backup/data", Type: zfs.Filesystem, EncryptionRoot: "-"})
	s.backend.destination = zfs.State{Objects: []zfs.Object{
		{Name: receive.Root, Type: "filesystem", GUID: 20, CreateTXG: 20},
		{Name: receive.Root + "@" + component, Type: "snapshot", GUID: endpoint.GUID, CreateTXG: 21},
	}}
	return zfs.Progress{Bytes: estimate.Bytes}, nil
}

func TestApplyResumesHeldReceiveAfterRestart(t *testing.T) {
	t.Parallel()
	backend, request := newLocalBackend(t)
	failed, err := NewLocal(backend, localTestStream{backend: backend, fail: true}, fixtureInstallation)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := failed.Apply(t.Context(), request, nil); err == nil {
		t.Fatal("initial interrupted transfer succeeded")
	}
	backend.destExists = true
	backend.inventory = append(backend.inventory, zfs.Dataset{Name: "backup/data", Type: zfs.Filesystem, EncryptionRoot: "-"})
	backend.destination = zfs.State{
		Objects:      []zfs.Object{{Name: "backup/data", Type: "filesystem", GUID: 20, CreateTXG: 20}},
		ResumeTokens: map[string]string{"backup/data": "1-resume-token"},
	}

	// A new engine instance proves recovery is reconstructed from ZFS state,
	// rather than retained process memory.
	restarted, err := NewLocal(backend, localTestStream{backend: backend}, fixtureInstallation)
	if err != nil {
		t.Fatal(err)
	}
	preview, err := restarted.Preview(t.Context(), request)
	if err != nil || preview.Mode != "resume" || preview.Send.ResumeToken != "1-resume-token" {
		t.Fatalf("preview=%+v err=%v", preview, err)
	}
	result, err := restarted.Apply(t.Context(), request, nil)
	if err != nil || !result.Verified || result.Plan.Mode != "resume" {
		t.Fatalf("result=%+v err=%v writes=%v", result, err, backend.writes)
	}
	if len(backend.source.Holds[result.Plan.Snapshot]) != 0 {
		t.Fatal("verified resume retained source hold")
	}
}

func TestApplyResumeSelectsEndpointProofAheadOfHeldIncrementalBase(t *testing.T) {
	t.Parallel()
	request, view := testFixture(t)
	addDestinationBase(&view)
	backend := &localBackend{inventory: slices.Clone(view.Inventory), source: view.Source, destination: view.Destination, destExists: true}
	failed, err := NewLocal(backend, localTestStream{backend: backend, fail: true}, fixtureInstallation)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := failed.Apply(t.Context(), request, nil); err == nil {
		t.Fatal("initial incremental transfer succeeded")
	}
	if len(backend.source.Holds) < 2 {
		t.Fatalf("incremental endpoint and base were not both held: %v", backend.source.Holds)
	}
	backend.destination.ResumeTokens = map[string]string{"backup/data": "1-incremental-token"}
	restarted, err := NewLocal(backend, localTestStream{backend: backend}, fixtureInstallation)
	if err != nil {
		t.Fatal(err)
	}
	result, err := restarted.Apply(t.Context(), request, nil)
	if err != nil || !result.Verified || result.Plan.Mode != "resume" || result.Plan.Snapshot != view.Source.Objects[2].Name {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

func TestRemoteApplyKeepsSourceAndDestinationExecutorsSeparate(t *testing.T) {
	t.Parallel()
	source, request := newLocalBackend(t)
	destination := &localBackend{inventory: []zfs.Dataset{{Name: "backup", Type: zfs.Filesystem, EncryptionRoot: "-"}}}
	request.Transport = "ssh"
	request.RemoteName = "home"
	request.CanonicalTarget = "ssh://replicator@backup.example.net:22/backup/data"
	request.Policy.Remote = []string{"home"}
	engine, err := NewRemote(source, destination, remoteTestStream{source: source, destination: destination}, fixtureInstallation)
	if err != nil {
		t.Fatal(err)
	}
	result, err := engine.Apply(t.Context(), request, nil)
	if err != nil || !result.Verified || result.Plan.TargetBinding.Transport != "ssh" {
		t.Fatalf("result=%+v err=%v source writes=%v destination writes=%v", result, err, source.writes, destination.writes)
	}
	if !slices.ContainsFunc(source.writes, func(write string) bool { return strings.Contains(write, targetBindingPrefix) }) {
		t.Fatal("source-side target binding was not persisted locally")
	}
	if !slices.ContainsFunc(destination.writes, func(write string) bool { return strings.HasPrefix(write, "set backup/data") }) {
		t.Fatal("destination reconciliation did not use the remote executor")
	}
}

func newLocalBackend(t *testing.T) (*localBackend, Request) {
	t.Helper()
	request, view := testFixture(t)
	return &localBackend{inventory: slices.Clone(view.Inventory), source: view.Source}, request
}

func TestApplyFailureRetainsBoundRecoveryProof(t *testing.T) {
	t.Parallel()
	backend, request := newLocalBackend(t)
	engine, err := NewLocal(backend, localTestStream{backend: backend, fail: true}, fixtureInstallation)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Apply(t.Context(), request, nil); err == nil {
		t.Fatal("injected stream failure succeeded")
	}
	if len(backend.writes) < 4 || !strings.Contains(backend.writes[0], targetBindingPrefix) || !strings.Contains(backend.writes[1], lifecycle.ReferencePrefix) || !strings.HasPrefix(backend.writes[2], "hold ") || backend.writes[3] != "stream" {
		t.Fatalf("unsafe preparation order: %v", backend.writes)
	}
	if len(backend.source.Holds) != 1 {
		t.Fatal("failed transfer did not retain source hold")
	}
}

func TestInspectLocalTargetReportsUnboundAndMismatch(t *testing.T) {
	t.Parallel()
	backend, request := newLocalBackend(t)
	inspection, err := InspectLocalTarget(t.Context(), backend, request, backend.source)
	if err != nil || inspection.Status != "unbound" || inspection.Resolved == nil || inspection.Resolved.Anchor != "backup" {
		t.Fatalf("inspection=%+v err=%v", inspection, err)
	}
	encoded, err := encodeBinding(*inspection.Resolved)
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.SetProperties(t.Context(), request.Source, map[string]string{targetBindingProperty(inspection.CanonicalEndpoint): encoded}); err != nil {
		t.Fatal(err)
	}
	inspection, err = InspectLocalTarget(t.Context(), backend, request, backend.source)
	if err != nil || inspection.Status != "verified" {
		t.Fatalf("verified inspection=%+v err=%v", inspection, err)
	}
	backend.source.Properties[len(backend.source.Properties)-1].Value = `{"version":1}`
	inspection, err = InspectLocalTarget(t.Context(), backend, request, backend.source)
	if err == nil || inspection.Status != "invalid-binding" {
		t.Fatalf("invalid inspection=%+v err=%v", inspection, err)
	}
}

func TestApplyPromotesBindingAfterGUIDVerification(t *testing.T) {
	t.Parallel()
	backend, request := newLocalBackend(t)
	engine, err := NewLocal(backend, localTestStream{backend: backend}, fixtureInstallation)
	if err != nil {
		t.Fatal(err)
	}
	result, err := engine.Apply(t.Context(), request, nil)
	if err != nil || !result.Verified {
		t.Fatalf("result=%+v err=%v writes=%v", result, err, backend.writes)
	}
	if result.Plan.TargetBinding.Anchor != result.Plan.Destination || result.Plan.TargetBinding.AnchorGUID != 20 || result.Plan.BindingNew {
		t.Fatalf("binding was not promoted: %+v", result.Plan.TargetBinding)
	}
	if len(backend.source.Holds[result.Plan.Snapshot]) != 0 {
		t.Fatal("verified transfer retained endpoint hold")
	}
}
