package daemon

import (
	"context"
	"testing"
	"time"

	"github.com/pdf/boomerangz/internal/config"
	"github.com/pdf/boomerangz/internal/lifecycle"
	"github.com/pdf/boomerangz/internal/policy"
	"github.com/pdf/boomerangz/internal/zfs"
)

type runtimeBackend struct {
	scanned  chan struct{}
	state    zfs.State
	identity zfs.DatasetIdentity
}

func (b *runtimeBackend) ListDatasets(context.Context) ([]zfs.Dataset, error) {
	select {
	case b.scanned <- struct{}{}:
	default:
	}
	return nil, nil
}
func (b *runtimeBackend) InspectDatasetIdentity(context.Context, string) (zfs.DatasetIdentity, error) {
	return b.identity, nil
}
func (*runtimeBackend) GetActivationProperties(context.Context) ([]zfs.Property, error) {
	return nil, nil
}
func (*runtimeBackend) GetLifecycleProperties(context.Context) ([]zfs.Property, error) {
	return nil, nil
}
func (*runtimeBackend) GetStoredProperties(context.Context, []string) ([]zfs.Property, error) {
	return nil, nil
}
func (b *runtimeBackend) InspectState(context.Context, string, bool) (zfs.State, error) {
	return b.state, nil
}
func (*runtimeBackend) SetProperties(context.Context, string, map[string]string) error { return nil }
func (*runtimeBackend) InheritProperty(context.Context, string, string) error          { return nil }
func (*runtimeBackend) Snapshot(context.Context, string, string, bool, map[string]string) error {
	return nil
}
func (*runtimeBackend) DestroySnapshot(context.Context, string) error  { return nil }
func (*runtimeBackend) Bookmark(context.Context, string, string) error { return nil }
func (*runtimeBackend) DestroyBookmark(context.Context, string) error  { return nil }
func (*runtimeBackend) Hold(context.Context, string, string) error     { return nil }
func (*runtimeBackend) Release(context.Context, string, string) error  { return nil }
func (*runtimeBackend) EstimateSend(context.Context, zfs.SendOptions) (zfs.Estimate, error) {
	return zfs.Estimate{}, nil
}

func TestRuntimeGracefulShutdownWithoutPolicies(t *testing.T) {
	t.Parallel()
	cfg := config.Defaults()
	cfg.Daemon.ReconcileInterval.Duration = time.Hour
	backend := &runtimeBackend{scanned: make(chan struct{}, 1)}
	runtime, err := New(cfg, backend, "11111111-1111-4111-8111-111111111111", nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	select {
	case <-backend.scanned:
		cancel()
	case <-time.After(time.Second):
		t.Fatal("initial discovery did not run")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("daemon did not shut down")
	}
}

func TestSafetyRejectsLocalResumeState(t *testing.T) {
	t.Parallel()
	gate := &lifecycle.Gate{}
	canonical := "local:backup/data"
	bindingProperty := policy.StateNamespace + "target:" + lifecycle.TargetID(canonical)
	backend := &runtimeBackend{identity: zfs.DatasetIdentity{Name: "backup/data", Type: zfs.Filesystem, GUID: 91, Pool: "backup", PoolGUID: 90}, state: zfs.State{Properties: []zfs.Property{{Dataset: "tank/data", Name: bindingProperty, Value: `{"version":1,"transport":"local","canonical_target":"local:backup/data","destination_root":"backup/data","mapped_dataset":"backup/data","pool":"backup","pool_guid":90,"anchor":"backup/data","anchor_guid":91,"relative_path":""}`, Source: zfs.SourceLocal}}, ResumeTokens: map[string]string{"backup/data": "token"}}}
	safety := newSafety(gate, backend, nil, nil)
	if err := safety.CheckTarget(t.Context(), "tank/data", canonical); err == nil {
		t.Fatal("resumable target was accepted")
	}
}

func TestStandaloneTargetCheckerUsesSameLocalResumeCheck(t *testing.T) {
	t.Parallel()
	canonical := "local:backup/data"
	bindingProperty := policy.StateNamespace + "target:" + lifecycle.TargetID(canonical)
	backend := &runtimeBackend{identity: zfs.DatasetIdentity{Name: "backup/data", Type: zfs.Filesystem, GUID: 91, Pool: "backup", PoolGUID: 90}, state: zfs.State{Properties: []zfs.Property{{Dataset: "tank/data", Name: bindingProperty, Value: `{"version":1,"transport":"local","canonical_target":"local:backup/data","destination_root":"backup/data","mapped_dataset":"backup/data","pool":"backup","pool_guid":90,"anchor":"backup/data","anchor_guid":91,"relative_path":""}`, Source: zfs.SourceLocal}}, ResumeTokens: map[string]string{"backup/data": "token"}}}
	checker, err := NewTargetChecker(backend, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := checker.CheckTarget(t.Context(), "tank/data", canonical); err == nil {
		t.Fatal("resumable target was accepted")
	}
}
