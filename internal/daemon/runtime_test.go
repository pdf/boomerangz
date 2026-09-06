package daemon

import (
	"context"
	"testing"
	"time"

	"github.com/pdf/boomerangz/internal/config"
	"github.com/pdf/boomerangz/internal/lifecycle"
	"github.com/pdf/boomerangz/internal/zfs"
)

type runtimeBackend struct {
	scanned chan struct{}
	state   zfs.State
}

func (b *runtimeBackend) ListDatasets(context.Context) ([]zfs.Dataset, error) {
	select {
	case b.scanned <- struct{}{}:
	default:
	}
	return nil, nil
}
func (*runtimeBackend) InspectDatasetIdentity(context.Context, string) (zfs.DatasetIdentity, error) {
	return zfs.DatasetIdentity{}, nil
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
	backend := &runtimeBackend{state: zfs.State{ResumeTokens: map[string]string{"backup/data": "token"}}}
	safety := newSafety(gate, backend, nil, nil)
	if err := safety.CheckTarget(t.Context(), "local:backup/data"); err == nil {
		t.Fatal("resumable target was accepted")
	}
}
