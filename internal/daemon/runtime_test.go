package daemon

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/pdf/boomerangz/internal/config"
	"github.com/pdf/boomerangz/internal/discovery"
	"github.com/pdf/boomerangz/internal/lifecycle"
	"github.com/pdf/boomerangz/internal/policy"
	"github.com/pdf/boomerangz/internal/zfs"
)

func TestWorkerStateLogLevels(t *testing.T) {
	t.Parallel()
	var output bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&output, nil))
	status := &StatusStore{}
	for _, event := range []Event{{Job: "normal", State: "succeeded"}, {Job: "blocked", State: "blocked"}, {Job: "failed", State: "failed"}} {
		reportWorkerState(logger, status, event)
	}
	logged := output.String()
	for _, want := range []string{"level=INFO", "level=WARN", "level=ERROR"} {
		if !strings.Contains(logged, want) {
			t.Fatalf("missing %s in %q", want, logged)
		}
	}
	if len(status.Snapshot()) != 3 {
		t.Fatal("logging did not retain worker status")
	}
}

func TestBlockedOrCancelledOutcome(t *testing.T) {
	t.Parallel()
	cancelled := blockedOrCancelled(fmt.Errorf("start work: %w", context.Canceled))
	if cancelled.State != "cancelled" || cancelled.Reason == "" {
		t.Fatalf("cancelled outcome=%+v", cancelled)
	}
	blocked := blockedOrCancelled(errors.New("unsafe state"))
	if blocked.State != "blocked" {
		t.Fatalf("blocked outcome=%+v", blocked)
	}
}

func TestNearestReceiveLockScope(t *testing.T) {
	t.Parallel()
	inventory := []zfs.Dataset{
		{Name: "backup/root", Type: zfs.Filesystem},
		{Name: "backup/root/data", Type: zfs.Filesystem},
		{Name: "backup/root/data/existing", Type: zfs.Filesystem},
	}
	for _, test := range []struct {
		mapped string
		want   string
	}{
		{mapped: "backup/root/data/existing", want: "backup/root/data/existing"},
		{mapped: "backup/root/data/new", want: "backup/root/data"},
		{mapped: "backup/root/other/new", want: "backup/root"},
	} {
		if got := nearestReceiveLockScope("backup/root", test.mapped, inventory); got != test.want {
			t.Errorf("nearestReceiveLockScope(%q) = %q, want %q", test.mapped, got, test.want)
		}
	}
	if got := nearestReceiveLockScope("missing/root", "missing/root/data", inventory); got != "" {
		t.Fatalf("missing receive root lock scope = %q, want exclusive target lock", got)
	}
}

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
func (*runtimeBackend) CheckPermissions(context.Context, string, []string) error { return nil }
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
func (*runtimeBackend) CreateReceiveParent(context.Context, string) error              { return nil }
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

func TestNewWithLocalStreamRequiresStream(t *testing.T) {
	t.Parallel()
	cfg := config.Defaults()
	backend := &runtimeBackend{}
	if _, err := NewWithLocalStream(cfg, backend, "11111111-1111-4111-8111-111111111111", nil, nil); err == nil {
		t.Fatal("nil local transfer stream accepted")
	}
}

func TestScheduledSnapshotRetriesWhenActivationGateIsNotReady(t *testing.T) {
	t.Parallel()
	cfg := config.Defaults()
	cfg.Daemon.ReconcileInterval.Duration = 10 * time.Millisecond
	scheduler := NewScheduler()
	entry := scheduledEntry(t, "tank/data", "1x5m")
	if _, _, err := scheduler.Update([]discovery.Entry{entry}, time.Now()); err != nil {
		t.Fatal(err)
	}
	schedule, ok := scheduler.Next(t.Context())
	if !ok {
		t.Fatal("initial schedule was not due")
	}
	runtime := &Runtime{config: cfg, gate: &lifecycle.Gate{}, scheduler: scheduler, now: time.Now}
	if runtime.enqueueSnapshot(schedule) {
		t.Fatal("disabled activation gate accepted snapshot work")
	}
	if err := runtime.gate.SetEnabled(entry.Dataset.Name, true); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	retry, ok := scheduler.Next(ctx)
	if !ok || retry.Dataset != entry.Dataset.Name {
		t.Fatalf("schedule was not retried after activation: %#v", retry)
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

func TestApplyConfigPublishesGenerationAndRetainsIdentityDirectory(t *testing.T) {
	t.Parallel()
	cfg := config.Defaults()
	backend := &runtimeBackend{scanned: make(chan struct{}, 1)}
	runtime, err := New(cfg, backend, "11111111-1111-4111-8111-111111111111", nil)
	if err != nil {
		t.Fatal(err)
	}
	next := cfg.Clone()
	next.Daemon.LocalTransferWorkers = 3
	next.Paths.IdentityDir = "/different/identity"
	next.Paths.SocketPath = "/run/boomerangz/reloaded.sock"
	result, err := runtime.ApplyConfig(next)
	if err != nil {
		t.Fatal(err)
	}
	if result.Generation != 2 || !reflect.DeepEqual(result.RestartRequired, []string{"paths.identity_dir"}) {
		t.Fatalf("result=%#v", result)
	}
	if got := runtime.CurrentConfig(); got.Paths.IdentityDir != cfg.Paths.IdentityDir || got.Paths.SocketPath != next.Paths.SocketPath {
		t.Fatalf("live config=%#v", got.Paths)
	}
	if runtime.local.workers != 3 || runtime.ControlStatus().ConfigGeneration != 2 {
		t.Fatalf("workers=%d status=%#v", runtime.local.workers, runtime.ControlStatus())
	}
}

func TestPrepareConfigFailureLeavesRuntimeUnchanged(t *testing.T) {
	t.Parallel()
	cfg := config.Defaults()
	backend := &runtimeBackend{scanned: make(chan struct{}, 1)}
	runtime, err := New(cfg, backend, "11111111-1111-4111-8111-111111111111", nil)
	if err != nil {
		t.Fatal(err)
	}
	next := cfg.Clone()
	next.Remotes["missing"] = config.RemoteConfig{Transport: "native", Credential: "absent", Root: "tank/backups"}
	if _, err := runtime.ApplyConfig(next); err == nil {
		t.Fatal("missing native credential was accepted")
	}
	if got := runtime.CurrentConfig(); !reflect.DeepEqual(got, cfg) {
		t.Fatalf("failed preparation changed config: %#v", got)
	}
	if runtime.ControlStatus().ConfigGeneration != 1 {
		t.Fatal("failed preparation advanced generation")
	}
}

func TestListenerReloadFailureRollsBackDaemonConfiguration(t *testing.T) {
	t.Parallel()
	cfg := config.Defaults()
	backend := &runtimeBackend{scanned: make(chan struct{}, 1)}
	runtime, err := New(cfg, backend, "11111111-1111-4111-8111-111111111111", nil)
	if err != nil {
		t.Fatal(err)
	}
	next := cfg.Clone()
	next.Daemon.LocalTransferWorkers = 4
	next.Paths.SocketPath = "/run/boomerangz/replacement.sock"
	wantErr := errors.New("listener unavailable")
	_, err = runtime.ReloadConfig(next, func(config.Config) error { return wantErr })
	if !errors.Is(err, wantErr) {
		t.Fatalf("reload error=%v", err)
	}
	if got := runtime.CurrentConfig(); !reflect.DeepEqual(got, cfg) {
		t.Fatalf("listener failure changed config: %#v", got)
	}
	if runtime.local.workers != cfg.Daemon.LocalTransferWorkers || runtime.ControlStatus().ConfigGeneration != 1 {
		t.Fatal("listener failure changed live runtime state")
	}
}
