package daemon

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"testing"

	"github.com/pdf/boomerangz/internal/config"
	"github.com/pdf/boomerangz/internal/discovery"
	"github.com/pdf/boomerangz/internal/policy"
	"github.com/pdf/boomerangz/internal/transfer"
	"github.com/pdf/boomerangz/internal/zfs"
)

// permissionBackend is a destination whose account lacks the permissions in
// missing, and which records every permission check it answers.
type permissionBackend struct {
	runtimeBackend
	datasets []zfs.Dataset

	mu      sync.Mutex
	missing []string
	checks  [][]string // the dataset, then the permissions, of each check
}

func (b *permissionBackend) ListDatasets(context.Context) ([]zfs.Dataset, error) {
	return slices.Clone(b.datasets), nil
}

func (b *permissionBackend) CheckPermissions(_ context.Context, dataset string, permissions []string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.checks = append(b.checks, append([]string{dataset}, permissions...))
	var missing []string
	for _, permission := range permissions {
		if slices.Contains(b.missing, permission) {
			missing = append(missing, permission)
		}
	}
	if len(missing) > 0 {
		return &zfs.MissingPermissionsError{Account: "boomerangz", Dataset: dataset, Missing: missing}
	}
	return nil
}

func (b *permissionBackend) grant(permission string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.missing = slices.DeleteFunc(b.missing, func(missing string) bool { return missing == permission })
}

func (b *permissionBackend) checked() [][]string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return slices.Clone(b.checks)
}

func filesystems(names ...string) []zfs.Dataset {
	var datasets []zfs.Dataset
	for _, name := range names {
		datasets = append(datasets, zfs.Dataset{Name: name, Type: zfs.Filesystem, EncryptionRoot: "-"})
	}
	return datasets
}

// sendingGeneration scans one enabled dataset, tank/data, with the given
// org.boomerangz properties.
func sendingGeneration(t *testing.T, remotes []string, properties map[string]string) *discovery.Generation {
	t.Helper()
	rows := []zfs.Property{{Dataset: "tank/data", Name: policy.Namespace + "enabled", Value: "on", Source: zfs.SourceLocal}}
	for name, value := range properties {
		rows = append(rows, zfs.Property{Dataset: "tank/data", Name: policy.Namespace + name, Value: value, Source: zfs.SourceLocal})
	}
	scanner, err := discovery.New(collisionReader{datasets: []string{"tank", "tank/data"}, properties: rows}, discovery.Options{Remotes: remotes})
	if err != nil {
		t.Fatal(err)
	}
	generation, err := scanner.Scan(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	entry, _ := generation.Inspect("tank/data")
	if !entry.Policy.Enabled || !entry.Policy.Valid() {
		t.Fatalf("fixture policy = %+v", entry.Policy)
	}
	return generation
}

func newPermissionRuntime(t *testing.T, cfg config.Config, backend *permissionBackend) (*Runtime, *lineLog) {
	t.Helper()
	log := &lineLog{}
	runtime, err := New(cfg, backend, "11111111-1111-4111-8111-111111111111", slog.New(log))
	if err != nil {
		t.Fatal(err)
	}
	return runtime, log
}

func permissionWarnings(log *lineLog) []map[string]string {
	return slices.DeleteFunc(log.snapshot(), func(line map[string]string) bool {
		return line["msg"] != "destination lacks delegated ZFS permissions boomerangz may need; operations that need them will fail"
	})
}

// A destination missing a permission only a rare operation needs is reported
// in status and the log when the daemon first reaches it, as a warning.
func TestLocalDestinationMissingPermissionsAreReported(t *testing.T) {
	t.Parallel()
	backend := &permissionBackend{datasets: filesystems("tank", "tank/data", "backup", "backup/boomerangz"), missing: []string{"rename"}}
	runtime, log := newPermissionRuntime(t, config.Defaults(), backend)
	generation := sendingGeneration(t, nil, map[string]string{"local": "backup/boomerangz", "ignore_prop:atime": "on"})
	runtime.applyGeneration(generation)

	entry, _ := generation.Inspect("tank/data")
	want := append([]string{"backup/boomerangz"}, transfer.DestinationPermissions(entry.Policy)...)
	if checks := backend.checked(); len(checks) != 1 || !slices.Equal(checks[0], want) {
		t.Fatalf("checks = %v, want one of %v", checks, want)
	}
	if !slices.Contains(want, "rename") || !slices.Contains(want, "atime") {
		t.Fatalf("the check left out what placeholder replacement or the policy needs: %v", want)
	}
	destinations := runtime.ControlStatus().Destinations
	if len(destinations) != 1 {
		t.Fatalf("destinations = %+v", destinations)
	}
	got := destinations[0]
	if got.Destination != "backup/boomerangz" || got.Remote || got.Dataset != "backup/boomerangz" || got.Account != "boomerangz" || !slices.Equal(got.Missing, []string{"rename"}) || got.Error != "" || got.Checked.IsZero() {
		t.Fatalf("destination = %+v", got)
	}
	warnings := permissionWarnings(log)
	if len(warnings) != 1 || warnings[0]["level"] != "WARN" || warnings[0]["destination"] != "backup/boomerangz" || warnings[0]["missing"] != "rename" || warnings[0]["account"] != "boomerangz" {
		t.Fatalf("warnings = %v", warnings)
	}
}

// While the destination root is absent, as with discard=off, the check runs
// where a transfer's own preflight does: its nearest existing ancestor.
func TestAbsentLocalDestinationIsCheckedAtItsNearestAncestor(t *testing.T) {
	t.Parallel()
	backend := &permissionBackend{datasets: filesystems("tank", "tank/data", "backup")}
	runtime, _ := newPermissionRuntime(t, config.Defaults(), backend)
	runtime.applyGeneration(sendingGeneration(t, nil, map[string]string{"local": "backup/boomerangz"}))
	if checks := backend.checked(); len(checks) != 1 || checks[0][0] != "backup" {
		t.Fatalf("checks = %v", checks)
	}
	if destinations := runtime.ControlStatus().Destinations; len(destinations) != 1 || destinations[0].Dataset != "backup" || len(destinations[0].Missing) != 0 || destinations[0].Error != "" {
		t.Fatalf("destinations = %+v", destinations)
	}
}

// Nothing is checked on a timer: a rescan that changes nothing does not check
// again; a reload, or a change in what the destination must grant, does.
func TestDestinationPermissionsAreRecheckedOnChangeOnly(t *testing.T) {
	t.Parallel()
	backend := &permissionBackend{datasets: filesystems("tank", "tank/data", "backup", "backup/boomerangz"), missing: []string{"rename"}}
	cfg := config.Defaults()
	runtime, log := newPermissionRuntime(t, cfg, backend)
	properties := map[string]string{"local": "backup/boomerangz"}
	runtime.applyGeneration(sendingGeneration(t, nil, properties))
	runtime.applyGeneration(sendingGeneration(t, nil, properties))
	if checks := backend.checked(); len(checks) != 1 {
		t.Fatalf("an unchanged rescan checked again: %v", checks)
	}

	// The operator grants the permission and reloads.
	backend.grant("rename")
	if _, err := runtime.ApplyConfig(cfg.Clone()); err != nil {
		t.Fatal(err)
	}
	runtime.applyGeneration(sendingGeneration(t, nil, properties))
	if checks := backend.checked(); len(checks) != 2 {
		t.Fatalf("a reload did not check again: %v", checks)
	}
	if destinations := runtime.ControlStatus().Destinations; len(destinations) != 1 || len(destinations[0].Missing) != 0 {
		t.Fatalf("destinations after the grant = %+v", destinations)
	}
	if !slices.ContainsFunc(log.snapshot(), func(line map[string]string) bool {
		return line["level"] == "INFO" && line["msg"] == "destination grants every delegated ZFS permission boomerangz may need" && line["destination"] == "backup/boomerangz"
	}) {
		t.Fatalf("the cleared warning was not logged: %v", log.snapshot())
	}

	// A policy that now sets a receive property needs its permission too.
	properties["set_prop:compression"] = "lz4"
	runtime.applyGeneration(sendingGeneration(t, nil, properties))
	checks := backend.checked()
	if len(checks) != 3 || !slices.Contains(checks[2], "compression") {
		t.Fatalf("a changed need did not check again: %v", checks)
	}

	// A destination nothing sends to any more leaves status.
	runtime.applyGeneration(sendingGeneration(t, nil, map[string]string{}))
	if destinations := runtime.ControlStatus().Destinations; len(destinations) != 0 {
		t.Fatalf("destinations after the policy dropped the target = %+v", destinations)
	}
}

// A remote is checked through the endpoint a transfer opens, once, and again
// after it fails to open and then reconnects.
func TestRemoteDestinationIsCheckedWhenItsEndpointOpens(t *testing.T) {
	t.Parallel()
	cfg := config.Defaults()
	cfg.Remotes["offsite"] = sshRemoteConfig(22)
	runtime, log := newPermissionRuntime(t, cfg, &permissionBackend{})
	runtime.applyGeneration(sendingGeneration(t, []string{"offsite"}, map[string]string{"remote": "offsite"}))
	if destinations := runtime.ControlStatus().Destinations; len(destinations) != 0 {
		t.Fatalf("a remote was reported before its endpoint opened: %+v", destinations)
	}
	remote := &permissionBackend{datasets: filesystems("tank", "tank/backups"), missing: []string{"canmount", "rename"}}
	runtime.mu.Lock()
	client := runtime.remotes["offsite"]
	runtime.mu.Unlock()

	runtime.remoteReached(t.Context(), "offsite", client, remote, nil)
	runtime.remoteReached(t.Context(), "offsite", client, remote, nil)
	if checks := remote.checked(); len(checks) != 1 || checks[0][0] != "tank/backups" {
		t.Fatalf("checks = %v", checks)
	}
	destinations := runtime.ControlStatus().Destinations
	if len(destinations) != 1 || destinations[0].Destination != "offsite" || !destinations[0].Remote || !slices.Equal(destinations[0].Missing, []string{"canmount", "rename"}) {
		t.Fatalf("destinations = %+v", destinations)
	}
	if warnings := permissionWarnings(log); len(warnings) != 1 || warnings[0]["remote"] != "true" || warnings[0]["missing"] != "canmount,rename" {
		t.Fatalf("warnings = %v", warnings)
	}

	runtime.remoteReached(t.Context(), "offsite", client, nil, errors.New("connection refused"))
	runtime.remoteReached(t.Context(), "offsite", client, remote, nil)
	if checks := remote.checked(); len(checks) != 2 {
		t.Fatalf("a reconnect did not check again: %v", checks)
	}
	// The same finding is not logged twice.
	if warnings := permissionWarnings(log); len(warnings) != 1 {
		t.Fatalf("warnings = %v", warnings)
	}
	// An endpoint from a client a reload has since replaced checks nothing.
	runtime.remoteReached(t.Context(), "offsite", &sshRemote{setting: sshRemoteConfig(2222)}, remote, nil)
	if checks := remote.checked(); len(checks) != 2 {
		t.Fatalf("a replaced client checked: %v", checks)
	}
}

// A check that cannot run is a warning too, naming why.
func TestDestinationPermissionCheckFailureIsReported(t *testing.T) {
	t.Parallel()
	got := checkDestination(t.Context(), &permissionBackend{datasets: filesystems("tank")}, destinationNeed{root: "backup/boomerangz", required: []string{"create"}})
	if got.Error == "" || got.Dataset != "" || len(got.Missing) != 0 {
		t.Fatalf("result = %+v", got)
	}
	failing := &failingPermissions{permissionBackend: permissionBackend{datasets: filesystems("backup")}}
	got = checkDestination(t.Context(), failing, destinationNeed{root: "backup", required: []string{"create"}})
	if got.Error != "inspect delegated ZFS permissions on backup: boom" || got.Dataset != "backup" {
		t.Fatalf("result = %+v", got)
	}
}

type failingPermissions struct{ permissionBackend }

func (*failingPermissions) CheckPermissions(context.Context, string, []string) error {
	return errors.New("inspect delegated ZFS permissions on backup: boom")
}

// The applier tells the runtime about every open, failed ones included.
func TestRemoteApplierReportsEachOpen(t *testing.T) {
	t.Parallel()
	var reached []error
	applier := remoteApplier{client: failingRemote{}, reached: func(_ context.Context, _ zfs.Executor, err error) { reached = append(reached, err) }}
	if _, err := applier.Apply(t.Context(), transfer.Request{}, nil); err == nil {
		t.Fatal("a failed open applied")
	}
	if len(reached) != 1 || reached[0] == nil {
		t.Fatalf("reached = %v", reached)
	}
}

type failingRemote struct{}

func (failingRemote) CanonicalTarget() string { return "ssh://backup.example.net:22/tank/backups" }
func (failingRemote) Transport() string       { return "ssh" }
func (failingRemote) Open(context.Context) (openedRemote, error) {
	return openedRemote{}, errors.New("connection refused")
}
