//go:build integration

package daemon_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pdf/boomerangz/internal/config"
	"github.com/pdf/boomerangz/internal/control"
	"github.com/pdf/boomerangz/internal/daemon"
	"github.com/pdf/boomerangz/internal/lifecycle"
	"github.com/pdf/boomerangz/internal/policy"
	"github.com/pdf/boomerangz/internal/testutil/zfstest"
	"github.com/pdf/boomerangz/internal/zfs"
)

const guestInstallation = "abcdefab-cdef-4abc-8def-abcdefabcdef"

// guestPools resolves the run's disposable pools, skipping outside the guest.
func guestPools(t *testing.T) (string, string) {
	t.Helper()
	runID := os.Getenv("BOOMERANGZ_DAEMON_GUEST_RUN")
	if runID == "" {
		t.Fatal("BOOMERANGZ_DAEMON_GUEST_RUN is unset: the disposable guest harness did not provide a run ID")
	}
	sourceDevice := os.Getenv("BOOMERANGZ_INTEGRATION_SOURCE_DEVICE")
	destinationDevice := os.Getenv("BOOMERANGZ_INTEGRATION_DESTINATION_DEVICE")
	if sourceDevice == "" || destinationDevice == "" {
		t.Fatal("source and destination test devices are required")
	}
	sourcePool, err := zfstest.VerifyGuestPool(t.Context(), runID, zfstest.SourceDisk, sourceDevice)
	if err != nil {
		t.Fatal(err)
	}
	destinationPool, err := zfstest.VerifyGuestPool(t.Context(), runID, zfstest.DestinationDisk, destinationDevice)
	if err != nil {
		t.Fatal(err)
	}
	return sourcePool, destinationPool
}

func zfsCommand(t *testing.T, args ...string) {
	t.Helper()
	if output, err := exec.CommandContext(t.Context(), "zfs", args...).CombinedOutput(); err != nil {
		t.Fatalf("guest zfs %s: %v: %s", args[0], err, output)
	}
}

// runDaemon starts a runtime and returns it already serving, stopping it when
// the test that owns the state ends.
func runDaemon(t *testing.T, cfg config.Config, backend *scopedDaemonBackend) *daemon.Runtime {
	t.Helper()
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	runtime, err := daemon.New(cfg, backend, guestInstallation, logger)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case runErr := <-done:
			if runErr != nil {
				t.Errorf("daemon shutdown: %v", runErr)
			}
		case <-time.After(10 * time.Second):
			t.Error("daemon did not shut down")
		}
	})
	return runtime
}

func waitForJobState(t *testing.T, runtime *daemon.Runtime, job, state string, timeout time.Duration) daemon.Event {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, event := range runtime.Status() {
			if event.Job == job && event.State == state {
				return event
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s to reach %q: %+v", job, state, runtime.Status())
	return daemon.Event{}
}

// freePort reserves and releases a loopback port. The pairing bundle has to
// name the endpoint before anything listens on it, which is exactly the
// arrangement the backoff test needs.
func freePort(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	_, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return port
}

// importedNativeCredential issues a pairing for the listener definition and
// imports it under credentialsDir, returning the local credential name. The
// managed server identity is keyed by advertised host, so issuing before the
// listener binds produces the same certificate the server later presents.
func importedNativeCredential(t *testing.T, identityDir, credentialsDir, name string, listener config.ListenerConfig) string {
	t.Helper()
	if err := os.MkdirAll(identityDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(credentialsDir, 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := control.NewTokenStore(identityDir)
	if err != nil {
		t.Fatal(err)
	}
	bundle, err := control.CreateListenerPairing(store, identityDir, "replication", listener, "", "", []string{"replicate"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(bundle)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := control.ImportPairingBundle(credentialsDir, name, encoded); err != nil {
		t.Fatal(err)
	}
	return name
}

// TestGuestDaemonNativeReplication drives a daemon-scheduled remote transfer
// over the native transport, resolved from a credential on disk rather than a
// bundle handed to the transport in process. Destination state is read with
// the local executor: the guest is both hosts, so that is the pool the remote
// side actually wrote rather than the remote's own view of it.
func TestGuestDaemonNativeReplication(t *testing.T) {
	sourcePool, destinationPool := guestPools(t)
	direct, err := zfs.NewDirect("zfs")
	if err != nil {
		t.Fatal(err)
	}
	source := zfstest.PayloadVolume(t, zfstest.FixtureName(sourcePool, "daemon-native"), 128, 8)
	destinationRoot := zfstest.FixtureName(destinationPool, "daemon-native")
	zfstest.RegisterCleanup(t, destinationRoot)

	pkiDir := t.TempDir()
	identityDir := filepath.Join(pkiDir, "identity")
	credentialsDir := filepath.Join(pkiDir, "credentials")
	listener := config.ListenerConfig{Network: "tcp", Address: "127.0.0.1:" + freePort(t), AuthMode: "token", ReplicationRoots: []string{destinationRoot}}
	listener.AdvertisedAddress = strings.Replace(listener.Address, "127.0.0.1:", "localhost:", 1)
	credential := importedNativeCredential(t, identityDir, credentialsDir, "backup", listener)

	listenerConfig := config.Defaults()
	listenerConfig.Paths.SocketPath = filepath.Join(pkiDir, "control.sock")
	listenerConfig.Paths.IdentityDir = identityDir
	listenerConfig.Paths.CredentialsDir = credentialsDir
	listenerConfig.Listeners["replication"] = listener
	server, err := control.StartServerWithReplication(listenerConfig, nil, direct, "zfs", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })

	zfsCommand(t, "set", policy.Namespace+"enabled=on", policy.Namespace+"policy=1x1m", policy.Namespace+"remote=backup", source)

	cfg := config.Defaults()
	cfg.Daemon.ReconcileInterval.Duration = 200 * time.Millisecond
	cfg.Daemon.ManagementWorkers = 2
	cfg.Paths.IdentityDir = identityDir
	cfg.Paths.CredentialsDir = credentialsDir
	cfg.Paths.SocketPath = filepath.Join(pkiDir, "daemon.sock")
	cfg.Remotes["backup"] = config.RemoteConfig{Transport: "native", Credential: credential, Root: destinationRoot}
	runtime := runDaemon(t, cfg, &scopedDaemonBackend{Direct: direct, root: source})

	success := waitForJobState(t, runtime, "remote:"+source+":backup", "succeeded", 3*time.Minute)
	if !strings.HasPrefix(success.Target, "native://") || !strings.HasSuffix(success.Target, "/"+destinationRoot) {
		t.Fatalf("native transfer did not record a canonical native target: %q", success.Target)
	}
	identity, err := direct.InspectDatasetIdentity(t.Context(), destinationRoot)
	if err != nil {
		t.Fatalf("destination was not created by the native receive: %v", err)
	}
	if identity.Type != zfs.Volume {
		t.Fatalf("destination type=%q, want the source volume type", identity.Type)
	}
	state, err := direct.InspectState(t.Context(), destinationRoot, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Objects) == 0 {
		t.Fatal("destination carries no received snapshot")
	}
	t.Logf("daemon replicated %s to %s over %s", source, destinationRoot, success.Target)
}

// TestGuestDaemonPruneJob asserts the prune job the scheduler emits after a
// successful snapshot both reaches succeeded and does the work: only unit
// tests have ever observed lifecycle.Service.Prune, and only by calling it.
func TestGuestDaemonPruneJob(t *testing.T) {
	sourcePool, _ := guestPools(t)
	direct, err := zfs.NewDirect("zfs")
	if err != nil {
		t.Fatal(err)
	}
	source := zfstest.FixtureName(sourcePool, "daemon-prune")
	zfsCommand(t, "create", "-u", source)
	zfstest.RegisterCleanup(t, source)
	zfsCommand(t, "set", policy.Namespace+"enabled=on", policy.Namespace+"policy=1x1m", source)

	rows, err := direct.GetStoredProperties(t.Context(), []string{source})
	if err != nil {
		t.Fatal(err)
	}
	effective := policy.Resolve(zfs.Dataset{Name: source, Type: zfs.Filesystem, EncryptionRoot: "-"}, nil, rows, nil)
	service, err := lifecycle.NewService(direct, guestInstallation)
	if err != nil {
		t.Fatal(err)
	}
	// Three snapshots well outside a one-minute horizon. The daemon's own
	// snapshot becomes the single retained one, so a prune that runs but
	// retains everything is distinguishable from one that works.
	now := time.Now().UTC()
	expired := make([]string, 0, 3)
	for _, age := range []time.Duration{30 * time.Minute, 20 * time.Minute, 10 * time.Minute} {
		metadata, createErr := service.CreateSnapshot(t.Context(), source, false, now.Add(-age), effective)
		if createErr != nil {
			t.Fatal(createErr)
		}
		expired = append(expired, source+"@"+metadata.Name())
	}

	cfg := config.Defaults()
	cfg.Daemon.ReconcileInterval.Duration = 200 * time.Millisecond
	cfg.Daemon.ManagementWorkers = 2
	runtime := runDaemon(t, cfg, &scopedDaemonBackend{Direct: direct, root: source})

	waitForJobState(t, runtime, "snapshot:"+source, "succeeded", 60*time.Second)
	waitForJobState(t, runtime, "prune:"+source, "succeeded", 60*time.Second)

	state, err := direct.InspectState(t.Context(), source, false)
	if err != nil {
		t.Fatal(err)
	}
	remaining := make(map[string]bool)
	for _, snapshot := range lifecycle.Snapshots(state, source) {
		remaining[snapshot.Name] = true
	}
	for _, snapshot := range expired {
		if remaining[snapshot] {
			t.Fatalf("prune job succeeded but retained %s outside the grid: %v", snapshot, remaining)
		}
	}
	if len(remaining) != 1 {
		t.Fatalf("prune retained %d snapshots, want the single in-grid one: %v", len(remaining), remaining)
	}
	t.Logf("prune job destroyed %d expired snapshots on %s", len(expired), source)
}

// TestGuestDaemonRemoteBackoff asserts the retry sequence between an outage
// and its recovery, not just the endpoints. Every real attempt records its
// transport failure as the reason, while the reconciler's early returns while
// a backoff is still running record none, so counting reasoned events counts
// attempts. The endpoint is named before anything binds it, then the listener
// is started under that same name to close the sequence.
func TestGuestDaemonRemoteBackoff(t *testing.T) {
	sourcePool, destinationPool := guestPools(t)
	direct, err := zfs.NewDirect("zfs")
	if err != nil {
		t.Fatal(err)
	}
	source := zfstest.FixtureName(sourcePool, "daemon-backoff")
	zfsCommand(t, "create", "-u", source)
	zfstest.RegisterCleanup(t, source)
	destinationRoot := zfstest.FixtureName(destinationPool, "daemon-backoff")
	zfstest.RegisterCleanup(t, destinationRoot)

	pkiDir := t.TempDir()
	identityDir := filepath.Join(pkiDir, "identity")
	credentialsDir := filepath.Join(pkiDir, "credentials")
	listener := config.ListenerConfig{Network: "tcp", Address: "127.0.0.1:" + freePort(t), AuthMode: "token", ReplicationRoots: []string{destinationRoot}}
	listener.AdvertisedAddress = strings.Replace(listener.Address, "127.0.0.1:", "localhost:", 1)
	credential := importedNativeCredential(t, identityDir, credentialsDir, "backup", listener)

	zfsCommand(t, "set", policy.Namespace+"enabled=on", policy.Namespace+"policy=1x1m", policy.Namespace+"remote=backup", source)

	cfg := config.Defaults()
	cfg.Daemon.ReconcileInterval.Duration = 500 * time.Millisecond
	cfg.Daemon.ManagementWorkers = 2
	cfg.Paths.IdentityDir = identityDir
	cfg.Paths.CredentialsDir = credentialsDir
	cfg.Paths.SocketPath = filepath.Join(pkiDir, "daemon.sock")
	cfg.Remotes["backup"] = config.RemoteConfig{Transport: "native", Credential: credential, Root: destinationRoot}
	runtime := runDaemon(t, cfg, &scopedDaemonBackend{Direct: direct, root: source})

	job := "remote:" + source + ":backup"
	attempts := make([]time.Time, 0, 3)
	deadline := time.Now().Add(90 * time.Second)
	for len(attempts) < 3 && time.Now().Before(deadline) {
		for _, event := range runtime.Status() {
			if event.Job != job || event.State != "waiting-retry" || event.Reason == "" {
				continue
			}
			if len(attempts) == 0 || event.At.After(attempts[len(attempts)-1]) {
				attempts = append(attempts, event.At)
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	if len(attempts) < 3 {
		t.Fatalf("observed %d retried attempts in 90s, want 3: %+v", len(attempts), runtime.Status())
	}
	first, second := attempts[1].Sub(attempts[0]), attempts[2].Sub(attempts[1])
	// DefaultRetryPolicy is 5s doubling with 20% symmetric jitter. The bounds
	// are the jittered range plus scheduling slack, and the ordering check is
	// what distinguishes backoff from a fixed-delay retry.
	if first < 3500*time.Millisecond || first > 8*time.Second {
		t.Fatalf("first retry delay %s is outside the jittered 5s band", first)
	}
	if second < 7*time.Second || second > 15*time.Second {
		t.Fatalf("second retry delay %s is outside the jittered 10s band", second)
	}
	if second <= first {
		t.Fatalf("retry delays did not grow: %s then %s", first, second)
	}

	listenerConfig := config.Defaults()
	listenerConfig.Paths.SocketPath = filepath.Join(pkiDir, "control.sock")
	listenerConfig.Paths.IdentityDir = identityDir
	listenerConfig.Paths.CredentialsDir = credentialsDir
	listenerConfig.Listeners["replication"] = listener
	server, err := control.StartServerWithReplication(listenerConfig, nil, direct, "zfs", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })

	success := waitForJobState(t, runtime, job, "succeeded", 3*time.Minute)
	if success.Target == "" {
		t.Fatal("recovered transfer did not retain canonical target identity")
	}
	if _, err := direct.InspectDatasetIdentity(t.Context(), destinationRoot); err != nil {
		t.Fatalf("recovered transfer did not create the destination: %v", err)
	}
	t.Logf("backoff grew %s then %s before %s recovered", first, second, success.Target)
}
