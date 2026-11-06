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
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pdf/boomerangz/internal/config"
	"github.com/pdf/boomerangz/internal/control"
	"github.com/pdf/boomerangz/internal/daemon"
	"github.com/pdf/boomerangz/internal/lifecycle"
	"github.com/pdf/boomerangz/internal/policy"
	"github.com/pdf/boomerangz/internal/testutil/zfstest"
	"github.com/pdf/boomerangz/internal/zfs"
	"github.com/pdf/boomerangz/test/integration/internal/statuswait"
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
// the test that owns the state ends. The waiter subscribes before the runtime
// runs, so it observes every transition the runtime records.
func runDaemon(t *testing.T, cfg config.Config, backend *scopedDaemonBackend) (*daemon.Runtime, *statuswait.Waiter) {
	t.Helper()
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	runtime, err := daemon.New(cfg, backend, guestInstallation, logger)
	if err != nil {
		t.Fatal(err)
	}
	waiter := statuswait.New(t, runtime)
	serveDaemon(t, runtime)
	return runtime, waiter
}

// serveDaemon runs runtime until the test that owns it ends, and returns a
// stop that shuts it down early and reports how it stopped. Cleanups run in
// reverse order of registration, so a runtime served after its fixtures
// registered theirs is stopped before they are destroyed, also when the test
// fails partway; a runtime still running then would have jobs fail on the
// datasets destroyed under them.
func serveDaemon(t *testing.T, runtime *daemon.Runtime) func(*testing.T) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	var once sync.Once
	stop := func(t *testing.T) {
		t.Helper()
		once.Do(func() {
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
	}
	t.Cleanup(func() { stop(t) })
	return stop
}

// freePort reserves and releases a loopback port. The pairing bundle has to
// name the endpoint before anything listens on it, which is exactly the
// arrangement the backoff test needs.
func freePort(t *testing.T) string {
	t.Helper()
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
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
	_, waiter := runDaemon(t, cfg, &scopedDaemonBackend{Direct: direct, root: source})

	// The guest is both hosts, so the remote receives into this host's own
	// pool: the first attempt is refused and retried once the daemon has
	// revalidated the remote's replicas as local.
	success, _ := waiter.Outcome(t, 3*time.Minute, 0, "remote:"+source+":backup", "succeeded", "waiting-retry")
	if !strings.HasPrefix(success.Target, "native://") || !strings.HasSuffix(success.Target, "/"+destinationRoot) {
		t.Fatalf("native transfer did not record a canonical native target: %q", success.Target)
	}
	if success.Destination != destinationRoot {
		t.Fatalf("native transfer landed on %q, want %s", success.Destination, destinationRoot)
	}
	waitForCreation(t, waiter, 30*time.Second, 0, source, success.Snapshot)
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
	_, component, _ := strings.Cut(success.Snapshot, "@")
	if !slices.ContainsFunc(state.Objects, func(object zfs.Object) bool { return object.Name == destinationRoot+"@"+component }) {
		t.Fatalf("destination does not carry %s, which the transfer reported landing: %+v", success.Snapshot, state.Objects)
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
		metadata, createErr := service.CreateSnapshot(t.Context(), source, []string{source}, now.Add(-age), effective)
		if createErr != nil {
			t.Fatal(createErr)
		}
		expired = append(expired, source+"@"+metadata.Name())
	}

	cfg := config.Defaults()
	cfg.Daemon.ReconcileInterval.Duration = 200 * time.Millisecond
	cfg.Daemon.ManagementWorkers = 2
	_, waiter := runDaemon(t, cfg, &scopedDaemonBackend{Direct: direct, root: source})

	// The prune a snapshot job queues names what it destroyed: the expired
	// snapshots and not the one the snapshot job created, which the grid
	// keeps. The pool is read once against those names.
	kept, _ := createdSnapshot(t, waiter, 60*time.Second, 0, source)
	pruned, _ := waiter.Outcome(t, 60*time.Second, 0, "prune:"+source, "succeeded")
	if pruned.DestroyedCount != len(pruned.Destroyed) || slices.Contains(pruned.Destroyed, kept) {
		t.Fatalf("prune destroyed %v (count %d), want the expired snapshots and not %s", pruned.Destroyed, pruned.DestroyedCount, kept)
	}
	for _, snapshot := range expired {
		if !slices.Contains(pruned.Destroyed, snapshot) {
			t.Fatalf("prune job succeeded without destroying %s outside the grid: %v", snapshot, pruned.Destroyed)
		}
	}
	state, err := direct.InspectState(t.Context(), source, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, snapshot := range lifecycle.Snapshots(state, source) {
		if slices.Contains(pruned.Destroyed, snapshot.Name) {
			t.Fatalf("prune reported destroying %s, which remains", snapshot.Name)
		}
	}
	requireSnapshotAgreesWithEvents(t, direct, waiter, source, kept)
	t.Logf("prune job destroyed %d expired snapshots on %s", len(expired), source)
}

// rewriteVolumeBlock overwrites one 64K block of a volume with fresh random
// data and flushes it, so the next snapshot differs from the one before.
func rewriteVolumeBlock(t *testing.T, volume string, block int) {
	t.Helper()
	write := exec.CommandContext(t.Context(), "dd", "if=/dev/urandom", "of=/dev/zvol/"+volume, "bs=64K", "count=1",
		"seek="+strconv.Itoa(block), "conv=notrunc,fsync", "status=none")
	if output, err := write.CombinedOutput(); err != nil {
		t.Fatalf("rewrite %s block %d: %v: %s", volume, block, err, output)
	}
}

// TestGuestDaemonDestinationPruneKeepsReplicaBase asserts that the destination
// prune following a transfer keeps the snapshot it just received, and that the
// next incremental is received onto it. Retention counts back from the newest
// snapshot and keeps the oldest in each window, so when the replica's previous
// snapshot is less than a window older - as after a policy moves to a coarser
// first tier - the newest used to lose its window and be destroyed. The
// replica's head then held data newer than its newest remaining snapshot, and
// every later receive was refused with "destination ... has been modified
// since most recent snapshot".
func TestGuestDaemonDestinationPruneKeepsReplicaBase(t *testing.T) {
	sourcePool, destinationPool := guestPools(t)
	direct, err := zfs.NewDirect("zfs")
	if err != nil {
		t.Fatal(err)
	}
	source := zfstest.PayloadVolume(t, zfstest.FixtureName(sourcePool, "daemon-replica-base"), 64, 4)
	target := zfstest.FixtureName(destinationPool, "daemon-replica-base")
	zfsCommand(t, "create", "-u", target)
	zfstest.RegisterCleanup(t, target)
	zfsCommand(t, "set", policy.Namespace+"enabled=on", policy.Namespace+"policy=24x1h", policy.Namespace+"local="+target, policy.Namespace+"discard=first", source)

	rows, err := direct.GetStoredProperties(t.Context(), []string{source})
	if err != nil {
		t.Fatal(err)
	}
	effective := policy.Resolve(zfs.Dataset{Name: source, Type: zfs.Volume, EncryptionRoot: "-"}, nil, rows, nil)
	service, err := lifecycle.NewService(direct, guestInstallation)
	if err != nil {
		t.Fatal(err)
	}
	// The replica's first snapshot is well inside the hour the first window
	// spans, and not yet due a successor, so the scheduler takes nothing of its
	// own while the test runs.
	first, err := service.CreateSnapshot(t.Context(), source, []string{source}, time.Now().UTC().Add(-45*time.Minute), effective)
	if err != nil {
		t.Fatal(err)
	}
	base := source + "@" + first.Name()

	cfg := config.Defaults()
	cfg.Daemon.ReconcileInterval.Duration = 200 * time.Millisecond
	cfg.Daemon.ManagementWorkers = 2
	runtime, waiter := runDaemon(t, cfg, &scopedDaemonBackend{Direct: direct, root: source})
	job := "local:" + source + ":" + target
	prune := "destination-prune:" + source + ":" + lifecycle.TargetID("local:"+target)

	// Activating a root that already has an owned snapshot sends it.
	bootstrap, _ := waiter.Outcome(t, 90*time.Second, 0, job, "succeeded")
	if bootstrap.Snapshot != base {
		t.Fatalf("the activation transfer carried %q, want %s", bootstrap.Snapshot, base)
	}
	replica := bootstrap.Destination

	// transfer changes the volume, forces a snapshot, and waits for the
	// transfer carrying it and the destination prune that follows.
	transfer := func(t *testing.T, block int) string {
		t.Helper()
		rewriteVolumeBlock(t, source, block)
		from := waiter.Settle(t, 30*time.Second)
		if accepted, triggerErr := runtime.Trigger([]string{source}); triggerErr != nil || len(accepted) != 1 {
			t.Fatalf("trigger %s: accepted=%v err=%v", source, accepted, triggerErr)
		}
		snapshot, after := createdSnapshot(t, waiter, 45*time.Second, from, source, "scheduled")
		waitForTransferred(t, waiter, job, source, after-1)
		_, landed := waiter.Next(t, job+" to report landing "+snapshot, 5*time.Second, after-1, func(event daemon.Event) bool {
			return event.Job == job && event.State == "succeeded" && event.Snapshot == snapshot
		})
		waiter.Outcome(t, 60*time.Second, landed, prune, "succeeded")
		return snapshot
	}

	newest := transfer(t, 1)
	_, component, _ := strings.Cut(newest, "@")
	state, err := direct.InspectState(t.Context(), replica, false)
	if err != nil {
		t.Fatal(err)
	}
	var kept []string
	for _, snapshot := range lifecycle.Snapshots(state, replica) {
		kept = append(kept, snapshot.Name)
	}
	if !slices.Contains(kept, replica+"@"+component) {
		t.Fatalf("destination prune destroyed the snapshot the replica just received, %s; the replica keeps %v", replica+"@"+component, kept)
	}
	written, err := exec.CommandContext(t.Context(), "zfs", "get", "-H", "-p", "-o", "value", "written", replica).CombinedOutput()
	if err != nil || strings.TrimSpace(string(written)) != "0" {
		t.Fatalf("replica %s has data written since its newest snapshot: %q err=%v", replica, written, err)
	}

	// The next incremental is received onto the snapshot destination prune kept.
	next := transfer(t, 2)
	t.Logf("destination prune kept %s, and %s was received onto it", newest, next)
}

// TestGuestDaemonRemoteBackoff asserts the retry sequence between an outage
// and its recovery, not just the endpoints. Every real attempt records its
// transport failure as the reason, while the reconciler's early returns while
// a backoff is still running record nothing, so each outcome of the job is an
// attempt, counted from the subscription. The endpoint is named before anything binds it, then the listener
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
	_, waiter := runDaemon(t, cfg, &scopedDaemonBackend{Direct: direct, root: source})

	job := "remote:" + source + ":backup"
	attempts := make([]time.Time, 0, 3)
	var attempted statuswait.Cursor
	for range 3 {
		var attempt daemon.Event
		attempt, attempted = waiter.Outcome(t, 90*time.Second, attempted, job, "waiting-retry")
		if attempt.Reason == "" {
			t.Fatalf("attempt %d reported waiting-retry without its transport failure", len(attempts)+1)
		}
		// An attempt names the pending snapshot it carried, if it carried
		// one; see TestGuestRemoteOutageReconnection.
		if attempt.Snapshot != "" {
			waitForCreation(t, waiter, 30*time.Second, 0, source, attempt.Snapshot)
		}
		attempts = append(attempts, attempt.At)
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

	success, _ := waiter.Outcome(t, 3*time.Minute, attempted, job, "succeeded", "waiting-retry")
	if success.Target == "" {
		t.Fatal("recovered transfer did not retain canonical target identity")
	}
	waitForCreation(t, waiter, 30*time.Second, 0, source, success.Snapshot)
	if _, err := direct.InspectDatasetIdentity(t.Context(), destinationRoot); err != nil {
		t.Fatalf("recovered transfer did not create the destination: %v", err)
	}
	t.Logf("backoff grew %s then %s before %s recovered", first, second, success.Target)
}
