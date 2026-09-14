//go:build integration

package daemon_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pdf/boomerangz/internal/config"
	"github.com/pdf/boomerangz/internal/daemon"
	"github.com/pdf/boomerangz/internal/lifecycle"
	"github.com/pdf/boomerangz/internal/policy"
	"github.com/pdf/boomerangz/internal/testutil/zfstest"
	"github.com/pdf/boomerangz/internal/transfer"
	"github.com/pdf/boomerangz/internal/zfs"
	"github.com/pdf/boomerangz/test/integration/internal/statuswait"
)

type sendInterval struct{ start, end int64 }

type scopedDaemonBackend struct {
	*zfs.Direct
	root string
}

func (b *scopedDaemonBackend) scoped(properties []zfs.Property) []zfs.Property {
	return slices.DeleteFunc(properties, func(property zfs.Property) bool {
		return property.Dataset != b.root && !strings.HasPrefix(property.Dataset, b.root+"/")
	})
}

func (b *scopedDaemonBackend) GetActivationProperties(ctx context.Context) ([]zfs.Property, error) {
	properties, err := b.Direct.GetActivationProperties(ctx)
	return b.scoped(properties), err
}

func (b *scopedDaemonBackend) GetLifecycleProperties(ctx context.Context) ([]zfs.Property, error) {
	properties, err := b.Direct.GetLifecycleProperties(ctx)
	return b.scoped(properties), err
}

// observedLocalStream records when each tracked send ran, and which snapshot
// it sent, so a test can tie a send to the snapshot job that created it.
type observedLocalStream struct {
	delegate transfer.Stream
	delay    time.Duration
	tracked  map[string]bool
	mu       sync.Mutex
	changed  chan struct{} // closed and replaced on every recorded send
	sends    []observedSend
}

type observedSend struct {
	snapshot string
	sendInterval
}

func newObservedLocalStream(delegate transfer.Stream, delay time.Duration, tracked map[string]bool) *observedLocalStream {
	return &observedLocalStream{delegate: delegate, delay: delay, tracked: tracked, changed: make(chan struct{})}
}

func (s *observedLocalStream) Run(ctx context.Context, send zfs.SendOptions, receive zfs.ReceiveOptions, estimate zfs.Estimate, report func(zfs.Progress)) (zfs.Progress, error) {
	if !s.tracked[send.Source] {
		return s.delegate.Run(ctx, send, receive, estimate, report)
	}
	started := time.Now().UnixNano()
	timer := time.NewTimer(s.delay)
	select {
	case <-ctx.Done():
		timer.Stop()
		return zfs.Progress{}, ctx.Err()
	case <-timer.C:
	}
	progress, err := s.delegate.Run(ctx, send, receive, estimate, report)
	s.mu.Lock()
	s.sends = append(s.sends, observedSend{snapshot: send.Snapshot, sendInterval: sendInterval{start: started, end: time.Now().UnixNano()}})
	close(s.changed)
	s.changed = make(chan struct{})
	s.mu.Unlock()
	return progress, err
}

// recorded returns the sends recorded so far, and a channel closed on the
// next one.
func (s *observedLocalStream) recorded() ([]observedSend, <-chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.sends), s.changed
}

// waitForSends waits for each dataset's send of the snapshot its snapshot job
// created at created[dataset], or of one its snapshot jobs created later,
// which supersedes it, and returns when each ran.
func waitForSends(t *testing.T, stream *observedLocalStream, waiter *statuswait.Waiter, created map[string]statuswait.Cursor) map[string]sendInterval {
	t.Helper()
	timeout := time.NewTimer(45 * time.Second)
	defer timeout.Stop()
	for {
		sends, sent := stream.recorded()
		delivered := waiter.Changed()
		transitions := waiter.Transitions()
		intervals := make(map[string]sendInterval, len(created))
		for dataset, from := range created {
			accepted := snapshotsCreated(transitions[from:], dataset)
			if index := slices.IndexFunc(sends, func(send observedSend) bool { return slices.Contains(accepted, send.snapshot) }); index >= 0 {
				intervals[dataset] = sends[index].sendInterval
			}
		}
		if len(intervals) == len(created) {
			return intervals
		}
		select {
		case <-sent:
		case <-delivered:
		case <-timeout.C:
			t.Fatalf("timed out waiting for sends of %v, found %+v among %+v; transitions: %+v", created, intervals, sends, transitions)
		}
	}
}

func sendIntervalsOverlap(a, b sendInterval) bool { return a.start < b.end && b.start < a.end }

// waitForTransferred waits for job to succeed naming the snapshot dataset's
// snapshot job created at created, or one it created later. A run that
// succeeds naming an older snapshot found its target already holding that one
// and is skipped; so are the outcomes in retried. Any other outcome fails the
// wait at once.
func waitForTransferred(t *testing.T, waiter *statuswait.Waiter, job, dataset string, created statuswait.Cursor, retried ...string) daemon.Event {
	t.Helper()
	var landed daemon.Event
	waiter.Until(t, job+" to transfer the snapshot created at transition "+strconv.Itoa(int(created)), 45*time.Second, created, func(transitions []daemon.Event) (bool, error) {
		accepted := snapshotsCreated(transitions, dataset)
		for _, event := range transitions {
			switch {
			case event.Job != job || statuswait.Running(event) || slices.Contains(retried, event.State):
			case event.State == "succeeded" && slices.Contains(accepted, event.Snapshot):
				landed = event
				return true, nil
			case event.State == "succeeded":
			default:
				return false, fmt.Errorf("%s ended %s carrying %q: %s", job, event.State, event.Snapshot, event.Reason)
			}
		}
		return false, nil
	})
	return landed
}

// This test is opt-in and must run in the disposable guest, never on the host.
func TestGuestDaemonSchedulingAndRetirement(t *testing.T) {
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
	if _, err := zfstest.VerifyGuestPool(t.Context(), runID, zfstest.DestinationDisk, destinationDevice); err != nil {
		t.Fatal(err)
	}
	command := func(args ...string) {
		t.Helper()
		if output, commandErr := exec.CommandContext(t.Context(), "zfs", args...).CombinedOutput(); commandErr != nil {
			t.Fatalf("guest zfs %s: %v: %s", args[0], commandErr, output)
		}
	}
	source := sourcePool + "/data/daemon-" + time.Now().UTC().Format("150405000")
	child := source + "/child"
	command("create", "-u", source)
	command("create", "-u", child)
	zfstest.RegisterCleanup(t, source)
	command("set", policy.Namespace+"enabled=on", policy.Namespace+"policy=1x1m", source)
	direct, err := zfs.NewDirect("zfs")
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Defaults()
	cfg.Daemon.ReconcileInterval.Duration = 100 * time.Millisecond
	cfg.Daemon.InactiveGracePeriod.Duration = 2 * time.Second
	cfg.Daemon.ManagementWorkers = 2
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	runtime, err := daemon.New(cfg, direct, "abcdefab-cdef-4abc-8def-abcdefabcdef", logger)
	if err != nil {
		t.Fatal(err)
	}
	waiter := statuswait.New(t, runtime)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	// Each wait is for the outcome the behaviour is about, which names what it
	// acted on. The pool is then read once and checked against those names,
	// as far as the runs recorded before the read decide them.
	sourceSnapshot, _ := createdSnapshot(t, waiter, 30*time.Second, 0, source)
	childSnapshot, _ := createdSnapshot(t, waiter, 30*time.Second, 0, child)
	requireSnapshotAgreesWithEvents(t, direct, waiter, source, sourceSnapshot)
	requireSnapshotAgreesWithEvents(t, direct, waiter, child, childSnapshot)

	// Disabling a dataset reconciles it inactive, which reports setting the
	// marker once it has verified it. Retirement clears the marker, and is
	// eligible only while the marker is present, so a read that misses the
	// marker is a failure unless retirement had started by then, and
	// retirement must then succeed.
	requireInactiveMarker := func(t *testing.T, dataset string) {
		t.Helper()
		command("set", policy.Namespace+"enabled=off", dataset)
		reconciled, _ := waiter.Outcome(t, 30*time.Second, 0, "inactive:"+dataset+":false", "succeeded")
		if reconciled.Marker != "set" {
			t.Fatalf("inactive reconciliation of %s reported marker action %q, want set", dataset, reconciled.Marker)
		}
		state, inspectErr := direct.InspectState(t.Context(), dataset, false)
		if inspectErr != nil {
			t.Fatal(inspectErr)
		}
		settled := waiter.Settle(t, 30*time.Second)
		if slices.ContainsFunc(state.Properties, func(property zfs.Property) bool {
			return property.Dataset == dataset && property.Name == lifecycle.InactiveProperty && property.Source == zfs.SourceLocal
		}) {
			return
		}
		retire := "retire:" + dataset
		if !slices.ContainsFunc(statuswait.Runs(waiter.Transitions()[:settled], retire), statuswait.Run.Started) {
			t.Fatalf("inactive reconciliation of %s set its marker, but the marker was absent before retirement started; transitions: %+v", dataset, waiter.Transitions()[:settled])
		}
		waiter.Outcome(t, 30*time.Second, 0, retire, "succeeded", "waiting-retry")
	}
	requireInactiveMarker(t, child)
	requireInactiveMarker(t, source)

	// Retirement destroys every owned snapshot of the source, so it names
	// each one the source's snapshot jobs created and its prunes did not
	// destroy. Every snapshot it names is then absent: a destroyed name does
	// not come back.
	retired, after := waiter.Outcome(t, 30*time.Second, 0, "retire:"+source, "succeeded", "waiting-retry")
	if retired.DestroyedCount != len(retired.Destroyed) {
		t.Fatalf("source retirement destroyed %d snapshots but named %d", retired.DestroyedCount, len(retired.Destroyed))
	}
	before := waiter.Transitions()[:after]
	for _, snapshot := range snapshotsCreated(before, source) {
		if !slices.Contains(retired.Destroyed, snapshot) && destructionOf(before, snapshot, "prune:"+source) == notDestroyed {
			t.Fatalf("source retirement did not destroy %s, which no prune destroyed: %v", snapshot, retired.Destroyed)
		}
	}
	sourceState, err := direct.InspectState(t.Context(), source, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, snapshot := range lifecycle.Snapshots(sourceState, source) {
		if slices.Contains(retired.Destroyed, snapshot.Name) {
			t.Fatalf("source retirement reported destroying %s, which remains", snapshot.Name)
		}
	}
	cancel()
	select {
	case runErr := <-done:
		if runErr != nil {
			t.Fatal(runErr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("daemon did not shut down after guest test")
	}
	t.Logf("verified daemon scheduling, descendant deactivation, and retirement on %s", source)
}

func TestGuestLocalTransferConcurrency(t *testing.T) {
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
	command := func(args ...string) {
		t.Helper()
		if output, commandErr := exec.CommandContext(t.Context(), "zfs", args...).CombinedOutput(); commandErr != nil {
			t.Fatalf("guest zfs %s: %v: %s", args[0], commandErr, output)
		}
	}
	suffix := time.Now().UTC().Format("150405000")
	root := sourcePool + "/data/concurrency-" + suffix
	left, right := root+"/left", root+"/right"
	target := destinationPool + "/data/concurrency-" + suffix
	command("create", "-u", root)
	command("create", "-u", left)
	command("create", "-u", right)
	command("create", "-u", target)
	zfstest.RegisterCleanup(t, root)
	zfstest.RegisterCleanup(t, target)
	command("set", policy.Namespace+"enabled=on", policy.Namespace+"policy=1x1h", policy.Namespace+"local="+target, policy.Namespace+"discard=first", root)

	direct, err := zfs.NewDirect("zfs")
	if err != nil {
		t.Fatal(err)
	}
	localStream, err := zfs.NewLocalStream("zfs")
	if err != nil {
		t.Fatal(err)
	}
	// Keep each acquired transfer lock observable without relying on fixture
	// data volume, while delegating every stream to the real local ZFS pipeline.
	observed := newObservedLocalStream(localStream, 2*time.Second, map[string]bool{root: true, left: true, right: true})
	cfg := config.Defaults()
	cfg.Daemon.ReconcileInterval.Duration = 100 * time.Millisecond
	cfg.Daemon.ManagementWorkers = 3
	cfg.Daemon.LocalTransferWorkers = 2
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	backend := &scopedDaemonBackend{Direct: direct, root: root}
	runtime, err := daemon.NewWithLocalStream(cfg, backend, "abcdefab-cdef-4abc-8def-abcdefabcdef", logger, observed)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	job := func(dataset string) string { return "local:" + dataset + ":" + target }
	waiter := statuswait.New(t, runtime)
	go func() { done <- runtime.Run(ctx) }()

	// Each phase names the snapshots it transfers: the ones its snapshot jobs
	// report creating. Its sends and transfer outcomes are those that carry
	// them, whatever else ran around them.
	phase := func(t *testing.T, from statuswait.Cursor, datasets []string, retried ...string) map[string]sendInterval {
		t.Helper()
		created := make(map[string]statuswait.Cursor, len(datasets))
		for _, dataset := range datasets {
			_, started := waiter.Next(t, "a snapshot job for "+dataset, 45*time.Second, from, statuswait.Job("snapshot:"+dataset, "snapshotting"))
			// A scheduled run found the deadline not yet due and is followed
			// by the forced one.
			_, after := createdSnapshot(t, waiter, 45*time.Second, started, dataset, "scheduled")
			created[dataset] = after - 1
		}
		intervals := waitForSends(t, observed, waiter, created)
		for _, dataset := range datasets {
			waitForTransferred(t, waiter, job(dataset), dataset, created[dataset], retried...)
		}
		return intervals
	}

	// All mapped destinations are initially absent, so the root must establish
	// the shared hierarchy before either descendant starts. Once it has, the
	// sibling setup streams no longer conflict with one another. A descendant
	// that runs first reports waiting-retry for the missing ancestor and is
	// retried; once the destinations exist, nothing should wait.
	initial := phase(t, 0, []string{root, left, right}, "waiting-retry")
	if initial[root].end > initial[left].start || initial[root].end > initial[right].start {
		t.Fatalf("initial destination setup overlapped hierarchy-conflicting sends: %+v", initial)
	}

	// A triggered snapshot job starts after the trigger, so settling first
	// finds the snapshot job the trigger ran rather than an earlier one.
	from := waiter.Settle(t, 30*time.Second)
	accepted, err := runtime.Trigger([]string{left, right})
	if err != nil || len(accepted) != 2 {
		t.Fatalf("trigger siblings=%v err=%v", accepted, err)
	}
	siblings := phase(t, from, []string{left, right})
	if !sendIntervalsOverlap(siblings[left], siblings[right]) {
		t.Fatalf("existing sibling destinations did not send concurrently: %+v", siblings)
	}

	from = waiter.Settle(t, 30*time.Second)
	accepted, err = runtime.Trigger([]string{root, left})
	if err != nil || len(accepted) != 2 {
		t.Fatalf("trigger ancestor pair=%v err=%v", accepted, err)
	}
	ancestorPair := phase(t, from, []string{root, left})
	if sendIntervalsOverlap(ancestorPair[root], ancestorPair[left]) {
		t.Fatalf("ancestor and descendant destinations sent concurrently: %+v", ancestorPair)
	}

	cancel()
	select {
	case runErr := <-done:
		if runErr != nil {
			t.Fatal(runErr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("daemon did not shut down after concurrency test")
	}
	t.Log("verified conservative destination setup, concurrent siblings, and ancestor exclusion")
}

func TestGuestRemoteOutageReconnection(t *testing.T) {
	runID := os.Getenv("BOOMERANGZ_DAEMON_GUEST_RUN")
	if runID == "" {
		t.Fatal("BOOMERANGZ_DAEMON_GUEST_RUN is unset: the disposable guest harness did not provide a run ID")
	}
	remoteHost := os.Getenv("BOOMERANGZ_REMOTE_GUEST_HOST")
	remoteRoot := os.Getenv("BOOMERANGZ_REMOTE_GUEST_ROOT")
	remoteKey := os.Getenv("BOOMERANGZ_REMOTE_GUEST_KEY")
	remoteUser := os.Getenv("BOOMERANGZ_REMOTE_GUEST_USER")
	if remoteHost == "" || remoteRoot == "" || remoteKey == "" || remoteUser == "" {
		t.Fatal("remote guest host, root, key, and user are required")
	}
	remotePort := 22
	if value := os.Getenv("BOOMERANGZ_REMOTE_GUEST_PORT"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil {
			t.Fatal(err)
		}
		remotePort = parsed
	}
	remoteEndpoint := os.Getenv("BOOMERANGZ_REMOTE_GUEST_ENDPOINT")
	if remoteEndpoint == "" {
		remoteEndpoint = "direct"
	}
	remoteCLI := os.Getenv("BOOMERANGZ_REMOTE_GUEST_CLI")
	if remoteEndpoint == "ssh-shell" && remoteCLI == "" {
		t.Fatal("remote boomerangz executable is required for SSH-shell")
	}
	sourceDevice := os.Getenv("BOOMERANGZ_INTEGRATION_SOURCE_DEVICE")
	if sourceDevice == "" {
		t.Fatal("source test device is required")
	}
	sourcePool, err := zfstest.VerifyGuestPool(t.Context(), runID, zfstest.SourceDisk, sourceDevice)
	if err != nil {
		t.Fatal(err)
	}
	command := func(args ...string) {
		t.Helper()
		if output, commandErr := exec.CommandContext(t.Context(), "zfs", args...).CombinedOutput(); commandErr != nil {
			t.Fatalf("guest zfs %s: %v: %s", args[0], commandErr, output)
		}
	}
	source := sourcePool + "/data/outage-" + time.Now().UTC().Format("150405000")
	command("create", "-u", source)
	zfstest.RegisterCleanup(t, source)
	command("set", policy.Namespace+"enabled=on", policy.Namespace+"policy=1x1m", policy.Namespace+"remote=home", source)
	direct, err := zfs.NewDirect("zfs")
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Defaults()
	cfg.Daemon.ReconcileInterval.Duration = time.Second
	cfg.Daemon.ManagementWorkers = 2
	cfg.Remotes["home"] = config.RemoteConfig{
		Transport: "ssh", Endpoint: remoteEndpoint, Host: remoteHost, Port: remotePort,
		User: remoteUser, Root: remoteRoot, IdentityFile: remoteKey, SSHShellPath: remoteCLI,
		ConnectTimeout: config.Duration{Duration: 10 * time.Second},
	}
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	runtime, err := daemon.New(cfg, direct, "abcdefab-cdef-4abc-8def-abcdefabcdef", logger)
	if err != nil {
		t.Fatal(err)
	}
	waiter := statuswait.New(t, runtime)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	remoteJob := "remote:" + source + ":home"
	snapshotJob := "snapshot:" + source
	failure, afterFailure := waiter.Outcome(t, 30*time.Second, 0, remoteJob, "waiting-retry")
	if failure.Reason == "" {
		t.Fatal("outage did not retain its transport failure reason")
	}
	// An attempt names the pending snapshot it carried, which a snapshot job
	// of this dataset created. A run discovery queued when the snapshot first
	// appeared can start before the snapshot job has protected it, and then
	// carries none.
	if failure.Snapshot != "" {
		waitForCreation(t, waiter, 30*time.Second, 0, source, failure.Snapshot)
	}

	// requireHeld reads the source once and requires snapshot to hold a
	// pending reference for the target. A later snapshot job coalescing
	// supersedes the reference and may release it, so the requirement holds
	// only while no snapshot job after the one that created snapshot had
	// started before the read. No transfer can release it during the outage,
	// and the events must agree.
	requireHeld := func(t *testing.T, snapshot string, created statuswait.Cursor) {
		t.Helper()
		state, inspectErr := direct.InspectState(t.Context(), source, true)
		if inspectErr != nil {
			t.Fatal(inspectErr)
		}
		settled := waiter.Settle(t, 30*time.Second)
		recorded := waiter.Transitions()[:settled]
		if slices.ContainsFunc(recorded, statuswait.Job(remoteJob, "succeeded")) {
			t.Fatalf("a transfer succeeded during the outage; transitions: %+v", recorded)
		}
		if slices.ContainsFunc(statuswait.Runs(recorded[created:], snapshotJob), func(run statuswait.Run) bool {
			// A run that found the deadline not yet due touched no holds.
			outcome, ended := run.Outcome()
			return run.Started() && (!ended || outcome.State != "scheduled")
		}) {
			return
		}
		lineage, authorityErr := lifecycle.RootAuthority(state, source, "abcdefab-cdef-4abc-8def-abcdefabcdef")
		if authorityErr != nil {
			t.Fatal(authorityErr)
		}
		references, referenceErr := lifecycle.References(state, source, lineage)
		if referenceErr != nil {
			t.Fatal(referenceErr)
		}
		if !slices.ContainsFunc(references, func(reference lifecycle.Reference) bool {
			return reference.Target == failure.Target && reference.SnapshotName(source) == snapshot && slices.Contains(state.Holds[snapshot], reference.HoldName())
		}) {
			t.Fatalf("%s holds no pending reference for %s, and no later snapshot job had started", snapshot, failure.Target)
		}
	}
	initial, afterInitial := createdSnapshot(t, waiter, 30*time.Second, 0, source)
	requireHeld(t, initial, afterInitial)
	// A snapshot run that finds the deadline not yet due is scheduled again.
	newer, afterNewer := createdSnapshot(t, waiter, 90*time.Second, afterInitial, source, "scheduled")
	if newer == initial {
		t.Fatalf("two snapshot jobs reported creating the same snapshot %s", newer)
	}
	requireHeld(t, newer, afterNewer)
	if _, err := fmt.Fprintln(os.Stdout, "BOOMERANGZ_REMOTE_OUTAGE_OBSERVED"); err != nil {
		t.Fatal(err)
	}
	success, _ := waiter.Outcome(t, 3*time.Minute, afterFailure, remoteJob, "succeeded", "waiting-retry")
	if success.Target == "" {
		t.Fatal("reconnected transfer did not retain canonical target identity")
	}
	// The reconnected transfer lands the newest coalesced snapshot: the newer
	// one or a snapshot created after it.
	waitForCreation(t, waiter, 30*time.Second, afterInitial, source, success.Snapshot)
	cancel()
	select {
	case runErr := <-done:
		if runErr != nil {
			t.Fatal(runErr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("daemon did not stop after remote outage test")
	}
	t.Logf("remote outage retried and verified %s via %s", source, success.Target)
}
