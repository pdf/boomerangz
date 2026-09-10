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
)

type sendInterval struct{ start, end int64 }

type observedLocalStream struct {
	delegate transfer.Stream
	delay    time.Duration
	tracked  map[string]bool
	mu       sync.Mutex
	interval []struct {
		dataset string
		sendInterval
	}
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
	s.interval = append(s.interval, struct {
		dataset string
		sendInterval
	}{dataset: send.Source, sendInterval: sendInterval{start: started, end: time.Now().UnixNano()}})
	s.mu.Unlock()
	return progress, err
}

func (s *observedLocalStream) reset() {
	s.mu.Lock()
	s.interval = nil
	s.mu.Unlock()
}

func (s *observedLocalStream) snapshot() map[string]sendInterval {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make(map[string]sendInterval, len(s.interval))
	for _, interval := range s.interval {
		if _, exists := result[interval.dataset]; !exists {
			result[interval.dataset] = interval.sendInterval
		}
	}
	return result
}

func waitForSendIntervals(t *testing.T, stream *observedLocalStream, runtime *daemon.Runtime, datasets []string) map[string]sendInterval {
	t.Helper()
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		intervals := stream.snapshot()
		complete := true
		for _, dataset := range datasets {
			if intervals[dataset].end == 0 {
				complete = false
				break
			}
		}
		if complete {
			return intervals
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for ZFS sends from %v: %+v", datasets, runtime.Status())
	return nil
}

func sendIntervalsOverlap(a, b sendInterval) bool { return a.start < b.end && b.start < a.end }

func waitForTransferSuccesses(t *testing.T, runtime *daemon.Runtime, jobs []string, after time.Time) {
	t.Helper()
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		latest := make(map[string]daemon.Event, len(jobs))
		for _, event := range runtime.Status() {
			if slices.Contains(jobs, event.Job) && event.At.After(after) {
				latest[event.Job] = event
			}
		}
		succeeded := 0
		for _, job := range jobs {
			event := latest[job]
			if event.State == "succeeded" {
				succeeded++
			}
			if event.State == "failed" || event.State == "blocked" || event.State == "waiting-retry" {
				t.Fatalf("transfer %s ended in %s: %s", job, event.State, event.Reason)
			}
		}
		if succeeded == len(jobs) {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for completed transfers %v: %+v", jobs, runtime.Status())
}

// This test is opt-in and must run in the disposable guest, never on the host.
func TestGuestDaemonSchedulingAndRetirement(t *testing.T) {
	runID := os.Getenv("BOOMERANGZ_DAEMON_GUEST_RUN")
	if runID == "" {
		t.Skip("disposable guest only")
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
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	waitFor := func(description string, check func() bool) {
		t.Helper()
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) {
			if check() {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Fatalf("timed out waiting for %s", description)
	}
	waitFor("scheduled source and descendant snapshots", func() bool {
		sourceState, sourceErr := direct.InspectState(t.Context(), source, false)
		childState, childErr := direct.InspectState(t.Context(), child, false)
		return sourceErr == nil && childErr == nil && len(lifecycle.Snapshots(sourceState, source)) == 1 && len(lifecycle.Snapshots(childState, child)) == 1
	})
	command("set", policy.Namespace+"enabled=off", child)
	waitFor("independent descendant deactivation", func() bool {
		state, inspectErr := direct.InspectState(t.Context(), child, false)
		if inspectErr != nil {
			return false
		}
		for _, property := range state.Properties {
			if property.Dataset == child && property.Name == lifecycle.InactiveProperty && property.Source == zfs.SourceLocal {
				return true
			}
		}
		return false
	})
	command("set", policy.Namespace+"enabled=off", source)
	waitFor("inactive marker", func() bool {
		state, inspectErr := direct.InspectState(t.Context(), source, false)
		if inspectErr != nil {
			return false
		}
		for _, property := range state.Properties {
			if property.Dataset == source && property.Name == lifecycle.InactiveProperty && property.Source == zfs.SourceLocal {
				return true
			}
		}
		return false
	})
	waitFor("source retirement", func() bool {
		sourceState, sourceErr := direct.InspectState(t.Context(), source, false)
		return sourceErr == nil && len(lifecycle.Snapshots(sourceState, source)) == 0
	})
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
		t.Skip("disposable guest only")
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
	observed := &observedLocalStream{
		delegate: localStream,
		delay:    2 * time.Second,
		tracked:  map[string]bool{root: true, left: true, right: true},
	}
	cfg := config.Defaults()
	cfg.Daemon.ReconcileInterval.Duration = 100 * time.Millisecond
	cfg.Daemon.ManagementWorkers = 3
	cfg.Daemon.LocalTransferWorkers = 2
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	runtime, err := daemon.NewWithLocalStream(cfg, direct, "abcdefab-cdef-4abc-8def-abcdefabcdef", logger, observed)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	job := func(dataset string) string { return "local:" + dataset + ":" + target }
	phase := time.Now().UTC()
	go func() { done <- runtime.Run(ctx) }()

	// All mapped destinations are initially absent, so the root must establish
	// the shared hierarchy before either descendant starts. Once it has, the
	// sibling setup streams no longer conflict with one another.
	initial := waitForSendIntervals(t, observed, runtime, []string{root, left, right})
	if initial[root].end > initial[left].start || initial[root].end > initial[right].start {
		t.Fatalf("initial destination setup overlapped hierarchy-conflicting sends: %+v", initial)
	}
	waitForTransferSuccesses(t, runtime, []string{job(root), job(left), job(right)}, phase)

	observed.reset()
	phase = time.Now().UTC()
	accepted, err := runtime.Trigger([]string{left, right})
	if err != nil || len(accepted) != 2 {
		t.Fatalf("trigger siblings=%v err=%v", accepted, err)
	}
	siblings := waitForSendIntervals(t, observed, runtime, []string{left, right})
	if !sendIntervalsOverlap(siblings[left], siblings[right]) {
		t.Fatalf("existing sibling destinations did not send concurrently: %+v", siblings)
	}
	waitForTransferSuccesses(t, runtime, []string{job(left), job(right)}, phase)

	observed.reset()
	phase = time.Now().UTC()
	accepted, err = runtime.Trigger([]string{root, left})
	if err != nil || len(accepted) != 2 {
		t.Fatalf("trigger ancestor pair=%v err=%v", accepted, err)
	}
	ancestorPair := waitForSendIntervals(t, observed, runtime, []string{root, left})
	if sendIntervalsOverlap(ancestorPair[root], ancestorPair[left]) {
		t.Fatalf("ancestor and descendant destinations sent concurrently: %+v", ancestorPair)
	}
	waitForTransferSuccesses(t, runtime, []string{job(root), job(left)}, phase)

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
		t.Skip("disposable guest only")
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
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	remoteJob := "remote:" + source + ":home"
	waitForEvent := func(state string, timeout time.Duration) daemon.Event {
		t.Helper()
		deadline := time.Now().Add(timeout)
		for time.Now().Before(deadline) {
			for _, event := range runtime.Status() {
				if event.Job == remoteJob && event.State == state {
					return event
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Fatalf("timed out waiting for remote state %q: %+v", state, runtime.Status())
		return daemon.Event{}
	}
	failure := waitForEvent("waiting-retry", 30*time.Second)
	if failure.Reason == "" {
		t.Fatal("outage did not retain its transport failure reason")
	}
	initialName := ""
	initialDeadline := time.Now().Add(30 * time.Second)
	for initialName == "" {
		state, inspectErr := direct.InspectState(t.Context(), source, true)
		if inspectErr == nil {
			lineage, authorityErr := lifecycle.RootAuthority(state, source, "abcdefab-cdef-4abc-8def-abcdefabcdef")
			references, referenceErr := lifecycle.References(state, source, lineage)
			for _, reference := range references {
				snapshot := reference.SnapshotName(source)
				if authorityErr == nil && referenceErr == nil && reference.Target == failure.Target && slices.Contains(state.Holds[snapshot], reference.HoldName()) {
					initialName = snapshot
				}
			}
		}
		if time.Now().After(initialDeadline) {
			t.Fatalf("timed out waiting for the initial pending snapshot hold: %+v", runtime.Status())
		}
		if initialName == "" {
			time.Sleep(100 * time.Millisecond)
		}
	}
	coalesceDeadline := time.Now().Add(90 * time.Second)
	for {
		state, inspectErr := direct.InspectState(t.Context(), source, true)
		if inspectErr == nil {
			lineage, authorityErr := lifecycle.RootAuthority(state, source, "abcdefab-cdef-4abc-8def-abcdefabcdef")
			references, referenceErr := lifecycle.References(state, source, lineage)
			if authorityErr == nil && referenceErr == nil && slices.ContainsFunc(references, func(reference lifecycle.Reference) bool {
				snapshot := reference.SnapshotName(source)
				return reference.Target == failure.Target && snapshot != initialName && slices.Contains(state.Holds[snapshot], reference.HoldName())
			}) {
				break
			}
		}
		if time.Now().After(coalesceDeadline) {
			t.Fatalf("timed out waiting for a newer coalesced snapshot hold: %+v", runtime.Status())
		}
		time.Sleep(100 * time.Millisecond)
	}
	if _, err := fmt.Fprintln(os.Stdout, "BOOMERANGZ_REMOTE_OUTAGE_OBSERVED"); err != nil {
		t.Fatal(err)
	}
	success := waitForEvent("succeeded", 3*time.Minute)
	if success.Target == "" {
		t.Fatal("reconnected transfer did not retain canonical target identity")
	}
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
