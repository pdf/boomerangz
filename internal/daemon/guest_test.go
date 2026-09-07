//go:build integration

package daemon

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/pdf/boomerangz/internal/config"
	"github.com/pdf/boomerangz/internal/lifecycle"
	"github.com/pdf/boomerangz/internal/policy"
	"github.com/pdf/boomerangz/internal/testutil/zfstest"
	"github.com/pdf/boomerangz/internal/zfs"
)

// This test is opt-in and must run in the disposable guest, never on the host.
func TestGuestDaemonSchedulingAndRetirement(t *testing.T) {
	runID := os.Getenv("BOOMERANGZ_DAEMON_GUEST_RUN")
	if runID == "" {
		t.Skip("disposable guest only")
	}
	sourcePool, err := zfstest.VerifyGuestPool(t.Context(), runID, zfstest.SourceDisk, "/dev/vdb")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := zfstest.VerifyGuestPool(t.Context(), runID, zfstest.DestinationDisk, "/dev/vdc"); err != nil {
		t.Fatal(err)
	}
	command := func(args ...string) {
		t.Helper()
		if output, commandErr := exec.CommandContext(t.Context(), "zfs", args...).CombinedOutput(); commandErr != nil {
			t.Fatalf("guest zfs %s: %v: %s", args[0], commandErr, output)
		}
	}
	source := sourcePool + "/data/daemon-" + time.Now().UTC().Format("150405000")
	command("create", "-u", source)
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
	runtime, err := New(cfg, direct, "abcdefab-cdef-4abc-8def-abcdefabcdef", logger)
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
	waitFor("scheduled source snapshot", func() bool {
		sourceState, sourceErr := direct.InspectState(t.Context(), source, false)
		return sourceErr == nil && len(lifecycle.Snapshots(sourceState, source)) == 1
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
	t.Logf("verified daemon scheduling, deactivation, and retirement on %s", source)
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
	sourcePool, err := zfstest.VerifyGuestPool(t.Context(), runID, zfstest.SourceDisk, "/dev/vdb")
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
	runtime, err := New(cfg, direct, "abcdefab-cdef-4abc-8def-abcdefabcdef", logger)
	if err != nil {
		t.Fatal(err)
	}
	events := make(chan Event, 256)
	record := runtime.remote.report
	runtime.remote.report = func(event Event) {
		record(event)
		events <- event
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	remoteJob := "remote:" + source + ":home"
	waitForEvent := func(state string, timeout time.Duration) Event {
		t.Helper()
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		for {
			select {
			case event := <-events:
				if event.Job == remoteJob && event.State == state {
					return event
				}
			case <-timer.C:
				t.Fatalf("timed out waiting for remote state %q: %+v", state, runtime.status.Snapshot())
				return Event{}
			}
		}
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
			t.Fatalf("timed out waiting for the initial pending snapshot hold: %+v", runtime.status.Snapshot())
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
			t.Fatalf("timed out waiting for a newer coalesced snapshot hold: %+v", runtime.status.Snapshot())
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
