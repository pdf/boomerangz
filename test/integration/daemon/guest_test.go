//go:build integration

package daemon_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/pdf/boomerangz/internal/config"
	"github.com/pdf/boomerangz/internal/daemon"
	"github.com/pdf/boomerangz/internal/lifecycle"
	"github.com/pdf/boomerangz/internal/policy"
	"github.com/pdf/boomerangz/internal/testutil/zfstest"
	"github.com/pdf/boomerangz/internal/zfs"
)

func installDelayedZFSSend(t *testing.T, delay time.Duration) {
	t.Helper()
	realZFS, err := exec.LookPath("zfs")
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	wrapper := filepath.Join(directory, "zfs")
	const script = `#!/bin/sh
set -eu
if [ "${1-}" = send ]; then
	for argument in "$@"; do
		if [ "$argument" = -nP ]; then
			exec "$BOOMERANGZ_TEST_REAL_ZFS" "$@"
		fi
	done
	sleep "$BOOMERANGZ_TEST_SEND_DELAY"
fi
exec "$BOOMERANGZ_TEST_REAL_ZFS" "$@"
`
	if err := os.WriteFile(wrapper, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("BOOMERANGZ_TEST_REAL_ZFS", realZFS)
	t.Setenv("BOOMERANGZ_TEST_SEND_DELAY", strconv.FormatFloat(delay.Seconds(), 'f', 3, 64))
	t.Setenv("PATH", directory+string(os.PathListSeparator)+os.Getenv("PATH"))
}

func observeTransferJobs(t *testing.T, runtime *daemon.Runtime, description string, jobs []string, phase time.Time, expectOverlap bool) {
	t.Helper()
	deadline := time.Now().Add(45 * time.Second)
	sawOverlap := false
	for time.Now().Before(deadline) {
		latest := make(map[string]daemon.Event, len(jobs))
		for _, event := range runtime.Status() {
			if slices.Contains(jobs, event.Job) && event.At.After(phase) {
				latest[event.Job] = event
			}
		}
		sending, succeeded := 0, 0
		for _, job := range jobs {
			switch latest[job].State {
			case "sending":
				sending++
			case "succeeded":
				succeeded++
			}
		}
		if sending > 1 {
			sawOverlap = true
			if !expectOverlap {
				t.Fatalf("%s overlapped hierarchy-conflicting transfers: %+v", description, latest)
			}
		}
		if succeeded == len(jobs) {
			if expectOverlap && !sawOverlap {
				t.Fatalf("%s completed without concurrent sibling transfers: %+v", description, latest)
			}
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out observing %s: %+v", description, runtime.Status())
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

	// Keep each acquired transfer lock observable without changing the daemon's
	// production stream implementation or relying on fixture data volume.
	installDelayedZFSSend(t, 2*time.Second)
	direct, err := zfs.NewDirect("zfs")
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Defaults()
	cfg.Daemon.ReconcileInterval.Duration = 100 * time.Millisecond
	cfg.Daemon.ManagementWorkers = 3
	cfg.Daemon.LocalTransferWorkers = 2
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	runtime, err := daemon.New(cfg, direct, "abcdefab-cdef-4abc-8def-abcdefabcdef", logger)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	phase := time.Now().UTC()
	go func() { done <- runtime.Run(ctx) }()
	job := func(dataset string) string { return "local:" + dataset + ":" + target }

	// All mapped destinations are initially absent, so setup work must use the
	// common existing receive root and serialize even with two workers.
	observeTransferJobs(t, runtime, "initial destination setup", []string{job(root), job(left), job(right)}, phase, false)

	phase = time.Now().UTC()
	accepted, err := runtime.Trigger([]string{left, right})
	if err != nil || len(accepted) != 2 {
		t.Fatalf("trigger siblings=%v err=%v", accepted, err)
	}
	observeTransferJobs(t, runtime, "existing sibling destinations", []string{job(left), job(right)}, phase, true)

	phase = time.Now().UTC()
	accepted, err = runtime.Trigger([]string{root, left})
	if err != nil || len(accepted) != 2 {
		t.Fatalf("trigger ancestor pair=%v err=%v", accepted, err)
	}
	observeTransferJobs(t, runtime, "ancestor and descendant destinations", []string{job(root), job(left)}, phase, false)

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
