package daemon

import (
	"context"
	"log/slog"
	"os"
	"os/exec"
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
