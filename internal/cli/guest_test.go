package cli

import (
	"bytes"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/pdf/boomerangz/internal/lifecycle"
	"github.com/pdf/boomerangz/internal/policy"
	"github.com/pdf/boomerangz/internal/testutil/zfstest"
	"github.com/pdf/boomerangz/internal/zfs"
)

// This test is opt-in and must run in the disposable guest, never on the host.
func TestGuestDaemonControl(t *testing.T) {
	runID := os.Getenv("BOOMERANGZ_CONTROL_GUEST_RUN")
	if runID == "" {
		t.Skip("disposable guest only")
	}
	binary := os.Getenv("BOOMERANGZ_CONTROL_GUEST_CLI")
	configPath := os.Getenv("BOOMERANGZ_CONTROL_GUEST_CONFIG")
	if binary == "" || configPath == "" {
		t.Fatal("guest boomerangz executable and configuration paths are required")
	}
	sourcePool, err := zfstest.VerifyGuestPool(t.Context(), runID, zfstest.SourceDisk, "/dev/vdb")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := zfstest.VerifyGuestPool(t.Context(), runID, zfstest.DestinationDisk, "/dev/vdc"); err != nil {
		t.Fatal(err)
	}
	direct, err := zfs.NewDirect("zfs")
	if err != nil {
		t.Fatal(err)
	}
	source := sourcePool + "/data/control-" + time.Now().UTC().Format("150405000")
	command := func(name string, args ...string) string {
		t.Helper()
		output, commandErr := exec.CommandContext(t.Context(), name, args...).CombinedOutput()
		if commandErr != nil {
			t.Fatalf("guest %s: %v: %s", name, commandErr, output)
		}
		return strings.TrimSpace(string(output))
	}
	command("zfs", "create", "-u", source)
	command("zfs", "set", policy.Namespace+"enabled=on", policy.Namespace+"policy=1x5m", source)

	var daemonLog bytes.Buffer
	daemon := exec.CommandContext(t.Context(), binary, "daemon", "--config", configPath)
	daemon.Stdout = &daemonLog
	daemon.Stderr = &daemonLog
	daemon.WaitDelay = 5 * time.Second
	if err := daemon.Start(); err != nil {
		t.Fatal(err)
	}
	stopped := false
	t.Cleanup(func() {
		if !stopped && daemon.Process != nil {
			_ = daemon.Process.Kill()
			_ = daemon.Wait()
		}
	})

	socket := os.Getenv("BOOMERANGZ_CONTROL_GUEST_SOCKET")
	if socket == "" {
		t.Fatal("guest control socket path is required")
	}
	waitFor := func(description string, check func() bool) {
		t.Helper()
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) {
			if check() {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Fatalf("timed out waiting for %s; daemon log: %s", description, daemonLog.String())
	}
	waitFor("control socket", func() bool {
		info, statErr := os.Lstat(socket)
		return statErr == nil && info.Mode()&os.ModeSocket != 0 && info.Mode().Perm() == 0o660
	})
	waitFor("dataset in control status", func() bool {
		output, statusErr := exec.CommandContext(t.Context(), binary, "status", "--config", configPath).CombinedOutput()
		return statusErr == nil && bytes.Contains(output, []byte(source))
	})
	initialSnapshots := 0
	waitFor("initial owned snapshot", func() bool {
		state, inspectErr := direct.InspectState(t.Context(), source, false)
		if inspectErr != nil {
			return false
		}
		initialSnapshots = len(lifecycle.Snapshots(state, source))
		return initialSnapshots > 0
	})
	trigger := command(binary, "trigger", "--config", configPath, source)
	if !strings.Contains(trigger, source) {
		t.Fatalf("trigger response did not accept %s: %s", source, trigger)
	}
	waitFor("triggered owned snapshot", func() bool {
		state, inspectErr := direct.InspectState(t.Context(), source, false)
		return inspectErr == nil && len(lifecycle.Snapshots(state, source)) > initialSnapshots
	})

	if err := daemon.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := daemon.Wait(); err != nil {
		t.Fatalf("daemon did not stop cleanly: %v: %s", err, daemonLog.String())
	}
	stopped = true
	if _, err := os.Lstat(socket); !os.IsNotExist(err) {
		t.Fatalf("daemon left its control socket behind: %v", err)
	}
	t.Logf("verified service-account Unix control, trigger, and graceful shutdown on %s", source)
}
