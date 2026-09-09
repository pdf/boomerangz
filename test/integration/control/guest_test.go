//go:build integration

package control_test

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"slices"
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
	configFile, err := os.OpenFile(configPath, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := configFile.WriteString("\n[daemon]\nlocal_transfer_workers = 3\n"); err != nil {
		_ = configFile.Close()
		t.Fatal(err)
	}
	if err := configFile.Close(); err != nil {
		t.Fatal(err)
	}
	reloadOutput := command(binary, "config", "reload", "--socket", socket)
	var reload struct {
		Generation      uint64   `json:"generation"`
		Applied         []string `json:"applied"`
		RestartRequired []string `json:"restart_required"`
	}
	if err := json.Unmarshal([]byte(reloadOutput), &reload); err != nil {
		t.Fatalf("decode reload result: %v: %s", err, reloadOutput)
	}
	if reload.Generation != 2 || !slices.Contains(reload.Applied, "daemon.local_transfer_workers") || len(reload.RestartRequired) != 0 {
		t.Fatalf("unexpected reload result: %#v", reload)
	}
	waitFor("reloaded configuration generation", func() bool {
		output, statusErr := exec.CommandContext(t.Context(), binary, "status", "--config", configPath).CombinedOutput()
		if statusErr != nil {
			return false
		}
		var status struct {
			ConfigGeneration uint64 `json:"config_generation"`
		}
		return json.Unmarshal(output, &status) == nil && status.ConfigGeneration == 2
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
	waitFor("completed initial snapshot job", func() bool {
		output, statusErr := exec.CommandContext(t.Context(), binary, "status", "--config", configPath).CombinedOutput()
		if statusErr != nil {
			return false
		}
		type jobStatus struct {
			Job   string `json:"job"`
			State string `json:"state"`
		}
		var status struct {
			Jobs []jobStatus `json:"jobs"`
		}
		if json.Unmarshal(output, &status) != nil {
			return false
		}
		return slices.ContainsFunc(status.Jobs, func(job jobStatus) bool {
			return job.Job == "snapshot:"+source && job.State == "succeeded"
		})
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

func TestGuestDaemonAbruptRestart(t *testing.T) {
	runID := os.Getenv("BOOMERANGZ_CONTROL_GUEST_RUN")
	if runID == "" {
		t.Skip("disposable guest only")
	}
	binary := os.Getenv("BOOMERANGZ_CONTROL_GUEST_CLI")
	configPath := os.Getenv("BOOMERANGZ_CONTROL_GUEST_CONFIG")
	socket := os.Getenv("BOOMERANGZ_CONTROL_GUEST_SOCKET")
	if binary == "" || configPath == "" || socket == "" {
		t.Fatal("guest boomerangz executable, configuration, and socket paths are required")
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
	direct, err := zfs.NewDirect("zfs")
	if err != nil {
		t.Fatal(err)
	}
	source := sourcePool + "/data/restart-" + time.Now().UTC().Format("150405000")
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

	var firstLog bytes.Buffer
	first := exec.CommandContext(t.Context(), binary, "daemon", "--config", configPath)
	first.Stdout, first.Stderr = &firstLog, &firstLog
	if err := first.Start(); err != nil {
		t.Fatal(err)
	}
	firstStopped := false
	t.Cleanup(func() {
		if !firstStopped && first.Process != nil {
			_ = first.Process.Kill()
			_ = first.Wait()
		}
	})
	waitFor := func(description string, log *bytes.Buffer, check func() bool) {
		t.Helper()
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) {
			if check() {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Fatalf("timed out waiting for %s; daemon log: %s", description, log.String())
	}
	firstSnapshots := 0
	waitFor("initial snapshot before abrupt stop", &firstLog, func() bool {
		state, inspectErr := direct.InspectState(t.Context(), source, false)
		if inspectErr != nil {
			return false
		}
		firstSnapshots = len(lifecycle.Snapshots(state, source))
		return firstSnapshots > 0
	})
	if err := first.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := first.Wait(); err == nil {
		t.Fatal("abruptly stopped daemon exited successfully")
	}
	firstStopped = true
	if info, err := os.Lstat(socket); err != nil || info.Mode()&os.ModeSocket == 0 {
		t.Fatalf("abrupt stop did not leave the expected stale socket: %v", err)
	}

	var secondLog bytes.Buffer
	second := exec.CommandContext(t.Context(), binary, "daemon", "--config", configPath)
	second.Stdout, second.Stderr = &secondLog, &secondLog
	if err := second.Start(); err != nil {
		t.Fatal(err)
	}
	secondStopped := false
	t.Cleanup(func() {
		if !secondStopped && second.Process != nil {
			_ = second.Process.Kill()
			_ = second.Wait()
		}
	})
	waitFor("control socket after restart", &secondLog, func() bool {
		output, statusErr := exec.CommandContext(t.Context(), binary, "status", "--config", configPath).CombinedOutput()
		return statusErr == nil && bytes.Contains(output, []byte(source))
	})
	time.Sleep(500 * time.Millisecond)
	state, err := direct.InspectState(t.Context(), source, false)
	if err != nil {
		t.Fatal(err)
	}
	if count := len(lifecycle.Snapshots(state, source)); count != firstSnapshots {
		t.Fatalf("restart created a duplicate snapshot: before=%d after=%d", firstSnapshots, count)
	}
	if err := second.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := second.Wait(); err != nil {
		t.Fatalf("restarted daemon did not stop cleanly: %v: %s", err, secondLog.String())
	}
	secondStopped = true
	if _, err := os.Lstat(socket); !os.IsNotExist(err) {
		t.Fatalf("restarted daemon left its control socket behind: %v", err)
	}
	t.Logf("daemon replaced its stale socket and reconstructed schedule for %s", source)
}
