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
	"github.com/pdf/boomerangz/test/integration/internal/statuswait"
)

// This test is opt-in and must run in the disposable guest, never on the host.
func TestGuestDaemonControl(t *testing.T) {
	runID := os.Getenv("BOOMERANGZ_CONTROL_GUEST_RUN")
	if runID == "" {
		t.Fatal("BOOMERANGZ_CONTROL_GUEST_RUN is unset: the disposable guest harness did not provide a run ID")
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
	zfstest.RegisterCleanup(t, source)
	command("zfs", "set", policy.Namespace+"enabled=on", policy.Namespace+"policy=1x5m", source)

	daemon := runGuestDaemon(t, binary, "", "daemon", "--config", configPath)
	socket := os.Getenv("BOOMERANGZ_CONTROL_GUEST_SOCKET")
	if socket == "" {
		t.Fatal("guest control socket path is required")
	}
	status := func(t *testing.T) []byte {
		t.Helper()
		output, statusErr := exec.CommandContext(t.Context(), binary, "status", "--config", configPath).CombinedOutput()
		if statusErr != nil {
			t.Fatalf("guest status: %v: %s", statusErr, output)
		}
		return output
	}
	ownedSnapshot := func(t *testing.T, name string) {
		t.Helper()
		state, inspectErr := direct.InspectState(t.Context(), source, false)
		if inspectErr != nil {
			t.Fatal(inspectErr)
		}
		if !slices.ContainsFunc(lifecycle.Snapshots(state, source), func(snapshot lifecycle.Snapshot) bool { return snapshot.Name == name }) {
			t.Fatalf("the snapshot job reported creating %s, which the pool does not hold: %+v", name, state.Objects)
		}
	}

	daemon.waitStarted(t, 30*time.Second)
	if info, statErr := os.Lstat(socket); statErr != nil || info.Mode()&os.ModeSocket == 0 || info.Mode().Perm() != 0o660 {
		t.Fatalf("the started daemon's control socket is not a 0660 socket: %v %v", info, statErr)
	}
	// The dataset exists before the daemon starts, so the first discovery
	// generation holds it, and the generation's line follows status
	// reflecting it.
	daemon.log.Next(t, "the first discovery generation", 30*time.Second, 0, statuswait.Discovered)
	if output := status(t); !bytes.Contains(output, []byte(source)) {
		t.Fatalf("status after the first discovery generation does not list %s: %s", source, output)
	}
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
	reloadMark := daemon.log.Mark()
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
	// The reload publishes the generation and records itself before it
	// replies, so one read sees it without waiting.
	var reloaded struct {
		ConfigGeneration uint64 `json:"config_generation"`
	}
	if output := status(t); json.Unmarshal(output, &reloaded) != nil || reloaded.ConfigGeneration != 2 {
		t.Fatalf("status after the reload replied does not carry configuration generation 2: %s", output)
	}
	// The log writer can lag the reply, so the reload's own line is waited
	// for.
	reloadEvent, _ := daemon.log.Outcome(t, 30*time.Second, reloadMark, "config:reload", "succeeded")
	if reloadEvent.ConfigGeneration != 2 {
		t.Fatalf("the reload's log line names configuration generation %d, want 2: %+v", reloadEvent.ConfigGeneration, reloadEvent)
	}

	snapshotJob := "snapshot:" + source
	initial, afterInitial := daemon.log.Outcome(t, 30*time.Second, 0, snapshotJob, "succeeded")
	ownedSnapshot(t, initial.Snapshot)
	trigger := command(binary, "trigger", "--config", configPath, source)
	if !strings.Contains(trigger, source) {
		t.Fatalf("trigger response did not accept %s: %s", source, trigger)
	}
	// Counted from the first success rather than from the trigger's reply,
	// which the log writer can lag. A forced run skips the deadline check, so
	// the next success is the triggered snapshot; a scheduled run between them
	// found the first snapshot's deadline and changed nothing.
	triggered, _ := daemon.log.Outcome(t, 30*time.Second, afterInitial, snapshotJob, "succeeded", "scheduled")
	if triggered.Snapshot == initial.Snapshot {
		t.Fatalf("the triggered run reported the initial snapshot %s again", initial.Snapshot)
	}
	ownedSnapshot(t, triggered.Snapshot)

	if err := daemon.command.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := <-daemon.exited; err != nil {
		t.Fatalf("daemon did not stop cleanly: %v: %s", err, daemon.log.String())
	}
	if _, err := os.Lstat(socket); !os.IsNotExist(err) {
		t.Fatalf("daemon left its control socket behind: %v", err)
	}
	t.Logf("verified service-account Unix control, trigger, and graceful shutdown on %s", source)
}

func TestGuestDaemonAbruptRestart(t *testing.T) {
	runID := os.Getenv("BOOMERANGZ_CONTROL_GUEST_RUN")
	if runID == "" {
		t.Fatal("BOOMERANGZ_CONTROL_GUEST_RUN is unset: the disposable guest harness did not provide a run ID")
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
	zfstest.RegisterCleanup(t, source)
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
