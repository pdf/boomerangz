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
	binary, sourcePool, _ := guestCLI(t)
	// The test reloads a changed configuration, so it owns one rather than
	// changing a file another test starts a daemon from.
	configPath, dropInDir, _ := scratchConfig(t)
	socket := controlSocket(configPath)
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

	daemon := startGuestDaemon(t, binary, configPath, dropInDir, "")
	status := func(t *testing.T) []byte {
		t.Helper()
		output, statusErr := exec.CommandContext(t.Context(), binary, "status", "--config", configPath, "--config-dir", dropInDir).CombinedOutput()
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
	trigger := command(binary, "trigger", "--config", configPath, "--config-dir", dropInDir, source)
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
	binary, sourcePool, _ := guestCLI(t)
	// Both daemons share this test's own configuration, so what they start
	// with does not depend on which other tests ran before it.
	configPath, dropInDir, _ := scratchConfig(t)
	socket := controlSocket(configPath)
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

	snapshotJob := "snapshot:" + source
	first := startGuestDaemon(t, binary, configPath, dropInDir, "")
	initial, _ := first.log.Outcome(t, 30*time.Second, 0, snapshotJob, "succeeded")
	state, err := direct.InspectState(t.Context(), source, false)
	if err != nil {
		t.Fatal(err)
	}
	firstSnapshots := lifecycle.Snapshots(state, source)
	if !slices.ContainsFunc(firstSnapshots, func(snapshot lifecycle.Snapshot) bool { return snapshot.Name == initial.Snapshot }) {
		t.Fatalf("the snapshot job reported creating %s, which the pool does not hold: %+v", initial.Snapshot, state.Objects)
	}
	if err := first.command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := <-first.exited; err == nil {
		t.Fatal("abruptly stopped daemon exited successfully")
	}
	if info, err := os.Lstat(socket); err != nil || info.Mode()&os.ModeSocket == 0 {
		t.Fatalf("abrupt stop did not leave the expected stale socket: %v", err)
	}

	// The restarted daemon's first scan makes the root due at once, so its
	// snapshot job always runs. With the first daemon's snapshot adopted, that
	// snapshot sets a later deadline and the job ends scheduled naming it; a
	// restart that lost track of it would create a duplicate and end
	// succeeded. The job's first outcome is the answer, rather than a count
	// taken after a guessed delay.
	second := startGuestDaemon(t, binary, configPath, dropInDir, "")
	restarted, _ := second.log.Ended(t, 30*time.Second, 0, snapshotJob)
	if restarted.State == "succeeded" {
		t.Fatalf("restart created a duplicate snapshot %s beside %s\n%s", restarted.Snapshot, initial.Snapshot, second.log.Describe())
	}
	if restarted.State != "scheduled" || restarted.Reason != "existing owned snapshot sets the next deadline" || restarted.Snapshot != initial.Snapshot {
		t.Fatalf("the restarted daemon did not take its deadline from %s: %+v\n%s", initial.Snapshot, restarted, second.log.Describe())
	}
	state, err = direct.InspectState(t.Context(), source, false)
	if err != nil {
		t.Fatal(err)
	}
	if count := len(lifecycle.Snapshots(state, source)); count != len(firstSnapshots) {
		t.Fatalf("restart created a duplicate snapshot: before=%d after=%d", len(firstSnapshots), count)
	}
	// The job ran, so the restarted daemon had started and replaced the stale
	// socket; one read shows it serving the root it scheduled.
	if output, statusErr := exec.CommandContext(t.Context(), binary, "status", "--config", configPath, "--config-dir", dropInDir).CombinedOutput(); statusErr != nil || !bytes.Contains(output, []byte(source)) {
		t.Fatalf("the restarted daemon does not serve status listing %s: %v: %s", source, statusErr, output)
	}
	if err := second.command.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := <-second.exited; err != nil {
		t.Fatalf("restarted daemon did not stop cleanly: %v: %s", err, second.log.String())
	}
	if _, err := os.Lstat(socket); !os.IsNotExist(err) {
		t.Fatalf("restarted daemon left its control socket behind: %v", err)
	}
	t.Logf("daemon replaced its stale socket and reconstructed schedule for %s", source)
}
