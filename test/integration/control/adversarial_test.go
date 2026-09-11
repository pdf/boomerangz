//go:build integration

package control_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/pdf/boomerangz/internal/lifecycle"
	"github.com/pdf/boomerangz/internal/policy"
	"github.com/pdf/boomerangz/internal/testutil/zfstest"
	"github.com/pdf/boomerangz/internal/zfs"
)

// lockedBuffer collects a subprocess's output for reporting while the test
// reads it concurrently.
type lockedBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.String()
}

// daemonProcess is one boomerangz daemon subprocess under test.
type daemonProcess struct {
	command *exec.Cmd
	log     *lockedBuffer
	exited  chan error
}

// startGuestDaemon runs the packaged daemon against one configuration,
// optionally with a directory prepended to PATH so a wrapper can stand in for
// zfs. Cleanup is registered against the passed t, which must be the scope
// that owns the process.
func startGuestDaemon(t *testing.T, binary, configPath, dropInDir, pathPrefix string) *daemonProcess {
	t.Helper()
	log := &lockedBuffer{}
	command := exec.CommandContext(t.Context(), binary, "daemon", "--config", configPath, "--config-dir", dropInDir)
	command.Stdout = log
	command.Stderr = log
	command.WaitDelay = 5 * time.Second
	if pathPrefix != "" {
		command.Env = append(os.Environ(), "PATH="+pathPrefix+string(os.PathListSeparator)+os.Getenv("PATH"))
	}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	process := &daemonProcess{command: command, log: log, exited: make(chan error, 1)}
	go func() { process.exited <- command.Wait() }()
	t.Cleanup(func() {
		if command.Process != nil {
			_ = command.Process.Kill()
		}
		select {
		case <-process.exited:
		case <-time.After(10 * time.Second):
		}
	})
	return process
}

// waitForCondition polls until check passes, reporting the daemon log on
// timeout so a failure says what the daemon was doing.
func waitForCondition(t *testing.T, description string, log *lockedBuffer, timeout time.Duration, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s; daemon log: %s", description, log.String())
}

// killingZFS writes a zfs stand-in that runs the real binary and then kills
// its own parent - the daemon - as soon as the matching operation has
// committed to the pool.
//
// This is the fault injection the chunk is named for. Nothing in boomerangz
// exposes a seam between one ZFS operation and the next, but every operation
// is a subprocess resolved through PATH, so the boundary between "committed"
// and "the daemon acted on it" is reachable from outside the process. The
// wrapper disarms itself, so only the first matching operation is fatal and
// the restarted daemon runs unimpeded.
func killingZFS(t *testing.T, subcommand, match, armPath string) string {
	t.Helper()
	real, err := exec.LookPath("zfs")
	if err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(t.TempDir(), "bin")
	if err := os.MkdirAll(directory, 0o755); err != nil {
		t.Fatal(err)
	}
	script := fmt.Sprintf(`#!/bin/sh
if [ -f %[3]q ] && [ "$1" = %[2]q ]; then
	for argument in "$@"; do
		# zfs snapshot names its target dataset@name, receive names the
		# dataset itself, so both spellings have to match.
		case "$argument" in
		%[4]q|%[4]q@*) ;;
		*) continue ;;
		esac
		%[1]q "$@"
		status=$?
		if [ $status -ne 0 ]; then
			exit $status
		fi
		# The operation is durable now and the daemon has not yet seen it
		# return. This is the power cut.
		rm -f %[3]q
		kill -9 "$PPID"
		exit 0
	done
fi
exec %[1]q "$@"
`, real, subcommand, armPath, match)
	path := filepath.Join(directory, "zfs")
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(armPath, []byte("armed\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return directory
}

// TestGuestDaemonPowerLoss kills a running daemon at the instant a ZFS
// operation has committed but nothing has acted on it, and asserts the state
// it left behind is recoverable.
//
// The work plan called this "power loss between snapshot creation and property
// write". Reading the code says that particular window cannot open, because
// Service.CreateSnapshot passes its metadata to `zfs snapshot -o` and the two
// commit together - but reading the code is not what this chunk is for. The
// first phase kills the daemon at exactly that boundary and shows the claim
// holds against a real SIGKILL; the second kills it at a boundary that is two
// operations wide and genuinely can tear.
func TestGuestDaemonPowerLoss(t *testing.T) {
	binary, sourcePool, destinationPool := guestCLI(t)

	// Each phase owns a daemon, a configuration and a dataset, so neither
	// inherits the other's corpse.
	t.Run("snapshot-commit", func(t *testing.T) {
		configPath, dropInDir, _ := scratchConfig(t)
		direct, err := zfs.NewDirect("zfs")
		if err != nil {
			t.Fatal(err)
		}
		dataset := sourcePool + "/data/powerloss-snapshot-" + time.Now().UTC().Format("150405.000")
		zfsCreate(t, dataset)
		zfstest.RegisterCleanup(t, dataset)
		zfsApply(t, "set", policy.Namespace+"enabled=on", policy.Namespace+"policy=1x5m", dataset)

		arm := filepath.Join(t.TempDir(), "arm-snapshot")
		wrapper := killingZFS(t, "snapshot", dataset, arm)
		victim := startGuestDaemon(t, binary, configPath, dropInDir, wrapper)

		select {
		case err := <-victim.exited:
			if err == nil {
				t.Fatalf("the daemon exited cleanly instead of being killed: %s", victim.log.String())
			}
			if !strings.Contains(err.Error(), "killed") {
				t.Fatalf("the daemon died of something other than the injected kill: %v: %s", err, victim.log.String())
			}
		case <-time.After(60 * time.Second):
			t.Fatalf("the daemon never reached its first snapshot: %s", victim.log.String())
		}
		if _, err := os.Stat(arm); !os.IsNotExist(err) {
			t.Fatalf("the injected kill did not fire: %v", err)
		}

		// What survived the cut has to be a complete snapshot, not a snapshot
		// missing the metadata that makes it boomerangz's.
		state, err := direct.InspectState(t.Context(), dataset, false)
		if err != nil {
			t.Fatal(err)
		}
		owned := lifecycle.Snapshots(state, dataset)
		if len(owned) != 1 {
			t.Fatalf("a snapshot committed before the kill was not recoverable as owned: %+v", state.Objects)
		}
		survivor := owned[0]
		lineage := localProperty(t, state, dataset, lifecycle.LineageProperty)
		if lineage == "" || localProperty(t, state, dataset, lifecycle.OwnerProperty) == "" {
			t.Fatalf("the killed daemon left a snapshot without local root authority: %+v", state.Properties)
		}

		// A restarted daemon must adopt what it finds rather than fork.
		survivorName := survivor.Name
		replacement := startGuestDaemon(t, binary, configPath, dropInDir, "")
		waitForCondition(t, "the replacement daemon to serve", replacement.log, 60*time.Second, func() bool {
			return exec.CommandContext(t.Context(), binary, "status", "--config", configPath, "--config-dir", dropInDir).Run() == nil
		})
		after, err := direct.InspectState(t.Context(), dataset, false)
		if err != nil {
			t.Fatal(err)
		}
		if got := localProperty(t, after, dataset, lifecycle.LineageProperty); got != lineage {
			t.Fatalf("the restarted daemon forked the lineage: %s, was %s", got, lineage)
		}
		found := false
		for _, snapshot := range lifecycle.Snapshots(after, dataset) {
			found = found || snapshot.Name == survivorName
		}
		if !found {
			t.Fatalf("the restarted daemon discarded the snapshot that survived the kill: %s", survivorName)
		}
	})

	t.Run("receive-commit", func(t *testing.T) {
		configPath, dropInDir, _ := scratchConfig(t)
		direct, err := zfs.NewDirect("zfs")
		if err != nil {
			t.Fatal(err)
		}
		suffix := time.Now().UTC().Format("150405.000")
		dataset := sourcePool + "/data/powerloss-binding-" + suffix
		destinationRoot := destinationPool + "/data/powerloss-binding-" + suffix
		zfsCreate(t, dataset)
		zfstest.RegisterCleanup(t, dataset)
		zfstest.RegisterCleanup(t, destinationRoot)
		zfsApply(t, "set", policy.Namespace+"enabled=on", policy.Namespace+"policy=1x5m",
			policy.Namespace+"local="+destinationRoot, dataset)

		// The receive is the durable half of a transfer and the verification
		// and checkpoint that follow it are not, so a crash here leaves a
		// replica on disk that no completed transfer accounts for.
		//
		// It is not, as first written, a crash between the receive and the
		// binding write: the binding is stored before the stream runs
		// ([local.go:413]) and re-checked after it, so boomerangz records the
		// commitment before doing the destructive work rather than after. The
		// assertion below keeps that honest - if a future change moves the
		// binding write past the stream, this phase starts failing.
		arm := filepath.Join(t.TempDir(), "arm-receive")
		wrapper := killingZFS(t, "receive", destinationRoot, arm)
		victim := startGuestDaemon(t, binary, configPath, dropInDir, wrapper)
		select {
		case err := <-victim.exited:
			if err == nil || !strings.Contains(err.Error(), "killed") {
				t.Fatalf("the daemon was not killed at the receive boundary: %v: %s", err, victim.log.String())
			}
		case <-time.After(120 * time.Second):
			t.Fatalf("the daemon never reached a receive: %s", victim.log.String())
		}

		if _, err := direct.InspectDatasetIdentity(t.Context(), destinationRoot); err != nil {
			t.Fatalf("the kill landed before the receive committed, so this phase tested nothing: %v", err)
		}
		source, err := direct.InspectState(t.Context(), dataset, false)
		if err != nil {
			t.Fatal(err)
		}
		bound := false
		for _, property := range source.Properties {
			bound = bound || strings.HasPrefix(property.Name, policy.StateNamespace+"target:")
		}
		if !bound {
			t.Fatal("the binding was not written before the stream; this phase's premise no longer holds")
		}

		// The recovery question: a durable replica exists that no completed
		// transfer accounts for. The daemon does NOT converge on its own, and
		// this is the behaviour the phase pins.
		//
		// The binding recorded the nearest existing ancestor as its anchor,
		// because the mapped dataset did not exist when the transfer was
		// planned. The receive then created it, so on the next pass
		// anchor == mapped, the drift guard at binding.go:104 does not fire,
		// and the identity read from the newly created dataset does not match
		// the stored anchor. The target blocks and asks for a reseed. Only the
		// bootstrap transfer is exposed: once a target is verified its anchor
		// is the mapped dataset, so later crashes re-plan cleanly.
		//
		// If this is ever fixed so the daemon adopts the replica it created,
		// invert this phase - see chunk F in design/integration-coverage.md.
		replacement := startGuestDaemon(t, binary, configPath, dropInDir, "")
		job := "local:" + dataset + ":" + destinationRoot
		waitForCondition(t, "the crashed target to report its state", replacement.log, 180*time.Second, func() bool {
			reported, found := jobStatus(t, binary, configPath, dropInDir, job)
			return found && (reported.State == "blocked" || reported.State == "succeeded")
		})
		reported, _ := jobStatus(t, binary, configPath, dropInDir, job)
		if reported.State == "succeeded" {
			t.Fatal("the daemon recovered unattended; this phase is stale and should be inverted")
		}
		if !strings.Contains(reported.Reason, "reseed") {
			t.Fatalf("the block does not tell an operator how to recover: %+v", reported)
		}
		if _, err := direct.InspectDatasetIdentity(t.Context(), destinationRoot); err != nil {
			t.Fatalf("the blocked target discarded the replica: %v", err)
		}

		// The remedy the message names has to actually work, or the advice is
		// worse than none. The lifecycle lock is keyed on the socket path, so
		// the daemon has to be stopped before an offline reseed can take it.
		if err := replacement.command.Process.Signal(syscall.SIGTERM); err != nil {
			t.Fatal(err)
		}
		if err := <-replacement.exited; err != nil {
			t.Fatalf("daemon did not stop cleanly before the reseed: %v", err)
		}
		if output, err := exec.CommandContext(t.Context(), binary, "dataset", "reseed",
			dataset, destinationRoot, "--apply", "--config", configPath, "--config-dir", dropInDir).CombinedOutput(); err != nil {
			t.Fatalf("the reseed the block asked for failed: %v: %s", err, output)
		}
		recovered := startGuestDaemon(t, binary, configPath, dropInDir, "")
		waitForCondition(t, "the reseeded target to converge", recovered.log, 180*time.Second, func() bool {
			state, inspectErr := direct.InspectState(t.Context(), dataset, false)
			if inspectErr == nil {
				for _, object := range state.Objects {
					if object.Type == "bookmark" {
						return true
					}
				}
			}
			convergent, found := jobStatus(t, binary, configPath, dropInDir, job)
			return found && convergent.State == "succeeded"
		})
		if _, err := direct.InspectDatasetIdentity(t.Context(), destinationRoot); err != nil {
			t.Fatalf("the reseeded target reported success without a destination: %v", err)
		}
	})
}

// TestGuestDaemonSocketContention covers the three ways a second process can
// collide with a running daemon's control plane.
//
// The lifecycle lock is derived from the socket path, so two daemons sharing a
// configuration always collide on the lock and never reach the socket at all.
// Reaching the socket needs something else already listening there, which is
// what the second phase arranges.
func TestGuestDaemonSocketContention(t *testing.T) {
	binary, sourcePool, _ := guestCLI(t)
	configPath, dropInDir, _ := scratchConfig(t)
	socket := filepath.Join(filepath.Dir(configPath), "control.sock")

	root := sourcePool + "/data/contention-" + time.Now().UTC().Format("150405.000")
	zfsCreate(t, root)
	zfstest.RegisterCleanup(t, root)
	zfsApply(t, "set", policy.Namespace+"enabled=on", policy.Namespace+"policy=1x1h", root)

	serving := func(t *testing.T) bool {
		t.Helper()
		info, err := os.Lstat(socket)
		if err != nil || info.Mode()&os.ModeSocket == 0 {
			return false
		}
		return exec.CommandContext(t.Context(), binary, "status", "--config", configPath, "--config-dir", dropInDir).Run() == nil
	}

	// The live-socket phase needs the path free of a daemon, so it runs first
	// and the long-lived daemon starts after it.
	t.Run("live-socket-refused", func(t *testing.T) {
		// Anything already bound to the control path - a daemon under another
		// configuration, or an unrelated process - must stop a daemon taking
		// it over, because taking it over would silently orphan the owner.
		listener, err := net.Listen("unix", socket)
		if err != nil {
			t.Fatal(err)
		}
		accepting := make(chan struct{})
		go func() {
			defer close(accepting)
			for {
				connection, acceptErr := listener.Accept()
				if acceptErr != nil {
					return
				}
				_ = connection.Close()
			}
		}()
		t.Cleanup(func() {
			_ = listener.Close()
			<-accepting
			_ = os.Remove(socket)
		})

		intruder := startGuestDaemon(t, binary, configPath, dropInDir, "")
		select {
		case err := <-intruder.exited:
			if err == nil {
				t.Fatalf("a daemon started on an occupied control socket: %s", intruder.log.String())
			}
		case <-time.After(30 * time.Second):
			t.Fatalf("the daemon neither refused nor exited: %s", intruder.log.String())
		}
		if !strings.Contains(intruder.log.String(), "control socket is already active") {
			t.Fatalf("the refusal does not name the occupied socket: %s", intruder.log.String())
		}
		// The occupant must still be there.
		if _, err := os.Lstat(socket); err != nil {
			t.Fatalf("the refused daemon removed the socket it did not own: %v", err)
		}
	})

	first := startGuestDaemon(t, binary, configPath, dropInDir, "")
	waitForCondition(t, "the first daemon to serve", first.log, 60*time.Second, func() bool { return serving(t) })

	chainOK := true
	chain := func(name string, fn func(*testing.T)) {
		if !chainOK {
			t.Run(name, func(t *testing.T) { t.Skip("depends on an earlier phase that failed") })
			return
		}
		chainOK = t.Run(name, fn)
	}

	chain("lock-refuses-second-daemon", func(t *testing.T) {
		// This is the lifecycle lock, not the socket: lifecycleLock is keyed
		// on the socket path and is taken before the control server starts, so
		// a second daemon under the same configuration never gets as far as
		// binding.
		second := startGuestDaemon(t, binary, configPath, dropInDir, "")
		select {
		case err := <-second.exited:
			if err == nil {
				t.Fatalf("a second daemon started alongside the first: %s", second.log.String())
			}
		case <-time.After(30 * time.Second):
			t.Fatalf("the second daemon neither refused nor exited: %s", second.log.String())
		}
		if !strings.Contains(second.log.String(), "another lifecycle operation is running") {
			t.Fatalf("the refusal does not name the contention: %s", second.log.String())
		}
		if !serving(t) {
			t.Fatalf("the first daemon stopped serving after the second was refused: %s", first.log.String())
		}
	})

	chain("stale-socket-reclaimed", func(t *testing.T) {
		if err := first.command.Process.Kill(); err != nil {
			t.Fatal(err)
		}
		<-first.exited
		if info, err := os.Lstat(socket); err != nil || info.Mode()&os.ModeSocket == 0 {
			t.Fatalf("killed daemon left no socket to reclaim: %v", err)
		}
		replacement := startGuestDaemon(t, binary, configPath, dropInDir, "")
		waitForCondition(t, "the replacement daemon to serve", replacement.log, 60*time.Second, func() bool { return serving(t) })
		if err := replacement.command.Process.Signal(syscall.SIGTERM); err != nil {
			t.Fatal(err)
		}
		if err := <-replacement.exited; err != nil {
			t.Fatalf("replacement daemon did not stop cleanly: %v: %s", err, replacement.log.String())
		}
	})
}

// TestGuestDatasetContention runs two daemons that nothing stops from starting
// - separate socket paths mean separate lifecycle locks - over one dataset.
//
// This is the other half of the work plan's "two daemons contending for the
// same socket or the same dataset", and it is the one an operator can actually
// cause: a second installation pointed at a pool the first already manages.
// Nothing in the process model prevents it, so the refusal has to come from
// the ownership markers on the dataset itself - and it arrives at the
// scheduling layer, before any snapshot is attempted.
func TestGuestDatasetContention(t *testing.T) {
	binary, sourcePool, _ := guestCLI(t)
	direct, err := zfs.NewDirect("zfs")
	if err != nil {
		t.Fatal(err)
	}
	contended := sourcePool + "/data/twodaemons-" + time.Now().UTC().Format("150405.000")
	zfsCreate(t, contended)
	zfstest.RegisterCleanup(t, contended)
	zfsApply(t, "set", policy.Namespace+"enabled=on", policy.Namespace+"policy=1x5m", contended)

	ownerConfig, ownerDropIns, _ := scratchConfig(t)
	owner := startGuestDaemon(t, binary, ownerConfig, ownerDropIns, "")
	var lineage string
	waitForCondition(t, "the first daemon to claim the dataset", owner.log, 60*time.Second, func() bool {
		state, inspectErr := direct.InspectState(t.Context(), contended, false)
		if inspectErr != nil || len(lifecycle.Snapshots(state, contended)) == 0 {
			return false
		}
		lineage = localProperty(t, state, contended, lifecycle.LineageProperty)
		return lineage != ""
	})
	claimed := len(snapshotNames(t, direct, contended))

	// The contention is mediated by the markers on the dataset, not by two
	// live processes, so the owner's work is done once it has claimed one.
	// Stopping it also keeps it from racing the second installation for the
	// control-arm dataset below: both daemons discover every dataset in the
	// pools, so an owner left running would claim that one too and the
	// control arm could never succeed.
	if err := owner.command.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := <-owner.exited; err != nil {
		t.Fatalf("the first daemon did not stop cleanly: %v: %s", err, owner.log.String())
	}

	// Created before the second installation starts, so it is present in that
	// daemon's first discovery pass rather than waiting on a later one.
	uncontended := sourcePool + "/data/twodaemons-free-" + time.Now().UTC().Format("150405.000")
	zfsCreate(t, uncontended)
	zfstest.RegisterCleanup(t, uncontended)
	zfsApply(t, "set", policy.Namespace+"enabled=on", policy.Namespace+"policy=1x1h", uncontended)

	// A second installation, with its own identity and its own socket, so
	// neither the lock nor the socket stops it.
	intruderConfig, intruderDropIns, _ := scratchConfig(t)
	intruder := startGuestDaemon(t, binary, intruderConfig, intruderDropIns, "")
	waitForCondition(t, "the second daemon to serve", intruder.log, 60*time.Second, func() bool {
		return exec.CommandContext(t.Context(), binary, "status", "--config", intruderConfig, "--config-dir", intruderDropIns).Run() == nil
	})

	// The refusal lands at the scheduling layer, earlier and quieter than the
	// ownership guard in CreateSnapshot: actionableRoot admits a root only
	// when it carries no local owner or carries this installation's own
	// ([runtime.go:408]), so a dataset another installation owns never enters
	// the scheduler and there is no job to refuse. The deeper guard is the
	// belt behind those braces, asserted directly against a pool by
	// TestGuestInterruptedLineageInitialization/refuses-a-foreign-claim.
	//
	// "is not an active scheduling root" is also what a daemon says about a
	// dataset it has not discovered yet, so the contended trigger on its own
	// would pass against a daemon that had simply not finished starting. The
	// uncontended dataset is the control arm: the same daemon, the same
	// moment, one trigger accepted and one refused, which is what makes the
	// refusal attributable to ownership rather than to readiness.
	trigger := func(t *testing.T, target string) (string, error) {
		t.Helper()
		output, runErr := exec.CommandContext(t.Context(), binary, "trigger",
			"--config", intruderConfig, "--config-dir", intruderDropIns, target).CombinedOutput()
		return string(output), runErr
	}
	// Waiting on the control arm is also how this waits for discovery, so the
	// contended trigger below is asked of a daemon that is demonstrably ready.
	waitForCondition(t, "the second daemon to accept an uncontended root", intruder.log, 90*time.Second, func() bool {
		_, runErr := trigger(t, uncontended)
		return runErr == nil
	})

	output, triggerErr := trigger(t, contended)
	if triggerErr == nil {
		t.Fatalf("the second installation accepted work on a dataset it does not own: %s", output)
	}
	if !strings.Contains(output, "is not an active scheduling root") {
		t.Fatalf("the refusal does not name the cause: %v: %s", triggerErr, output)
	}
	if _, found := jobStatus(t, binary, intruderConfig, intruderDropIns, "snapshot:"+contended); found {
		t.Fatal("the second installation scheduled a job for a dataset it does not own")
	}

	// The refusal has to be inert: the owner's lineage and snapshots intact,
	// and nothing of the intruder's written to the contended.
	state, err := direct.InspectState(t.Context(), contended, false)
	if err != nil {
		t.Fatal(err)
	}
	if got := localProperty(t, state, contended, lifecycle.LineageProperty); got != lineage {
		t.Fatalf("the second daemon rewrote the lineage: %s, was %s", got, lineage)
	}
	if len(lifecycle.Snapshots(state, contended)) < claimed {
		t.Fatalf("the second daemon destroyed the owner's snapshots: %d, was %d",
			len(lifecycle.Snapshots(state, contended)), claimed)
	}
	t.Logf("second installation refused the contended contended at scheduling: %s", strings.TrimSpace(string(output)))
}

func zfsCreate(t *testing.T, dataset string) {
	t.Helper()
	if output, err := exec.CommandContext(t.Context(), "zfs", "create", "-u", dataset).CombinedOutput(); err != nil {
		t.Fatalf("guest zfs create: %v: %s", err, output)
	}
}

func zfsApply(t *testing.T, args ...string) {
	t.Helper()
	if output, err := exec.CommandContext(t.Context(), "zfs", args...).CombinedOutput(); err != nil {
		t.Fatalf("guest zfs %s: %v: %s", args[0], err, output)
	}
}

func localProperty(t *testing.T, state zfs.State, dataset, name string) string {
	t.Helper()
	for _, property := range state.Properties {
		if property.Dataset == dataset && property.Name == name && property.Source == zfs.SourceLocal {
			return property.Value
		}
	}
	return ""
}

func snapshotNames(t *testing.T, direct *zfs.Direct, dataset string) []string {
	t.Helper()
	state, err := direct.InspectState(t.Context(), dataset, false)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, snapshot := range lifecycle.Snapshots(state, dataset) {
		names = append(names, snapshot.Name)
	}
	return names
}

type reportedJob struct {
	Job    string `json:"job"`
	State  string `json:"state"`
	Reason string `json:"reason"`
}

func jobStatus(t *testing.T, binary, configPath, dropInDir, job string) (reportedJob, bool) {
	t.Helper()
	output, err := exec.CommandContext(t.Context(), binary, "status", "--config", configPath, "--config-dir", dropInDir).Output()
	if err != nil {
		return reportedJob{}, false
	}
	var status struct {
		Jobs []reportedJob `json:"jobs"`
	}
	if json.Unmarshal(output, &status) != nil {
		return reportedJob{}, false
	}
	for _, candidate := range status.Jobs {
		if candidate.Job == job {
			return candidate, true
		}
	}
	return reportedJob{}, false
}
