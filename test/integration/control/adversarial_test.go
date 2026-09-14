//go:build integration

package control_test

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
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

// daemonProcess is one boomerangz daemon subprocess under test.
type daemonProcess struct {
	command *exec.Cmd
	log     *statuswait.Log
	exited  chan error    // receives the process's exit once
	done    chan struct{} // closed once the process has exited
}

// startGuestDaemon runs the packaged daemon against one configuration,
// optionally with a directory prepended to PATH so a wrapper can stand in for
// zfs. Cleanup is registered against the passed t, which must be the scope
// that owns the process.
func startGuestDaemon(t *testing.T, binary, configPath, dropInDir, pathPrefix string) *daemonProcess {
	t.Helper()
	return runGuestDaemon(t, binary, pathPrefix, "daemon", "--config", configPath, "--config-dir", dropInDir)
}

// runGuestDaemon runs binary with args, its output decoded by a log waiter
// attached before it starts, so the log holds every transition the daemon
// records. The log ends when the process exits, so a wait on a daemon that
// died fails at once rather than on its bound.
func runGuestDaemon(t *testing.T, binary, pathPrefix string, args ...string) *daemonProcess {
	t.Helper()
	log := statuswait.NewLog()
	command := exec.CommandContext(t.Context(), binary, args...)
	command.Stdout = log
	command.Stderr = log
	command.WaitDelay = 5 * time.Second
	if pathPrefix != "" {
		command.Env = append(os.Environ(), "PATH="+pathPrefix+string(os.PathListSeparator)+os.Getenv("PATH"))
	}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	process := &daemonProcess{command: command, log: log, exited: make(chan error, 1), done: make(chan struct{})}
	go func() {
		// Wait returns once the output is copied, so the log is complete when
		// it ends.
		err := command.Wait()
		log.End(err)
		process.exited <- err
		close(process.done)
	}()
	t.Cleanup(func() {
		if command.Process != nil {
			_ = command.Process.Kill()
		}
		select {
		case <-process.done:
		case <-time.After(10 * time.Second):
		}
	})
	return process
}

// waitStarted waits for the daemon's start line, which it writes once its
// control socket is serving: the command binds the socket before it runs the
// daemon.
func (p *daemonProcess) waitStarted(t *testing.T, bound time.Duration) {
	t.Helper()
	p.log.Next(t, "the daemon to start", bound, 0, statuswait.Message(statuswait.MessageStarted))
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
	zfsPath, err := exec.LookPath("zfs")
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
`, zfsPath, subcommand, armPath, match)
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

		// A restarted daemon must adopt what it finds rather than fork. Its
		// first scan makes the root due at once, so its snapshot job always
		// runs; the survivor, adopted as owned, sets the next deadline, and the
		// job ends scheduled naming it. That outcome is the point at which the
		// replacement has looked at the dataset, so the checks follow it.
		survivorName := survivor.Name
		replacement := startGuestDaemon(t, binary, configPath, dropInDir, "")
		adopted, _ := replacement.log.Ended(t, 60*time.Second, 0, "snapshot:"+dataset)
		if adopted.State != "scheduled" || adopted.Reason != "existing owned snapshot sets the next deadline" || adopted.Snapshot != survivorName {
			t.Fatalf("the restarted daemon did not adopt the survivor %s as its deadline: %+v\n%s", survivorName, adopted, replacement.log.Describe())
		}
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
		// A transfer refused by a concurrent change to its source retries as
		// waiting-retry, so only the outcome after any of those says how the
		// crashed target was judged.
		reported, _ := replacement.log.Ended(t, 180*time.Second, 0, job, "waiting-retry")
		switch {
		case reported.State == "succeeded":
			t.Fatal("the daemon recovered unattended; this phase is stale and should be inverted")
		case reported.State != "blocked":
			t.Fatalf("the crashed target ended %s rather than blocking: %+v\n%s", reported.State, reported, replacement.log.Describe())
		case !strings.Contains(reported.Reason, "reseed"):
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
		converged, _ := recovered.log.Outcome(t, 180*time.Second, 0, job, "succeeded", "waiting-retry")
		if converged.Destination != destinationRoot || converged.Snapshot == "" {
			t.Fatalf("the reseeded target's success does not name what it replicated to %s: %+v", destinationRoot, converged)
		}
		// A transfer reports success only after its checkpoint, so the source
		// holds the bookmark that records it and the destination exists.
		state, err := direct.InspectState(t.Context(), dataset, false)
		if err != nil {
			t.Fatal(err)
		}
		bookmarked := slices.ContainsFunc(state.Objects, func(object zfs.Object) bool { return object.Type == "bookmark" })
		if !bookmarked {
			t.Fatalf("the reseeded target reported success without a bookmark on the source: %+v", state.Objects)
		}
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
	socket := controlSocket(configPath)

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
		listener, err := (&net.ListenConfig{}).Listen(t.Context(), "unix", socket)
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
	first.waitStarted(t, 60*time.Second)
	if !serving(t) {
		t.Fatalf("the first daemon started without serving its control socket: %s", first.log.String())
	}

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
		replacement.waitStarted(t, 60*time.Second)
		if !serving(t) {
			t.Fatalf("the replacement daemon started without reclaiming the socket: %s", replacement.log.String())
		}
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
	// A root's first snapshot is what claims it: the lineage is written with
	// that snapshot, and the job reports success once both are committed.
	claim, _ := owner.log.Outcome(t, 60*time.Second, 0, "snapshot:"+contended, "succeeded")
	claimedState, err := direct.InspectState(t.Context(), contended, false)
	if err != nil {
		t.Fatal(err)
	}
	lineage := localProperty(t, claimedState, contended, lifecycle.LineageProperty)
	claimedSnapshots := lifecycle.Snapshots(claimedState, contended)
	if lineage == "" || !slices.ContainsFunc(claimedSnapshots, func(snapshot lifecycle.Snapshot) bool { return snapshot.Name == claim.Snapshot }) {
		t.Fatalf("the first daemon reported claiming %s without its lineage or snapshot %s: %+v", contended, claim.Snapshot, claimedState.Objects)
	}
	claimed := len(claimedSnapshots)

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
	//
	// Both datasets exist before the second daemon starts, so its first
	// discovery generation classifies both. The generation's line is written
	// once the scheduler has taken its roots and status reflects them, so
	// both triggers are asked of a daemon that has made up its mind, and each
	// is asked once.
	intruder.log.Next(t, "the second daemon's first discovery generation", 90*time.Second, 0, statuswait.Discovered)
	if output, runErr := trigger(t, uncontended); runErr != nil {
		t.Fatalf("the second installation refused an uncontended root it discovered: %v: %s", runErr, output)
	}

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

type reportedJob struct {
	Job    string `json:"job"`
	State  string `json:"state"`
	Reason string `json:"reason"`
}

// jobStatus reads the daemon's status once and returns job's row. A read
// that fails fails the test, so an absent row means the daemon has none.
func jobStatus(t *testing.T, binary, configPath, dropInDir, job string) (reportedJob, bool) {
	t.Helper()
	output, err := exec.CommandContext(t.Context(), binary, "status", "--config", configPath, "--config-dir", dropInDir).Output()
	if err != nil {
		t.Fatalf("read daemon status: %v", err)
	}
	var status struct {
		Jobs []reportedJob `json:"jobs"`
	}
	if err := json.Unmarshal(output, &status); err != nil {
		t.Fatalf("decode daemon status: %v: %s", err, output)
	}
	for _, candidate := range status.Jobs {
		if candidate.Job == job {
			return candidate, true
		}
	}
	return reportedJob{}, false
}
