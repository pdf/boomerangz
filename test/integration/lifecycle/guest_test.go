//go:build integration

package lifecycle_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/pdf/boomerangz/internal/lifecycle"
	"github.com/pdf/boomerangz/internal/policy"
	"github.com/pdf/boomerangz/internal/testutil/zfstest"
	"github.com/pdf/boomerangz/internal/zfs"
)

type cleanSafety struct{}

func (cleanSafety) Quiescent(context.Context, []string) error         { return nil }
func (cleanSafety) CheckTarget(context.Context, string, string) error { return nil }

func activePolicy(dataset string) policy.Effective {
	row := zfs.Property{Dataset: dataset, Name: policy.Namespace + "enabled", Value: "on", Source: zfs.SourceLocal}
	return policy.Resolve(zfs.Dataset{Name: dataset, Type: zfs.Filesystem, EncryptionRoot: "-"}, nil, []zfs.Property{row}, nil)
}

// This test is opt-in and must run in the disposable guest, never on the host.
func TestGuestLifecycle(t *testing.T) {
	runID := os.Getenv("BOOMERANGZ_LIFECYCLE_GUEST_RUN")
	if runID == "" {
		t.Skip("disposable guest only")
	}
	sourceDevice := os.Getenv("BOOMERANGZ_INTEGRATION_SOURCE_DEVICE")
	if sourceDevice == "" {
		t.Fatal("source test device is required")
	}
	pool, err := zfstest.VerifyGuestPool(t.Context(), runID, zfstest.SourceDisk, sourceDevice)
	if err != nil {
		t.Fatal(err)
	}
	marker := "/run/boomerangz-vmtest/guest-marker"
	// Takes the running *testing.T rather than closing over the parent's, so a
	// phase's failure is reported against that phase.
	command := func(t *testing.T, name string, args ...string) string {
		t.Helper()
		out, err := exec.CommandContext(t.Context(), name, args...).CombinedOutput()
		if err != nil {
			t.Fatalf("guest %s: %v: %s", name, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	// Guard and fixture setup stay outside the phases: nothing below is
	// meaningful if the pool under test is not the disposable one.
	serial := command(t, "lsblk", "-dn", "-o", "SERIAL", sourceDevice)
	if err := zfstest.VerifyGuestGuard(marker, runID, pool, []zfstest.Vdev{{Path: sourceDevice, Serial: serial}}); err != nil {
		t.Fatal(err)
	}
	if serial != zfstest.DiskSerial(runID, zfstest.SourceDisk) {
		t.Fatal("source disk role mismatch")
	}
	status := command(t, "zpool", "status", "-LP", pool)
	vdevs := 0
	for _, line := range strings.Split(status, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 || !strings.HasPrefix(fields[0], "/dev/") {
			continue
		}
		vdevs++
		parent := command(t, "lsblk", "-dn", "-o", "PKNAME", fields[0])
		if parent != filepath.Base(sourceDevice) && (parent != "" || fields[0] != sourceDevice) {
			t.Fatal("unexpected test-pool vdev")
		}
	}
	if vdevs != 1 {
		t.Fatal("unexpected source pool layout")
	}
	root := pool + "/data/lifecycle-" + time.Now().UTC().Format("150405000")
	command(t, "zfs", "create", "-u", root)
	command(t, "zfs", "create", "-u", root+"/child")
	command(t, "zfs", "set", policy.Namespace+"enabled=on", root)
	// Fixtures are intentionally retained for guarded pool teardown after testing.
	direct, err := zfs.NewDirect("zfs")
	if err != nil {
		t.Fatal(err)
	}
	const installation = "abcdefab-cdef-4abc-8def-abcdefabcdef"
	service, err := lifecycle.NewService(direct, installation)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	effective := activePolicy(root)

	// This test walks one dataset through its whole lifecycle, so every phase
	// consumes the state the previous one left - there is no meaningful way to
	// run adopt before the snapshots exist. chain therefore skips the remainder
	// once a phase fails, rather than reporting cascade failures that all trace
	// back to one cause.
	chainOK := true
	chain := func(name string, fn func(*testing.T)) {
		if !chainOK {
			t.Run(name, func(t *testing.T) { t.Skip("depends on an earlier phase that failed") })
			return
		}
		chainOK = t.Run(name, fn)
	}
	// State handed between phases.
	var (
		second lifecycle.Metadata
		old    string
		ref    lifecycle.Reference
	)

	chain("snapshot-scope", func(t *testing.T) {
		first, err := service.CreateSnapshot(t.Context(), root, true, now.Add(-2*time.Hour), effective)
		if err != nil {
			t.Fatal(err)
		}
		second, err = service.CreateSnapshot(t.Context(), root, false, now, effective)
		if err != nil {
			t.Fatal(err)
		}
		state, err := direct.InspectState(t.Context(), root, true)
		if err != nil {
			t.Fatal(err)
		}
		if len(lifecycle.Snapshots(state, root)) != 2 || len(lifecycle.Snapshots(state, root+"/child")) != 1 {
			t.Fatal("incorrect recursive/nonrecursive snapshot scope")
		}
		old = root + "@" + first.Name()
	})

	chain("reference-checkpoint", func(t *testing.T) {
		var err error
		ref, err = service.Protect(t.Context(), root, old, "local:disposable-target")
		if err != nil {
			t.Fatal(err)
		}
		// The correct GUID first, as the control arm: a Checkpoint that fails
		// for any reason - a missing hold, an unreadable state, a reference
		// that never took - would satisfy the refusal below on its own, so
		// the refusal is only attributable once the same call has been seen
		// to succeed here.
		//
		// This phase tests the lifecycle hook; actual destination verification is
		// the responsibility of the transfer implementation in phase 4.
		if err := service.Checkpoint(t.Context(), root, ref, ref.GUID); err != nil {
			t.Fatal(err)
		}
		bookmark := ref.BookmarkName(root)
		bookmarks := func(t *testing.T) []string {
			t.Helper()
			state, stateErr := direct.InspectState(t.Context(), root, true)
			if stateErr != nil {
				t.Fatal(stateErr)
			}
			var names []string
			for _, object := range state.Objects {
				if object.Type == "bookmark" {
					names = append(names, object.Name)
				}
			}
			return names
		}
		checkpointed := bookmarks(t)
		if !slices.Contains(checkpointed, bookmark) {
			t.Fatalf("checkpoint created no versioned bookmark: %v", checkpointed)
		}
		if err := service.Checkpoint(t.Context(), root, ref, ref.GUID+1); err == nil {
			t.Fatal("accepted wrong destination GUID")
		}
		if after := bookmarks(t); !slices.Equal(after, checkpointed) {
			t.Fatalf("refused checkpoint changed the bookmarks: %v, was %v", after, checkpointed)
		}
		if err := service.ReleaseReference(t.Context(), root, ref); err != nil {
			t.Fatal(err)
		}
	})

	chain("prune-respects-holds", func(t *testing.T) {
		if err := direct.Hold(t.Context(), "foreign-test-hold", old); err != nil {
			t.Fatal(err)
		}
		grid, _ := policy.ParseGrid("1x5m")
		effective.Grid = grid
		preview, err := service.Prune(t.Context(), root, effective, true)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range preview {
			if d.Destroy {
				t.Fatal("held or newest snapshot marked for destruction")
			}
		}
		if err := direct.Release(t.Context(), "foreign-test-hold", old); err != nil {
			t.Fatal(err)
		}
		if _, err := service.Prune(t.Context(), root, effective, true); err != nil {
			t.Fatal(err)
		}
	})

	chain("adopt", func(t *testing.T) {
		if err := direct.InheritProperty(t.Context(), root, lifecycle.LineageProperty); err != nil {
			t.Fatal(err)
		}
		adopted, err := service.AdoptDataset(t.Context(), root, effective)
		if err != nil || adopted != second.Lineage {
			t.Fatalf("adopt=%s err=%v", adopted, err)
		}
		state, err := direct.InspectState(t.Context(), root, true)
		if err != nil {
			t.Fatal(err)
		}
		if len(lifecycle.Snapshots(state, root)) != 1 || len(lifecycle.Snapshots(state, root+"/child")) != 1 {
			t.Fatal("pruning changed a descendant snapshot")
		}
	})

	chain("clean", func(t *testing.T) {
		var err error
		ref, err = service.Protect(t.Context(), root, root+"@"+second.Name(), "local:disposable-target")
		if err != nil {
			t.Fatal(err)
		}
		if err := service.Checkpoint(t.Context(), root, ref, ref.GUID); err != nil {
			t.Fatal(err)
		}
		// No transfer has run against this synthetic target; the test knows it has
		// no active work or resume dependency. Production callers must probe targets.
		cleanPreview, err := service.Clean(t.Context(), root, lifecycle.CleanOptions{Recursive: true}, false, cleanSafety{})
		if err != nil || len(cleanPreview.Blockers) > 0 {
			t.Fatalf("clean preview=%v err=%v", cleanPreview, err)
		}
		applied, err := service.Clean(t.Context(), root, lifecycle.CleanOptions{Recursive: true}, true, cleanSafety{})
		if err != nil || applied.Applied != len(cleanPreview.Actions) {
			t.Fatalf("clean apply=%v err=%v", applied, err)
		}
		state, err := direct.InspectState(t.Context(), root, true)
		if err != nil {
			t.Fatal(err)
		}
		if len(state.Properties) != 0 || len(lifecycle.Snapshots(state, root)) != 1 || len(lifecycle.Snapshots(state, root+"/child")) != 1 {
			t.Fatal("clean did not preserve snapshots and clear metadata")
		}
	})

	// Optional tail: builds its own dataset under root, so it needs the chain
	// only to have got as far as creating root.
	binary := os.Getenv("BOOMERANGZ_LIFECYCLE_GUEST_CLI")
	if binary == "" {
		t.Logf("verified lifecycle on %s", root)
		return
	}
	chain("cli-adopt-and-clean", func(t *testing.T) {
		cliRoot := root + "/cli"
		command(t, "zfs", "create", "-u", cliRoot)
		command(t, "zfs", "set", policy.Namespace+"enabled=on", cliRoot)
		command(t, "zfs", "snapshot", cliRoot+"@foreign")
		if _, err := service.CreateSnapshot(t.Context(), cliRoot, false, time.Now(), activePolicy(cliRoot)); err != nil {
			t.Fatal(err)
		}
		if err := direct.InheritProperty(t.Context(), cliRoot, lifecycle.LineageProperty); err != nil {
			t.Fatal(err)
		}
		configPath := os.Getenv("BOOMERANGZ_LIFECYCLE_GUEST_CONFIG")
		command(t, binary, "dataset", "--config", configPath, "adopt", cliRoot)
		command(t, binary, "dataset", "--config", configPath, "adopt", cliRoot, "--apply")
		command(t, binary, "dataset", "--config", configPath, "clean", cliRoot, "--destroy-owned-snapshots")
		command(t, binary, "dataset", "--config", configPath, "clean", cliRoot, "--destroy-owned-snapshots", "--apply")
		state, err := direct.InspectState(t.Context(), cliRoot, false)
		if err != nil {
			t.Fatal(err)
		}
		remaining := lifecycle.Snapshots(state, cliRoot)
		if len(remaining) != 1 || remaining[0].Name != cliRoot+"@foreign" || len(state.Properties) != 0 {
			t.Fatal("CLI clean failed to delete owned snapshot or preserve foreign snapshot")
		}
	})
	t.Logf("verified lifecycle on %s", root)
}
