package lifecycle

import (
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/pdf/boomerangz/internal/policy"
	"github.com/pdf/boomerangz/internal/testutil/zfstest"
	"github.com/pdf/boomerangz/internal/zfs"
)

// This test is opt-in and must run in the disposable guest, never on the host.
func TestGuestLifecycle(t *testing.T) {
	runID := os.Getenv("BOOMERANGZ_LIFECYCLE_GUEST_RUN")
	if runID == "" {
		t.Skip("disposable guest only")
	}
	pool := "boomerangz-test-" + runID + "-src"
	marker := "/run/boomerangz-vmtest/guest-marker"
	// Check the marker before executing even read-only ZFS commands.
	if err := zfstest.VerifyGuestGuard(marker, runID, pool, []zfstest.Vdev{{Path: "/dev/vdb", Serial: zfstest.DiskSerial(runID, zfstest.SourceDisk)}}); err != nil {
		t.Fatal(err)
	}
	command := func(name string, args ...string) string {
		t.Helper()
		out, err := exec.CommandContext(t.Context(), name, args...).CombinedOutput()
		if err != nil {
			t.Fatalf("guest %s: %v: %s", name, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	serial := command("lsblk", "-dn", "-o", "SERIAL", "/dev/vdb")
	if err := zfstest.VerifyGuestGuard(marker, runID, pool, []zfstest.Vdev{{Path: "/dev/vdb", Serial: serial}}); err != nil {
		t.Fatal(err)
	}
	if serial != zfstest.DiskSerial(runID, zfstest.SourceDisk) {
		t.Fatal("source disk role mismatch")
	}
	status := command("zpool", "status", "-LP", pool)
	vdevs := 0
	for _, line := range strings.Split(status, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 || !strings.HasPrefix(fields[0], "/dev/") {
			continue
		}
		vdevs++
		parent := command("lsblk", "-dn", "-o", "PKNAME", fields[0])
		if parent != "vdb" && (parent != "" || fields[0] != "/dev/vdb") {
			t.Fatal("unexpected test-pool vdev")
		}
	}
	if vdevs != 1 {
		t.Fatal("unexpected source pool layout")
	}
	root := pool + "/data/lifecycle-" + time.Now().UTC().Format("150405000")
	command("zfs", "create", "-u", root)
	command("zfs", "create", "-u", root+"/child")
	// Fixtures are intentionally retained for guarded pool teardown after testing.
	direct, err := zfs.NewDirect("zfs")
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewService(direct)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	first, err := service.CreateSnapshot(t.Context(), root, true, now.Add(-2*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.CreateSnapshot(t.Context(), root, false, now)
	if err != nil {
		t.Fatal(err)
	}
	state, err := direct.InspectState(t.Context(), root, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshotsIn(state, root)) != 2 || len(snapshotsIn(state, root+"/child")) != 1 {
		t.Fatal("incorrect recursive/nonrecursive snapshot scope")
	}
	old := root + "@" + first.Name()
	ref, err := service.Protect(t.Context(), root, old, "local:disposable-target")
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Checkpoint(t.Context(), root, ref, ref.GUID+1); err == nil {
		t.Fatal("accepted wrong destination GUID")
	}
	// This phase tests the lifecycle hook; actual destination verification is
	// the responsibility of the transfer implementation in phase 4.
	if err := service.Checkpoint(t.Context(), root, ref, ref.GUID); err != nil {
		t.Fatal(err)
	}
	if err := service.ReleaseReference(t.Context(), root, ref); err != nil {
		t.Fatal(err)
	}
	if err := direct.Hold(t.Context(), "foreign-test-hold", old); err != nil {
		t.Fatal(err)
	}
	grid, _ := policy.ParseGrid("1x5m")
	preview, err := service.Prune(t.Context(), root, grid, true)
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
	if _, err := service.Prune(t.Context(), root, grid, true); err != nil {
		t.Fatal(err)
	}
	if err := direct.InheritProperty(t.Context(), root, LineageProperty); err != nil {
		t.Fatal(err)
	}
	adopted, err := service.AdoptDataset(t.Context(), root)
	if err != nil || adopted != second.Lineage {
		t.Fatalf("adopt=%s err=%v", adopted, err)
	}
	state, err = direct.InspectState(t.Context(), root, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshotsIn(state, root)) != 1 || len(snapshotsIn(state, root+"/child")) != 1 {
		t.Fatal("pruning changed a descendant snapshot")
	}
	ref, err = service.Protect(t.Context(), root, root+"@"+second.Name(), "local:disposable-target")
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Checkpoint(t.Context(), root, ref, ref.GUID); err != nil {
		t.Fatal(err)
	}
	// No transfer has run against this synthetic target; the test knows it has
	// no active work or resume dependency. Production callers must probe targets.
	cleanupPreview, err := service.Cleanup(t.Context(), root, CleanupOptions{Recursive: true}, false, testCleanupSafety{})
	if err != nil || len(cleanupPreview.Blockers) > 0 {
		t.Fatalf("cleanup preview=%v err=%v", cleanupPreview, err)
	}
	applied, err := service.Cleanup(t.Context(), root, CleanupOptions{Recursive: true}, true, testCleanupSafety{})
	if err != nil || applied.Applied != len(cleanupPreview.Actions) {
		t.Fatalf("cleanup apply=%v err=%v", applied, err)
	}
	state, err = direct.InspectState(t.Context(), root, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Properties) != 0 || len(snapshotsIn(state, root)) != 1 || len(snapshotsIn(state, root+"/child")) != 1 {
		t.Fatal("cleanup did not preserve snapshots and clear metadata")
	}
	if binary := os.Getenv("BOOMERANGZ_LIFECYCLE_GUEST_CLI"); binary != "" {
		cliRoot := root + "/cli"
		command("zfs", "create", "-u", cliRoot)
		command("zfs", "snapshot", cliRoot+"@foreign")
		if _, err := service.CreateSnapshot(t.Context(), cliRoot, false, time.Now()); err != nil {
			t.Fatal(err)
		}
		if err := direct.InheritProperty(t.Context(), cliRoot, LineageProperty); err != nil {
			t.Fatal(err)
		}
		configPath := os.Getenv("BOOMERANGZ_LIFECYCLE_GUEST_CONFIG")
		command(binary, "dataset", "--config", configPath, "adopt", cliRoot)
		command(binary, "dataset", "--config", configPath, "adopt", cliRoot, "--apply")
		command(binary, "dataset", "--config", configPath, "clean", cliRoot, "--destroy-owned-snapshots")
		command(binary, "dataset", "--config", configPath, "clean", cliRoot, "--destroy-owned-snapshots", "--apply")
		state, err := direct.InspectState(t.Context(), cliRoot, false)
		if err != nil {
			t.Fatal(err)
		}
		remaining := snapshotsIn(state, cliRoot)
		if len(remaining) != 1 || remaining[0].Name != cliRoot+"@foreign" || len(state.Properties) != 0 {
			t.Fatal("CLI cleanup failed to delete owned snapshot or preserve foreign snapshot")
		}
	}
	t.Logf("verified lifecycle on %s", root)
}
