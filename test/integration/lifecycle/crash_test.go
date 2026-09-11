//go:build integration

package lifecycle_test

import (
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/pdf/boomerangz/internal/lifecycle"
	"github.com/pdf/boomerangz/internal/policy"
	"github.com/pdf/boomerangz/internal/testutil/zfstest"
	"github.com/pdf/boomerangz/internal/zfs"
)

// TestGuestInterruptedLineageInitialization covers the state a source root is
// left in when a process dies part way through claiming it.
//
// The window the work plan named - between a snapshot and the write of its
// metadata - does not exist: `Service.Snapshot` passes the ownership
// properties to `zfs snapshot -o`, so a snapshot and its metadata are one
// transaction and a half-written snapshot is unreachable by construction. The
// window that does exist is the one before it, between writing the root's
// owner and lineage markers and creating the first snapshot, and these phases
// cover the three states that leaves behind.
//
// Each turns on which property source ZFS reports, which is why they are here
// rather than over a fake: local markers are authority, inherited ones are
// provenance, and the distinction is the kernel's to make.
func TestGuestInterruptedLineageInitialization(t *testing.T) {
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
	direct, err := zfs.NewDirect("zfs")
	if err != nil {
		t.Fatal(err)
	}
	const installation = "abcdefab-cdef-4abc-8def-abcdefabcdef"
	service, err := lifecycle.NewService(direct, installation)
	if err != nil {
		t.Fatal(err)
	}
	command := func(t *testing.T, args ...string) {
		t.Helper()
		if output, runErr := exec.CommandContext(t.Context(), "zfs", args...).CombinedOutput(); runErr != nil {
			t.Fatalf("guest zfs %s: %v: %s", args[0], runErr, output)
		}
	}
	// interrupted creates an activated root already carrying the markers
	// Service.CreateSnapshot writes before it takes the first snapshot, which
	// is exactly the state a crash in that window leaves behind.
	interrupted := func(t *testing.T, dataset, owner string) string {
		t.Helper()
		lineage, idErr := lifecycle.NewID()
		if idErr != nil {
			t.Fatal(idErr)
		}
		command(t, "create", "-u", dataset)
		command(t, "set", policy.Namespace+"enabled=on",
			lifecycle.LineageProperty+"="+lineage, lifecycle.OwnerProperty+"="+owner, dataset)
		return lineage
	}
	// Every root the phases below build hangs off one tree, so cleanup is
	// registered once, here, at the scope that declares it.
	root := zfstest.FixtureName(pool, "interrupted")
	command(t, "create", "-u", root)
	zfstest.RegisterCleanup(t, root)
	now := time.Now().UTC()

	t.Run("resumes-the-claimed-lineage", func(t *testing.T) {
		dataset := root + "/resumed"
		lineage := interrupted(t, dataset, installation)
		metadata, createErr := service.CreateSnapshot(t.Context(), dataset, false, now, activePolicy(dataset))
		if createErr != nil {
			t.Fatalf("a root claimed but never snapshotted refused its first snapshot: %v", createErr)
		}
		if metadata.Lineage != lineage {
			t.Fatalf("the resumed snapshot forked a new lineage: %s, want %s", metadata.Lineage, lineage)
		}
		state, inspectErr := direct.InspectState(t.Context(), dataset, false)
		if inspectErr != nil {
			t.Fatal(inspectErr)
		}
		snapshots := lifecycle.Snapshots(state, dataset)
		if len(snapshots) != 1 || snapshots[0].Name != dataset+"@"+metadata.Name() {
			t.Fatalf("unexpected snapshots after resuming: %+v", snapshots)
		}
	})

	t.Run("refuses-a-foreign-claim", func(t *testing.T) {
		dataset := root + "/foreign"
		// A different installation got as far as claiming this root. Taking a
		// snapshot under it would fork the lineage behind that installation's
		// back, so it must be refused rather than adopted.
		interrupted(t, dataset, "ffffffff-eeee-4ddd-8ccc-bbbbbbbbbbbb")
		_, createErr := service.CreateSnapshot(t.Context(), dataset, false, now, activePolicy(dataset))
		if createErr == nil {
			t.Fatal("a root claimed by another installation accepted a snapshot")
		}
		if !strings.Contains(createErr.Error(), "dormant foreign lineage") {
			t.Fatalf("the refusal does not name the foreign claim: %v", createErr)
		}
		state, inspectErr := direct.InspectState(t.Context(), dataset, false)
		if inspectErr != nil {
			t.Fatal(inspectErr)
		}
		if len(state.Objects) != 1 {
			t.Fatalf("the refused snapshot was created anyway: %+v", state.Objects)
		}
	})

	t.Run("inherited-markers-are-not-authority", func(t *testing.T) {
		parent := root + "/inheriting"
		lineage := interrupted(t, parent, installation)
		// The child sees the parent's markers through inheritance. ZFS reports
		// them with the parent as their source, and boomerangz only recognises
		// local markers as authority, so the child must claim a lineage of its
		// own rather than silently joining its parent's.
		child := parent + "/child"
		command(t, "create", "-u", child)
		command(t, "set", policy.Namespace+"enabled=on", child)
		inheritedLineage, inheritErr := exec.CommandContext(t.Context(), "zfs", "get", "-H", "-o", "value",
			lifecycle.LineageProperty, child).CombinedOutput()
		if inheritErr != nil {
			t.Fatalf("guest zfs get: %v: %s", inheritErr, inheritedLineage)
		}
		if strings.TrimSpace(string(inheritedLineage)) != lineage {
			t.Fatalf("the child did not inherit the parent's lineage marker: %q", inheritedLineage)
		}
		metadata, createErr := service.CreateSnapshot(t.Context(), child, false, now, activePolicy(child))
		if createErr != nil {
			t.Fatalf("a child under a claimed parent refused its first snapshot: %v", createErr)
		}
		if metadata.Lineage == lineage {
			t.Fatalf("the child adopted its parent's lineage through inheritance: %s", lineage)
		}
		state, inspectErr := direct.InspectState(t.Context(), child, false)
		if inspectErr != nil {
			t.Fatal(inspectErr)
		}
		local := 0
		for _, property := range state.Properties {
			if property.Dataset != child || property.Source != zfs.SourceLocal {
				continue
			}
			if property.Name == lifecycle.LineageProperty && property.Value == metadata.Lineage {
				local++
			}
			if property.Name == lifecycle.OwnerProperty && property.Value == installation {
				local++
			}
		}
		if local != 2 {
			t.Fatalf("the child did not record local authority of its own: %+v", state.Properties)
		}
	})
}
