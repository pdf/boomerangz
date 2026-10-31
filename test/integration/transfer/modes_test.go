//go:build integration

package transfer_test

import (
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/pdf/boomerangz/internal/discovery"
	"github.com/pdf/boomerangz/internal/lifecycle"
	"github.com/pdf/boomerangz/internal/policy"
	"github.com/pdf/boomerangz/internal/testutil/zfstest"
	"github.com/pdf/boomerangz/internal/transfer"
	"github.com/pdf/boomerangz/internal/zfs"
)

// TestGuestReplicateModeSwitch switches one root between per-member streams
// and a single send -R stream, in both directions, against an existing replica.
func TestGuestReplicateModeSwitch(t *testing.T) {
	runID := os.Getenv("BOOMERANGZ_TRANSFER_GUEST_RUN")
	if runID == "" {
		t.Fatal("BOOMERANGZ_TRANSFER_GUEST_RUN is unset: the disposable guest harness did not provide a run ID")
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
	destinationPool, err := zfstest.VerifyGuestPool(t.Context(), runID, zfstest.DestinationDisk, destinationDevice)
	if err != nil {
		t.Fatal(err)
	}
	command := func(t *testing.T, args ...string) {
		t.Helper()
		if out, err := exec.CommandContext(t.Context(), "zfs", args...).CombinedOutput(); err != nil {
			t.Fatalf("guest zfs %s: %v: %s", args[0], err, out)
		}
	}
	direct, err := zfs.NewDirect("zfs")
	if err != nil {
		t.Fatal(err)
	}
	stream, err := zfs.NewLocalStream("zfs")
	if err != nil {
		t.Fatal(err)
	}
	const installation = "abcdefab-cdef-4abc-8def-abcdefabcdef"
	engine, err := transfer.NewLocal(direct, stream, installation)
	if err != nil {
		t.Fatal(err)
	}
	service, err := lifecycle.NewService(direct, installation)
	if err != nil {
		t.Fatal(err)
	}

	root := zfstest.FixtureName(sourcePool, "modes")
	destinationRoot := zfstest.FixtureName(destinationPool, "modes-backup")
	zfstest.RegisterCleanup(t, root)
	zfstest.RegisterCleanup(t, destinationRoot)
	command(t, "create", "-u", destinationRoot)
	for _, name := range []string{"", "/member", "/excluded", "/hole", "/hole/deep"} {
		command(t, "create", "-u", root+name)
	}
	command(t, "set", policy.Namespace+"enabled=on", policy.Namespace+"policy=12x5m,24x1h",
		policy.Namespace+"local="+destinationRoot, policy.Namespace+"discard=all", policy.Namespace+"incremental=latest", root)
	command(t, "set", policy.Namespace+"enabled=off", root+"/excluded")
	command(t, "set", policy.Namespace+"enabled=off", root+"/hole")
	command(t, "set", policy.Namespace+"enabled=on", root+"/hole/deep")

	// request resolves the root the way the daemon does, and the datasets its
	// snapshot covers: the due members, or the whole subtree under -R.
	request := func(t *testing.T) (transfer.Request, []string) {
		t.Helper()
		scanner, scanErr := discovery.New(direct, discovery.Options{})
		if scanErr != nil {
			t.Fatal(scanErr)
		}
		generation, scanErr := scanner.Scan(t.Context(), nil)
		if scanErr != nil {
			t.Fatal(scanErr)
		}
		entry, found := generation.Inspect(root)
		if !found || entry.CoveredBy != "" || !entry.Policy.Valid() {
			t.Fatalf("root %s did not resolve: %+v", root, entry)
		}
		built := transfer.Request{Source: root, DestinationRoot: destinationRoot, Policy: entry.Policy}
		if entry.Policy.Send.Replicate {
			state, inspectErr := direct.InspectState(t.Context(), root, true)
			if inspectErr != nil {
				t.Fatal(inspectErr)
			}
			return built, lifecycle.Subtree(state, root)
		}
		for _, name := range entry.Members {
			member, _ := generation.Inspect(name)
			built.Members = append(built.Members, transfer.Member{Dataset: name, Policy: member.Policy})
		}
		return built, entry.Members
	}
	now := time.Now().UTC()
	step := func(t *testing.T, at time.Duration) (transfer.Result, error) {
		t.Helper()
		req, covered := request(t)
		if _, snapErr := service.CreateSnapshot(t.Context(), root, covered, now.Add(at), req.Policy); snapErr != nil {
			t.Fatal(snapErr)
		}
		return engine.Apply(t.Context(), req, nil)
	}
	report := func(t *testing.T, result transfer.Result, applyErr error) {
		t.Helper()
		for _, s := range result.Streams {
			t.Logf("stream %s mode=%s verified=%t", s.Plan.Source, s.Plan.Mode, s.Verified)
		}
		t.Logf("verified=%t err=%v", result.Verified, applyErr)
		out, listErr := exec.CommandContext(t.Context(), "zfs", "list", "-H", "-r", "-t", "all", "-o", "name", destinationRoot).CombinedOutput()
		t.Logf("destination (%v):\n%s", listErr, out)
	}

	chainOK := true
	chain := func(name string, fn func(*testing.T)) {
		if !chainOK {
			t.Run(name, func(t *testing.T) { t.Skip("depends on an earlier phase that failed") })
			return
		}
		chainOK = t.Run(name, fn)
	}

	chain("per-member-bootstrap", func(t *testing.T) {
		result, applyErr := step(t, -time.Hour)
		report(t, result, applyErr)
		if applyErr != nil || !result.Verified {
			t.Fatal("per-member bootstrap failed")
		}
	})

	chain("per-member-to-replicate-needs-a-reseed", func(t *testing.T) {
		// Holes and excluded datasets were never sent, so a send -R stream
		// has no common base: planning refuses before anything changes.
		command(t, "set", policy.Namespace+"replicate=on", root)
		result, applyErr := step(t, -50*time.Minute)
		report(t, result, applyErr)
		if applyErr == nil || !strings.Contains(applyErr.Error(), "reseed") {
			t.Fatalf("err=%v, want a reseed instruction", applyErr)
		}
		req, _ := request(t)
		reseed, reseedErr := transfer.NewReseedService(direct, direct, installation)
		if reseedErr != nil {
			t.Fatal(reseedErr)
		}
		if applied, reseedErr := reseed.Apply(t.Context(), req); reseedErr != nil || !applied.Applied {
			t.Fatalf("reseed=%+v err=%v", applied, reseedErr)
		}
		result, applyErr = engine.Apply(t.Context(), req, nil)
		report(t, result, applyErr)
		if applyErr != nil || !result.Verified || result.Plan.Mode != "full" || !result.Plan.Send.Recursive {
			t.Fatal("the reseeded root did not bootstrap one recursive stream")
		}
	})

	chain("replicate-to-per-member", func(t *testing.T) {
		// Every dataset already has a replica sharing its latest snapshot, so
		// each member continues incrementally on its own stream.
		command(t, "inherit", policy.Namespace+"replicate", root)
		result, applyErr := step(t, -40*time.Minute)
		report(t, result, applyErr)
		if applyErr != nil || !result.Verified {
			t.Fatal("switching back to per-member streams did not converge")
		}
		for _, s := range result.Streams {
			if s.Plan.Mode != "incremental-latest" {
				t.Errorf("%s continued as %q, want incremental-latest", s.Plan.Source, s.Plan.Mode)
			}
		}
	})
}
