//go:build integration

package transfer_test

import (
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pdf/boomerangz/internal/lifecycle"
	"github.com/pdf/boomerangz/internal/policy"
	"github.com/pdf/boomerangz/internal/testutil/zfstest"
	"github.com/pdf/boomerangz/internal/transfer"
	"github.com/pdf/boomerangz/internal/zfs"
)

// TestGuestDestinationExhaustion drives a receive into a destination that runs
// out of room part way through the stream, and then proves the failure was
// recoverable rather than terminal.
//
// A fake executor can only return the error it was told to return. What a pool
// establishes is that the failure arrives mid-stream rather than at setup,
// that boomerangz reports it as the space problem it is, and that the source
// keeps the recovery state a retry needs.
func TestGuestDestinationExhaustion(t *testing.T) {
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
		if output, runErr := exec.CommandContext(t.Context(), "zfs", args...).CombinedOutput(); runErr != nil {
			t.Fatalf("guest zfs %s: %v: %s", args[0], runErr, output)
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
	snapshots, err := lifecycle.NewService(direct, installation)
	if err != nil {
		t.Fatal(err)
	}

	suffix := time.Now().UTC().Format("150405.000")
	// The quota sits on the container rather than on the receive root, because
	// with discard=off the receive root is the received dataset itself and
	// would inherit nothing that constrains it.
	container := destinationPool + "/data/exhaust-" + suffix
	destinationRoot := container + "/replica"
	command(t, "create", "-u", container)
	zfstest.RegisterCleanup(t, container)
	const quotaMiB = 32
	command(t, "set", "quota=32M", container)

	// Random data so the stream cannot compress its way inside the quota.
	source := zfstest.PayloadVolume(t, zfstest.FixtureName(sourcePool, "exhaust"), 256, 96)
	command(t, "set", policy.Namespace+"enabled=on", policy.Namespace+"local="+destinationRoot, source)
	request := func(t *testing.T) transfer.Request {
		t.Helper()
		rows, storedErr := direct.GetStoredProperties(t.Context(), []string{source})
		if storedErr != nil {
			t.Fatal(storedErr)
		}
		return transfer.Request{
			Source:          source,
			DestinationRoot: destinationRoot,
			Policy:          policy.Resolve(zfs.Dataset{Name: source, Type: zfs.Volume, EncryptionRoot: "-"}, nil, rows, nil),
		}
	}
	metadata, err := snapshots.CreateSnapshot(t.Context(), source, false, time.Now().UTC(), request(t).Policy)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := source + "@" + metadata.Name()

	chainOK := true
	chain := func(name string, fn func(*testing.T)) {
		if !chainOK {
			t.Run(name, func(t *testing.T) { t.Skip("depends on an earlier phase that failed") })
			return
		}
		chainOK = t.Run(name, fn)
	}

	chain("receive-exhausts-destination", func(t *testing.T) {
		var observed atomic.Uint64
		result, applyErr := engine.Apply(t.Context(), request(t), func(progress zfs.Progress) {
			observed.Store(progress.Bytes)
		})
		if applyErr == nil || result.Verified {
			t.Fatalf("a %dMiB quota accepted the whole stream: result=%+v", quotaMiB, result)
		}
		// Bytes on the wire before the failure are what makes this an
		// exhausted receive rather than a refused one.
		if observed.Load() == 0 {
			t.Fatalf("the transfer failed before any data moved: %v", applyErr)
		}
		// An operator has to be able to tell an exhausted destination from
		// any other transfer failure, so the message naming it is part of the
		// behaviour rather than incidental.
		message := strings.ToLower(applyErr.Error())
		if !strings.Contains(message, "space") && !strings.Contains(message, "quota") {
			t.Fatalf("exhaustion surfaced without naming space or quota: %v", applyErr)
		}
		t.Logf("destination exhausted after %d bytes: %v", observed.Load(), applyErr)
	})

	chain("source-retains-recovery-state", func(t *testing.T) {
		state, inspectErr := direct.InspectState(t.Context(), source, false)
		if inspectErr != nil {
			t.Fatal(inspectErr)
		}
		retained := lifecycle.Snapshots(state, source)
		if len(retained) != 1 || retained[0].Name != snapshot {
			t.Fatalf("the failed transfer did not retain its source snapshot: %+v", retained)
		}
		if len(state.Holds[snapshot]) == 0 {
			t.Fatalf("the failed transfer released its source hold on %s", snapshot)
		}
	})

	chain("retry-after-room-is-restored", func(t *testing.T) {
		command(t, "set", "quota=none", container)
		result, applyErr := engine.Apply(t.Context(), request(t), nil)
		if applyErr != nil || !result.Verified {
			t.Fatalf("retry after the quota was lifted=%+v err=%v", result, applyErr)
		}
		if _, err := direct.InspectDatasetIdentity(t.Context(), destinationRoot); err != nil {
			t.Fatalf("the retry reported success without a destination: %v", err)
		}
	})
}
