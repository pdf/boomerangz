//go:build integration

package transfer_test

import (
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/pdf/boomerangz/internal/lifecycle"
	"github.com/pdf/boomerangz/internal/policy"
	"github.com/pdf/boomerangz/internal/testutil/zfstest"
	"github.com/pdf/boomerangz/internal/transfer"
	"github.com/pdf/boomerangz/internal/zfs"
)

// TestGuestReplicaPromotion promotes a replica the way PLAN.md 5.3 documents:
// one installation replicates a root recursively, and another adopts the
// replica and carries on from it.
//
// A recursive receive brings the source's internal state with it. The owner
// and lineage arrive as received values that adoption then masks with local
// ones, and the bindings and proofs the receive excludes still arrive as
// hidden received values. ZFS keeps them whatever the receive asks, which is
// what TestGuestReceivedPropertyLayers pins, so promotion has to read them as
// the source's provenance rather than as conflicting authority. The lineage
// is the exception: the replica continues the source's history, so its
// received lineage must still agree.
func TestGuestReplicaPromotion(t *testing.T) {
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
	command := func(t *testing.T, args ...string) string {
		t.Helper()
		out, err := exec.CommandContext(t.Context(), "zfs", args...).CombinedOutput()
		if err != nil {
			t.Fatalf("guest zfs %s: %v: %s", args[0], err, out)
		}
		return strings.TrimSpace(string(out))
	}
	direct, err := zfs.NewDirect("zfs")
	if err != nil {
		t.Fatal(err)
	}
	stream, err := zfs.NewLocalStream("zfs")
	if err != nil {
		t.Fatal(err)
	}
	resolve := func(t *testing.T, dataset string) policy.Effective {
		t.Helper()
		rows, err := direct.GetStoredProperties(t.Context(), []string{dataset})
		if err != nil {
			t.Fatal(err)
		}
		return policy.Resolve(zfs.Dataset{Name: dataset, Type: zfs.Filesystem, EncryptionRoot: "-"}, nil, rows, nil)
	}

	const original = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	const successor = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
	suffix := time.Now().UTC().Format("150405")
	tree := sourcePool + "/data/promote-" + suffix
	receiveRoot := destinationPool + "/data/promote-" + suffix
	onward := sourcePool + "/data/promoted-" + suffix
	command(t, "create", "-u", tree)
	command(t, "create", "-u", tree+"/child")
	zfstest.RegisterCleanup(t, tree)
	command(t, "create", "-u", "-p", receiveRoot)
	zfstest.RegisterCleanup(t, receiveRoot)
	command(t, "create", "-u", onward)
	zfstest.RegisterCleanup(t, onward)
	command(t, "set", policy.Namespace+"enabled=on", policy.Namespace+"replicate=on", policy.Namespace+"discard=all", policy.Namespace+"local="+receiveRoot, tree)

	now := time.Now().UTC()
	snapshots, err := lifecycle.NewService(direct, original)
	if err != nil {
		t.Fatal(err)
	}
	engine, err := transfer.NewLocal(direct, stream, original)
	if err != nil {
		t.Fatal(err)
	}
	treePolicy := resolve(t, tree)
	if _, err := snapshots.CreateSnapshot(t.Context(), tree, zfstest.Subtree(t, tree), now, treePolicy); err != nil {
		t.Fatal(err)
	}
	result, err := engine.Apply(t.Context(), transfer.Request{Source: tree, DestinationRoot: receiveRoot, Policy: treePolicy}, nil)
	if err != nil || !result.Verified || !result.Plan.Send.Recursive {
		t.Fatalf("recursive replication=%+v err=%v", result, err)
	}
	replica := result.Plan.Destination
	sourceState, err := direct.InspectState(t.Context(), tree, false)
	if err != nil {
		t.Fatal(err)
	}
	lineage, err := lifecycle.RootAuthority(sourceState, tree, original)
	if err != nil {
		t.Fatal(err)
	}

	// The successor's own policy on the promoted replica: the source's was
	// excluded at receive, so nothing about it carries over.
	command(t, "set", policy.Namespace+"enabled=on", policy.Namespace+"replicate=on", policy.Namespace+"discard=all", policy.Namespace+"local="+onward, replica)
	replicaPolicy := resolve(t, replica)

	t.Run("received-state-arrives", func(t *testing.T) {
		state, err := direct.InspectState(t.Context(), replica, false)
		if err != nil {
			t.Fatal(err)
		}
		// Control arm: the replica really does carry the source's owner, so
		// the adoption below is promoting over it rather than over nothing.
		if state.Received[replica][lifecycle.OwnerProperty] != original || state.Received[replica][lifecycle.LineageProperty] != lineage {
			t.Fatalf("replica received owner=%q lineage=%q, want %s/%s", state.Received[replica][lifecycle.OwnerProperty], state.Received[replica][lifecycle.LineageProperty], original, lineage)
		}
		if _, err := lifecycle.RootAuthority(state, replica, successor); err == nil {
			t.Fatal("a received owner was taken as the successor's authority before adoption")
		}
	})

	successorSnapshots, err := lifecycle.NewService(direct, successor)
	if err != nil {
		t.Fatal(err)
	}
	t.Run("adopt", func(t *testing.T) {
		adopted, err := successorSnapshots.AdoptDataset(t.Context(), replica, replicaPolicy)
		if err != nil {
			t.Fatalf("promotion refused: %v", err)
		}
		if adopted != lineage {
			t.Fatalf("adopted lineage %s, want the source's %s", adopted, lineage)
		}
		state, err := direct.InspectState(t.Context(), replica, false)
		if err != nil {
			t.Fatal(err)
		}
		if got, err := lifecycle.RootAuthority(state, replica, successor); err != nil || got != lineage {
			t.Fatalf("promoted authority lineage=%s err=%v", got, err)
		}
		// The source's owner is still there, masked: provenance, not authority.
		if state.Received[replica][lifecycle.OwnerProperty] != original {
			t.Fatalf("received owner %q was lost", state.Received[replica][lifecycle.OwnerProperty])
		}
	})

	t.Run("carries-on", func(t *testing.T) {
		if _, err := successorSnapshots.CreateSnapshot(t.Context(), replica, zfstest.Subtree(t, replica), now.Add(time.Minute), replicaPolicy); err != nil {
			t.Fatalf("promoted replica could not be snapshotted: %v", err)
		}
		successorEngine, err := transfer.NewLocal(direct, stream, successor)
		if err != nil {
			t.Fatal(err)
		}
		onwardResult, err := successorEngine.Apply(t.Context(), transfer.Request{Source: replica, DestinationRoot: onward, Policy: replicaPolicy}, nil)
		if err != nil || !onwardResult.Verified {
			t.Fatalf("promoted replica could not replicate onward: %+v err=%v", onwardResult, err)
		}
		state, err := direct.InspectState(t.Context(), replica, false)
		if err != nil {
			t.Fatal(err)
		}
		refs, err := lifecycle.References(state, replica, lineage)
		if err != nil || len(refs) == 0 {
			t.Fatalf("successor proofs=%+v err=%v", refs, err)
		}
	})
}
