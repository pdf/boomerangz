//go:build integration

package transfer_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/pdf/boomerangz/internal/lifecycle"
	"github.com/pdf/boomerangz/internal/migrate"
	"github.com/pdf/boomerangz/internal/policy"
	"github.com/pdf/boomerangz/internal/testutil/zfstest"
	"github.com/pdf/boomerangz/internal/transfer"
	"github.com/pdf/boomerangz/internal/zfs"
)

// TestGuestStateMigration migrates a root that has replicated recursively, in
// format v1, and replicates it onward incrementally from the endpoint its
// migrated proof describes.
//
// This build cannot write format v1, so the test writes the root's proofs and
// binding back the way v1 stored them, from the live holds, bookmarks and
// GUIDs a real transfer left. The v1 encodings are pinned against main's own
// output by internal/migrate's golden fixtures; what this test adds is the
// real pool: that the endpoint migration carries over is the one ZFS holds,
// and that a transfer builds on it rather than starting again.
func TestGuestStateMigration(t *testing.T) {
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
	const installation = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
	suffix := time.Now().UTC().Format("150405")
	tree := sourcePool + "/data/migrate-" + suffix
	destination := destinationPool + "/data/migrate-" + suffix
	canonical := "local:" + destination
	command(t, "create", "-u", tree)
	command(t, "create", "-u", tree+"/child")
	zfstest.RegisterCleanup(t, tree)
	zfstest.RegisterCleanup(t, destination)
	command(t, "set", policy.Namespace+"enabled=on", policy.Namespace+"replicate=on", policy.Namespace+"local="+destination, tree)
	rows, err := direct.GetStoredProperties(t.Context(), []string{tree})
	if err != nil {
		t.Fatal(err)
	}
	effective := policy.Resolve(zfs.Dataset{Name: tree, Type: zfs.Filesystem, EncryptionRoot: "-"}, nil, rows, nil)
	snapshots, err := lifecycle.NewService(direct, installation)
	if err != nil {
		t.Fatal(err)
	}
	engine, err := transfer.NewLocal(direct, stream, installation)
	if err != nil {
		t.Fatal(err)
	}
	request := transfer.Request{Source: tree, DestinationRoot: destination, Policy: effective}
	now := time.Now().UTC()
	replicate := func(t *testing.T, at time.Time) transfer.Result {
		t.Helper()
		if _, err := snapshots.CreateSnapshot(t.Context(), tree, zfstest.Subtree(t, tree), at, effective); err != nil {
			t.Fatal(err)
		}
		result, err := engine.Apply(t.Context(), request, nil)
		if err != nil || !result.Verified || !result.Plan.Send.Recursive {
			t.Fatalf("replication=%+v err=%v", result, err)
		}
		return result
	}
	replicate(t, now)

	t.Run("write-v1", func(t *testing.T) {
		state, err := direct.InspectState(t.Context(), tree, true)
		if err != nil {
			t.Fatal(err)
		}
		lineage, err := lifecycle.RootAuthority(state, tree, installation)
		if err != nil {
			t.Fatal(err)
		}
		refs, err := lifecycle.References(state, tree, lineage)
		if err != nil || len(refs) == 0 {
			t.Fatalf("refs=%+v err=%v", refs, err)
		}
		type metadata struct {
			Lineage  string    `json:"lineage"`
			Snapshot string    `json:"snapshot"`
			Created  time.Time `json:"created"`
		}
		type source struct {
			Dataset  string   `json:"dataset"`
			GUID     uint64   `json:"guid"`
			Metadata metadata `json:"metadata"`
		}
		for _, r := range refs {
			sources, err := r.Sources(state)
			if err != nil {
				t.Fatal(err)
			}
			m := metadata{Lineage: r.Metadata.Lineage, Snapshot: r.Metadata.Snapshot, Created: r.Metadata.Created.UTC()}
			record := struct {
				Target   string   `json:"target"`
				Dataset  string   `json:"dataset"`
				Metadata metadata `json:"metadata"`
				Sources  []source `json:"sources"`
			}{Target: canonical, Dataset: r.Dataset, Metadata: m}
			for _, s := range sources {
				record.Sources = append(record.Sources, source{Dataset: s.Dataset, GUID: s.GUID, Metadata: m})
				// v1 also stored the creation time on every snapshot.
				if snapshot := s.Dataset + "@" + r.Metadata.Name(); hasObject(state, snapshot) {
					command(t, "set", policy.StateNamespace+"created="+r.Metadata.Created.UTC().Format(time.RFC3339Nano), snapshot)
				}
			}
			value, _ := json.Marshal(record)
			command(t, "set", r.PropertyName()+"="+string(value), tree)
		}
		bindingName := lifecycle.TargetStatePrefix + lifecycle.TargetID(canonical)
		var current struct {
			PoolGUID   uint64 `json:"pool_guid"`
			Mapped     string `json:"mapped"`
			AnchorGUID uint64 `json:"anchor_guid"`
			Anchor     string `json:"anchor"`
		}
		for _, p := range state.Properties {
			if p.Dataset == tree && p.Name == bindingName && p.Source == zfs.SourceLocal {
				if err := json.Unmarshal([]byte(p.Value), &current); err != nil {
					t.Fatal(err)
				}
			}
		}
		if current.Mapped == "" {
			t.Fatal("the transfer left no binding")
		}
		anchor := current.Anchor
		if anchor == "" {
			anchor = current.Mapped
		}
		pool, _, _ := strings.Cut(anchor, "/")
		old, _ := json.Marshal(map[string]any{
			"version": 1, "transport": "local", "canonical_target": canonical, "destination_root": destination,
			"mapped_dataset": current.Mapped, "pool": pool, "pool_guid": current.PoolGUID, "anchor": anchor,
			"anchor_guid": current.AnchorGUID, "relative_path": strings.TrimPrefix(strings.TrimPrefix(current.Mapped, anchor), "/"),
		})
		command(t, "set", bindingName+"="+string(old), tree)
		command(t, "inherit", lifecycle.VersionProperty, tree)

		// Control arm: this build now refuses the root.
		if _, err := engine.Preview(t.Context(), request); err == nil || !strings.Contains(err.Error(), "boomerangz migrate") {
			t.Fatalf("a v1 root was planned: %v", err)
		}
	})

	t.Run("migrate", func(t *testing.T) {
		plan, err := migrate.Preview(t.Context(), direct, tree)
		if err != nil || plan.Status != migrate.StatusPending || plan.From != 1 {
			t.Fatalf("preview=%+v err=%v", plan, err)
		}
		applied, err := migrate.Apply(t.Context(), direct, tree)
		if err != nil || applied.Status != migrate.StatusMigrated || applied.Applied != len(plan.Actions) {
			t.Fatalf("apply=%+v err=%v", applied, err)
		}
		if again, err := migrate.Preview(t.Context(), direct, tree); err != nil || again.Status != migrate.StatusCurrent {
			t.Fatalf("second preview=%+v err=%v", again, err)
		}
	})

	t.Run("carries-on-incrementally", func(t *testing.T) {
		result := replicate(t, now.Add(time.Minute))
		if result.Plan.Base == "" || result.Plan.BindingNew {
			t.Fatalf("migrated root did not build on its replica: base=%q new binding=%v", result.Plan.Base, result.Plan.BindingNew)
		}
	})
}

func hasObject(state zfs.State, name string) bool {
	for _, object := range state.Objects {
		if object.Name == name {
			return true
		}
	}
	return false
}
