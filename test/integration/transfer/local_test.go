//go:build integration

package transfer_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/pdf/boomerangz/internal/lifecycle"
	"github.com/pdf/boomerangz/internal/policy"
	"github.com/pdf/boomerangz/internal/testutil/zfstest"
	"github.com/pdf/boomerangz/internal/transfer"
	"github.com/pdf/boomerangz/internal/zfs"
)

const interruptedReceiveArgsEnv = "BOOMERANGZ_INTERRUPTED_RECEIVE_ARGS"

type interruptedReceiveStream struct{}

func (interruptedReceiveStream) Run(ctx context.Context, send zfs.SendOptions, receive zfs.ReceiveOptions, estimate zfs.Estimate, report func(zfs.Progress)) (zfs.Progress, error) {
	return zfs.RunPipeline(ctx, send, receive, estimate, report,
		func(ctx context.Context, args []string) *exec.Cmd {
			return exec.CommandContext(ctx, "zfs", args...)
		},
		func(ctx context.Context, args []string) *exec.Cmd {
			encoded, err := json.Marshal(args)
			if err != nil {
				panic(err)
			}
			command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestGuestInterruptedReceiveHelper$")
			command.Env = append(os.Environ(), interruptedReceiveArgsEnv+"="+base64.RawStdEncoding.EncodeToString(encoded))
			return command
		},
	)
}

// TestGuestInterruptedReceiveHelper is a subprocess boundary used by the
// disposable-guest fault test. It is inert in ordinary test runs.
func TestGuestInterruptedReceiveHelper(t *testing.T) {
	encoded := os.Getenv(interruptedReceiveArgsEnv)
	if encoded == "" {
		t.Skip("helper process only")
	}
	raw, err := base64.RawStdEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatal(err)
	}
	var args []string
	if err := json.Unmarshal(raw, &args); err != nil {
		t.Fatal(err)
	}
	receiver := exec.CommandContext(t.Context(), "zfs", args...)
	input, err := receiver.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	receiver.Stdout = os.Stdout
	receiver.Stderr = os.Stderr
	if err := receiver.Start(); err != nil {
		t.Fatal(err)
	}
	if _, err := io.CopyN(input, os.Stdin, 4*1024*1024); err != nil {
		t.Fatal(err)
	}
	if err := input.Close(); err != nil {
		t.Fatal(err)
	}
	if err := receiver.Wait(); err == nil {
		t.Fatal("truncated receive unexpectedly succeeded")
	}
	// The helper must fail so RunPipeline treats this as an interrupted receive.
	os.Exit(1)
}

func TestGuestLocalTransfer(t *testing.T) {
	runID := os.Getenv("BOOMERANGZ_TRANSFER_GUEST_RUN")
	if runID == "" {
		t.Skip("disposable guest only")
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
	// command and request take the running *testing.T rather than closing over
	// the parent's: a helper bound to the parent reports a subtest's failure
	// against the whole test, which is the miscount this split exists to fix.
	command := func(t *testing.T, args ...string) string {
		t.Helper()
		out, err := exec.CommandContext(t.Context(), "zfs", args...).CombinedOutput()
		if err != nil {
			t.Fatalf("guest zfs %s: %v: %s", args[0], err, out)
		}
		return strings.TrimSpace(string(out))
	}
	direct, _ := zfs.NewDirect("zfs")
	stream, _ := zfs.NewLocalStream("zfs")
	const installation = "abcdefab-cdef-4abc-8def-abcdefabcdef"
	engine, _ := transfer.NewLocal(direct, stream, installation)
	snapshots, _ := lifecycle.NewService(direct, installation)
	source := zfstest.PayloadVolume(t, zfstest.FixtureName(sourcePool, "local"), 256, 32)
	suffix := time.Now().UTC().Format("150405")
	latest := destinationPool + "/data/latest-" + suffix
	all := destinationPool + "/data/all-" + suffix
	command(t, "set", policy.Namespace+"enabled=on", policy.Namespace+"local="+latest+","+all, source)
	request := func(t *testing.T, root, snapshot string) transfer.Request {
		t.Helper()
		rows, err := direct.GetStoredProperties(t.Context(), []string{source})
		if err != nil {
			t.Fatal(err)
		}
		return transfer.Request{Source: source, DestinationRoot: root, Snapshot: snapshot, Policy: policy.Resolve(zfs.Dataset{Name: source, Type: zfs.Volume, EncryptionRoot: "-"}, nil, rows, nil)}
	}
	now := time.Now().UTC()

	// Most phases below build on the latest/all destinations the bootstrap
	// established, so a failure early in that chain leaves the rest testing
	// nothing - reporting them as red would be noise, not information. chain
	// runs a dependent phase only while its predecessors have passed and marks
	// the remainder skipped. Phases that build their own fixtures use t.Run
	// directly and always report, whatever the chain did.
	chainOK := true
	chain := func(name string, fn func(*testing.T)) {
		if !chainOK {
			t.Run(name, func(t *testing.T) { t.Skip("depends on an earlier phase that failed") })
			return
		}
		chainOK = t.Run(name, fn)
	}
	// Created by incremental-modes, destroyed by bookmark-incremental.
	var last lifecycle.Metadata

	chain("full-bootstrap", func(t *testing.T) {
		first, err := snapshots.CreateSnapshot(t.Context(), source, false, now.Add(-4*time.Hour), request(t, latest, "").Policy)
		if err != nil {
			t.Fatal(err)
		}
		for _, root := range []string{latest, all} {
			req := request(t, root, source+"@"+first.Name())
			preview, err := engine.Preview(t.Context(), req)
			if err != nil || preview.Mode != "full" {
				t.Fatalf("full preview=%v err=%v", preview, err)
			}
			var bytes uint64
			result, err := engine.Apply(t.Context(), req, func(p zfs.Progress) { bytes = p.Bytes })
			if err != nil || !result.Verified || bytes == 0 {
				t.Fatalf("full result=%+v err=%v", result, err)
			}
			t.Logf("full bootstrap verified: %s, %d bytes", root, bytes)
		}
	})

	// Self-contained: its own source and destination, independent of the chain.
	t.Run("incremental-all-base-retention", func(t *testing.T) {
		// incremental=all must retain its verified snapshot base across pruning,
		// then rotate that protection only after a newer receive is verified.
		retentionSource := sourcePool + "/data/retention-" + suffix
		retentionDestination := destinationPool + "/data/retention-" + suffix
		command(t, "create", "-u", retentionSource)
		command(t, "set", policy.Namespace+"enabled=on", policy.Namespace+"local="+retentionDestination, policy.Namespace+"policy=1x5m", retentionSource)
		retentionRequest := func() transfer.Request {
			t.Helper()
			rows, err := direct.GetStoredProperties(t.Context(), []string{retentionSource})
			if err != nil {
				t.Fatal(err)
			}
			effective := policy.Resolve(zfs.Dataset{Name: retentionSource, Type: zfs.Filesystem, EncryptionRoot: "-"}, nil, rows, nil)
			return transfer.Request{Source: retentionSource, DestinationRoot: retentionDestination, Policy: effective}
		}
		retentionPolicy := retentionRequest().Policy
		retentionBase, err := snapshots.CreateSnapshot(t.Context(), retentionSource, false, now.Add(-time.Hour), retentionPolicy)
		if err != nil {
			t.Fatal(err)
		}
		retentionResult, err := engine.Apply(t.Context(), retentionRequest(), nil)
		if err != nil || !retentionResult.Verified || retentionResult.Plan.Mode != "full" {
			t.Fatalf("retention bootstrap=%+v err=%v", retentionResult, err)
		}
		retentionNext, err := snapshots.CreateSnapshot(t.Context(), retentionSource, false, now, retentionPolicy)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := snapshots.Prune(t.Context(), retentionSource, retentionPolicy, true); err != nil {
			t.Fatal(err)
		}
		if err := exec.CommandContext(t.Context(), "zfs", "list", "-H", "-t", "snapshot", retentionSource+"@"+retentionBase.Name()).Run(); err != nil {
			t.Fatal("pruning removed the retained incremental-all base")
		}
		retentionResult, err = engine.Apply(t.Context(), retentionRequest(), nil)
		if err != nil || !retentionResult.Verified || retentionResult.Plan.Mode != "incremental-all" {
			t.Fatalf("retained-base incremental=%+v err=%v", retentionResult, err)
		}
		retentionState, err := direct.InspectState(t.Context(), retentionSource, false)
		if err != nil {
			t.Fatal(err)
		}
		if len(retentionState.Holds[retentionSource+"@"+retentionBase.Name()]) != 0 || len(retentionState.Holds[retentionSource+"@"+retentionNext.Name()]) == 0 {
			t.Fatalf("incremental-all hold did not rotate: %v", retentionState.Holds)
		}
		if _, err := snapshots.Prune(t.Context(), retentionSource, retentionPolicy, true); err != nil {
			t.Fatal(err)
		}
		if err := exec.CommandContext(t.Context(), "zfs", "list", "-H", "-t", "snapshot", retentionSource+"@"+retentionBase.Name()).Run(); err == nil {
			t.Fatal("retired incremental-all base remained protected from pruning")
		}
		t.Log("incremental-all base retention and rotation verified across pruning")
	})

	chain("incremental-modes", func(t *testing.T) {
		middle, err := snapshots.CreateSnapshot(t.Context(), source, false, now.Add(-3*time.Hour), request(t, latest, "").Policy)
		if err != nil {
			t.Fatal(err)
		}
		command(t, "snapshot", source+"@foreign-"+suffix)
		last, err = snapshots.CreateSnapshot(t.Context(), source, false, now.Add(-2*time.Hour), request(t, latest, "").Policy)
		if err != nil {
			t.Fatal(err)
		}
		command(t, "set", policy.Namespace+"incremental=latest", source)
		result, err := engine.Apply(t.Context(), request(t, latest, ""), nil)
		if err != nil || !result.Verified || result.Plan.Mode != "incremental-latest" {
			t.Fatalf("latest=%+v err=%v", result, err)
		}
		state, err := direct.InspectState(t.Context(), latest, false)
		if err != nil {
			t.Fatal(err)
		}
		if len(lifecycle.Snapshots(state, latest)) != 2 {
			t.Fatal("-i included intermediary snapshots")
		}
		command(t, "set", policy.Namespace+"incremental=all", policy.Namespace+"props=on", policy.Namespace+"set_prop:readonly=on", source)
		result, err = engine.Apply(t.Context(), request(t, all, ""), nil)
		if err != nil || !result.Verified || result.Plan.Mode != "incremental-all" {
			t.Fatalf("all=%+v err=%v", result, err)
		}
		state, err = direct.InspectState(t.Context(), all, false)
		if err != nil {
			t.Fatal(err)
		}
		if len(lifecycle.Snapshots(state, all)) != 4 {
			t.Fatal("-I omitted intermediary snapshots")
		}
		for _, p := range state.Properties {
			if policy.IsPublic(p.Name) {
				t.Fatalf("source public configuration remained effective: %v", p)
			}
		}
		if command(t, "get", "-H", "-o", "value", "readonly", all) != "on" {
			t.Fatal("receive override missing")
		}
		t.Logf("-i/-I, foreign intermediate, namespace isolation and receive override verified; middle=%s", middle.Name())
	})

	chain("bookmark-incremental", func(t *testing.T) {
		// Advance the incremental-all target so it no longer protects last. The
		// latest-only target must then remain able to use its bookmark after that
		// source snapshot is pruned.
		if _, err := snapshots.CreateSnapshot(t.Context(), source, false, now.Add(-time.Hour), request(t, all, "").Policy); err != nil {
			t.Fatal(err)
		}
		result, err := engine.Apply(t.Context(), request(t, all, ""), nil)
		if err != nil || !result.Verified || result.Plan.Mode != "incremental-all" {
			t.Fatalf("all base rotation=%+v err=%v", result, err)
		}
		if err := direct.DestroySnapshot(t.Context(), source+"@"+last.Name()); err != nil {
			t.Fatal(err)
		}
		command(t, "set", policy.Namespace+"incremental=latest", policy.Namespace+"props=off", source)
		result, err = engine.Apply(t.Context(), request(t, latest, ""), nil)
		if err != nil || !result.Verified || !strings.Contains(result.Plan.Base, "#") {
			t.Fatalf("bookmark=%+v err=%v", result, err)
		}
		t.Log("bookmark-based incremental verified")
	})

	chain("unrelated-destination-refused", func(t *testing.T) {
		// Existing unrelated destination history must be refused without starting send.
		command(t, "snapshot", latest+"@foreign-destination")
		if _, err := snapshots.CreateSnapshot(t.Context(), source, false, now, request(t, latest, "").Policy); err != nil {
			t.Fatal(err)
		}
		if _, err := engine.Preview(t.Context(), request(t, latest, "")); err == nil {
			t.Fatal("accepted unrelated latest destination snapshot")
		}
	})

	chain("reseed-recovery", func(t *testing.T) {
		// Model an initial receive that created the exact destination before the
		// source-side binding could be persisted. The explicit reseed path must
		// recover this otherwise-blocked target without disturbing the source or the
		// other configured destination. It depends on the refusal above having
		// left the destination blocked.
		command(t, "inherit", policy.StateNamespace+"target:"+lifecycle.TargetID("local:"+latest), source)
		reseed, err := transfer.NewReseedService(direct, direct, installation)
		if err != nil {
			t.Fatal(err)
		}
		reseedRequest := request(t, latest, "")
		reseedPreview, err := reseed.Plan(t.Context(), reseedRequest)
		if err != nil || reseedPreview.BindingStored || !reseedPreview.DestinationExists || len(reseedPreview.DestinationObjects) == 0 {
			t.Fatalf("reseed preview=%+v err=%v", reseedPreview, err)
		}
		reseedResult, err := reseed.Apply(t.Context(), reseedRequest)
		if err != nil || !reseedResult.Applied {
			t.Fatalf("reseed result=%+v err=%v", reseedResult, err)
		}
		if err := exec.CommandContext(t.Context(), "zfs", "list", "-H", latest).Run(); err == nil {
			t.Fatal("reseed retained mapped destination")
		}
		result, err := engine.Apply(t.Context(), request(t, latest, ""), nil)
		if err != nil || !result.Verified || result.Plan.Mode != "full" {
			t.Fatalf("post-reseed full transfer=%+v err=%v", result, err)
		}
	})

	// Self-contained: each mapping builds its own source tree.
	t.Run("recursive-mapping", func(t *testing.T) {
		// Full recursive package with both native path mapping modes.
		for _, discard := range []string{"first", "all"} {
			t.Run(discard, func(t *testing.T) {
				// Mapping is part of the persistent target binding. Exercise each mapping
				// on a distinct authoritative source root rather than silently rebinding
				// one source when its discard policy changes.
				tree := sourcePool + "/data/tree-" + discard + "-" + suffix
				command(t, "create", "-u", tree)
				command(t, "create", "-u", tree+"/child")
				command(t, "snapshot", "-r", tree+"@foreign-recursive")
				command(t, "set", policy.Namespace+"enabled=on", policy.Namespace+"replicate=on", policy.Namespace+"discard="+discard, policy.Namespace+"local="+destinationPool+"/data", tree)
				rows, err := direct.GetStoredProperties(t.Context(), []string{tree})
				if err != nil {
					t.Fatal(err)
				}
				treePolicy := policy.Resolve(zfs.Dataset{Name: tree, Type: zfs.Filesystem}, nil, rows, nil)
				if _, err := snapshots.CreateSnapshot(t.Context(), tree, true, now, treePolicy); err != nil {
					t.Fatal(err)
				}
				req := transfer.Request{Source: tree, DestinationRoot: destinationPool + "/data", Policy: treePolicy}
				result, err := engine.Apply(t.Context(), req, nil)
				if err != nil || !result.Verified {
					t.Fatalf("recursive %s=%+v err=%v", discard, result, err)
				}
				t.Logf("recursive %s mapping verified: %s", discard, result.Plan.Destination)
				if _, err := snapshots.CreateSnapshot(t.Context(), tree, true, now.Add(time.Minute), treePolicy); err != nil {
					t.Fatal(err)
				}
				result, err = engine.Apply(t.Context(), req, nil)
				if err != nil || !result.Verified || result.Plan.Mode != "incremental-all" {
					t.Fatalf("recursive incremental %s=%+v err=%v", discard, result, err)
				}
				command(t, "snapshot", result.Plan.Destination+"@foreign-destination")
				if _, err := snapshots.CreateSnapshot(t.Context(), tree, true, now.Add(2*time.Minute), treePolicy); err != nil {
					t.Fatal(err)
				}
				if _, err := engine.Preview(t.Context(), req); err == nil {
					t.Fatal("accepted foreign latest recursive destination snapshot")
				}
			})
		}
	})

	// Self-contained: its own source tree and receive root.
	t.Run("linux-ancestor-preparation", func(t *testing.T) {
		// Linux cannot rely on delegated mounting. Start from an ordinary destination
		// root whose descendants would be mountable, then verify Boomerangz prepares
		// every implicit -d ancestor as a container and makes the received leaf noauto.
		linuxSource := sourcePool + "/data/linux-" + suffix + "/home/pdf"
		for _, dataset := range []string{strings.TrimSuffix(linuxSource, "/home/pdf"), strings.TrimSuffix(linuxSource, "/pdf"), linuxSource} {
			command(t, "create", "-u", dataset)
		}
		linuxRoot := destinationPool + "/data/linux-root-" + suffix
		ordinaryMountpoint := "/mnt/boomerangz-linux-root-" + suffix
		command(t, "create", "-u", "-o", "mountpoint="+ordinaryMountpoint, linuxRoot)
		command(t, "set", policy.Namespace+"enabled=on", policy.Namespace+"discard=first", policy.Namespace+"local="+linuxRoot, linuxSource)
		rows, err := direct.GetStoredProperties(t.Context(), []string{linuxSource})
		if err != nil {
			t.Fatal(err)
		}
		linuxPolicy := policy.Resolve(zfs.Dataset{Name: linuxSource, Type: zfs.Filesystem, EncryptionRoot: "-"}, nil, rows, nil)
		if _, err := snapshots.CreateSnapshot(t.Context(), linuxSource, false, now.Add(3*time.Minute), linuxPolicy); err != nil {
			t.Fatal(err)
		}
		linuxResult, err := engine.Apply(t.Context(), transfer.Request{Source: linuxSource, DestinationRoot: linuxRoot, Policy: linuxPolicy}, nil)
		if err != nil || !linuxResult.Verified {
			t.Fatalf("Linux unmounted bootstrap=%+v err=%v", linuxResult, err)
		}
		if got := command(t, "get", "-H", "-o", "value", "mountpoint", linuxRoot); got != ordinaryMountpoint {
			t.Fatalf("configured receive root mountpoint changed: %s", got)
		}
		for dataset := linuxRoot + "/data"; ; {
			wantCanmount := "noauto"
			if got := command(t, "get", "-H", "-o", "value", "canmount", dataset); got != wantCanmount {
				t.Fatalf("receive dataset %s canmount=%s want=%s", dataset, got, wantCanmount)
			}
			if got := command(t, "get", "-H", "-o", "value", "mounted", dataset); got != "no" {
				t.Fatalf("receive dataset %s mounted=%s", dataset, got)
			}
			if dataset == linuxResult.Plan.Destination {
				break
			}
			remainder := strings.TrimPrefix(linuxResult.Plan.Destination, dataset+"/")
			next, _, _ := strings.Cut(remainder, "/")
			dataset += "/" + next
		}
	})
}

func TestGuestInterruptedTransferRecovery(t *testing.T) {
	runID := os.Getenv("BOOMERANGZ_TRANSFER_GUEST_RUN")
	if runID == "" {
		t.Skip("disposable guest only")
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
	suffix := time.Now().UTC().Format("150405000")
	source := zfstest.PayloadVolume(t, zfstest.FixtureName(sourcePool, "interrupted"), 256, 32)
	direct, err := zfs.NewDirect("zfs")
	if err != nil {
		t.Fatal(err)
	}
	const installation = "abcdefab-cdef-4abc-8def-abcdefabcdef"
	snapshots, err := lifecycle.NewService(direct, installation)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := zfs.NewLocalStream("zfs")
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"all", "latest"} {
		t.Run(mode, func(t *testing.T) {
			destination := destinationPool + "/data/interrupted-" + mode + "-" + suffix
			output, err := exec.CommandContext(t.Context(), "zfs", "set", policy.Namespace+"enabled=on", policy.Namespace+"incremental="+mode, policy.Namespace+"local="+destination, source).CombinedOutput()
			if err != nil {
				t.Fatalf("configure source: %v: %s", err, output)
			}
			rows, err := direct.GetStoredProperties(t.Context(), []string{source})
			if err != nil {
				t.Fatal(err)
			}
			effective := policy.Resolve(zfs.Dataset{Name: source, Type: zfs.Volume, EncryptionRoot: "-"}, nil, rows, nil)
			metadata, err := snapshots.CreateSnapshot(t.Context(), source, false, time.Now().UTC(), effective)
			if err != nil {
				t.Fatal(err)
			}
			request := transfer.Request{Source: source, DestinationRoot: destination, Snapshot: source + "@" + metadata.Name(), Policy: effective}
			interrupted, err := transfer.NewLocal(direct, interruptedReceiveStream{}, installation)
			if err != nil {
				t.Fatal(err)
			}
			result, err := interrupted.Apply(t.Context(), request, nil)
			if err == nil || len(result.ResumeDatasets) != 1 {
				t.Fatalf("interrupted result=%+v err=%v", result, err)
			}

			restarted, err := transfer.NewLocal(direct, stream, installation)
			if err != nil {
				t.Fatal(err)
			}
			recovered, err := restarted.Apply(t.Context(), request, nil)
			if err != nil || !recovered.Verified || recovered.Plan.Mode != "resume" {
				t.Fatalf("recovered result=%+v err=%v", recovered, err)
			}
			destinationState, err := direct.InspectState(t.Context(), destination, false)
			if err != nil {
				t.Fatal(err)
			}
			if len(destinationState.ResumeTokens) != 0 {
				t.Fatalf("successful recovery retained resume token: %v", destinationState.ResumeTokens)
			}
			sourceState, err := direct.InspectState(t.Context(), source, false)
			if err != nil {
				t.Fatal(err)
			}
			lineage, err := lifecycle.RootAuthority(sourceState, source, installation)
			if err != nil {
				t.Fatal(err)
			}
			references, err := lifecycle.References(sourceState, source, lineage)
			if err != nil {
				t.Fatal(err)
			}
			target, found, checkpointed, retained := "local:"+destination, false, false, false
			for _, reference := range references {
				if reference.Target != target {
					continue
				}
				found = true
				snapshot := reference.SnapshotName(source)
				retained = slices.Contains(sourceState.Holds[snapshot], reference.HoldName())
				checkpointed = slices.ContainsFunc(sourceState.Objects, func(object zfs.Object) bool {
					return object.Name == reference.BookmarkName(source) && object.Type == "bookmark" && object.GUID == reference.GUID
				})
			}
			if !found || !checkpointed || retained != (mode == "all") {
				t.Fatalf("unexpected %s recovery reference: found=%t checkpointed=%t retained=%t refs=%+v holds=%v", mode, found, checkpointed, retained, references, sourceState.Holds)
			}
			t.Logf("%s interrupted receive resumed from durable state on %s", mode, destination)
		})
	}
}
