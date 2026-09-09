//go:build integration

package transfer_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
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
	command := func(args ...string) string {
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
	source := sourcePool + "/data/payload"
	suffix := time.Now().UTC().Format("150405")
	latest := destinationPool + "/data/latest-" + suffix
	all := destinationPool + "/data/all-" + suffix
	command("set", policy.Namespace+"enabled=on", policy.Namespace+"local="+latest+","+all, source)
	request := func(root, snapshot string) transfer.Request {
		t.Helper()
		rows, err := direct.GetStoredProperties(t.Context(), []string{source})
		if err != nil {
			t.Fatal(err)
		}
		return transfer.Request{Source: source, DestinationRoot: root, Snapshot: snapshot, Policy: policy.Resolve(zfs.Dataset{Name: source, Type: zfs.Volume, EncryptionRoot: "-"}, nil, rows, nil)}
	}
	now := time.Now().UTC()
	first, err := snapshots.CreateSnapshot(t.Context(), source, false, now.Add(-4*time.Hour), request(latest, "").Policy)
	if err != nil {
		t.Fatal(err)
	}
	for _, root := range []string{latest, all} {
		req := request(root, source+"@"+first.Name())
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
	middle, err := snapshots.CreateSnapshot(t.Context(), source, false, now.Add(-3*time.Hour), request(latest, "").Policy)
	if err != nil {
		t.Fatal(err)
	}
	command("snapshot", source+"@foreign-"+suffix)
	last, err := snapshots.CreateSnapshot(t.Context(), source, false, now.Add(-2*time.Hour), request(latest, "").Policy)
	if err != nil {
		t.Fatal(err)
	}
	command("set", policy.Namespace+"incremental=latest", source)
	result, err := engine.Apply(t.Context(), request(latest, ""), nil)
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
	command("set", policy.Namespace+"incremental=all", policy.Namespace+"props=on", policy.Namespace+"set_prop:readonly=on", source)
	result, err = engine.Apply(t.Context(), request(all, ""), nil)
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
	if command("get", "-H", "-o", "value", "readonly", all) != "on" {
		t.Fatal("receive override missing")
	}
	t.Logf("-i/-I, foreign intermediate, namespace isolation and receive override verified; middle=%s", middle.Name())
	// A source bookmark must remain usable after its protected snapshot is pruned.
	if err := direct.DestroySnapshot(t.Context(), source+"@"+last.Name()); err != nil {
		t.Fatal(err)
	}
	if _, err := snapshots.CreateSnapshot(t.Context(), source, false, now.Add(-time.Hour), request(latest, "").Policy); err != nil {
		t.Fatal(err)
	}
	command("set", policy.Namespace+"incremental=latest", policy.Namespace+"props=off", source)
	result, err = engine.Apply(t.Context(), request(latest, ""), nil)
	if err != nil || !result.Verified || !strings.Contains(result.Plan.Base, "#") {
		t.Fatalf("bookmark=%+v err=%v", result, err)
	}
	t.Log("bookmark-based incremental verified")
	// Existing unrelated destination history must be refused without starting send.
	command("snapshot", latest+"@foreign-destination")
	if _, err := snapshots.CreateSnapshot(t.Context(), source, false, now, request(latest, "").Policy); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Preview(t.Context(), request(latest, "")); err == nil {
		t.Fatal("accepted unrelated latest destination snapshot")
	}
	// Model an initial receive that created the exact destination before the
	// source-side binding could be persisted. The explicit reseed path must
	// recover this otherwise-blocked target without disturbing the source or the
	// other configured destination.
	command("inherit", policy.StateNamespace+"target:"+lifecycle.TargetID("local:"+latest), source)
	reseed, err := transfer.NewReseedService(direct, direct, installation)
	if err != nil {
		t.Fatal(err)
	}
	reseedRequest := request(latest, "")
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
	result, err = engine.Apply(t.Context(), request(latest, ""), nil)
	if err != nil || !result.Verified || result.Plan.Mode != "full" {
		t.Fatalf("post-reseed full transfer=%+v err=%v", result, err)
	}
	// Full recursive package with both native path mapping modes.
	for _, discard := range []string{"first", "all"} {
		// Mapping is part of the persistent target binding. Exercise each mapping
		// on a distinct authoritative source root rather than silently rebinding
		// one source when its discard policy changes.
		tree := sourcePool + "/data/tree-" + discard + "-" + suffix
		command("create", "-u", tree)
		command("create", "-u", tree+"/child")
		command("snapshot", "-r", tree+"@foreign-recursive")
		command("set", policy.Namespace+"enabled=on", policy.Namespace+"replicate=on", policy.Namespace+"discard="+discard, policy.Namespace+"local="+destinationPool+"/data", tree)
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
		command("snapshot", result.Plan.Destination+"@foreign-destination")
		if _, err := snapshots.CreateSnapshot(t.Context(), tree, true, now.Add(2*time.Minute), treePolicy); err != nil {
			t.Fatal(err)
		}
		if _, err := engine.Preview(t.Context(), req); err == nil {
			t.Fatal("accepted foreign latest recursive destination snapshot")
		}
	}

	// Linux cannot rely on delegated mounting. Start from an ordinary destination
	// root whose descendants would be mountable, then verify Boomerangz prepares
	// every implicit -d ancestor as a container and makes the received leaf noauto.
	linuxSource := sourcePool + "/data/linux-" + suffix + "/home/pdf"
	for _, dataset := range []string{strings.TrimSuffix(linuxSource, "/home/pdf"), strings.TrimSuffix(linuxSource, "/pdf"), linuxSource} {
		command("create", "-u", dataset)
	}
	linuxRoot := destinationPool + "/data/linux-root-" + suffix
	ordinaryMountpoint := "/mnt/boomerangz-linux-root-" + suffix
	command("create", "-u", "-o", "mountpoint="+ordinaryMountpoint, linuxRoot)
	command("set", policy.Namespace+"enabled=on", policy.Namespace+"discard=first", policy.Namespace+"local="+linuxRoot, linuxSource)
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
	if got := command("get", "-H", "-o", "value", "mountpoint", linuxRoot); got != ordinaryMountpoint {
		t.Fatalf("configured receive root mountpoint changed: %s", got)
	}
	for dataset := linuxRoot + "/data"; ; {
		wantCanmount := "noauto"
		if got := command("get", "-H", "-o", "value", "canmount", dataset); got != wantCanmount {
			t.Fatalf("receive dataset %s canmount=%s want=%s", dataset, got, wantCanmount)
		}
		if got := command("get", "-H", "-o", "value", "mounted", dataset); got != "no" {
			t.Fatalf("receive dataset %s mounted=%s", dataset, got)
		}
		if dataset == linuxResult.Plan.Destination {
			break
		}
		remainder := strings.TrimPrefix(linuxResult.Plan.Destination, dataset+"/")
		next, _, _ := strings.Cut(remainder, "/")
		dataset += "/" + next
	}
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
	command := func(args ...string) string {
		t.Helper()
		out, commandErr := exec.CommandContext(t.Context(), "zfs", args...).CombinedOutput()
		if commandErr != nil {
			t.Fatalf("guest zfs %s: %v: %s", args[0], commandErr, out)
		}
		return strings.TrimSpace(string(out))
	}
	suffix := time.Now().UTC().Format("150405000")
	source := sourcePool + "/data/payload"
	destination := destinationPool + "/data/interrupted-" + suffix
	command("set", policy.Namespace+"enabled=on", policy.Namespace+"local="+destination, source)
	direct, err := zfs.NewDirect("zfs")
	if err != nil {
		t.Fatal(err)
	}
	const installation = "abcdefab-cdef-4abc-8def-abcdefabcdef"
	snapshots, err := lifecycle.NewService(direct, installation)
	if err != nil {
		t.Fatal(err)
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
	state, err := direct.InspectState(t.Context(), source, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Holds) == 0 {
		t.Fatal("interrupted transfer did not retain source recovery holds")
	}

	stream, err := zfs.NewLocalStream("zfs")
	if err != nil {
		t.Fatal(err)
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
	if len(sourceState.Holds) != 0 {
		t.Fatalf("successful recovery retained source holds: %v", sourceState.Holds)
	}
	t.Logf("interrupted receive resumed from durable state on %s", destination)
}
