package transfer

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

func TestGuestLocalTransfer(t *testing.T) {
	runID := os.Getenv("BOOMERANGZ_TRANSFER_GUEST_RUN")
	if runID == "" {
		t.Skip("disposable guest only")
	}
	sourcePool, err := zfstest.VerifyGuestPool(t.Context(), runID, zfstest.SourceDisk, "/dev/vdb")
	if err != nil {
		t.Fatal(err)
	}
	destinationPool, err := zfstest.VerifyGuestPool(t.Context(), runID, zfstest.DestinationDisk, "/dev/vdc")
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
	engine, _ := NewLocal(direct, stream, installation)
	snapshots, _ := lifecycle.NewService(direct, installation)
	source := sourcePool + "/data/payload"
	suffix := time.Now().UTC().Format("150405")
	latest := destinationPool + "/data/latest-" + suffix
	all := destinationPool + "/data/all-" + suffix
	command("set", policy.Namespace+"enabled=on", policy.Namespace+"local="+latest+","+all, source)
	request := func(root, snapshot string) Request {
		t.Helper()
		rows, err := direct.GetStoredProperties(t.Context(), []string{source})
		if err != nil {
			t.Fatal(err)
		}
		return Request{Source: source, DestinationRoot: root, Snapshot: snapshot, Policy: policy.Resolve(zfs.Dataset{Name: source, Type: zfs.Volume, EncryptionRoot: "-"}, nil, rows, nil)}
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
		req := Request{Source: tree, DestinationRoot: destinationPool + "/data", Policy: treePolicy}
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
}
