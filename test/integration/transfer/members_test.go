//go:build integration

package transfer_test

import (
	"os"
	"os/exec"
	"slices"
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

// TestGuestRootActorScope replays the layout of the incident that motivated
// root actor scope - a home dataset with active and excluded descendants
// replicating to one local target - and the reseed that wedged it. A root owns
// its subtree: its members stream into its replica, and a reseed of the root
// resets every member with it, so the next transfer converges instead of
// leaving descendants pointing at replicas that no longer exist.
func TestGuestRootActorScope(t *testing.T) {
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
	exists := func(t *testing.T, name string) bool {
		t.Helper()
		return exec.CommandContext(t.Context(), "zfs", "list", "-H", name).Run() == nil
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

	root := zfstest.FixtureName(sourcePool, "home")
	destinationRoot := zfstest.FixtureName(destinationPool, "backup")
	zfstest.RegisterCleanup(t, root)
	zfstest.RegisterCleanup(t, destinationRoot)
	command(t, "create", "-u", destinationRoot)
	for _, name := range []string{"", "/.cache", "/.local", "/.local/share", "/.local/share/Steam", "/hole", "/hole/deep"} {
		command(t, "create", "-u", root+name)
	}
	command(t, "set", policy.Namespace+"enabled=on", policy.Namespace+"policy=12x5m,24x1h,14x1d",
		policy.Namespace+"local="+destinationRoot, policy.Namespace+"discard=all", policy.Namespace+"incremental=latest", root)
	command(t, "set", policy.Namespace+"enabled=off", root+"/.cache")
	command(t, "set", policy.Namespace+"enabled=off", root+"/.local/share/Steam")
	command(t, "set", policy.Namespace+"enabled=off", root+"/hole")
	command(t, "set", policy.Namespace+"enabled=on", root+"/hole/deep")

	// discard=all keeps only the root's last component. A member mapped by its
	// own name would land at the destination root beside the root's replica.
	mapped := destinationRoot + root[strings.LastIndexByte(root, '/'):]
	members := []string{root, root + "/.local", root + "/.local/share", root + "/hole/deep"}

	// request resolves the root and its members the way the daemon does, from a
	// discovery generation, so ownership comes from the real rules.
	request := func(t *testing.T) transfer.Request {
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
		if !found || entry.CoveredBy != "" || !slices.Equal(entry.Members, members) {
			t.Fatalf("root %s resolved with members %q, want %q", root, entry.Members, members)
		}
		for _, excluded := range []string{root + "/.cache", root + "/.local/share/Steam", root + "/hole"} {
			if other, _ := generation.Inspect(excluded); other.CoveredBy != root || other.Member {
				t.Fatalf("%s should be excluded from %s: %+v", excluded, root, other)
			}
		}
		built := transfer.Request{Source: root, DestinationRoot: destinationRoot, Policy: entry.Policy}
		for _, name := range entry.Members {
			member, _ := generation.Inspect(name)
			built.Members = append(built.Members, transfer.Member{Dataset: name, Policy: member.Policy})
		}
		return built
	}
	references := func(t *testing.T) map[string]lifecycle.Reference {
		t.Helper()
		state, inspectErr := direct.InspectState(t.Context(), root, false)
		if inspectErr != nil {
			t.Fatal(inspectErr)
		}
		refs, refErr := lifecycle.References(state, root, lineageOf(t, direct, root, installation))
		if refErr != nil {
			t.Fatal(refErr)
		}
		byStream := map[string]lifecycle.Reference{}
		for _, ref := range refs {
			byStream[ref.Dataset] = ref
		}
		return byStream
	}
	now := time.Now().UTC()
	var first lifecycle.Metadata

	chainOK := true
	chain := func(name string, fn func(*testing.T)) {
		if !chainOK {
			t.Run(name, func(t *testing.T) { t.Skip("depends on an earlier phase that failed") })
			return
		}
		chainOK = t.Run(name, fn)
	}

	chain("members-nest-under-the-root", func(t *testing.T) {
		req := request(t)
		var snapErr error
		first, snapErr = service.CreateSnapshot(t.Context(), root, members, now.Add(-time.Hour), req.Policy)
		if snapErr != nil {
			t.Fatal(snapErr)
		}
		result, applyErr := engine.Apply(t.Context(), req, nil)
		if applyErr != nil || !result.Verified || len(result.Streams) != len(members) {
			t.Fatalf("result=%+v err=%v", result, applyErr)
		}
		for _, member := range members {
			if want := mapped + strings.TrimPrefix(member, root) + "@" + first.Name(); !exists(t, want) {
				t.Errorf("missing replica %s", want)
			}
		}
		for _, absent := range []string{mapped + "/.cache", mapped + "/.local/share/Steam", destinationRoot + "/.local", destinationRoot + "/deep"} {
			if exists(t, absent) {
				t.Errorf("%s was received", absent)
			}
		}
		// The hole is a placeholder: present for its member, sent nothing.
		hole, inspectErr := direct.InspectState(t.Context(), mapped+"/hole", false)
		if inspectErr != nil {
			t.Fatal(inspectErr)
		}
		if snapshots := lifecycle.Snapshots(hole, mapped+"/hole"); len(snapshots) != 0 {
			t.Errorf("the hole received snapshots: %v", snapshots)
		}
		// Members carry no state; the root holds one proof per stream.
		for _, member := range members[1:] {
			state, memberErr := direct.InspectState(t.Context(), member, false)
			if memberErr != nil {
				t.Fatal(memberErr)
			}
			for _, property := range state.Properties {
				if property.Dataset == member && property.Source == zfs.SourceLocal && strings.HasPrefix(property.Name, policy.StateNamespace) {
					t.Errorf("member %s gained state %s", member, property.Name)
				}
			}
		}
		if refs := references(t); len(refs) != len(members) {
			t.Fatalf("proofs = %+v, want one per member", refs)
		}
	})

	chain("a-member-advances-alone", func(t *testing.T) {
		req := request(t)
		later, snapErr := service.CreateSnapshot(t.Context(), root, []string{root + "/.local"}, now.Add(-30*time.Minute), req.Policy)
		if snapErr != nil {
			t.Fatal(snapErr)
		}
		result, applyErr := engine.Apply(t.Context(), req, nil)
		if applyErr != nil || !result.Verified {
			t.Fatalf("result=%+v err=%v", result, applyErr)
		}
		if !exists(t, mapped+"/.local@"+later.Name()) {
			t.Fatalf("the member's newer snapshot was not sent")
		}
		refs := references(t)
		if refs[root+"/.local"].Metadata != later || refs[root].Metadata != first || refs[root+"/.local/share"].Metadata != first {
			t.Fatalf("one member's advance disturbed another's proof: %+v", refs)
		}
	})

	chain("reseed-resets-every-member", func(t *testing.T) {
		// The incident: a reseed destroyed the parent's replica recursively
		// but reset only the parent's own state, so each descendant kept
		// pointing at a replica that no longer existed and blocked.
		req := request(t)
		reseed, reseedErr := transfer.NewReseedService(direct, direct, installation)
		if reseedErr != nil {
			t.Fatal(reseedErr)
		}
		preview, reseedErr := reseed.Plan(t.Context(), req)
		if reseedErr != nil || len(preview.References) != len(members) {
			t.Fatalf("reseed preview=%+v err=%v", preview, reseedErr)
		}
		applied, reseedErr := reseed.Apply(t.Context(), req)
		if reseedErr != nil || !applied.Applied {
			t.Fatalf("reseed=%+v err=%v", applied, reseedErr)
		}
		if exists(t, mapped) {
			t.Fatal("reseed retained the root's replica")
		}
		if refs := references(t); len(refs) != 0 {
			t.Fatalf("reseed left member proofs behind: %+v", refs)
		}
		result, applyErr := engine.Apply(t.Context(), request(t), nil)
		if applyErr != nil || !result.Verified || len(result.Streams) != len(members) {
			t.Fatalf("post-reseed transfer=%+v err=%v", result, applyErr)
		}
		for _, stream := range result.Streams {
			if stream.Plan.Mode != "full" {
				t.Errorf("%s re-bootstrapped as %q, want full", stream.Plan.Source, stream.Plan.Mode)
			}
		}
		state, inspectErr := direct.InspectState(t.Context(), root, false)
		if inspectErr != nil {
			t.Fatal(inspectErr)
		}
		bound := false
		for _, property := range state.Properties {
			if property.Dataset == root && strings.HasPrefix(property.Name, policy.StateNamespace+"target:") && strings.Contains(property.Value, `"anchor":"`+mapped+`"`) {
				bound = true
			}
		}
		if !bound {
			t.Fatal("the rebuilt binding was not promoted to the root's replica")
		}
	})
}

// lineageOf reads the root's lineage through the authority check every
// lifecycle operation uses.
func lineageOf(t *testing.T, direct *zfs.Direct, root, installation string) string {
	t.Helper()
	state, err := direct.InspectState(t.Context(), root, false)
	if err != nil {
		t.Fatal(err)
	}
	lineage, err := lifecycle.RootAuthority(state, root, installation)
	if err != nil {
		t.Fatal(err)
	}
	return lineage
}
