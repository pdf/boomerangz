//go:build integration

package control_test

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pdf/boomerangz/internal/identity"
	"github.com/pdf/boomerangz/internal/lifecycle"
	"github.com/pdf/boomerangz/internal/policy"
	"github.com/pdf/boomerangz/internal/testutil/zfstest"
	"github.com/pdf/boomerangz/internal/transfer"
	"github.com/pdf/boomerangz/internal/zfs"
)

// guestCLI resolves the run's pools and the installed CLI, skipping outside
// the disposable guest.
func guestCLI(t *testing.T) (string, string, string) {
	t.Helper()
	runID := os.Getenv("BOOMERANGZ_CONTROL_GUEST_RUN")
	if runID == "" {
		t.Fatal("BOOMERANGZ_CONTROL_GUEST_RUN is unset: the disposable guest harness did not provide a run ID")
	}
	binary := os.Getenv("BOOMERANGZ_CONTROL_GUEST_CLI")
	if binary == "" {
		t.Fatal("guest boomerangz executable path is required")
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
	return binary, sourcePool, destinationPool
}

// scratchConfig writes a configuration naming only this test's own paths and
// returns it with the identity the CLI will adopt from it. The shared guest
// configuration is not usable here: TestGuestDaemonControl appends to it and
// asserts a reload generation, and its identity directory already belongs to
// another installation.
func scratchConfig(t *testing.T) (string, string, string) {
	t.Helper()
	root := t.TempDir()
	configPath := filepath.Join(root, "config.toml")
	dropInDir := filepath.Join(root, "config.d")
	identityDir := filepath.Join(root, "identity")
	document := fmt.Sprintf("[paths]\ncredentials_dir = %q\nidentity_dir = %q\nsocket_path = %q\n",
		filepath.Join(root, "credentials"), identityDir, filepath.Join(root, "control.sock"))
	if err := os.WriteFile(configPath, []byte(document), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, directory := range []string{filepath.Join(root, "credentials"), identityDir} {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	return configPath, dropInDir, identityDir
}

// datasetStatuses parses the `dataset list` table into dataset to status. The
// table is tab-elastic, so the columns are read as fields rather than by
// offset.
func datasetStatuses(t *testing.T, output string) map[string]string {
	t.Helper()
	statuses := make(map[string]string)
	for line := range strings.SplitSeq(output, "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 || fields[0] == "STATUS" {
			continue
		}
		statuses[fields[1]] = fields[0]
	}
	if len(statuses) == 0 {
		t.Fatalf("dataset list reported no rows:\n%s", output)
	}
	return statuses
}

// TestGuestDatasetCLI covers the dataset subcommands that read real pool
// state: `list`'s status classification, `inspect`'s rendering of property
// provenance the kernel reports, and `reseed` resolving a configured target
// and destroying the replica it names.
//
// The rendering of both `list` and `inspect` is unit-tested over a fake
// reader. What only a pool can establish is that ZFS's own value/source
// columns drive it - a locally activated root reads as active, its replicated
// descendant as covered, and a received activation as provenance rather than
// as activation.
func TestGuestDatasetCLI(t *testing.T) {
	binary, sourcePool, destinationPool := guestCLI(t)
	configPath, dropInDir, identityDir := scratchConfig(t)
	installation, err := identity.LoadOrCreate(identityDir)
	if err != nil {
		t.Fatal(err)
	}
	direct, err := zfs.NewDirect("zfs")
	if err != nil {
		t.Fatal(err)
	}
	stream, err := zfs.NewLocalStream("zfs")
	if err != nil {
		t.Fatal(err)
	}
	engine, err := transfer.NewLocal(direct, stream, installation)
	if err != nil {
		t.Fatal(err)
	}
	snapshots, err := lifecycle.NewService(direct, installation)
	if err != nil {
		t.Fatal(err)
	}

	// Helpers take the running *testing.T so a phase reports its own failures.
	cli := func(t *testing.T, args ...string) string {
		t.Helper()
		args = append(args, "--config", configPath, "--config-dir", dropInDir)
		output, runErr := exec.CommandContext(t.Context(), binary, args...).CombinedOutput()
		if runErr != nil {
			t.Fatalf("boomerangz %s: %v: %s", strings.Join(args, " "), runErr, output)
		}
		return string(output)
	}
	zfsSet := func(t *testing.T, args ...string) {
		t.Helper()
		if output, setErr := exec.CommandContext(t.Context(), "zfs", args...).CombinedOutput(); setErr != nil {
			t.Fatalf("guest zfs %s: %v: %s", args[0], setErr, output)
		}
	}

	suffix := time.Now().UTC().Format("150405.000")
	active := sourcePool + "/data/datasetcli-" + suffix
	child := active + "/child"
	invalid := sourcePool + "/data/datasetcli-invalid-" + suffix
	inactive := sourcePool + "/data/datasetcli-inactive-" + suffix
	destinationRoot := destinationPool + "/data/datasetcli-" + suffix
	for _, dataset := range []string{active, invalid, inactive} {
		zfsSet(t, "create", "-u", dataset)
		zfstest.RegisterCleanup(t, dataset)
	}
	zfstest.RegisterCleanup(t, destinationRoot)
	zfsSet(t, "create", "-u", child)
	// replicate makes the child covered rather than a root of its own; props
	// is what sends the activation property, so the received replica can be
	// distinguished from a locally configured one.
	zfsSet(t, "set", policy.Namespace+"enabled=on", policy.Namespace+"replicate=on",
		policy.Namespace+"props=on", policy.Namespace+"policy=1x1h",
		policy.Namespace+"local="+destinationRoot, active)
	// No remote of this name is configured, so the policy cannot resolve.
	zfsSet(t, "set", policy.Namespace+"enabled=on", policy.Namespace+"remote=absent", invalid)

	t.Run("list", func(t *testing.T) {
		statuses := datasetStatuses(t, cli(t, "dataset", "list"))
		for dataset, want := range map[string]string{
			active:   "active",
			child:    "covered",
			invalid:  "invalid",
			inactive: "inactive",
		} {
			if statuses[dataset] != want {
				t.Errorf("dataset list reported %s as %q, want %q", dataset, statuses[dataset], want)
			}
		}
		if !strings.Contains(cli(t, "dataset", "list"), "dataset inspect <dataset>") {
			t.Error("dataset list did not point at inspect despite reporting an invalid dataset")
		}
		var entries []struct {
			Dataset struct {
				Name string `json:"name"`
			} `json:"dataset"`
			CoveredBy string `json:"covered_by"`
		}
		if err := json.Unmarshal([]byte(cli(t, "dataset", "list", "--json")), &entries); err != nil {
			t.Fatal(err)
		}
		found := false
		for _, entry := range entries {
			if entry.Dataset.Name == child {
				found = entry.CoveredBy == active
			}
		}
		if !found {
			t.Errorf("dataset list --json did not report %s as covered by %s", child, active)
		}
	})

	t.Run("inspect-source", func(t *testing.T) {
		output := cli(t, "dataset", "inspect", active)
		for _, want := range []string{
			"Dataset: " + active,
			"Activation: active",
			"Policy: valid",
			"Encryption root: none",
			"Local destinations: " + destinationRoot,
			"enabled = on (local on " + active + ")",
		} {
			if !strings.Contains(output, want) {
				t.Errorf("dataset inspect is missing %q:\n%s", want, output)
			}
		}
		covered := cli(t, "dataset", "inspect", child)
		if !strings.Contains(covered, "Covered by: "+active) {
			t.Errorf("dataset inspect did not report %s as covered:\n%s", child, covered)
		}
	})

	// Below here each phase consumes the replica the previous one made, so a
	// failure leaves the rest testing nothing.
	chainOK := true
	chain := func(name string, fn func(*testing.T)) {
		if !chainOK {
			t.Run(name, func(t *testing.T) { t.Skip("depends on an earlier phase that failed") })
			return
		}
		chainOK = t.Run(name, fn)
	}

	effective := func(t *testing.T) policy.Effective {
		t.Helper()
		rows, storedErr := direct.GetStoredProperties(t.Context(), []string{active})
		if storedErr != nil {
			t.Fatal(storedErr)
		}
		return policy.Resolve(zfs.Dataset{Name: active, Type: zfs.Filesystem, EncryptionRoot: "-"}, nil, rows, nil)
	}

	// Carried from bootstrap into inspect-received.
	var lineage string

	chain("bootstrap", func(t *testing.T) {
		metadata, err := snapshots.CreateSnapshot(t.Context(), active, true, time.Now().UTC(), effective(t))
		if err != nil {
			t.Fatal(err)
		}
		lineage = metadata.Lineage
		request := transfer.Request{Source: active, DestinationRoot: destinationRoot, Policy: effective(t)}
		result, err := engine.Apply(t.Context(), request, nil)
		if err != nil || !result.Verified {
			t.Fatalf("bootstrap result=%+v err=%v", result, err)
		}
	})

	chain("inspect-received", func(t *testing.T) {
		// The send asked for properties, and the source is activated with a
		// non-default policy naming a destination - so what reaches the
		// replica is the whole of receive isolation in one view. Every public
		// org.boomerangz key is excluded at receive, and what crosses is the
		// state namespace, which is provenance rather than authority.
		output := cli(t, "dataset", "inspect", destinationRoot)
		for _, want := range []string{
			"Activation: inactive\n",
			"enabled = off (default)",
			"policy = " + policy.DefaultGrid + " (default)",
			"Local destinations: none",
			"state:lineage = " + lineage + " (received on " + destinationRoot + ")",
			"state:owner = " + installation + " (received on " + destinationRoot + ")",
		} {
			if !strings.Contains(output, want) {
				t.Errorf("dataset inspect is missing %q:\n%s", want, output)
			}
		}
		// Any public key arriving as a received value would be a receive
		// isolation failure, whatever the CLI then rendered.
		if strings.Contains(output, "(received on") && strings.Contains(output, "enabled = on") {
			t.Errorf("a public property crossed the receive boundary:\n%s", output)
		}
		statuses := datasetStatuses(t, cli(t, "dataset", "list"))
		if statuses[destinationRoot] != "inactive" {
			t.Errorf("dataset list reported the replica as %q, want inactive", statuses[destinationRoot])
		}
	})

	chain("reseed", func(t *testing.T) {
		var preview struct {
			Dataset           string `json:"dataset"`
			DestinationExists bool   `json:"destination_exists"`
			Applied           bool   `json:"applied"`
			Warning           string `json:"warning"`
		}
		output := cli(t, "dataset", "reseed", active, destinationRoot)
		if err := json.Unmarshal([]byte(output), &preview); err != nil {
			t.Fatalf("decode reseed preview: %v: %s", err, output)
		}
		if preview.Dataset != active || !preview.DestinationExists || preview.Applied || preview.Warning == "" {
			t.Fatalf("reseed preview=%+v", preview)
		}
		if _, err := direct.InspectDatasetIdentity(t.Context(), destinationRoot); err != nil {
			t.Fatalf("reseed preview destroyed the replica: %v", err)
		}

		var applied struct {
			Applied bool `json:"applied"`
		}
		output = cli(t, "dataset", "reseed", active, destinationRoot, "--apply")
		if err := json.Unmarshal([]byte(output), &applied); err != nil {
			t.Fatalf("decode reseed result: %v: %s", err, output)
		}
		if !applied.Applied {
			t.Fatalf("reseed --apply did not apply: %s", output)
		}
		if _, err := direct.InspectDatasetIdentity(t.Context(), destinationRoot); err == nil {
			t.Fatal("reseed --apply left the replica in place")
		}
		// The reset has to leave the source sendable again rather than merely
		// destroying the destination.
		result, err := engine.Apply(t.Context(), transfer.Request{Source: active, DestinationRoot: destinationRoot, Policy: effective(t)}, nil)
		if err != nil || !result.Verified || result.Plan.Mode != "full" {
			t.Fatalf("post-reseed transfer=%+v err=%v", result, err)
		}
	})

	t.Run("reseed-refuses-unconfigured-target", func(t *testing.T) {
		args := []string{"dataset", "reseed", active, destinationPool + "/data/never-configured", "--config", configPath, "--config-dir", dropInDir}
		output, runErr := exec.CommandContext(t.Context(), binary, args...).CombinedOutput()
		if runErr == nil {
			t.Fatalf("reseed accepted a target the source policy does not name: %s", output)
		}
		if !strings.Contains(string(output), "exactly one effective local destination or remote") {
			t.Fatalf("reseed refusal does not name the cause: %s", output)
		}
	})
}

// TestGuestIdentityRecoverCLI covers `identity recover` against owner and
// lineage markers a real snapshot wrote, read back through real property
// sources. The command's unit test hands it a fabricated InspectState; what a
// pool adds is that the markers ZFS actually stores satisfy the root
// authority and lineage evidence checks.
func TestGuestIdentityRecoverCLI(t *testing.T) {
	binary, sourcePool, _ := guestCLI(t)
	configPath, dropInDir, identityDir := scratchConfig(t)
	direct, err := zfs.NewDirect("zfs")
	if err != nil {
		t.Fatal(err)
	}
	// The installation that owns the lineage is deliberately not the one the
	// scratch configuration will create: recovery exists for exactly that
	// split, after an identity directory is lost.
	const owner = "bbbbbbbb-cccc-4ddd-8eee-ffffffffffff"
	snapshots, err := lifecycle.NewService(direct, owner)
	if err != nil {
		t.Fatal(err)
	}
	current, err := identity.LoadOrCreate(identityDir)
	if err != nil {
		t.Fatal(err)
	}
	if current == owner {
		t.Fatal("scratch installation collided with the lineage owner")
	}

	root := sourcePool + "/data/recover-" + time.Now().UTC().Format("150405.000")
	if output, createErr := exec.CommandContext(t.Context(), "zfs", "create", "-u", root).CombinedOutput(); createErr != nil {
		t.Fatalf("guest zfs create: %v: %s", createErr, output)
	}
	zfstest.RegisterCleanup(t, root)
	if output, setErr := exec.CommandContext(t.Context(), "zfs", "set",
		policy.Namespace+"enabled=on", policy.Namespace+"policy=1x1h", root).CombinedOutput(); setErr != nil {
		t.Fatalf("guest zfs set: %v: %s", setErr, output)
	}
	rows, err := direct.GetStoredProperties(t.Context(), []string{root})
	if err != nil {
		t.Fatal(err)
	}
	effective := policy.Resolve(zfs.Dataset{Name: root, Type: zfs.Filesystem, EncryptionRoot: "-"}, nil, rows, nil)
	if _, err := snapshots.CreateSnapshot(t.Context(), root, false, time.Now().UTC(), effective); err != nil {
		t.Fatal(err)
	}

	cli := func(t *testing.T, args ...string) string {
		t.Helper()
		args = append(args, "--config", configPath, "--config-dir", dropInDir)
		output, runErr := exec.CommandContext(t.Context(), binary, args...).CombinedOutput()
		if runErr != nil {
			t.Fatalf("boomerangz %s: %v: %s", strings.Join(args, " "), runErr, output)
		}
		return string(output)
	}
	type recovery struct {
		Current   string `json:"current"`
		Recovered string `json:"recovered"`
		Roots     []struct {
			Dataset string `json:"dataset"`
			Owner   string `json:"owner"`
			Lineage string `json:"lineage"`
		} `json:"roots"`
		Applied bool `json:"applied"`
	}
	decode := func(t *testing.T, output string) recovery {
		t.Helper()
		var result recovery
		if err := json.Unmarshal([]byte(output), &result); err != nil {
			t.Fatalf("decode identity recover: %v: %s", err, output)
		}
		return result
	}

	// The owner is named explicitly because the scan covers every dataset in
	// both pools, and any other test's activated root would otherwise make the
	// candidate set ambiguous.
	chainOK := true
	chain := func(name string, fn func(*testing.T)) {
		if !chainOK {
			t.Run(name, func(t *testing.T) { t.Skip("depends on an earlier phase that failed") })
			return
		}
		chainOK = t.Run(name, fn)
	}

	chain("preview", func(t *testing.T) {
		result := decode(t, cli(t, "identity", "recover", "--owner", owner))
		if result.Current != current || result.Recovered != owner || result.Applied {
			t.Fatalf("identity recover preview=%+v", result)
		}
		index := -1
		for i, candidate := range result.Roots {
			if candidate.Dataset == root {
				index = i
			}
		}
		if index < 0 || result.Roots[index].Owner != owner || !lifecycle.ValidID(result.Roots[index].Lineage) {
			t.Fatalf("identity recover did not report %s as evidence: %+v", root, result.Roots)
		}
		if after, readErr := identity.Read(identityDir); readErr != nil || after != current {
			t.Fatalf("preview replaced the installation identity: %s (%v)", after, readErr)
		}
	})

	chain("apply", func(t *testing.T) {
		result := decode(t, cli(t, "identity", "recover", "--owner", owner, "--apply"))
		if !result.Applied || result.Recovered != owner {
			t.Fatalf("identity recover --apply=%+v", result)
		}
		after, readErr := identity.Read(identityDir)
		if readErr != nil || after != owner {
			t.Fatalf("recovered identity=%s (%v)", after, readErr)
		}
		// Proof that the recovered identity is usable authority and not just a
		// file: the lineage this installation now owns accepts a new snapshot.
		recovered, serviceErr := lifecycle.NewService(direct, after)
		if serviceErr != nil {
			t.Fatal(serviceErr)
		}
		if _, err := recovered.CreateSnapshot(t.Context(), root, false, time.Now().UTC().Add(time.Minute), effective); err != nil {
			t.Fatalf("recovered installation could not extend the lineage it adopted: %v", err)
		}
	})

	chain("refuses-repeat", func(t *testing.T) {
		args := []string{"identity", "recover", "--owner", owner, "--config", configPath, "--config-dir", dropInDir}
		output, runErr := exec.CommandContext(t.Context(), binary, args...).CombinedOutput()
		if runErr == nil {
			t.Fatalf("identity recover repeated itself: %s", output)
		}
		if !strings.Contains(string(output), "already matches recovered owner") {
			t.Fatalf("repeat refusal does not name the cause: %s", output)
		}
	})
}
