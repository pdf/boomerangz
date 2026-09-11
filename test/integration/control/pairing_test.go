//go:build integration

package control_test

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/pdf/boomerangz/internal/config"
	"github.com/pdf/boomerangz/internal/control"
	"github.com/pdf/boomerangz/internal/lifecycle"
	"github.com/pdf/boomerangz/internal/policy"
	replicationnative "github.com/pdf/boomerangz/internal/replication/native"
	"github.com/pdf/boomerangz/internal/testutil/zfstest"
	"github.com/pdf/boomerangz/internal/transfer"
	"github.com/pdf/boomerangz/internal/zfs"
)

const pairingInstallation = "abcdefab-cdef-4abc-8def-abcdefabcdef"

// pairingView mirrors the `pairing list` rendering, which is the only place
// issued and imported records are reported together.
type pairingView struct {
	Role     string `json:"role"`
	Name     string `json:"name,omitempty"`
	ID       string `json:"id"`
	Listener string `json:"listener,omitempty"`
	Endpoint string `json:"endpoint,omitempty"`
	AuthMode string `json:"auth_mode"`
	Revoked  bool   `json:"revoked,omitempty"`
}

// TestGuestPairingLifecycle drives `pairing create`, `import`, `list` and
// `revoke` over the CLI against a real listener, and proves each step by what
// the listener then does: the imported credential replicates, and the same
// credential is refused once revoked.
func TestGuestPairingLifecycle(t *testing.T) {
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
	direct, err := zfs.NewDirect("zfs")
	if err != nil {
		t.Fatal(err)
	}

	// A configuration of this test's own: the pairing CLI reads paths and the
	// listener definition from it, and the shared guest configuration carries
	// neither a TCP listener nor a scratch identity directory.
	root := t.TempDir()
	configPath := filepath.Join(root, "config.toml")
	dropInDir := filepath.Join(root, "config.d")
	destinationRoot := zfstest.FixtureName(destinationPool, "pairing")
	zfstest.RegisterCleanup(t, destinationRoot)
	port := reservePort(t)
	configText := fmt.Sprintf(`[paths]
credentials_dir = %q
identity_dir = %q
socket_path = %q

[listeners.replication]
network = "tcp"
address = "127.0.0.1:%s"
advertised_address = "localhost:%s"
auth_mode = "token"
replication_roots = [%q]
`, filepath.Join(root, "credentials"), filepath.Join(root, "identity"),
		filepath.Join(root, "control.sock"), port, port, destinationRoot)
	if err := os.WriteFile(configPath, []byte(configText), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "credentials"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "identity"), 0o700); err != nil {
		t.Fatal(err)
	}
	// Takes the running *testing.T so a phase reports its own failures.
	cli := func(t *testing.T, args ...string) string {
		t.Helper()
		args = append(args, "--config", configPath, "--config-dir", dropInDir)
		output, runErr := exec.CommandContext(t.Context(), binary, args...).CombinedOutput()
		if runErr != nil {
			t.Fatalf("boomerangz %s: %v: %s", strings.Join(args, " "), runErr, output)
		}
		return string(output)
	}
	list := func(t *testing.T) []pairingView {
		t.Helper()
		var views []pairingView
		if err := json.Unmarshal([]byte(cli(t, "pairing", "list")), &views); err != nil {
			t.Fatal(err)
		}
		return views
	}

	loaded, err := config.Load(configPath, dropInDir)
	if err != nil {
		t.Fatal(err)
	}
	server, err := control.StartServerWithReplication(loaded.Config, nil, direct, "zfs", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })

	source := zfstest.PayloadVolume(t, zfstest.FixtureName(sourcePool, "pairing"), 128, 8)
	if output, err := exec.CommandContext(t.Context(), "zfs", "set",
		policy.Namespace+"enabled=on", policy.Namespace+"remote=backup", source).CombinedOutput(); err != nil {
		t.Fatalf("guest zfs set: %v: %s", err, output)
	}
	rows, err := direct.GetStoredProperties(t.Context(), []string{source})
	if err != nil {
		t.Fatal(err)
	}
	effective := policy.Resolve(zfs.Dataset{Name: source, Type: zfs.Volume, EncryptionRoot: "-"}, nil, rows, map[string]struct{}{"backup": {}})
	snapshots, err := lifecycle.NewService(direct, pairingInstallation)
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := snapshots.CreateSnapshot(t.Context(), source, false, time.Now().UTC(), effective)
	if err != nil {
		t.Fatal(err)
	}

	// State handed between phases.
	var (
		pairingID  string
		bundlePath = filepath.Join(root, "bundle.json")
	)
	chainOK := true
	chain := func(name string, fn func(*testing.T)) {
		if !chainOK {
			t.Run(name, func(t *testing.T) { t.Skip("depends on an earlier phase that failed") })
			return
		}
		chainOK = t.Run(name, fn)
	}

	chain("create", func(t *testing.T) {
		output := cli(t, "pairing", "create", "--listener", "replication", "--scope", "replicate")
		var bundle control.PairingBundle
		if err := json.Unmarshal([]byte(output), &bundle); err != nil {
			t.Fatalf("decode pairing bundle: %v: %s", err, output)
		}
		if bundle.PairingID == "" || bundle.TokenID == "" || bundle.Secret == "" {
			t.Fatalf("pairing bundle carries no token credential: %+v", bundle)
		}
		if bundle.Endpoint != "localhost:"+port || !slices.Contains(bundle.Scopes, "replicate") {
			t.Fatalf("pairing bundle endpoint=%q scopes=%v", bundle.Endpoint, bundle.Scopes)
		}
		pairingID = bundle.PairingID
		if err := os.WriteFile(bundlePath, []byte(output), 0o600); err != nil {
			t.Fatal(err)
		}
		issued := slices.IndexFunc(list(t), func(view pairingView) bool { return view.ID == pairingID })
		if issued < 0 {
			t.Fatalf("pairing list did not report the issued pairing %s", pairingID)
		}
	})

	chain("import", func(t *testing.T) {
		stored := strings.TrimSpace(cli(t, "pairing", "import", "backup", bundlePath))
		if stored != filepath.Join(root, "credentials", "backup.json") {
			t.Fatalf("pairing import stored %q", stored)
		}
		views := list(t)
		index := slices.IndexFunc(views, func(view pairingView) bool { return view.Role == "imported" })
		if index < 0 {
			t.Fatalf("pairing list did not report the imported credential: %+v", views)
		}
		if views[index].Name != "backup" || views[index].ID != pairingID || views[index].Endpoint != "localhost:"+port {
			t.Fatalf("imported pairing view=%+v", views[index])
		}
	})

	chain("replicate", func(t *testing.T) {
		bundle, err := control.LoadPairingBundle(filepath.Join(root, "credentials", "backup.json"))
		if err != nil {
			t.Fatal(err)
		}
		endpoint, err := replicationnative.Open(t.Context(), bundle, destinationRoot, "zfs")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = endpoint.Close() }()
		engine, err := transfer.NewRemote(direct, endpoint.Executor(), endpoint.Stream(), pairingInstallation)
		if err != nil {
			t.Fatal(err)
		}
		request := transfer.Request{Source: source, DestinationRoot: destinationRoot, Snapshot: source + "@" + metadata.Name(), Policy: effective, Transport: "native", RemoteName: "backup", CanonicalTarget: endpoint.CanonicalTarget()}
		result, err := engine.Apply(t.Context(), request, nil)
		if err != nil || !result.Verified {
			t.Fatalf("imported credential did not replicate: result=%+v err=%v", result, err)
		}
		// Read the destination with the local executor rather than the
		// transport's: the guest is both hosts, so this is the pool the
		// listener actually wrote.
		if _, err := direct.InspectDatasetIdentity(t.Context(), destinationRoot); err != nil {
			t.Fatalf("destination was not created through the listener: %v", err)
		}
	})

	chain("revoke", func(t *testing.T) {
		output := cli(t, "pairing", "revoke", pairingID)
		if !strings.Contains(output, pairingID) {
			t.Fatalf("pairing revoke output=%q", output)
		}
		views := list(t)
		index := slices.IndexFunc(views, func(view pairingView) bool { return view.Role == "issued" && view.ID == pairingID })
		if index < 0 || !views[index].Revoked {
			t.Fatalf("pairing list did not report %s as revoked: %+v", pairingID, views)
		}
		bundle, err := control.LoadPairingBundle(filepath.Join(root, "credentials", "backup.json"))
		if err != nil {
			t.Fatal(err)
		}
		endpoint, openErr := replicationnative.Open(t.Context(), bundle, destinationRoot, "zfs")
		if openErr == nil {
			_ = endpoint.Close()
			t.Fatal("revoked credential was still accepted by the listener")
		}
		// A revoked credential must be final: classifying it as a transport
		// outage would put the target into an unbounded retry instead of
		// surfacing it.
		if replicationnative.IsUnavailable(openErr) {
			t.Fatalf("revoked credential was reported as a temporary outage: %v", openErr)
		}
		if !strings.Contains(openErr.Error(), "scope") && !strings.Contains(openErr.Error(), "token") {
			t.Fatalf("revocation error does not name the credential problem: %v", openErr)
		}
	})
}

// reservePort returns a loopback port that is free right now. The listener
// address has to appear in the configuration file before anything binds it.
func reservePort(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	_, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return port
}
