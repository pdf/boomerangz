//go:build integration

package transfer_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pdf/boomerangz/internal/lifecycle"
	"github.com/pdf/boomerangz/internal/policy"
	replicationssh "github.com/pdf/boomerangz/internal/replication/ssh"
	"github.com/pdf/boomerangz/internal/testutil/zfstest"
	"github.com/pdf/boomerangz/internal/transfer"
	"github.com/pdf/boomerangz/internal/zfs"
)

// TestGuestAccessControlRefusals covers the refusals that only a real
// endpoint and a real delegation table can produce: the ssh-shell endpoint
// bounding every operation and every receive to its configured replication
// roots, and the permission preflight reporting a missing `zfs allow` grant in
// terms an operator can act on.
//
// The guest's /etc/boomerangz/config.toml sets ssh_shell.replication_roots to
// the destination pool's delegated container, so anything elsewhere in either
// pool is outside the set by construction.
func TestGuestAccessControlRefusals(t *testing.T) {
	runID := os.Getenv("BOOMERANGZ_REMOTE_GUEST_RUN")
	if runID == "" {
		t.Skip("disposable guest only")
	}
	key := os.Getenv("BOOMERANGZ_REMOTE_GUEST_KEY")
	shellPath := os.Getenv("BOOMERANGZ_REMOTE_GUEST_CLI")
	shellUser := os.Getenv("BOOMERANGZ_REMOTE_GUEST_USER")
	if key == "" || shellPath == "" || shellUser == "" || !filepath.IsAbs(key) || !filepath.IsAbs(shellPath) {
		t.Fatal("guest SSH key, boomerangz executable path, and shell user are required")
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
	const installation = "abcdefab-cdef-4abc-8def-abcdefabcdef"
	snapshots, err := lifecycle.NewService(direct, installation)
	if err != nil {
		t.Fatal(err)
	}
	// This root sits beside the destination pool's delegated container, so it
	// is outside ssh_shell.replication_roots and outside every grant the
	// service account holds.
	outsideRoot := destinationPool + "/access-outside-" + time.Now().UTC().Format("150405.000")
	zfstest.RegisterCleanup(t, outsideRoot)
	source := zfstest.PayloadVolume(t, zfstest.FixtureName(sourcePool, "access"), 64, 4)
	// The local target is configured on the source because the planner refuses
	// a destination the source's own properties do not name, which would
	// otherwise stop the permission preflight from ever running.
	if output, setErr := exec.CommandContext(t.Context(), "zfs", "set",
		policy.Namespace+"enabled=on", policy.Namespace+"remote=home",
		policy.Namespace+"local="+outsideRoot, source).CombinedOutput(); setErr != nil {
		t.Fatalf("guest zfs set: %v: %s", setErr, output)
	}
	rows, err := direct.GetStoredProperties(t.Context(), []string{source})
	if err != nil {
		t.Fatal(err)
	}
	effective := policy.Resolve(zfs.Dataset{Name: source, Type: zfs.Volume, EncryptionRoot: "-"}, nil, rows, map[string]struct{}{"home": {}})
	metadata, err := snapshots.CreateSnapshot(t.Context(), source, false, time.Now().UTC(), effective)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := source + "@" + metadata.Name()

	// Takes the running *testing.T so each phase owns its endpoint and reports
	// its own failures.
	openShell := func(t *testing.T, root string) *replicationssh.Endpoint {
		t.Helper()
		client, clientErr := replicationssh.New("ssh", replicationssh.Config{Host: "127.0.0.1", Port: 22, User: shellUser, Root: root, IdentityFile: key, ShellPath: shellPath, ConnectTimeout: 5 * time.Second})
		if clientErr != nil {
			t.Fatal(clientErr)
		}
		endpoint, endpointErr := replicationssh.OpenEndpoint(t.Context(), client, "zfs", "ssh-shell")
		if endpointErr != nil {
			t.Fatal(endpointErr)
		}
		t.Cleanup(func() { _ = endpoint.Close() })
		return endpoint
	}

	t.Run("operation-outside-replication-roots", func(t *testing.T) {
		endpoint := openShell(t, outsideRoot)
		// Ancestors of an allowed root are visible for hierarchy inspection
		// but are not themselves receivable, so state inspection on the pool
		// must still be refused.
		if _, err := endpoint.Executor.InspectState(t.Context(), destinationPool, false); err == nil {
			t.Fatal("ssh-shell inspected state above its configured replication roots")
		} else if !strings.Contains(err.Error(), "scope") {
			t.Fatalf("refusal does not name the destination scope: %v", err)
		}
		// A dataset in the other pool is unrelated to every allowed root.
		if _, err := endpoint.Executor.InspectDatasetIdentity(t.Context(), source); err == nil {
			t.Fatal("ssh-shell resolved a dataset outside its configured replication roots")
		} else if !strings.Contains(err.Error(), "scope") {
			t.Fatalf("refusal does not name the destination scope: %v", err)
		}
		if err := endpoint.Executor.SetProperties(t.Context(), outsideRoot, map[string]string{"readonly": "on"}); err == nil {
			t.Fatal("ssh-shell wrote properties outside its configured replication roots")
		} else if !strings.Contains(err.Error(), "scope") {
			t.Fatalf("refusal does not name the destination scope: %v", err)
		}
	})

	t.Run("receive-root-outside-replication-roots", func(t *testing.T) {
		endpoint := openShell(t, outsideRoot)
		send := zfs.SendOptions{Source: source, Snapshot: snapshot}
		receive := zfs.ReceiveOptions{Root: outsideRoot, Discard: zfs.ReceiveDiscard(effective.Discard)}
		if _, err := endpoint.Stream.Run(t.Context(), send, receive, zfs.Estimate{}, nil); err == nil {
			t.Fatal("ssh-shell received a stream outside its configured replication roots")
		} else if !strings.Contains(err.Error(), "scope") {
			t.Fatalf("receive refusal does not name the destination scope: %v", err)
		}
		// Nothing may have been created on the way to the refusal.
		if _, err := direct.InspectDatasetIdentity(t.Context(), outsideRoot); err == nil {
			t.Fatalf("refused receive still created %s", outsideRoot)
		}
	})

	t.Run("missing-delegation", func(t *testing.T) {
		stream, streamErr := zfs.NewLocalStream("zfs")
		if streamErr != nil {
			t.Fatal(streamErr)
		}
		engine, engineErr := transfer.NewLocal(direct, stream, installation)
		if engineErr != nil {
			t.Fatal(engineErr)
		}
		// The service account holds destination grants on the pool's delegated
		// container only, so a root beside it anchors the preflight on the
		// pool, where it holds nothing.
		request := transfer.Request{Source: source, DestinationRoot: outsideRoot, Snapshot: snapshot, Policy: effective}
		_, err := engine.Preview(t.Context(), request)
		if err == nil {
			t.Fatal("undelegated destination anchor passed the permission preflight")
		}
		for _, expected := range []string{"destination permission preflight", "lacks delegated ZFS permissions", destinationPool} {
			if !strings.Contains(err.Error(), expected) {
				t.Fatalf("permission refusal does not report %q: %v", expected, err)
			}
		}
		// The source side is delegated, so the refusal must not be reported
		// against it.
		if strings.Contains(err.Error(), "source permission preflight") {
			t.Fatalf("source permission preflight failed unexpectedly: %v", err)
		}
	})
}
