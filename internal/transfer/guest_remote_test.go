//go:build integration

package transfer

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pdf/boomerangz/internal/lifecycle"
	"github.com/pdf/boomerangz/internal/policy"
	replicationssh "github.com/pdf/boomerangz/internal/replication/ssh"
	"github.com/pdf/boomerangz/internal/testutil/zfstest"
	"github.com/pdf/boomerangz/internal/zfs"
)

func TestGuestSSHTransfer(t *testing.T) {
	runID := os.Getenv("BOOMERANGZ_REMOTE_GUEST_RUN")
	if runID == "" {
		t.Skip("disposable guest only")
	}
	key := os.Getenv("BOOMERANGZ_REMOTE_GUEST_KEY")
	shellPath := os.Getenv("BOOMERANGZ_REMOTE_GUEST_CLI")
	if key == "" || shellPath == "" || !filepath.IsAbs(key) || !filepath.IsAbs(shellPath) {
		t.Fatal("guest SSH key and boomerangz executable paths are required")
	}
	sourcePool, err := zfstest.VerifyGuestPool(t.Context(), runID, zfstest.SourceDisk, "/dev/vdb")
	if err != nil {
		t.Fatal(err)
	}
	destinationPool, err := zfstest.VerifyGuestPool(t.Context(), runID, zfstest.DestinationDisk, "/dev/vdc")
	if err != nil {
		t.Fatal(err)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	sshDirectory := filepath.Join(home, ".ssh")
	if err := os.MkdirAll(sshDirectory, 0700); err != nil {
		t.Fatal(err)
	}
	hostKey, err := exec.CommandContext(t.Context(), "ssh-keyscan", "-t", "ed25519", "127.0.0.1").Output()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sshDirectory, "known_hosts"), hostKey, 0600); err != nil {
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
	source := sourcePool + "/data/payload"
	suffix := time.Now().UTC().Format("150405")
	command := func(args ...string) {
		t.Helper()
		if output, commandErr := exec.CommandContext(t.Context(), "zfs", args...).CombinedOutput(); commandErr != nil {
			t.Fatalf("guest zfs %s: %v: %s", args[0], commandErr, output)
		}
	}
	command("set", policy.Namespace+"enabled=on", policy.Namespace+"remote=home", source)
	rows, err := direct.GetStoredProperties(t.Context(), []string{source})
	if err != nil {
		t.Fatal(err)
	}
	known := map[string]struct{}{"home": {}}
	effective := policy.Resolve(zfs.Dataset{Name: source, Type: zfs.Volume, EncryptionRoot: "-"}, nil, rows, known)
	metadata, err := snapshots.CreateSnapshot(t.Context(), source, false, time.Now().UTC(), effective)
	if err != nil {
		t.Fatal(err)
	}
	user := os.Getenv("USER")
	if user == "" {
		user = "boomerangz"
	}
	run := func(mode, root string) Result {
		t.Helper()
		client, clientErr := replicationssh.New("ssh", replicationssh.Config{Host: "127.0.0.1", Port: 22, User: user, Root: root, IdentityFile: key, ShellPath: shellPath, ConnectTimeout: 5 * time.Second})
		if clientErr != nil {
			t.Fatal(clientErr)
		}
		endpoint, endpointErr := replicationssh.OpenEndpoint(t.Context(), client, "zfs", mode)
		if endpointErr != nil {
			t.Fatal(endpointErr)
		}
		defer func() {
			if closeErr := endpoint.Close(); closeErr != nil {
				t.Error(closeErr)
			}
		}()
		request := Request{Source: source, DestinationRoot: root, Snapshot: source + "@" + metadata.Name(), Policy: effective, Transport: "ssh", RemoteName: "home", CanonicalTarget: client.CanonicalTarget()}
		engine, engineErr := NewRemote(direct, endpoint.Executor, endpoint.Stream, installation)
		if engineErr != nil {
			t.Fatal(engineErr)
		}
		result, applyErr := engine.Apply(t.Context(), request, nil)
		if applyErr != nil || !result.Verified {
			t.Fatalf("%s result=%+v err=%v", mode, result, applyErr)
		}
		return result
	}
	for index, mode := range []string{"direct", "ssh-shell"} {
		root := destinationPool + "/data/remote-" + mode + "-" + suffix + "-" + strconv.Itoa(index)
		result := run(mode, root)
		if result.Plan.TargetBinding.Transport != "ssh" || !strings.HasPrefix(result.Plan.TargetBinding.CanonicalTarget, "ssh://") {
			t.Fatalf("%s target binding=%+v", mode, result.Plan.TargetBinding)
		}
		t.Logf("%s SSH transfer verified: %d bytes", mode, result.Progress.Bytes)
	}

	// Keep a short connection check separate from the data transfer diagnostics.
	probeCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := exec.CommandContext(probeCtx, "ssh", "-T", "-o", "BatchMode=yes", "-i", key, user+"@127.0.0.1", "true").Run(); err != nil {
		t.Fatal(err)
	}
}
