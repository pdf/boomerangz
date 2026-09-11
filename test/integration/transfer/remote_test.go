//go:build integration

package transfer_test

import (
	"context"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pdf/boomerangz/internal/config"
	"github.com/pdf/boomerangz/internal/control"
	"github.com/pdf/boomerangz/internal/lifecycle"
	"github.com/pdf/boomerangz/internal/policy"
	replicationnative "github.com/pdf/boomerangz/internal/replication/native"
	replicationssh "github.com/pdf/boomerangz/internal/replication/ssh"
	"github.com/pdf/boomerangz/internal/testutil/zfstest"
	"github.com/pdf/boomerangz/internal/transfer"
	"github.com/pdf/boomerangz/internal/zfs"
)

func TestGuestSSHTransfer(t *testing.T) {
	runGuestRemoteTransfers(t, nil)
}

func BenchmarkGuestRemoteTransfer(b *testing.B) {
	runGuestRemoteTransfers(b, b)
}

func runGuestRemoteTransfers(t testing.TB, benchmark *testing.B) {
	runID := os.Getenv("BOOMERANGZ_REMOTE_GUEST_RUN")
	if runID == "" {
		t.Fatal("BOOMERANGZ_REMOTE_GUEST_RUN is unset: the disposable guest harness did not provide a run ID")
	}
	key := os.Getenv("BOOMERANGZ_REMOTE_GUEST_KEY")
	shellPath := os.Getenv("BOOMERANGZ_REMOTE_GUEST_CLI")
	if key == "" || shellPath == "" || !filepath.IsAbs(key) || !filepath.IsAbs(shellPath) {
		t.Fatal("guest SSH key and boomerangz executable paths are required")
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
	// Append: known_hosts is shared with the runner, which seeds entries for
	// ports this test does not use. Truncating it here made the daemon stage
	// depend on running after this one.
	knownHosts, err := os.OpenFile(filepath.Join(sshDirectory, "known_hosts"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := knownHosts.Write(hostKey); err != nil {
		t.Fatal(err)
	}
	if err := knownHosts.Close(); err != nil {
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
	source := zfstest.PayloadVolume(t, zfstest.FixtureName(sourcePool, "remote"), 256, 32)
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
	directUser := os.Getenv("BOOMERANGZ_REMOTE_DIRECT_SSH_USER")
	if directUser == "" {
		t.Fatal("direct SSH user is required")
	}
	restrictedUser := os.Getenv("BOOMERANGZ_REMOTE_GUEST_USER")
	if restrictedUser == "" {
		restrictedUser = directUser
	}
	runSSH := func(t testing.TB, mode, root string) transfer.Result {
		t.Helper()
		user := directUser
		if mode == "ssh-shell" {
			user = restrictedUser
		}
		client, clientErr := replicationssh.New("ssh", replicationssh.Config{Host: "127.0.0.1", Port: 22, User: user, Root: root, IdentityFile: key, ShellPath: shellPath, ConnectTimeout: 5 * time.Second})
		if clientErr != nil {
			t.Fatal(clientErr)
		}
		endpoint, endpointErr := replicationssh.OpenEndpoint(t.Context(), client, "zfs", mode)
		if endpointErr != nil {
			t.Fatal(endpointErr)
		}
		request := transfer.Request{Source: source, DestinationRoot: root, Snapshot: source + "@" + metadata.Name(), Policy: effective, Transport: "ssh", RemoteName: "home", CanonicalTarget: client.CanonicalTarget()}
		engine, engineErr := transfer.NewRemote(direct, endpoint.Executor, endpoint.Stream, installation)
		if engineErr != nil {
			t.Fatal(engineErr)
		}
		result, applyErr := engine.Apply(t.Context(), request, nil)
		if applyErr != nil || !result.Verified {
			t.Fatalf("%s result=%+v err=%v", mode, result, applyErr)
		}
		if closeErr := endpoint.Close(); closeErr != nil {
			t.Fatal(closeErr)
		}
		return result
	}
	for _, mode := range []string{"direct", "ssh-shell"} {
		if benchmark != nil {
			benchmark.Run(mode, func(b *testing.B) {
				benchmarkSuffix := strconv.FormatInt(time.Now().UTC().UnixNano(), 10)
				b.ResetTimer()
				for index := range b.N {
					root := destinationPool + "/data/benchmark-" + mode + "-" + benchmarkSuffix + "-" + strconv.Itoa(index)
					result := runSSH(b, mode, root)
					b.SetBytes(int64(result.Progress.Bytes))
				}
				b.StopTimer()
			})
			continue
		}
		root := destinationPool + "/data/remote-" + mode + "-" + suffix
		result := runSSH(t, mode, root)
		if result.Plan.TargetBinding.Transport != "ssh" || !strings.HasPrefix(result.Plan.TargetBinding.CanonicalTarget, "ssh://") {
			t.Fatalf("%s target binding=%+v", mode, result.Plan.TargetBinding)
		}
	}

	// Takes the TB to report against: under benchmark.Run the failures below
	// belong to the sub-benchmark, not to the parent that built the closure.
	runNative := func(tb testing.TB, roots []string, execute func(int, func(string) transfer.Result)) {
		tb.Helper()
		pkiDir := tb.TempDir()
		nativeConfig := config.Defaults()
		nativeConfig.Paths.SocketPath = filepath.Join(pkiDir, "control.sock")
		nativeConfig.Paths.IdentityDir = filepath.Join(pkiDir, "identity")
		listenerConfig := config.ListenerConfig{Network: "tcp", Address: "127.0.0.1:0", AdvertisedAddress: "localhost:7443", AuthMode: "token", ReplicationRoots: roots}
		nativeConfig.Listeners["replication"] = listenerConfig
		server, startErr := control.StartServerWithReplication(nativeConfig, nil, direct, "zfs", nil)
		if startErr != nil {
			tb.Fatal(startErr)
		}
		defer func() { _ = server.Close() }()
		addresses := server.Addresses("tcp")
		if len(addresses) != 1 {
			tb.Fatalf("native listener addresses=%v", addresses)
		}
		_, port, splitErr := net.SplitHostPort(addresses[0])
		if splitErr != nil {
			tb.Fatal(splitErr)
		}
		listenerConfig.AdvertisedAddress = net.JoinHostPort("localhost", port)
		store, storeErr := control.NewTokenStore(nativeConfig.Paths.IdentityDir)
		if storeErr != nil {
			tb.Fatal(storeErr)
		}
		bundle, pairingErr := control.CreateListenerPairing(store, nativeConfig.Paths.IdentityDir, "replication", listenerConfig, "", "", []string{"replicate"}, nil)
		if pairingErr != nil {
			tb.Fatal(pairingErr)
		}
		transferRoot := func(root string) transfer.Result {
			nativeEndpoint, openErr := replicationnative.Open(tb.Context(), bundle, root, "zfs")
			if openErr != nil {
				tb.Fatal(openErr)
			}
			nativeRequest := transfer.Request{Source: source, DestinationRoot: root, Snapshot: source + "@" + metadata.Name(), Policy: effective, Transport: "native", RemoteName: "home", CanonicalTarget: nativeEndpoint.CanonicalTarget()}
			nativeEngine, engineErr := transfer.NewRemote(direct, nativeEndpoint.Executor(), nativeEndpoint.Stream(), installation)
			if engineErr != nil {
				tb.Fatal(engineErr)
			}
			nativeResult, applyErr := nativeEngine.Apply(tb.Context(), nativeRequest, nil)
			if applyErr != nil || !nativeResult.Verified || nativeResult.Plan.TargetBinding.Transport != "native" {
				tb.Fatalf("native result=%+v err=%v", nativeResult, applyErr)
			}
			if closeErr := nativeEndpoint.Close(); closeErr != nil {
				tb.Fatal(closeErr)
			}
			return nativeResult
		}
		execute(len(roots), transferRoot)
	}
	if benchmark != nil {
		benchmark.Run("native", func(b *testing.B) {
			benchmarkSuffix := strconv.FormatInt(time.Now().UTC().UnixNano(), 10)
			roots := make([]string, b.N)
			for index := range b.N {
				roots[index] = destinationPool + "/data/benchmark-native-" + benchmarkSuffix + "-" + strconv.Itoa(index)
			}
			runNative(b, roots, func(count int, transferRoot func(string) transfer.Result) {
				b.ResetTimer()
				for index := range count {
					result := transferRoot(roots[index])
					b.SetBytes(int64(result.Progress.Bytes))
				}
				b.StopTimer()
			})
		})
		return
	}
	nativeRoot := destinationPool + "/data/remote-native-" + suffix
	runNative(t, []string{nativeRoot}, func(_ int, transferRoot func(string) transfer.Result) {
		transferRoot(nativeRoot)
	})

	// Keep a short connection check separate from the data transfer diagnostics.
	probeCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := exec.CommandContext(probeCtx, "ssh", "-T", "-o", "BatchMode=yes", "-i", key, directUser+"@127.0.0.1", "true").Run(); err != nil {
		t.Fatal(err)
	}
}
