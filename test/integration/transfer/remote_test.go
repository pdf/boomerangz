//go:build integration

package transfer_test

import (
	"context"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
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
	runID := os.Getenv("BOOMERANGZ_REMOTE_GUEST_RUN")
	if runID == "" {
		t.Skip("disposable guest only")
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
	directUser := os.Getenv("BOOMERANGZ_REMOTE_DIRECT_SSH_USER")
	if directUser == "" {
		t.Fatal("direct SSH user is required")
	}
	restrictedUser := os.Getenv("BOOMERANGZ_REMOTE_GUEST_USER")
	if restrictedUser == "" {
		restrictedUser = directUser
	}
	type measurement struct {
		open  time.Duration
		apply time.Duration
		total time.Duration
		bytes uint64
	}
	const sampleCount = 3
	run := func(mode, root string) (transfer.Result, measurement) {
		t.Helper()
		totalStarted := time.Now()
		user := directUser
		if mode == "ssh-shell" {
			user = restrictedUser
		}
		client, clientErr := replicationssh.New("ssh", replicationssh.Config{Host: "127.0.0.1", Port: 22, User: user, Root: root, IdentityFile: key, ShellPath: shellPath, ConnectTimeout: 5 * time.Second})
		if clientErr != nil {
			t.Fatal(clientErr)
		}
		openStarted := time.Now()
		endpoint, endpointErr := replicationssh.OpenEndpoint(t.Context(), client, "zfs", mode)
		openElapsed := time.Since(openStarted)
		if endpointErr != nil {
			t.Fatal(endpointErr)
		}
		request := transfer.Request{Source: source, DestinationRoot: root, Snapshot: source + "@" + metadata.Name(), Policy: effective, Transport: "ssh", RemoteName: "home", CanonicalTarget: client.CanonicalTarget()}
		engine, engineErr := transfer.NewRemote(direct, endpoint.Executor, endpoint.Stream, installation)
		if engineErr != nil {
			t.Fatal(engineErr)
		}
		started := time.Now()
		result, applyErr := engine.Apply(t.Context(), request, nil)
		applyElapsed := time.Since(started)
		if applyErr != nil || !result.Verified {
			t.Fatalf("%s result=%+v err=%v", mode, result, applyErr)
		}
		if closeErr := endpoint.Close(); closeErr != nil {
			t.Fatal(closeErr)
		}
		return result, measurement{open: openElapsed, apply: applyElapsed, total: time.Since(totalStarted), bytes: result.Progress.Bytes}
	}
	report := func(name string, samples []measurement) {
		t.Helper()
		sort.Slice(samples, func(i, j int) bool { return samples[i].total < samples[j].total })
		median := samples[len(samples)/2]
		t.Logf("%s: %d samples, median total=%s (open=%s apply=%s, %.2f MiB/s), range=%s..%s", name, len(samples), median.total, median.open, median.apply, float64(median.bytes)/(1024*1024)/median.total.Seconds(), samples[0].total, samples[len(samples)-1].total)
	}
	for _, mode := range []string{"direct", "ssh-shell"} {
		samples := make([]measurement, 0, sampleCount)
		for index := range sampleCount {
			root := destinationPool + "/data/remote-" + mode + "-" + suffix + "-" + strconv.Itoa(index)
			result, measured := run(mode, root)
			if result.Plan.TargetBinding.Transport != "ssh" || !strings.HasPrefix(result.Plan.TargetBinding.CanonicalTarget, "ssh://") {
				t.Fatalf("%s target binding=%+v", mode, result.Plan.TargetBinding)
			}
			samples = append(samples, measured)
		}
		report(mode+" SSH", samples)
	}

	nativeRoots := make([]string, sampleCount)
	for index := range sampleCount {
		nativeRoots[index] = destinationPool + "/data/remote-native-" + suffix + "-" + strconv.Itoa(index)
	}
	pkiDir := t.TempDir()
	nativeConfig := config.Defaults()
	nativeConfig.Paths.SocketPath = filepath.Join(pkiDir, "control.sock")
	nativeConfig.Paths.IdentityDir = filepath.Join(pkiDir, "identity")
	listenerConfig := config.ListenerConfig{Network: "tcp", Address: "127.0.0.1:0", AdvertisedAddress: "localhost:7443", AuthMode: "token", ReplicationRoots: nativeRoots}
	nativeConfig.Listeners["replication"] = listenerConfig
	server, err := control.StartServerWithReplication(nativeConfig, nil, direct, "zfs", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = server.Close() }()
	addresses := server.Addresses("tcp")
	if len(addresses) != 1 {
		t.Fatalf("native listener addresses=%v", addresses)
	}
	_, port, err := net.SplitHostPort(addresses[0])
	if err != nil {
		t.Fatal(err)
	}
	listenerConfig.AdvertisedAddress = net.JoinHostPort("localhost", port)
	store, err := control.NewTokenStore(nativeConfig.Paths.IdentityDir)
	if err != nil {
		t.Fatal(err)
	}
	bundle, err := control.CreateListenerPairing(store, nativeConfig.Paths.IdentityDir, "replication", listenerConfig, "", "", []string{"replicate"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	nativeSamples := make([]measurement, 0, sampleCount)
	for _, nativeRoot := range nativeRoots {
		totalStarted := time.Now()
		openStarted := time.Now()
		nativeEndpoint, openErr := replicationnative.Open(t.Context(), bundle, nativeRoot, "zfs")
		openElapsed := time.Since(openStarted)
		if openErr != nil {
			t.Fatal(openErr)
		}
		nativeRequest := transfer.Request{Source: source, DestinationRoot: nativeRoot, Snapshot: source + "@" + metadata.Name(), Policy: effective, Transport: "native", RemoteName: "home", CanonicalTarget: nativeEndpoint.CanonicalTarget()}
		nativeEngine, engineErr := transfer.NewRemote(direct, nativeEndpoint.Executor(), nativeEndpoint.Stream(), installation)
		if engineErr != nil {
			t.Fatal(engineErr)
		}
		started := time.Now()
		nativeResult, applyErr := nativeEngine.Apply(t.Context(), nativeRequest, nil)
		applyElapsed := time.Since(started)
		if applyErr != nil || !nativeResult.Verified || nativeResult.Plan.TargetBinding.Transport != "native" {
			t.Fatalf("native result=%+v err=%v", nativeResult, applyErr)
		}
		if closeErr := nativeEndpoint.Close(); closeErr != nil {
			t.Fatal(closeErr)
		}
		nativeSamples = append(nativeSamples, measurement{open: openElapsed, apply: applyElapsed, total: time.Since(totalStarted), bytes: nativeResult.Progress.Bytes})
	}
	report("native TLS gRPC", nativeSamples)

	// Keep a short connection check separate from the data transfer diagnostics.
	probeCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := exec.CommandContext(probeCtx, "ssh", "-T", "-o", "BatchMode=yes", "-i", key, directUser+"@127.0.0.1", "true").Run(); err != nil {
		t.Fatal(err)
	}
}
