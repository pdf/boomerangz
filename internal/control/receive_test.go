package control

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/pdf/boomerangz/internal/config"
	controlrpc "github.com/pdf/boomerangz/internal/control/rpc"
	"github.com/pdf/boomerangz/internal/daemonstate"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// trackingRuntime is a runtime that lists receives, as the daemon's does.
type trackingRuntime struct {
	fakeRuntime
	mu      sync.Mutex
	begun   []daemonstate.Receive
	bytes   []uint64
	ended   int
	changed chan struct{}
}

func (r *trackingRuntime) BeginReceive(receive daemonstate.Receive) daemonstate.ReceiveReporter {
	r.mu.Lock()
	r.begun = append(r.begun, receive)
	r.mu.Unlock()
	r.signal()
	return r
}

func (r *trackingRuntime) Progress(bytes uint64, _ float64) {
	r.mu.Lock()
	r.bytes = append(r.bytes, bytes)
	r.mu.Unlock()
	r.signal()
}

func (r *trackingRuntime) End() {
	r.mu.Lock()
	r.ended++
	r.mu.Unlock()
	r.signal()
}

func (r *trackingRuntime) signal() {
	select {
	case r.changed <- struct{}{}:
	default:
	}
}

// waitFor waits until done holds, failing the test after five seconds.
func (r *trackingRuntime) waitFor(t *testing.T, done func() bool) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		r.mu.Lock()
		ok := done()
		r.mu.Unlock()
		if ok {
			return
		}
		select {
		case <-r.changed:
		case <-deadline:
			t.Fatal("the daemon never saw the expected receive report")
		}
	}
}

func startTracking(t *testing.T) (*trackingRuntime, config.Config) {
	t.Helper()
	dir := t.TempDir()
	cfg := config.Defaults()
	cfg.Paths.SocketPath = filepath.Join(dir, "control.sock")
	cfg.Paths.IdentityDir = filepath.Join(dir, "identity")
	runtime := &trackingRuntime{changed: make(chan struct{}, 1)}
	server, err := StartServer(cfg, runtime, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })
	return runtime, cfg
}

// TestReceiveForwarderListsTheReceiveWithTheDaemon pins the ssh-shell path:
// a forwarded receive reaches the daemon over the control socket as an
// ssh-shell receive with the sender's estimate, its newest count arrives, and
// its end ends it there.
func TestReceiveForwarderListsTheReceiveWithTheDaemon(t *testing.T) {
	t.Parallel()
	runtime, cfg := startTracking(t)
	reporter := NewReceiveForwarder(cfg.Paths.SocketPath).BeginReceive(daemonstate.Receive{Dataset: "tank/backups/data", Sender: "host-a\x1b[2J", TotalBytes: 4096, TotalKnown: true})
	runtime.waitFor(t, func() bool { return len(runtime.begun) == 1 })
	reporter.Progress(1024, 10)
	runtime.waitFor(t, func() bool { return len(runtime.bytes) > 0 && runtime.bytes[len(runtime.bytes)-1] == 1024 })
	reporter.End()
	runtime.waitFor(t, func() bool { return runtime.ended == 1 })
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	want := daemonstate.Receive{Dataset: "tank/backups/data", Sender: "host-a2J", Transport: TransportSSHShell, TotalBytes: 4096, TotalKnown: true}
	if runtime.begun[0] != want {
		t.Fatalf("the daemon listed %+v, want %+v", runtime.begun[0], want)
	}
}

// TestReceiveForwarderWithoutADaemonStaysSilent pins that a host with no
// daemon listening costs the receive nothing: reports go nowhere and End
// returns promptly.
func TestReceiveForwarderWithoutADaemonStaysSilent(t *testing.T) {
	t.Parallel()
	reporter := NewReceiveForwarder(filepath.Join(t.TempDir(), "absent.sock")).BeginReceive(daemonstate.Receive{Dataset: "tank/backups/data"})
	reporter.Progress(1, 1)
	started := time.Now()
	reporter.End()
	if elapsed := time.Since(started); elapsed > 2*receiveDialTimeout {
		t.Fatalf("End took %v without a daemon", elapsed)
	}
}

// TestReportReceiveEndsTheReceiveWhenTheReporterVanishes pins that a receive
// an ssh-shell process reported is no longer listed once its stream breaks,
// as it does when the process dies.
func TestReportReceiveEndsTheReceiveWhenTheReporterVanishes(t *testing.T) {
	t.Parallel()
	runtime, cfg := startTracking(t)
	client, err := DialLocal(t.Context(), cfg.Paths.SocketPath)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	stream, err := client.Control.ReportReceive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&controlrpc.ReportReceiveRequest{Start: &controlrpc.ReceiveStart{Dataset: "tank/backups/data"}}); err != nil {
		t.Fatal(err)
	}
	runtime.waitFor(t, func() bool { return len(runtime.begun) == 1 })
	cancel()
	_ = client.Connection.Close()
	runtime.waitFor(t, func() bool { return runtime.ended == 1 })
}

func TestReportReceiveRefusals(t *testing.T) {
	t.Parallel()
	runtime := &trackingRuntime{changed: make(chan struct{}, 1)}
	tcp := &service{runtime: runtime}
	if err := tcp.ReportReceive(nil); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("a TCP listener answered a receive report with %v", err)
	}
	_, cfg := startTracking(t)
	client, err := DialLocal(t.Context(), cfg.Paths.SocketPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Connection.Close() })
	for name, first := range map[string]*controlrpc.ReportReceiveRequest{
		"no start":    {Bytes: 1},
		"bad dataset": {Start: &controlrpc.ReceiveStart{Dataset: "tank/../etc"}},
	} {
		stream, err := client.Control.ReportReceive(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		_ = stream.Send(first)
		if _, err := stream.CloseAndRecv(); status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: answered %v, want InvalidArgument", name, err)
		}
	}
	untracked := &fakeRuntime{}
	dir := t.TempDir()
	plain := config.Defaults()
	plain.Paths.SocketPath = filepath.Join(dir, "control.sock")
	plain.Paths.IdentityDir = filepath.Join(dir, "identity")
	server, err := StartServer(plain, untracked, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })
	other, err := DialLocal(t.Context(), plain.Paths.SocketPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = other.Connection.Close() })
	stream, err := other.Control.ReportReceive(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	_ = stream.Send(&controlrpc.ReportReceiveRequest{Start: &controlrpc.ReceiveStart{Dataset: "tank/backups/data"}})
	if _, err := stream.CloseAndRecv(); status.Code(err) != codes.Unimplemented {
		t.Fatalf("a runtime that lists no receives answered %v", err)
	}
}
