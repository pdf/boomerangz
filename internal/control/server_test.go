package control

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pdf/boomerangz/internal/config"
	controlrpc "github.com/pdf/boomerangz/internal/control/rpc"
	"github.com/pdf/boomerangz/internal/daemonstate"
	"github.com/pdf/boomerangz/internal/lifecycle"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// heldRuntime holds each Clean call until it is released or its context ends,
// and reports the context of the call it is holding.
type heldRuntime struct {
	*fakeRuntime
	entered chan context.Context
	release chan struct{}
}

func newHeldRuntime() *heldRuntime {
	return &heldRuntime{fakeRuntime: &fakeRuntime{}, entered: make(chan context.Context, 1), release: make(chan struct{})}
}

func (r *heldRuntime) Clean(ctx context.Context, names []string, recursive, all, destroy, apply bool) ([]lifecycle.CleanPlan, error) {
	r.entered <- ctx
	select {
	case <-r.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return r.fakeRuntime.Clean(ctx, names, recursive, all, destroy, apply)
}

// startHeldCall starts a Clean call through client and returns its outcome
// channel once the server is holding it, along with the handler's context.
func startHeldCall(t *testing.T, client *Client, runtime *heldRuntime) (<-chan error, context.Context) {
	t.Helper()
	result := make(chan error, 1)
	go func() {
		_, err := client.Control.Clean(context.Background(), &controlrpc.CleanRequest{Datasets: []string{"tank/data"}})
		result <- err
	}()
	select {
	case ctx := <-runtime.entered:
		return result, ctx
	case err := <-result:
		t.Fatalf("held call returned before the server held it: %v", err)
	case <-t.Context().Done():
		t.Fatal(t.Context().Err())
	}
	return nil, nil
}

// awaitCall bounds a wait on a call the test expects to have finished; the
// bound only turns a hang into a failure.
func awaitCall(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("call did not finish")
	}
	return nil
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func movedSocketServer(t *testing.T, runtime runtime, logger *slog.Logger) (*Server, config.Config, config.Config) {
	t.Helper()
	dir := t.TempDir()
	cfg := config.Defaults()
	cfg.Paths.SocketPath = filepath.Join(dir, "before.sock")
	cfg.Paths.IdentityDir = filepath.Join(dir, "identity")
	server, err := StartServer(cfg, runtime, logger)
	if err != nil {
		t.Fatal(err)
	}
	next := cfg.Clone()
	next.Paths.SocketPath = filepath.Join(dir, "after.sock")
	return server, cfg, next
}

func TestServerReloadCompletesCallInFlightOnRetiredListener(t *testing.T) {
	t.Parallel()
	runtime := newHeldRuntime()
	server, cfg, next := movedSocketServer(t, runtime, nil)
	defer func() { _ = server.Close() }()
	server.drainBound = time.Hour
	client, err := DialLocal(t.Context(), cfg.Paths.SocketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Connection.Close() }()
	result, _ := startHeldCall(t, client, runtime)
	if err := server.Reload(next); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(cfg.Paths.SocketPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("retired socket still present after reload returned: %v", err)
	}
	if conn, err := (&net.Dialer{}).DialContext(t.Context(), "unix", cfg.Paths.SocketPath); err == nil {
		_ = conn.Close()
		t.Fatal("retired listener accepted a connection after reload returned")
	}
	close(runtime.release)
	if err := awaitCall(t, result); err != nil {
		t.Fatalf("call in flight across reload failed: %v", err)
	}
}

func TestServerReloadRPCThatRetiresItsOwnListenerReturns(t *testing.T) {
	t.Parallel()
	server, cfg, next := movedSocketServer(t, &fakeRuntime{}, nil)
	defer func() { _ = server.Close() }()
	server.drainBound = time.Hour
	server.SetReloadHandler(func(context.Context) (daemonstate.ReloadResult, error) {
		return daemonstate.ReloadResult{Generation: 2}, server.Reload(next)
	})
	client, err := DialLocal(t.Context(), cfg.Paths.SocketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Connection.Close() }()
	response, err := client.Control.Reload(t.Context(), &controlrpc.ReloadRequest{})
	if err != nil || response.GetGeneration() != 2 {
		t.Fatalf("reload=%v err=%v", response, err)
	}
	moved, err := DialLocal(t.Context(), next.Paths.SocketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = moved.Connection.Close() }()
	if _, err := moved.Status.GetStatus(t.Context(), &controlrpc.GetStatusRequest{}); err != nil {
		t.Fatal(err)
	}
}

func TestServerCloseStopsCallDrainingFromReload(t *testing.T) {
	t.Parallel()
	runtime := newHeldRuntime()
	server, cfg, next := movedSocketServer(t, runtime, nil)
	server.drainBound = time.Hour
	client, err := DialLocal(t.Context(), cfg.Paths.SocketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Connection.Close() }()
	result, held := startHeldCall(t, client, runtime)
	if err := server.Reload(next); err != nil {
		t.Fatal(err)
	}
	closed := make(chan error, 1)
	go func() { closed <- server.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("close waited on a draining listener instead of stopping it")
	}
	if held.Err() == nil {
		t.Fatal("call on a draining listener was still running after close returned")
	}
	if err := awaitCall(t, result); err == nil {
		t.Fatal("call stopped by close reported success")
	}
}

func TestServerReloadDrainBoundStopsAndLogsCallInFlight(t *testing.T) {
	t.Parallel()
	runtime := newHeldRuntime()
	var logs syncBuffer
	server, cfg, next := movedSocketServer(t, runtime, slog.New(slog.NewJSONHandler(&logs, nil)))
	server.drainBound = 10 * time.Millisecond
	client, err := DialLocal(t.Context(), cfg.Paths.SocketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Connection.Close() }()
	result, _ := startHeldCall(t, client, runtime)
	if err := server.Reload(next); err != nil {
		t.Fatal(err)
	}
	if err := awaitCall(t, result); err == nil {
		t.Fatal("call past the drain bound reported success")
	}
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	output := logs.String()
	if !strings.Contains(output, `"msg":"control listener drain bound expired"`) || !strings.Contains(output, `"name":"@default"`) {
		t.Fatalf("drain bound expiry was not logged:\n%s", output)
	}
}

func TestServerReloadEndsWatchOnRetiredListener(t *testing.T) {
	t.Parallel()
	var logs syncBuffer
	server, cfg, next := movedSocketServer(t, &fakeRuntime{}, slog.New(slog.NewJSONHandler(&logs, nil)))
	server.drainBound = time.Hour
	client, err := DialLocal(t.Context(), cfg.Paths.SocketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Connection.Close() }()
	stream, err := client.Status.WatchStatus(t.Context(), &controlrpc.WatchStatusRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatal(err)
	}
	if err := server.Reload(next); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		for {
			if _, err := stream.Recv(); err != nil {
				result <- err
				return
			}
		}
	}()
	err = awaitCall(t, result)
	if status.Code(err) != codes.Unavailable || !strings.Contains(err.Error(), errListenerRetired) {
		t.Fatalf("watch on a retired listener ended with %v", err)
	}
	closed := make(chan error, 1)
	go func() { closed <- server.Close() }()
	if err := awaitCall(t, closed); err != nil {
		t.Fatal(err)
	}
	if output := logs.String(); strings.Contains(output, "drain bound expired") {
		t.Fatalf("watch held the drain to its bound:\n%s", output)
	}
}

func TestServerReloadSameAddressDoesNotWaitForCallInFlight(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ca, certificate, key, clientCertificate, clientKey := writeMTLSPKI(t, dir)
	reservation, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := reservation.Addr().String()
	if err := reservation.Close(); err != nil {
		t.Fatal(err)
	}
	cfg := config.Defaults()
	cfg.Paths.SocketPath = filepath.Join(dir, "control.sock")
	cfg.Paths.IdentityDir = filepath.Join(dir, "identity")
	cfg.Listeners["network"] = config.ListenerConfig{Network: "tcp", Address: address, AuthMode: "token", TLSCert: certificate, TLSKey: key}
	runtime := newHeldRuntime()
	server, err := StartServer(cfg, runtime, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = server.Close() }()
	server.drainBound = time.Hour
	bundle, err := CreatePairingBundle(server.store, address, certificate, ca, "localhost", false, "", "", []string{"admin"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	client, err := DialBundle(t.Context(), bundle)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Connection.Close() }()
	result, _ := startHeldCall(t, client, runtime)
	next := cfg.Clone()
	listener := next.Listeners["network"]
	listener.AuthMode = "mtls"
	listener.ClientCA = ca
	next.Listeners["network"] = listener
	reloaded := make(chan error, 1)
	go func() { reloaded <- server.Reload(next) }()
	if err := awaitCall(t, reloaded); err != nil {
		t.Fatal(err)
	}
	caPEM, err := os.ReadFile(ca)
	if err != nil {
		t.Fatal(err)
	}
	replacement, err := DialBundle(t.Context(), PairingBundle{Version: 1, Endpoint: address, TrustMode: "ca", CAPEM: string(caPEM), ServerName: "localhost", ClientCert: string(clientCertificate), ClientKey: string(clientKey)})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = replacement.Connection.Close() }()
	if _, err := replacement.Status.GetStatus(t.Context(), &controlrpc.GetStatusRequest{}); err != nil {
		t.Fatalf("replacement listener did not serve while the previous one drained: %v", err)
	}
	close(runtime.release)
	if err := awaitCall(t, result); err != nil {
		t.Fatalf("call in flight across a same-address reload failed: %v", err)
	}
}
