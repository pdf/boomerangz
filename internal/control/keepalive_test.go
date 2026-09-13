package control

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	controlrpc "github.com/pdf/boomerangz/internal/control/rpc"
	"github.com/pdf/boomerangz/internal/daemonstate"
)

func TestKeepaliveClientNeverPingsFasterThanTheServerAllows(t *testing.T) {
	t.Parallel()
	if clientKeepalive.Time < serverKeepaliveEnforcement.MinTime {
		t.Fatalf("client pings every %v, but the server allows no more often than every %v and would close the connection", clientKeepalive.Time, serverKeepaliveEnforcement.MinTime)
	}
	if clientKeepalive.PermitWithoutStream || serverKeepaliveEnforcement.PermitWithoutStream {
		t.Fatal("keepalive pings are permitted without an active call")
	}
}

// stallingProxy forwards TCP connections until stall is called, then stops
// forwarding in both directions while holding every connection open, as a
// peer that vanished without closing its connection does.
type stallingProxy struct {
	listener net.Listener
	target   string
	stalled  chan struct{}
	closed   chan struct{}
	stall    func()
	mu       sync.Mutex
	conns    []net.Conn
}

func newStallingProxy(t *testing.T, target string) *stallingProxy {
	t.Helper()
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &stallingProxy{listener: listener, target: target, stalled: make(chan struct{}), closed: make(chan struct{})}
	p.stall = sync.OnceFunc(func() { close(p.stalled) })
	t.Cleanup(func() {
		close(p.closed)
		_ = listener.Close()
		p.mu.Lock()
		defer p.mu.Unlock()
		for _, conn := range p.conns {
			_ = conn.Close()
		}
	})
	go p.serve()
	return p
}

func (p *stallingProxy) serve() {
	for {
		client, err := p.listener.Accept()
		if err != nil {
			return
		}
		server, err := (&net.Dialer{}).Dial("tcp", p.target)
		if err != nil {
			_ = client.Close()
			continue
		}
		p.mu.Lock()
		p.conns = append(p.conns, client, server)
		p.mu.Unlock()
		go p.pump(server, client)
		go p.pump(client, server)
	}
}

func (p *stallingProxy) pump(dst io.Writer, src io.Reader) {
	buffer := make([]byte, 32*1024)
	for {
		n, err := src.Read(buffer)
		select {
		case <-p.stalled:
			// Forward nothing further, and read nothing further, until the
			// test ends.
			<-p.closed
			return
		default:
		}
		if n > 0 {
			if _, writeErr := dst.Write(buffer[:n]); writeErr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

// releaseRuntime records when the control plane releases a watch's
// subscription.
type releaseRuntime struct {
	*fakeRuntime
	released chan struct{}
}

func (r *releaseRuntime) SubscribeStatus(ctx context.Context) (daemonstate.Subscription, error) {
	context.AfterFunc(ctx, func() { close(r.released) })
	return r.fakeRuntime.SubscribeStatus(ctx)
}

// TestKeepaliveDetectsAVanishedPeerOnAWatch holds a watch over TCP through a
// proxy that stops forwarding without closing anything. Nothing is sent on an
// idle watch, so only transport keepalive can notice: the client's stream must
// fail, and the server must release the subscription, each within the
// keepalive bound.
func TestKeepaliveDetectsAVanishedPeerOnAWatch(t *testing.T) {
	t.Parallel()
	runtime := &releaseRuntime{fakeRuntime: &fakeRuntime{}, released: make(chan struct{})}
	_, cfg, bundle := mtlsServer(t, runtime)
	proxy := newStallingProxy(t, cfg.Listeners["network"].Address)
	bundle.Endpoint = proxy.listener.Addr().String()
	client, err := DialBundle(t.Context(), bundle)
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
	proxy.stall()
	started := time.Now()
	bound := keepaliveTime + keepaliveTimeout + 15*time.Second
	failed := make(chan error, 1)
	go func() {
		for {
			if _, err := stream.Recv(); err != nil {
				failed <- err
				return
			}
		}
	}()
	deadline := time.NewTimer(bound)
	defer deadline.Stop()
	var clientDetected, serverReleased bool
	for !clientDetected || !serverReleased {
		select {
		case err := <-failed:
			if errors.Is(err, io.EOF) {
				t.Fatalf("watch ended cleanly after the peer vanished: %v", err)
			}
			clientDetected = true
			t.Logf("client detected the vanished server after %v: %v", time.Since(started), err)
		case <-runtime.released:
			runtime.released = nil
			serverReleased = true
			t.Logf("server released the subscription after %v", time.Since(started))
		case <-deadline.C:
			t.Fatalf("after %v: client detected=%t, server released=%t", bound, clientDetected, serverReleased)
		}
	}
}
