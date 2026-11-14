package rpc

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pdf/boomerangz/internal/daemonstate"
	"github.com/pdf/boomerangz/internal/zfs"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type snapshotBackend struct {
	zfs.Destination
	destroyed []string
}

func (b *snapshotBackend) DestroySnapshot(_ context.Context, snapshot string) error {
	b.destroyed = append(b.destroyed, snapshot)
	return nil
}

// connect serves service over an in-process gRPC connection and returns a
// client of it.
func connect(t *testing.T, service RemoteServiceServer) *Client {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	RegisterRemoteServiceServer(server, service)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	connection, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	client, err := NewClient(NewRemoteServiceClient(connection), nil)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// pairedClient serves backend through the real service, scoped to root.
func pairedClient(t *testing.T, backend zfs.Destination, root string) *Client {
	t.Helper()
	service, err := NewServerWithReceiver(backend, root, func(ctx context.Context, _ []string) *exec.Cmd {
		return exec.CommandContext(ctx, "false")
	})
	if err != nil {
		t.Fatal(err)
	}
	return connect(t, service)
}

func TestDestroySnapshotReachesTheRemoteInsideItsRoot(t *testing.T) {
	t.Parallel()
	backend := &snapshotBackend{}
	executor := pairedClient(t, backend, "backup/data").Executor()
	for _, snapshot := range []string{"backup/data@a", "backup/data/child@b"} {
		if err := executor.DestroySnapshot(t.Context(), snapshot); err != nil {
			t.Fatalf("destroy %s: %v", snapshot, err)
		}
	}
	if !slices.Equal(backend.destroyed, []string{"backup/data@a", "backup/data/child@b"}) {
		t.Fatalf("remote destroyed %v", backend.destroyed)
	}
}

func TestDestroySnapshotRefusesAnythingButASnapshotInsideTheRoot(t *testing.T) {
	t.Parallel()
	backend := &snapshotBackend{}
	executor := pairedClient(t, backend, "backup/data").Executor()
	for snapshot, want := range map[string]codes.Code{
		"backup/other@a":      codes.PermissionDenied,
		"backup@a":            codes.PermissionDenied,
		"backup/data-other@a": codes.PermissionDenied,
		// A dataset or bookmark name would make `zfs destroy` remove more
		// than one snapshot.
		"backup/data":          codes.InvalidArgument,
		"backup/data/child":    codes.InvalidArgument,
		"backup/data#bookmark": codes.InvalidArgument,
		"backup/data@a@b":      codes.InvalidArgument,
		"backup/data@a,b":      codes.InvalidArgument,
		"backup/data@a%b":      codes.InvalidArgument,
	} {
		if got := status.Code(executor.DestroySnapshot(t.Context(), snapshot)); got != want {
			t.Errorf("destroy %s: code %v, want %v", snapshot, got, want)
		}
	}
	if len(backend.destroyed) != 0 {
		t.Fatalf("remote destroyed %v", backend.destroyed)
	}
}

// olderRemote is an endpoint built before DestroySnapshot existed.
type olderRemote struct {
	UnimplementedRemoteServiceServer
}

func TestDestroySnapshotOnAnOlderRemoteAsksForAnUpgrade(t *testing.T) {
	t.Parallel()
	err := connect(t, olderRemote{}).Executor().DestroySnapshot(t.Context(), "backup/data@a")
	if status.Code(err) != codes.Unimplemented || !strings.Contains(err.Error(), "upgrade boomerangz on the remote host") {
		t.Fatalf("err = %v", err)
	}
}

// fixedStream is the size of the "fixed" helper's stream.
const fixedStream = 3*MaxStreamChunk + 17

// Helper subprocesses stand in for zfs send and receive.
func TestRPCStreamHelperProcess(_ *testing.T) {
	switch os.Getenv("BOOMERANGZ_RPC_HELPER") {
	case "endless":
		chunk := bytes.Repeat([]byte{7}, 64*1024)
		for {
			if _, err := os.Stdout.Write(chunk); err != nil {
				os.Exit(2)
			}
		}
	case "fixed":
		if _, err := os.Stdout.Write(bytes.Repeat([]byte{7}, fixedStream)); err != nil {
			os.Exit(2)
		}
		os.Exit(0)
	case "sink":
		if _, err := io.Copy(io.Discard, os.Stdin); err != nil {
			os.Exit(2)
		}
		os.Exit(0)
	case "reject":
		fmt.Fprintln(os.Stderr, "cannot receive incremental stream: destination tank/backups has been modified")
		os.Exit(1)
	default:
		return
	}
}

func rpcHelper(ctx context.Context, mode string) *exec.Cmd {
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestRPCStreamHelperProcess$")
	command.Env = append(os.Environ(), "BOOMERANGZ_RPC_HELPER="+mode)
	return command
}

// TestStreamReportsRemoteReceiveCause pins that a remote zfs receive
// rejecting a stream reaches the sender as that receive's exit and stderr.
// The server used to answer the broken pipe its next write hit without
// them, and the client put the gRPC Send EOF and the sender it killed first,
// so the reason was lost behind "native stream: EOF | signal: killed".
func TestStreamReportsRemoteReceiveCause(t *testing.T) {
	t.Parallel()
	service, err := NewServerWithReceiver(&snapshotBackend{}, "tank/backups", func(ctx context.Context, _ []string) *exec.Cmd {
		return rpcHelper(ctx, "reject")
	})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := NewStreamWithSender(connect(t, service), func(ctx context.Context, _ []string) *exec.Cmd {
		return rpcHelper(ctx, "endless")
	}, "native")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	_, err = stream.Run(ctx, zfs.SendOptions{Source: "tank/data", Snapshot: "tank/data@end"}, zfs.ReceiveOptions{Root: "tank/backups", Discard: zfs.ReceiveExact}, zfs.Estimate{}, nil)
	if err == nil {
		t.Fatal("rejected stream succeeded")
	}
	message := err.Error()
	if !strings.HasPrefix(message, "native stream: ") || !strings.Contains(message, "zfs receive: exit status 1: cannot receive incremental stream") {
		t.Fatalf("remote receive cause is not reported: %q", message)
	}
	for _, consequence := range []string{"EOF", "signal: killed", "broken pipe", "sender:"} {
		if strings.Contains(message, consequence) {
			t.Fatalf("error reports consequence %q: %q", consequence, message)
		}
	}
}

// recordedReceive is what a tracker was told about one receive.
type recordedReceive struct {
	mu       sync.Mutex
	start    daemonstate.Receive
	bytes    []uint64
	ended    int
	lateness int // samples after End
}

func (r *recordedReceive) Progress(bytes uint64, _ float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.ended != 0 {
		r.lateness++
	}
	r.bytes = append(r.bytes, bytes)
}

func (r *recordedReceive) End() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ended++
}

type recordingTracker struct {
	mu       sync.Mutex
	receives []*recordedReceive
}

func (t *recordingTracker) BeginReceive(receive daemonstate.Receive) daemonstate.ReceiveReporter {
	t.mu.Lock()
	defer t.mu.Unlock()
	recorded := &recordedReceive{start: receive}
	t.receives = append(t.receives, recorded)
	return recorded
}

// TestReceiveIsShownToTheTracker pins what a tracked endpoint reports of a
// receive: its root, the sender's estimate when it has one and none when it
// does not, the sender's host name reduced to what a host name may hold,
// every byte zfs receive accepted, and exactly one end with nothing after it.
func TestReceiveIsShownToTheTracker(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name     string
		estimate zfs.Estimate
	}{
		{name: "estimated", estimate: zfs.Estimate{Known: true, Bytes: fixedStream + 100}},
		{name: "unestimated", estimate: zfs.Estimate{Reason: "send estimate unavailable"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			service, err := NewServerWithReceiver(&snapshotBackend{}, "tank/backups", func(ctx context.Context, _ []string) *exec.Cmd {
				return rpcHelper(ctx, "sink")
			})
			if err != nil {
				t.Fatal(err)
			}
			tracker := &recordingTracker{}
			service.TrackReceives(tracker, "native")
			stream, err := NewStreamWithSender(connect(t, service), func(ctx context.Context, _ []string) *exec.Cmd {
				return rpcHelper(ctx, "fixed")
			}, "native")
			if err != nil {
				t.Fatal(err)
			}
			stream.host = "host-a.example\x1b[2J\n"
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			if _, err := stream.Run(ctx, zfs.SendOptions{Source: "tank/data", Snapshot: "tank/data@end"}, zfs.ReceiveOptions{Root: "tank/backups/data", Discard: zfs.ReceiveExact}, test.estimate, nil); err != nil {
				t.Fatal(err)
			}
			tracker.mu.Lock()
			defer tracker.mu.Unlock()
			if len(tracker.receives) != 1 {
				t.Fatalf("tracker saw %d receives, want 1", len(tracker.receives))
			}
			receive := tracker.receives[0]
			receive.mu.Lock()
			defer receive.mu.Unlock()
			want := daemonstate.Receive{Dataset: "tank/backups/data", State: daemonstate.ReceiveReceiving, Sender: "host-a.example2J", Transport: "native"}
			if test.estimate.Known {
				want.TotalBytes, want.TotalKnown = test.estimate.Bytes, true
			}
			if receive.start != want {
				t.Fatalf("receive began as %+v, want %+v", receive.start, want)
			}
			if len(receive.bytes) == 0 || receive.bytes[len(receive.bytes)-1] != fixedStream {
				t.Fatalf("progress samples %v do not end at the stream's %d bytes", receive.bytes, fixedStream)
			}
			if receive.ended != 1 || receive.lateness != 0 {
				t.Fatalf("receive ended %d times with %d samples after its end", receive.ended, receive.lateness)
			}
		})
	}
}

// TestReceiveEndsForTheTrackerWhenItFails pins that a receive zfs rejects is
// still ended, so the receiving host does not go on listing it.
func TestReceiveEndsForTheTrackerWhenItFails(t *testing.T) {
	t.Parallel()
	service, err := NewServerWithReceiver(&snapshotBackend{}, "tank/backups", func(ctx context.Context, _ []string) *exec.Cmd {
		return rpcHelper(ctx, "reject")
	})
	if err != nil {
		t.Fatal(err)
	}
	tracker := &recordingTracker{}
	service.TrackReceives(tracker, "native")
	stream, err := NewStreamWithSender(connect(t, service), func(ctx context.Context, _ []string) *exec.Cmd {
		return rpcHelper(ctx, "endless")
	}, "native")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if _, err := stream.Run(ctx, zfs.SendOptions{Source: "tank/data", Snapshot: "tank/data@end"}, zfs.ReceiveOptions{Root: "tank/backups", Discard: zfs.ReceiveExact}, zfs.Estimate{}, nil); err == nil {
		t.Fatal("rejected stream succeeded")
	}
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	if len(tracker.receives) != 1 {
		t.Fatalf("tracker saw %d receives, want 1", len(tracker.receives))
	}
	receive := tracker.receives[0]
	receive.mu.Lock()
	defer receive.mu.Unlock()
	if receive.ended != 1 {
		t.Fatalf("a failed receive ended %d times, want 1", receive.ended)
	}
}

func TestDisplayHostKeepsOnlyHostNameCharacters(t *testing.T) {
	t.Parallel()
	for input, want := range map[string]string{
		"backup-01.example.net":  "backup-01.example.net",
		"evil\x1b]0;title\x07":   "evil0title",
		"":                       "",
		strings.Repeat("a", 300): strings.Repeat("a", maxHostLength),
	} {
		if got := DisplayHost(input); got != want {
			t.Errorf("DisplayHost(%q) = %q, want %q", input, got, want)
		}
	}
}
