package rpc

import (
	"context"
	"net"
	"os/exec"
	"slices"
	"strings"
	"testing"

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
