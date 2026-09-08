// Package native carries the shared replication RPCs over authenticated TLS.
package native

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"

	"github.com/pdf/boomerangz/internal/control"
	remoterpc "github.com/pdf/boomerangz/internal/replication/rpc"
	"github.com/pdf/boomerangz/internal/zfs"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// UnavailableError identifies a connection-level failure suitable for bounded
// retry. Authentication, scope, and destination validation failures are final.
type UnavailableError struct{ Err error }

func (e *UnavailableError) Error() string { return "native endpoint unavailable: " + e.Err.Error() }
func (e *UnavailableError) Unwrap() error { return e.Err }

// Temporary reports that reconnecting may make the endpoint available.
func (*UnavailableError) Temporary() bool { return true }

func mapRPCError(err error) error {
	if err == nil {
		return nil
	}
	if code := status.Code(err); code == codes.Unavailable || code == codes.DeadlineExceeded {
		return &UnavailableError{Err: err}
	}
	return err
}

// IsUnavailable reports a retryable native connection failure.
func IsUnavailable(err error) bool {
	var target *UnavailableError
	return errors.As(err, &target)
}

// Endpoint is one authenticated native connection and its shared RPC views.
type Endpoint struct {
	connection *grpc.ClientConn
	client     *remoterpc.Client
	executor   zfs.Executor
	stream     *remoterpc.Stream
	canonical  string
}

// Open verifies the pairing, negotiates the protocol, and prepares the
// destination executor and receive stream.
func Open(ctx context.Context, bundle control.PairingBundle, root, zfsPath string) (*Endpoint, error) {
	if err := zfs.ValidateDataset(root); err != nil {
		return nil, fmt.Errorf("native destination root: %w", err)
	}
	canonical, err := CanonicalTarget(bundle.Endpoint, root)
	if err != nil {
		return nil, err
	}
	connection, err := control.DialPairingConnection(bundle)
	if err != nil {
		return nil, err
	}
	client, err := remoterpc.NewClient(remoterpc.NewRemoteServiceClient(connection), mapRPCError)
	if err != nil {
		_ = connection.Close()
		return nil, err
	}
	if err := client.Negotiate(ctx); err != nil {
		_ = connection.Close()
		return nil, err
	}
	stream, err := remoterpc.NewStream(client, zfsPath, "native")
	if err != nil {
		_ = connection.Close()
		return nil, err
	}
	return &Endpoint{connection: connection, client: client, executor: client.Executor(), stream: stream, canonical: canonical}, nil
}

// CanonicalTarget returns the stable native endpoint and destination identity.
func CanonicalTarget(endpoint, root string) (string, error) {
	host, port, err := net.SplitHostPort(endpoint)
	if err != nil || host == "" || port == "" {
		return "", fmt.Errorf("native pairing endpoint must be host:port")
	}
	address := net.JoinHostPort(strings.ToLower(host), port)
	return (&url.URL{Scheme: "native", Host: address, Path: "/" + root}).String(), nil
}

// Executor exposes destination operations through the shared RPC contract.
func (e *Endpoint) Executor() zfs.Executor { return e.executor }

// Stream exposes bounded gRPC receive streaming.
func (e *Endpoint) Stream() *remoterpc.Stream { return e.stream }

// CanonicalTarget returns the identity persisted in source-side bindings.
func (e *Endpoint) CanonicalTarget() string { return e.canonical }

// Close releases the authenticated connection.
func (e *Endpoint) Close() error {
	if e == nil || e.connection == nil {
		return nil
	}
	return e.connection.Close()
}
