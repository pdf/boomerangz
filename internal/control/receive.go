package control

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	controlrpc "github.com/pdf/boomerangz/internal/control/rpc"
	"github.com/pdf/boomerangz/internal/daemonstate"
	remoterpc "github.com/pdf/boomerangz/internal/replication/rpc"
	"github.com/pdf/boomerangz/internal/zfs"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// Receive transports, as status names them.
const (
	// TransportNative is a receive the daemon serves on a listener.
	TransportNative = "native"
	// TransportSSHShell is a receive an ssh-shell process serves and reports.
	TransportSSHShell = "ssh-shell"
)

// receiveTracker is the part of a runtime that lists receives, where it
// lists them.
func receiveTracker(backend runtime) daemonstate.ReceiveTracker {
	tracker, _ := backend.(daemonstate.ReceiveTracker)
	return tracker
}

// ReportReceive lists a receive an ssh-shell process serves in the daemon's
// status for as long as the stream stays open. Only a Unix socket accepts it:
// what may connect there is decided by the socket's file permissions, while a
// TCP peer could otherwise list receives on a host it does not send to.
func (s *service) ReportReceive(stream controlrpc.ControlService_ReportReceiveServer) error {
	if !s.reportAllowed {
		return status.Error(codes.PermissionDenied, "receive reports are accepted only on a Unix socket")
	}
	tracker := receiveTracker(s.runtime)
	if tracker == nil {
		return status.Error(codes.Unimplemented, "receive status is unavailable")
	}
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	start := first.GetStart()
	if start == nil {
		return status.Error(codes.InvalidArgument, "the first receive report must start the receive")
	}
	if err := zfs.ValidateDataset(start.GetDataset()); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	receive := daemonstate.Receive{Dataset: start.GetDataset(), Sender: remoterpc.DisplayHost(start.GetSenderHost()), Transport: TransportSSHShell}
	if start.EstimatedBytes != nil {
		receive.TotalBytes, receive.TotalKnown = start.GetEstimatedBytes(), true
	}
	reporter := tracker.BeginReceive(receive)
	defer reporter.End()
	// A report stream lasts as long as its receive, so a draining listener
	// ends it rather than wait; the receive goes on unlisted.
	ctx := stream.Context()
	if s.drain != nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(ctx)
		defer cancel()
		defer context.AfterFunc(s.drain, cancel)()
	}
	reports := make(chan *controlrpc.ReportReceiveRequest)
	failed := make(chan error, 1)
	go func() {
		for {
			report, recvErr := stream.Recv()
			if recvErr != nil {
				failed <- recvErr
				return
			}
			select {
			case reports <- report:
			case <-ctx.Done():
				return
			}
		}
	}()
	for {
		select {
		case report := <-reports:
			if report.GetStart() != nil {
				return status.Error(codes.InvalidArgument, "a receive report stream starts one receive")
			}
			reporter.Progress(report.GetBytes(), report.GetBytesPerSecond())
		case recvErr := <-failed:
			if errors.Is(recvErr, io.EOF) {
				return stream.SendAndClose(&controlrpc.ReportReceiveResponse{})
			}
			return recvErr
		case <-ctx.Done():
			if stream.Context().Err() != nil {
				return stream.Context().Err()
			}
			return status.Error(codes.Unavailable, errListenerRetired)
		}
	}
}

// receiveDialTimeout bounds how long a forwarder tries to reach the daemon.
const receiveDialTimeout = time.Second

// ReceiveForwarder reports the receives a process serves outside the daemon,
// an ssh-shell process, to the daemon on the host's control socket, so that
// the daemon's status shows them. A daemon it cannot reach, or one that
// refuses the report, leaves the receive unlisted: the receive itself never
// waits on the forwarder or fails because of it.
type ReceiveForwarder struct {
	socket string
}

// NewReceiveForwarder forwards to the daemon listening on socket.
func NewReceiveForwarder(socket string) *ReceiveForwarder {
	return &ReceiveForwarder{socket: socket}
}

// BeginReceive starts forwarding one receive. It returns at once; the daemon
// is reached in the background.
func (f *ReceiveForwarder) BeginReceive(receive daemonstate.Receive) daemonstate.ReceiveReporter {
	forwarded := &forwardedReceive{wake: make(chan struct{}, 1), done: make(chan struct{}), exited: make(chan struct{})}
	go forwarded.run(f.socket, receive)
	return forwarded
}

// forwardedReceive forwards the newest sample whenever the stream can take
// one, so a slow daemon conflates samples instead of holding the receive.
type forwardedReceive struct {
	mu      sync.Mutex
	bytes   uint64
	rate    float64
	pending bool
	wake    chan struct{}
	done    chan struct{}
	exited  chan struct{}
	once    sync.Once
}

func (r *forwardedReceive) Progress(bytes uint64, bytesPerSecond float64) {
	r.mu.Lock()
	r.bytes, r.rate, r.pending = bytes, bytesPerSecond, true
	r.mu.Unlock()
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// End sends the last sample and closes the stream, waiting at most
// receiveDialTimeout for the daemon to take it.
func (r *forwardedReceive) End() {
	r.once.Do(func() { close(r.done) })
	select {
	case <-r.exited:
	case <-time.After(receiveDialTimeout):
	}
}

func (r *forwardedReceive) take() (*controlrpc.ReportReceiveRequest, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.pending {
		return nil, false
	}
	r.pending = false
	return &controlrpc.ReportReceiveRequest{Bytes: r.bytes, BytesPerSecond: r.rate}, true
}

func (r *forwardedReceive) run(socket string, receive daemonstate.Receive) {
	defer close(r.exited)
	connection, err := grpc.NewClient("passthrough:///"+socket, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return (&net.Dialer{Timeout: receiveDialTimeout}).DialContext(ctx, "unix", socket)
	}))
	if err != nil {
		return
	}
	defer func() { _ = connection.Close() }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := controlrpc.NewControlServiceClient(connection).ReportReceive(ctx)
	if err != nil {
		return
	}
	start := &controlrpc.ReceiveStart{Dataset: receive.Dataset, SenderHost: receive.Sender}
	if receive.TotalKnown {
		start.EstimatedBytes = &receive.TotalBytes
	}
	if stream.Send(&controlrpc.ReportReceiveRequest{Start: start}) != nil {
		return
	}
	for {
		select {
		case <-r.wake:
			if report, ok := r.take(); ok && stream.Send(report) != nil {
				return
			}
		case <-r.done:
			if report, ok := r.take(); ok && stream.Send(report) != nil {
				return
			}
			_, _ = stream.CloseAndRecv()
			return
		}
	}
}
