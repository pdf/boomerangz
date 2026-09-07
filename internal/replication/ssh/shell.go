package ssh

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os/exec"
	"sync"
	"time"

	remoterpc "github.com/pdf/boomerangz/internal/replication/rpc"
	"github.com/pdf/boomerangz/internal/zfs"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// ShellUnavailableError means the optional remote command is absent or does
// not speak the negotiated protocol. Explicit auto mode may fall back.
type ShellUnavailableError struct{ Err error }

func (e *ShellUnavailableError) Error() string {
	return "boomerangz SSH shell unavailable: " + e.Err.Error()
}
func (e *ShellUnavailableError) Unwrap() error { return e.Err }

type pipeAddress string

func (a pipeAddress) Network() string { return "ssh-command" }
func (a pipeAddress) String() string  { return string(a) }

type processConn struct {
	reader io.ReadCloser
	writer io.WriteCloser
	cancel context.CancelFunc
	once   sync.Once
}

func (c *processConn) Read(p []byte) (int, error)  { return c.reader.Read(p) }
func (c *processConn) Write(p []byte) (int, error) { return c.writer.Write(p) }
func (c *processConn) Close() error {
	var err error
	c.once.Do(func() {
		c.cancel()
		err = errors.Join(c.writer.Close(), c.reader.Close())
	})
	return err
}
func (c *processConn) LocalAddr() net.Addr              { return pipeAddress("local") }
func (c *processConn) RemoteAddr() net.Addr             { return pipeAddress("remote") }
func (c *processConn) SetDeadline(time.Time) error      { return nil }
func (c *processConn) SetReadDeadline(time.Time) error  { return nil }
func (c *processConn) SetWriteDeadline(time.Time) error { return nil }

// Shell is one persistent gRPC connection carried by an authenticated SSH
// command channel. It owns the remote process until Close.
type Shell struct {
	client      *Client
	connection  *grpc.ClientConn
	remote      *remoterpc.Client
	process     *exec.Cmd
	processConn *processConn
	diagnostics *boundedOutput
	done        chan error
	closeOnce   sync.Once
	closeErr    error
}

// NewShell starts and negotiates an optional boomerangz SSH shell just in time.
func NewShell(ctx context.Context, client *Client) (*Shell, error) {
	if client == nil {
		return nil, fmt.Errorf("SSH client is required")
	}
	processCtx, cancel := context.WithCancel(context.Background())
	remote := remoteCommand(client.shellPath(), []string{"ssh-shell", "--root", client.config.Root})
	command := client.command(processCtx, client.sshArguments(remote))
	stdout, err := command.StdoutPipe()
	if err != nil {
		cancel()
		return nil, err
	}
	stdin, err := command.StdinPipe()
	if err != nil {
		cancel()
		_ = stdout.Close()
		return nil, err
	}
	diagnostics := &boundedOutput{}
	command.Stderr = diagnostics
	if err := command.Start(); err != nil {
		cancel()
		_ = stdin.Close()
		_ = stdout.Close()
		return nil, &ShellUnavailableError{Err: err}
	}
	pipe := &processConn{reader: stdout, writer: stdin, cancel: cancel}
	used := false
	var dialMu sync.Mutex
	connection, err := grpc.NewClient("passthrough:///boomerangz-ssh-shell",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			dialMu.Lock()
			defer dialMu.Unlock()
			if used {
				return nil, fmt.Errorf("SSH shell connection cannot be redialed")
			}
			used = true
			return pipe, nil
		}),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(remoteOutputLimit), grpc.MaxCallSendMsgSize(remoteOutputLimit)),
	)
	if err != nil {
		_ = pipe.Close()
		_ = command.Wait()
		return nil, err
	}
	rpcClient, err := remoterpc.NewClient(remoterpc.NewRemoteServiceClient(connection), normalizeRPCError)
	if err != nil {
		_ = connection.Close()
		_ = pipe.Close()
		_ = command.Wait()
		return nil, err
	}
	shell := &Shell{client: client, connection: connection, remote: rpcClient, process: command, processConn: pipe, diagnostics: diagnostics, done: make(chan error, 1)}
	go func() { shell.done <- command.Wait() }()
	err = shell.remote.Negotiate(ctx)
	if err != nil {
		_ = shell.connection.Close()
		_ = shell.processConn.Close()
		var processErr error
		select {
		case processErr = <-shell.done:
		case <-time.After(3 * time.Second):
			processErr = fmt.Errorf("SSH shell process did not exit promptly")
		}
		joined := errors.Join(err, processErr, diagnosticsError(diagnostics))
		if unavailable(ctx, processErr) {
			return nil, &UnavailableError{Err: joined}
		}
		return nil, &ShellUnavailableError{Err: joined}
	}
	return shell, nil
}

func diagnosticsError(output *boundedOutput) error {
	if output == nil || output.buffer.Len() == 0 {
		return nil
	}
	return fmt.Errorf("remote diagnostics: %s", output.buffer.String())
}

// Close ends the gRPC connection and reaps the SSH process.
func (s *Shell) Close() error {
	if s == nil {
		return nil
	}
	s.closeOnce.Do(func() {
		s.closeErr = s.connection.Close()
		s.closeErr = errors.Join(s.closeErr, s.processConn.Close())
		select {
		case <-s.done:
			// Closing the owned connection deliberately cancels the SSH process;
			// any operational process failure is reported by the active RPC first.
		case <-time.After(3 * time.Second):
			s.closeErr = errors.Join(s.closeErr, fmt.Errorf("SSH shell process did not exit promptly"))
		}
	})
	return s.closeErr
}

// Executor exposes destination operations through the shared service.
func (s *Shell) Executor() zfs.Executor { return s.remote.Executor() }

// NewStream returns a gRPC receive stream over this same SSH session.
func (s *Shell) NewStream(zfsPath string) (*remoterpc.Stream, error) {
	return remoterpc.NewStream(s.remote, zfsPath, "SSH-shell")
}

func normalizeRPCError(err error) error {
	if err != nil && status.Code(err) == codes.Unavailable {
		return &UnavailableError{Err: err}
	}
	return err
}

// IsShellUnavailable reports a failed optional endpoint negotiation.
func IsShellUnavailable(err error) bool {
	var target *ShellUnavailableError
	return errors.As(err, &target)
}
