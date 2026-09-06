package ssh

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"os/exec"
	"slices"
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
	remote      remoterpc.RemoteServiceClient
	executor    *shellExecutor
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
	shell := &Shell{client: client, connection: connection, remote: remoterpc.NewRemoteServiceClient(connection), process: command, processConn: pipe, diagnostics: diagnostics, done: make(chan error, 1)}
	shell.executor = &shellExecutor{shell: shell}
	go func() { shell.done <- command.Wait() }()
	capabilities, err := shell.remote.Capabilities(ctx, &remoterpc.CapabilitiesRequest{})
	if err != nil || capabilities.GetProtocolVersion() != remoterpc.ProtocolVersion {
		if err == nil {
			err = fmt.Errorf("unsupported protocol version %d", capabilities.GetProtocolVersion())
		}
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
func (s *Shell) Executor() zfs.Executor { return s.executor }

// NewStream returns a gRPC receive stream over this same SSH session.
func (s *Shell) NewStream(zfsPath string) (*ShellStream, error) {
	if zfsPath == "" {
		return nil, fmt.Errorf("local ZFS executable is required")
	}
	return &ShellStream{shell: s, sender: func(ctx context.Context, args []string) *exec.Cmd {
		return exec.CommandContext(ctx, zfsPath, args...)
	}}, nil
}

type shellExecutor struct{ shell *Shell }

func (e *shellExecutor) ListDatasets(ctx context.Context) ([]zfs.Dataset, error) {
	response, err := e.shell.remote.ListDatasets(ctx, &remoterpc.ListDatasetsRequest{})
	if err != nil {
		return nil, normalizeRPCError(err)
	}
	result := make([]zfs.Dataset, 0, len(response.GetDatasets()))
	for _, dataset := range response.GetDatasets() {
		result = append(result, zfs.Dataset{Name: dataset.GetName(), Type: zfs.DatasetType(dataset.GetType()), EncryptionRoot: dataset.GetEncryptionRoot()})
	}
	return result, nil
}

func (e *shellExecutor) InspectDatasetIdentity(ctx context.Context, dataset string) (zfs.DatasetIdentity, error) {
	response, err := e.shell.remote.InspectDatasetIdentity(ctx, &remoterpc.InspectDatasetIdentityRequest{Dataset: dataset})
	if err != nil {
		return zfs.DatasetIdentity{}, normalizeRPCError(err)
	}
	return zfs.DatasetIdentity{Name: response.GetName(), Type: zfs.DatasetType(response.GetType()), GUID: response.GetGuid(), Pool: response.GetPool(), PoolGUID: response.GetPoolGuid()}, nil
}

func (e *shellExecutor) InspectState(ctx context.Context, dataset string, recursive bool) (zfs.State, error) {
	response, err := e.shell.remote.InspectState(ctx, &remoterpc.InspectStateRequest{Dataset: dataset, Recursive: recursive})
	if err != nil {
		return zfs.State{}, normalizeRPCError(err)
	}
	return decodeState(response), nil
}

func (e *shellExecutor) SetProperties(ctx context.Context, dataset string, properties map[string]string) error {
	_, err := e.shell.remote.SetProperties(ctx, &remoterpc.SetPropertiesRequest{Dataset: dataset, Properties: maps.Clone(properties)})
	return normalizeRPCError(err)
}

func (e *shellExecutor) InheritProperty(ctx context.Context, dataset, property string) error {
	_, err := e.shell.remote.InheritProperty(ctx, &remoterpc.InheritPropertyRequest{Dataset: dataset, Property: property})
	return normalizeRPCError(err)
}

func (e *shellExecutor) GetActivationProperties(context.Context) ([]zfs.Property, error) {
	return nil, fmt.Errorf("source activation queries are unavailable through SSH shell")
}
func (e *shellExecutor) GetStoredProperties(context.Context, []string) ([]zfs.Property, error) {
	return nil, fmt.Errorf("source property queries are unavailable through SSH shell")
}
func (e *shellExecutor) Snapshot(context.Context, string, string, bool, map[string]string) error {
	return fmt.Errorf("source snapshot mutations are unavailable through SSH shell")
}
func (e *shellExecutor) DestroySnapshot(context.Context, string) error {
	return fmt.Errorf("source snapshot mutations are unavailable through SSH shell")
}
func (e *shellExecutor) Bookmark(context.Context, string, string) error {
	return fmt.Errorf("source bookmark mutations are unavailable through SSH shell")
}
func (e *shellExecutor) DestroyBookmark(context.Context, string) error {
	return fmt.Errorf("source bookmark mutations are unavailable through SSH shell")
}
func (e *shellExecutor) Hold(context.Context, string, string) error {
	return fmt.Errorf("source hold mutations are unavailable through SSH shell")
}
func (e *shellExecutor) Release(context.Context, string, string) error {
	return fmt.Errorf("source hold mutations are unavailable through SSH shell")
}

func decodeState(response *remoterpc.InspectStateResponse) zfs.State {
	state := zfs.State{Received: make(map[string]map[string]string), ResumeTokens: make(map[string]string), Clones: make(map[string][]string), Holds: make(map[string][]string)}
	for _, object := range response.GetObjects() {
		state.Objects = append(state.Objects, zfs.Object{Name: object.GetName(), Type: object.GetType(), GUID: object.GetGuid(), Creation: object.GetCreation(), CreateTXG: object.GetCreateTxg()})
	}
	for _, property := range response.GetProperties() {
		state.Properties = append(state.Properties, zfs.Property{Dataset: property.GetDataset(), Name: property.GetName(), Value: property.GetValue(), Source: zfs.PropertySource(property.GetSource())})
	}
	for _, property := range response.GetReceived() {
		if state.Received[property.GetDataset()] == nil {
			state.Received[property.GetDataset()] = make(map[string]string)
		}
		state.Received[property.GetDataset()][property.GetName()] = property.GetValue()
	}
	for _, value := range response.GetResumeTokens() {
		state.ResumeTokens[value.GetName()] = value.GetValue()
	}
	for _, value := range response.GetClones() {
		state.Clones[value.GetName()] = slices.Clone(value.GetValues())
	}
	for _, value := range response.GetHolds() {
		state.Holds[value.GetName()] = slices.Clone(value.GetValues())
	}
	return state
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
