package rpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"os/exec"
	"slices"
	"time"

	"github.com/pdf/boomerangz/internal/zfs"
)

// ErrorMapper lets a transport classify connection failures without coupling
// the shared RPC implementation to SSH or native TCP details.
type ErrorMapper func(error) error

// Client exposes the transport-neutral remote replication endpoint.
type Client struct {
	remote   RemoteServiceClient
	mapError ErrorMapper
}

// NewClient wraps a generated client. The optional mapper is applied to every
// RPC error at the transport boundary.
func NewClient(remote RemoteServiceClient, mapper ErrorMapper) (*Client, error) {
	if remote == nil {
		return nil, fmt.Errorf("remote replication client is required")
	}
	if mapper == nil {
		mapper = func(err error) error { return err }
	}
	return &Client{remote: remote, mapError: mapper}, nil
}

// Negotiate verifies that both peers speak the same protocol revision.
func (c *Client) Negotiate(ctx context.Context) error {
	capabilities, err := c.remote.Capabilities(ctx, &CapabilitiesRequest{})
	if err != nil {
		return c.mapError(err)
	}
	if capabilities.GetProtocolVersion() != ProtocolVersion {
		return fmt.Errorf("unsupported remote protocol version %d", capabilities.GetProtocolVersion())
	}
	return nil
}

// Executor returns the destination-only ZFS surface implemented by the RPCs.
func (c *Client) Executor() zfs.Executor { return &clientExecutor{client: c} }

type clientExecutor struct{ client *Client }

func (e *clientExecutor) ListDatasets(ctx context.Context) ([]zfs.Dataset, error) {
	response, err := e.client.remote.ListDatasets(ctx, &ListDatasetsRequest{})
	if err != nil {
		return nil, e.client.mapError(err)
	}
	result := make([]zfs.Dataset, 0, len(response.GetDatasets()))
	for _, dataset := range response.GetDatasets() {
		result = append(result, zfs.Dataset{Name: dataset.GetName(), Type: zfs.DatasetType(dataset.GetType()), EncryptionRoot: dataset.GetEncryptionRoot()})
	}
	return result, nil
}

func (e *clientExecutor) InspectDatasetIdentity(ctx context.Context, dataset string) (zfs.DatasetIdentity, error) {
	response, err := e.client.remote.InspectDatasetIdentity(ctx, &InspectDatasetIdentityRequest{Dataset: dataset})
	if err != nil {
		return zfs.DatasetIdentity{}, e.client.mapError(err)
	}
	return zfs.DatasetIdentity{Name: response.GetName(), Type: zfs.DatasetType(response.GetType()), GUID: response.GetGuid(), Pool: response.GetPool(), PoolGUID: response.GetPoolGuid()}, nil
}

func (e *clientExecutor) InspectState(ctx context.Context, dataset string, recursive bool) (zfs.State, error) {
	response, err := e.client.remote.InspectState(ctx, &InspectStateRequest{Dataset: dataset, Recursive: recursive})
	if err != nil {
		return zfs.State{}, e.client.mapError(err)
	}
	return decodeClientState(response), nil
}

func (e *clientExecutor) SetProperties(ctx context.Context, dataset string, properties map[string]string) error {
	_, err := e.client.remote.SetProperties(ctx, &SetPropertiesRequest{Dataset: dataset, Properties: maps.Clone(properties)})
	return e.client.mapError(err)
}

func (e *clientExecutor) InheritProperty(ctx context.Context, dataset, property string) error {
	_, err := e.client.remote.InheritProperty(ctx, &InheritPropertyRequest{Dataset: dataset, Property: property})
	return e.client.mapError(err)
}

func (*clientExecutor) GetActivationProperties(context.Context) ([]zfs.Property, error) {
	return nil, fmt.Errorf("source activation queries are unavailable through remote replication RPC")
}
func (*clientExecutor) GetStoredProperties(context.Context, []string) ([]zfs.Property, error) {
	return nil, fmt.Errorf("source property queries are unavailable through remote replication RPC")
}
func (*clientExecutor) Snapshot(context.Context, string, string, bool, map[string]string) error {
	return fmt.Errorf("source snapshot mutations are unavailable through remote replication RPC")
}
func (*clientExecutor) DestroySnapshot(context.Context, string) error {
	return fmt.Errorf("source snapshot mutations are unavailable through remote replication RPC")
}
func (*clientExecutor) Bookmark(context.Context, string, string) error {
	return fmt.Errorf("source bookmark mutations are unavailable through remote replication RPC")
}
func (*clientExecutor) DestroyBookmark(context.Context, string) error {
	return fmt.Errorf("source bookmark mutations are unavailable through remote replication RPC")
}
func (*clientExecutor) Hold(context.Context, string, string) error {
	return fmt.Errorf("source hold mutations are unavailable through remote replication RPC")
}
func (*clientExecutor) Release(context.Context, string, string) error {
	return fmt.Errorf("source hold mutations are unavailable through remote replication RPC")
}

func decodeClientState(response *InspectStateResponse) zfs.State {
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

// Stream sends a local ZFS stream through the shared client-streaming RPC.
type Stream struct {
	client *Client
	sender zfs.CommandFactory
	label  string
}

// NewStream constructs a stream using an explicit local ZFS executable.
func NewStream(client *Client, zfsPath, label string) (*Stream, error) {
	if zfsPath == "" {
		return nil, fmt.Errorf("local ZFS executable is required")
	}
	return NewStreamWithSender(client, func(ctx context.Context, args []string) *exec.Cmd {
		return exec.CommandContext(ctx, zfsPath, args...)
	}, label)
}

// NewStreamWithSender injects the local send process for tests.
func NewStreamWithSender(client *Client, sender zfs.CommandFactory, label string) (*Stream, error) {
	if client == nil || sender == nil {
		return nil, fmt.Errorf("remote client and local sender are required")
	}
	if label == "" {
		label = "remote RPC"
	}
	return &Stream{client: client, sender: sender, label: label}, nil
}

type streamProgress struct {
	bytes    uint64
	estimate zfs.Estimate
	start    time.Time
	last     time.Time
	report   func(zfs.Progress)
}

func (p *streamProgress) emit(completed bool) zfs.Progress {
	now := time.Now()
	result := zfs.Progress{Bytes: p.bytes, Estimate: p.estimate, Completed: completed}
	if elapsed := now.Sub(p.start).Seconds(); elapsed > 0 {
		result.BytesPerSecond = float64(p.bytes) / elapsed
	}
	if p.estimate.Known && p.estimate.Bytes >= p.bytes && result.BytesPerSecond > 0 {
		eta := time.Duration(float64(p.estimate.Bytes-p.bytes) / result.BytesPerSecond * float64(time.Second))
		result.ETA = &eta
	}
	p.last = now
	if p.report != nil {
		p.report(result)
	}
	return result
}

// Run executes one bounded sender and gRPC client-streaming receive.
func (s *Stream) Run(ctx context.Context, send zfs.SendOptions, receive zfs.ReceiveOptions, estimate zfs.Estimate, report func(zfs.Progress)) (zfs.Progress, error) {
	args, err := zfs.SendArguments(send, false)
	if err != nil {
		return zfs.Progress{}, err
	}
	if _, err := zfs.ReceiveArguments(receive); err != nil {
		return zfs.Progress{}, err
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := s.client.remote.Receive(runCtx)
	if err != nil {
		return zfs.Progress{}, s.client.mapError(err)
	}
	options := &ReceiveOptions{Root: receive.Root, Discard: string(receive.Discard), Set: receive.Set, Exclude: receive.Exclude}
	if err := stream.Send(&ReceiveRequest{Options: options}); err != nil {
		return zfs.Progress{}, s.client.mapError(err)
	}
	sender := s.sender(runCtx, args)
	sender.WaitDelay = 2 * time.Second
	var diagnostics diagnosticBuffer
	sender.Stderr = &diagnostics
	output, err := sender.StdoutPipe()
	if err != nil {
		return zfs.Progress{}, err
	}
	if err := sender.Start(); err != nil {
		_ = output.Close()
		return zfs.Progress{}, err
	}
	progress := streamProgress{estimate: estimate, start: time.Now(), last: time.Now(), report: report}
	progress.emit(false)
	buffer := make([]byte, 128*1024)
	var copyErr error
	for {
		n, readErr := output.Read(buffer)
		if n > 0 {
			if sendErr := stream.Send(&ReceiveRequest{Data: buffer[:n]}); sendErr != nil {
				copyErr = s.client.mapError(sendErr)
				cancel()
				break
			}
			progress.bytes += uint64(n)
			if time.Since(progress.last) >= 250*time.Millisecond {
				progress.emit(false)
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			copyErr = readErr
			cancel()
			break
		}
	}
	senderErr := sender.Wait()
	response, receiveErr := stream.CloseAndRecv()
	receiveErr = s.client.mapError(receiveErr)
	if response != nil && response.GetBytes() != progress.bytes {
		receiveErr = errors.Join(receiveErr, fmt.Errorf("remote receive byte count differs from sent stream"))
	}
	err = errors.Join(copyErr, senderErr, receiveErr, ctx.Err())
	result := progress.emit(err == nil)
	if err != nil {
		return result, fmt.Errorf("%s stream: %w; sender: %s", s.label, err, diagnostics.String())
	}
	return result, nil
}
