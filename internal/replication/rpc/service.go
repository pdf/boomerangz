// Package rpc implements the shared remote replication gRPC service.
package rpc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"os/exec"
	"slices"
	"strings"

	"github.com/pdf/boomerangz/internal/replication/scope"
	"github.com/pdf/boomerangz/internal/zfs"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ProtocolVersion is negotiated by SSH-shell and future native clients.
const ProtocolVersion = 1

// MaxStreamChunk bounds each in-memory ZFS data message.
const MaxStreamChunk = 256 * 1024

var operations = []string{"capabilities", "probe", "receive", "resume", "verify", "reconcile"}

// Server implements the transport-neutral remote endpoint over a typed ZFS
// executor. AllowedRoot limits every mutation and receive to one configured
// destination subtree.
type Server struct {
	UnimplementedRemoteServiceServer
	backend      zfs.Executor
	allowedRoots []string
	receive      zfs.CommandFactory
}

// NewServer constructs a scoped remote endpoint using an explicit ZFS binary.
func NewServer(backend zfs.Executor, allowedRoot, zfsPath string) (*Server, error) {
	return NewServerForRoots(backend, []string{allowedRoot}, zfsPath)
}

// NewServerForRoots constructs one endpoint bounded to the configured
// destination subtrees.
func NewServerForRoots(backend zfs.Executor, allowedRoots []string, zfsPath string) (*Server, error) {
	if zfsPath == "" {
		return nil, fmt.Errorf("remote ZFS executor and executable are required")
	}
	return NewServerForRootsWithReceiver(backend, allowedRoots, func(ctx context.Context, args []string) *exec.Cmd {
		return exec.CommandContext(ctx, zfsPath, args...)
	})
}

// NewServerWithReceiver injects the receive process boundary for tests and
// privileged implementations while preserving validated receive arguments.
func NewServerWithReceiver(backend zfs.Executor, allowedRoot string, receive zfs.CommandFactory) (*Server, error) {
	return NewServerForRootsWithReceiver(backend, []string{allowedRoot}, receive)
}

// NewServerForRootsWithReceiver injects the receive process boundary while
// allowing one authenticated listener to serve several explicit roots.
func NewServerForRootsWithReceiver(backend zfs.Executor, allowedRoots []string, receive zfs.CommandFactory) (*Server, error) {
	if backend == nil || receive == nil {
		return nil, fmt.Errorf("remote ZFS executor and receive command are required")
	}
	if len(allowedRoots) == 0 {
		return nil, fmt.Errorf("at least one allowed destination root is required")
	}
	roots := slices.Clone(allowedRoots)
	slices.Sort(roots)
	roots = slices.Compact(roots)
	for _, root := range roots {
		if err := zfs.ValidateDataset(root); err != nil {
			return nil, fmt.Errorf("allowed destination root: %w", err)
		}
	}
	return &Server{backend: backend, allowedRoots: roots, receive: receive}, nil
}

func (s *Server) related(dataset string) bool {
	return slices.ContainsFunc(s.allowedRoots, func(root string) bool { return scope.Related(root, dataset) })
}

func (s *Server) inside(dataset string) bool {
	return slices.ContainsFunc(s.allowedRoots, func(root string) bool { return scope.Inside(root, dataset) })
}

// Capabilities negotiates protocol and operation support.
func (s *Server) Capabilities(context.Context, *CapabilitiesRequest) (*CapabilitiesResponse, error) {
	return &CapabilitiesResponse{ProtocolVersion: ProtocolVersion, Operations: slices.Clone(operations)}, nil
}

// ListDatasets returns only the configured root, its ancestors, and descendants.
func (s *Server) ListDatasets(ctx context.Context, _ *ListDatasetsRequest) (*ListDatasetsResponse, error) {
	datasets, err := s.backend.ListDatasets(ctx)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	response := &ListDatasetsResponse{}
	for _, dataset := range datasets {
		if s.related(dataset.Name) {
			response.Datasets = append(response.Datasets, &Dataset{Name: dataset.Name, Type: string(dataset.Type), EncryptionRoot: dataset.EncryptionRoot})
		}
	}
	return response, nil
}

// InspectDatasetIdentity resolves GUID identity inside the configured scope.
func (s *Server) InspectDatasetIdentity(ctx context.Context, request *InspectDatasetIdentityRequest) (*InspectDatasetIdentityResponse, error) {
	if !s.related(request.GetDataset()) {
		return nil, status.Error(codes.PermissionDenied, "dataset is outside the configured destination scope")
	}
	identity, err := s.backend.InspectDatasetIdentity(ctx, request.GetDataset())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &InspectDatasetIdentityResponse{Name: identity.Name, Type: string(identity.Type), Guid: identity.GUID, Pool: identity.Pool, PoolGuid: identity.PoolGUID}, nil
}

// InspectState returns bounded receive and verification state.
func (s *Server) InspectState(ctx context.Context, request *InspectStateRequest) (*InspectStateResponse, error) {
	if !s.inside(request.GetDataset()) {
		return nil, status.Error(codes.PermissionDenied, "dataset is outside the configured destination scope")
	}
	state, err := s.backend.InspectState(ctx, request.GetDataset(), request.GetRecursive())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return encodeState(state), nil
}

// SetProperties applies validated reconciliation properties within scope.
func (s *Server) SetProperties(ctx context.Context, request *SetPropertiesRequest) (*SetPropertiesResponse, error) {
	if !s.inside(request.GetDataset()) {
		return nil, status.Error(codes.PermissionDenied, "dataset is outside the configured destination scope")
	}
	if err := s.backend.SetProperties(ctx, request.GetDataset(), maps.Clone(request.GetProperties())); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &SetPropertiesResponse{}, nil
}

// InheritProperty removes a received or local property layer within scope.
func (s *Server) InheritProperty(ctx context.Context, request *InheritPropertyRequest) (*InheritPropertyResponse, error) {
	if !s.inside(request.GetDataset()) {
		return nil, status.Error(codes.PermissionDenied, "dataset is outside the configured destination scope")
	}
	if err := s.backend.InheritProperty(ctx, request.GetDataset(), request.GetProperty()); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &InheritPropertyResponse{}, nil
}

type diagnosticBuffer struct {
	buffer    bytes.Buffer
	truncated bool
}

func (b *diagnosticBuffer) Write(data []byte) (int, error) {
	n := len(data)
	remaining := 64*1024 - b.buffer.Len()
	if remaining <= 0 {
		b.truncated = true
		return n, nil
	}
	if len(data) > remaining {
		data = data[:remaining]
		b.truncated = true
	}
	_, _ = b.buffer.Write(data)
	return n, nil
}

func (b *diagnosticBuffer) String() string {
	value := strings.TrimSpace(b.buffer.String())
	if b.truncated {
		value += " [truncated]"
	}
	return value
}

// Receive validates one header then streams bounded chunks into zfs receive.
func (s *Server) Receive(stream RemoteService_ReceiveServer) error {
	first, err := stream.Recv()
	if err != nil {
		return status.Error(codes.InvalidArgument, "receive options are required")
	}
	if first.GetOptions() == nil || len(first.GetData()) != 0 {
		return status.Error(codes.InvalidArgument, "first receive message must contain only options")
	}
	options := decodeReceive(first.GetOptions())
	if !s.inside(options.Root) {
		return status.Error(codes.PermissionDenied, "receive root differs from the configured destination scope")
	}
	args, err := zfs.ReceiveArguments(options)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	ctx := stream.Context()
	command := s.receive(ctx, args)
	input, err := command.StdinPipe()
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	var diagnostics diagnosticBuffer
	command.Stderr = &diagnostics
	if err := command.Start(); err != nil {
		_ = input.Close()
		return status.Error(codes.Internal, err.Error())
	}
	var received uint64
	for {
		chunk, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			break
		}
		if recvErr != nil {
			_ = input.Close()
			_ = command.Wait()
			return recvErr
		}
		if chunk.GetOptions() != nil || len(chunk.GetData()) == 0 || len(chunk.GetData()) > MaxStreamChunk {
			_ = input.Close()
			_ = command.Wait()
			return status.Error(codes.InvalidArgument, "invalid receive data chunk")
		}
		n, writeErr := input.Write(chunk.GetData())
		received += uint64(n)
		if writeErr != nil || n != len(chunk.GetData()) {
			_ = input.Close()
			_ = command.Wait()
			return status.Error(codes.Internal, "write receive stream: "+errorText(writeErr))
		}
	}
	closeErr := input.Close()
	waitErr := command.Wait()
	if err := errors.Join(closeErr, waitErr); err != nil {
		return status.Errorf(codes.Internal, "zfs receive: %v: %s", err, diagnostics.String())
	}
	return stream.SendAndClose(&ReceiveResponse{Bytes: received})
}

func errorText(err error) string {
	if err == nil {
		return "short write"
	}
	return err.Error()
}

func decodeReceive(value *ReceiveOptions) zfs.ReceiveOptions {
	return zfs.ReceiveOptions{Root: value.GetRoot(), Discard: zfs.ReceiveDiscard(value.GetDiscard()), Set: maps.Clone(value.GetSet()), Exclude: slices.Clone(value.GetExclude())}
}

func encodeState(state zfs.State) *InspectStateResponse {
	response := &InspectStateResponse{}
	for _, object := range state.Objects {
		response.Objects = append(response.Objects, &Object{Name: object.Name, Type: object.Type, Guid: object.GUID, Creation: object.Creation, CreateTxg: object.CreateTXG})
	}
	for _, property := range state.Properties {
		response.Properties = append(response.Properties, &Property{Dataset: property.Dataset, Name: property.Name, Value: property.Value, Source: string(property.Source)})
	}
	for dataset, values := range state.Received {
		for name, value := range values {
			response.Received = append(response.Received, &ReceivedProperty{Dataset: dataset, Name: name, Value: value})
		}
	}
	for name, value := range state.ResumeTokens {
		response.ResumeTokens = append(response.ResumeTokens, &NamedValue{Name: name, Value: value})
	}
	for name, values := range state.Clones {
		response.Clones = append(response.Clones, &NamedValues{Name: name, Values: slices.Clone(values)})
	}
	for name, values := range state.Holds {
		response.Holds = append(response.Holds, &NamedValues{Name: name, Values: slices.Clone(values)})
	}
	slices.SortFunc(response.Received, func(a, b *ReceivedProperty) int {
		return strings.Compare(a.GetDataset()+"\x00"+a.GetName(), b.GetDataset()+"\x00"+b.GetName())
	})
	slices.SortFunc(response.ResumeTokens, func(a, b *NamedValue) int { return strings.Compare(a.GetName(), b.GetName()) })
	slices.SortFunc(response.Clones, func(a, b *NamedValues) int { return strings.Compare(a.GetName(), b.GetName()) })
	slices.SortFunc(response.Holds, func(a, b *NamedValues) int { return strings.Compare(a.GetName(), b.GetName()) })
	return response
}
