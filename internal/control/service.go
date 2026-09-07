// Package control exposes the daemon's versioned administrative gRPC API.
package control

import (
	"context"
	"time"

	controlrpc "github.com/pdf/boomerangz/internal/control/rpc"
	"github.com/pdf/boomerangz/internal/daemonstate"
	"github.com/pdf/boomerangz/internal/lifecycle"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type runtime interface {
	ControlStatus() daemonstate.ControlSnapshot
	WaitStatus(context.Context, uint64) error
	Trigger([]string) ([]string, error)
	Reconcile()
	Clean(context.Context, []string, bool, bool, bool, bool) ([]lifecycle.CleanPlan, error)
}

type service struct {
	controlrpc.UnimplementedStatusServiceServer
	controlrpc.UnimplementedControlServiceServer
	runtime runtime
}

func toSnapshot(snapshot daemonstate.ControlSnapshot) *controlrpc.StatusSnapshot {
	result := &controlrpc.StatusSnapshot{Revision: snapshot.Revision, ObservedUnixNano: snapshot.Observed.UnixNano(), Generation: snapshot.Generation}
	for _, dataset := range snapshot.Datasets {
		item := &controlrpc.DatasetStatus{Name: dataset.Name, Active: dataset.Active, Recursive: dataset.Recursive}
		if !dataset.NextSnapshot.IsZero() {
			item.NextSnapshotUnixNano = dataset.NextSnapshot.UnixNano()
		}
		result.Datasets = append(result.Datasets, item)
	}
	for name, queue := range snapshot.Queues {
		result.Queues = append(result.Queues, &controlrpc.QueueStatus{Name: name, Capacity: uint32(queue.Capacity), Pending: uint32(queue.Pending), JobIds: queue.IDs})
	}
	for _, event := range snapshot.Jobs {
		result.Jobs = append(result.Jobs, &controlrpc.JobStatus{Pool: event.Pool, Job: event.Job, Dataset: event.Scope, Target: event.Target, State: event.State, Reason: event.Reason, ChangedUnixNano: event.At.UnixNano(), Pending: uint32(event.Pending), QueuePosition: uint32(event.Position), Bytes: event.Bytes, TotalBytes: event.TotalBytes, BytesPerSecond: event.BytesPerSecond, EtaNanoseconds: int64(event.ETA), TotalKnown: event.TotalKnown})
	}
	return result
}

func (s *service) GetStatus(context.Context, *controlrpc.GetStatusRequest) (*controlrpc.GetStatusResponse, error) {
	return &controlrpc.GetStatusResponse{Status: toSnapshot(s.runtime.ControlStatus())}, nil
}

func (s *service) WatchStatus(request *controlrpc.WatchStatusRequest, stream controlrpc.StatusService_WatchStatusServer) error {
	interval := time.Duration(request.GetIntervalMilliseconds()) * time.Millisecond
	if interval == 0 {
		interval = 2 * time.Second
	}
	if interval < 100*time.Millisecond || interval > time.Hour {
		return status.Error(codes.InvalidArgument, "watch interval must be between 100ms and 1h")
	}
	for {
		snapshot := s.runtime.ControlStatus()
		if err := stream.Send(&controlrpc.WatchStatusResponse{Status: toSnapshot(snapshot)}); err != nil {
			return err
		}
		waitCtx, cancel := context.WithTimeout(stream.Context(), interval)
		err := s.runtime.WaitStatus(waitCtx, snapshot.Revision)
		cancel()
		if err != nil && stream.Context().Err() != nil {
			return stream.Context().Err()
		}
	}
}

func (s *service) ListDatasets(context.Context, *controlrpc.ListDatasetsRequest) (*controlrpc.ListDatasetsResponse, error) {
	snapshot := s.runtime.ControlStatus()
	result := &controlrpc.ListDatasetsResponse{Generation: snapshot.Generation}
	for _, dataset := range snapshot.Datasets {
		item := &controlrpc.DatasetStatus{Name: dataset.Name, Active: dataset.Active, Recursive: dataset.Recursive}
		if !dataset.NextSnapshot.IsZero() {
			item.NextSnapshotUnixNano = dataset.NextSnapshot.UnixNano()
		}
		result.Datasets = append(result.Datasets, item)
	}
	return result, nil
}

func (s *service) Trigger(_ context.Context, request *controlrpc.TriggerRequest) (*controlrpc.TriggerResponse, error) {
	accepted, err := s.runtime.Trigger(request.GetDatasets())
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return &controlrpc.TriggerResponse{Accepted: accepted}, nil
}

func (s *service) Reconcile(context.Context, *controlrpc.ReconcileRequest) (*controlrpc.ReconcileResponse, error) {
	s.runtime.Reconcile()
	return &controlrpc.ReconcileResponse{Accepted: true}, nil
}

func (s *service) Clean(ctx context.Context, request *controlrpc.CleanRequest) (*controlrpc.CleanResponse, error) {
	plans, cleanErr := s.runtime.Clean(ctx, request.GetDatasets(), request.GetRecursive(), request.GetAll(), request.GetDestroyOwnedSnapshots(), request.GetApply())
	response := &controlrpc.CleanResponse{}
	if cleanErr != nil {
		response.Error = cleanErr.Error()
	}
	for _, plan := range plans {
		item := &controlrpc.CleanPlan{Dataset: plan.Dataset, Recursive: plan.Options.Recursive, DestroyOwnedSnapshots: plan.Options.DestroyOwnedSnapshots, Blockers: plan.Blockers, Warnings: plan.Warnings, Applied: uint32(plan.Applied)}
		for _, action := range plan.Actions {
			item.Actions = append(item.Actions, &controlrpc.CleanAction{Operation: action.Operation, Object: action.Object, Property: action.Property, Guid: action.GUID})
		}
		response.Plans = append(response.Plans, item)
	}
	return response, nil
}
