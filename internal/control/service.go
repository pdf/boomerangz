// Package control exposes the daemon's versioned administrative gRPC API.
package control

import (
	"context"
	"sync"

	controlrpc "github.com/pdf/boomerangz/internal/control/rpc"
	"github.com/pdf/boomerangz/internal/daemonstate"
	"github.com/pdf/boomerangz/internal/lifecycle"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type runtime interface {
	ControlStatus() daemonstate.ControlSnapshot
	SubscribeStatus(context.Context) (daemonstate.Subscription, error)
	Trigger([]string) ([]string, error)
	Reconcile()
	Clean(context.Context, []string, bool, bool, bool, bool) ([]lifecycle.CleanPlan, error)
}

type reloadHandler struct {
	mu sync.RWMutex
	fn func(context.Context) (daemonstate.ReloadResult, error)
}

func (h *reloadHandler) call(ctx context.Context) (daemonstate.ReloadResult, error) {
	h.mu.RLock()
	fn := h.fn
	h.mu.RUnlock()
	if fn == nil {
		return daemonstate.ReloadResult{}, status.Error(codes.Unimplemented, "configuration reload is unavailable")
	}
	return fn(ctx)
}

type service struct {
	controlrpc.UnimplementedStatusServiceServer
	controlrpc.UnimplementedControlServiceServer
	runtime       runtime
	reloader      *reloadHandler
	reloadAllowed bool
	// drain is cancelled when the listener serving this service starts to
	// drain; calls with no natural end watch it and end early.
	drain context.Context
}

func toSnapshot(snapshot daemonstate.ControlSnapshot) *controlrpc.StatusSnapshot {
	result := &controlrpc.StatusSnapshot{Revision: snapshot.Revision, ObservedUnixNano: snapshot.Observed.UnixNano(), Generation: snapshot.Generation, ConfigGeneration: snapshot.ConfigGeneration}
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
		result.Jobs = append(result.Jobs, toJobStatus(event))
	}
	return result
}

func toJobStatus(event daemonstate.Event) *controlrpc.JobStatus {
	return &controlrpc.JobStatus{Pool: event.Pool, Job: event.Job, Dataset: event.Scope, Target: event.Target, State: event.State, Reason: event.Reason, ChangedUnixNano: event.At.UnixNano(), Pending: uint32(event.Pending), QueuePosition: uint32(event.Position), Bytes: event.Bytes, TotalBytes: event.TotalBytes, BytesPerSecond: event.BytesPerSecond, EtaNanoseconds: int64(event.ETA), TotalKnown: event.TotalKnown,
		RunId: event.RunID, Snapshot: event.Snapshot, Base: event.Base, Mode: event.Mode, Destination: event.Destination, Marker: event.Marker,
		Destroyed: event.Destroyed, DestroyedCount: uint32(event.DestroyedCount), ConfigGeneration: event.ConfigGeneration}
}

// toWatchResponse pairs an update's state with the transitions that produced
// it, so a watcher sees every transition even where the state has collapsed
// several of them into one row.
func toWatchResponse(update daemonstate.Update) *controlrpc.WatchStatusResponse {
	response := &controlrpc.WatchStatusResponse{Status: toSnapshot(update.State)}
	for _, event := range update.Transitions {
		response.Transitions = append(response.Transitions, toJobStatus(event))
	}
	return response
}

func (s *service) GetStatus(context.Context, *controlrpc.GetStatusRequest) (*controlrpc.GetStatusResponse, error) {
	return &controlrpc.GetStatusResponse{Status: toSnapshot(s.runtime.ControlStatus())}, nil
}

// WatchStatus sends one message per status update: the state at registration,
// then each change with the transitions that produced it. Every change reaches
// the subscription, so there is nothing to re-send while nothing moves; a
// vanished peer is noticed by transport keepalive, not by a periodic send.
func (s *service) WatchStatus(_ *controlrpc.WatchStatusRequest, stream controlrpc.StatusService_WatchStatusServer) error {
	// A watch never ends on its own, so a draining listener would otherwise
	// never finish draining.
	ctx := stream.Context()
	if s.drain != nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(ctx)
		defer cancel()
		defer context.AfterFunc(s.drain, cancel)()
	}
	ended := func() error {
		if streamErr := stream.Context().Err(); streamErr != nil {
			return streamErr
		}
		return status.Error(codes.Unavailable, errListenerRetired)
	}
	subscription, err := s.runtime.SubscribeStatus(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return ended()
		}
		return status.Error(codes.Internal, err.Error())
	}
	for {
		select {
		case update, ok := <-subscription.Updates():
			if !ok {
				if ctx.Err() != nil {
					return ended()
				}
				// The subscription's sequence is broken. End the stream rather
				// than continue with a gap a client could not see.
				return status.Errorf(codes.Aborted, "status watch ended: %v", subscription.Err())
			}
			if err := stream.Send(toWatchResponse(update)); err != nil {
				return err
			}
		case <-ctx.Done():
			return ended()
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

func (s *service) Reload(ctx context.Context, _ *controlrpc.ReloadRequest) (*controlrpc.ReloadResponse, error) {
	if !s.reloadAllowed {
		return nil, status.Error(codes.PermissionDenied, "configuration reload is available only on a local listener")
	}
	result, err := s.reloader.call(ctx)
	if err != nil {
		if status.Code(err) != codes.Unknown {
			return nil, err
		}
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return &controlrpc.ReloadResponse{Generation: result.Generation, Applied: result.Applied, RestartRequired: result.RestartRequired}, nil
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
