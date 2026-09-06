package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"reflect"
	"slices"
	"sync"
	"time"

	"github.com/pdf/boomerangz/internal/config"
	"github.com/pdf/boomerangz/internal/discovery"
	"github.com/pdf/boomerangz/internal/lifecycle"
	"github.com/pdf/boomerangz/internal/policy"
	replicationssh "github.com/pdf/boomerangz/internal/replication/ssh"
	"github.com/pdf/boomerangz/internal/transfer"
	"github.com/pdf/boomerangz/internal/zfs"
)

const defaultQueueCapacity = 1024

type backend interface {
	transfer.Backend
}

type remoteApplier struct {
	source       backend
	client       *replicationssh.Client
	setting      config.RemoteConfig
	installation string
}

func (a remoteApplier) Apply(ctx context.Context, request transfer.Request, report func(zfs.Progress)) (transfer.Result, error) {
	endpoint, err := replicationssh.OpenEndpoint(ctx, a.client, "zfs", a.setting.Endpoint)
	if err != nil {
		return transfer.Result{}, err
	}
	engine, err := transfer.NewRemote(a.source, endpoint.Executor, endpoint.Stream, a.installation)
	if err != nil {
		return transfer.Result{}, errors.Join(err, endpoint.Close())
	}
	result, applyErr := engine.Apply(ctx, request, report)
	return result, errors.Join(applyErr, endpoint.Close())
}

type roadState struct {
	coordinator *transfer.Roadwarrior
	request     transfer.Request
}

// Runtime owns one daemon's discovery coordinator, schedules, worker pools,
// recovery coordinators, and live lifecycle safety state.
type Runtime struct {
	config       config.Config
	backend      backend
	installation string
	gate         *lifecycle.Gate
	scanner      *discovery.Scanner
	scheduler    *Scheduler
	management   *Pool
	local        *Pool
	remote       *Pool
	localStream  transfer.Stream
	pending      *transfer.PendingSet
	remotes      map[string]*replicationssh.Client
	safety       *Safety
	status       *StatusStore
	logger       *slog.Logger
	now          func() time.Time

	mu             sync.Mutex
	known          map[string]bool
	active         map[string]bool
	recursive      map[string]bool
	policies       map[string]policy.Effective
	generation     *discovery.Generation
	roads          map[string]roadState
	retireFailures map[string]int
	delayed        map[string]time.Time
	dirty          map[string]bool
	delayContext   context.Context
	delayCancel    context.CancelFunc
	delayWait      sync.WaitGroup
	shuttingDown   bool
}

// New constructs an operational daemon around typed local ZFS execution.
func New(cfg config.Config, source backend, installation string, logger *slog.Logger) (*Runtime, error) {
	if source == nil || !lifecycle.ValidID(installation) {
		return nil, fmt.Errorf("daemon backend and installation identity are required")
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	stream, err := zfs.NewLocalStream("zfs")
	if err != nil {
		return nil, err
	}
	remoteNames := make([]string, 0, len(cfg.Remotes))
	clients := make(map[string]*replicationssh.Client, len(cfg.Remotes))
	for name, setting := range cfg.Remotes {
		client, clientErr := replicationssh.New("ssh", replicationssh.Config{
			Host: setting.Host, Port: setting.Port, User: setting.User, Root: setting.Root,
			IdentityFile: setting.IdentityFile, ShellPath: setting.SSHShellPath,
			ConnectTimeout: setting.ConnectTimeout.Duration,
		})
		if clientErr != nil {
			return nil, fmt.Errorf("remote %s: %w", name, clientErr)
		}
		remoteNames = append(remoteNames, name)
		clients[name] = client
	}
	slices.Sort(remoteNames)
	scanner, err := discovery.New(source, discovery.Options{Remotes: remoteNames})
	if err != nil {
		return nil, err
	}
	status := &StatusStore{}
	report := func(event Event) {
		status.Record(event)
		logger.Info("worker state", "pool", event.Pool, "job", event.Job, "scope", event.Scope, "target", event.Target, "state", event.State, "reason", event.Reason, "pending", event.Pending)
	}
	management, err := NewPool("management", cfg.Daemon.EffectiveManagementWorkers(), defaultQueueCapacity, report)
	if err != nil {
		return nil, err
	}
	local, err := NewPool("transfer", cfg.Daemon.LocalTransferWorkers, defaultQueueCapacity, report)
	if err != nil {
		return nil, err
	}
	remote, err := NewPool("transfer", cfg.Daemon.RemoteTransferWorkers, defaultQueueCapacity, report)
	if err != nil {
		return nil, err
	}
	sharedLocks := &keyLocks{}
	management.locks, local.locks, remote.locks = sharedLocks, sharedLocks, sharedLocks
	gate := &lifecycle.Gate{}
	runtime := &Runtime{
		config: cfg, backend: source, installation: installation,
		gate: gate, scanner: scanner, scheduler: NewScheduler(), management: management,
		local: local, remote: remote, localStream: stream, pending: &transfer.PendingSet{},
		remotes: clients, logger: logger, status: status, now: time.Now, known: make(map[string]bool), active: make(map[string]bool),
		recursive: make(map[string]bool), policies: make(map[string]policy.Effective),
		roads: make(map[string]roadState), retireFailures: make(map[string]int), delayed: make(map[string]time.Time), dirty: make(map[string]bool),
	}
	runtime.safety = newSafety(gate, source, clients, cfg.Remotes)
	return runtime, nil
}

func (r *Runtime) service() (*lifecycle.Service, error) {
	return lifecycle.NewService(r.backend, r.installation)
}

// Safety returns the live quiescence and target checker used by retirement.
func (r *Runtime) Safety() lifecycle.CleanSafety { return r.safety }

func (r *Runtime) retained() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := mapsKeys(r.known)
	slices.Sort(result)
	return result
}

func mapsKeys(values map[string]bool) []string {
	result := make([]string, 0, len(values))
	for key := range values {
		result = append(result, key)
	}
	return result
}

func ownedRoot(entry discovery.Entry, installation string) bool {
	owner, lineage := "", ""
	for _, row := range entry.Stored {
		if row.Dataset != entry.Dataset.Name || row.Source != zfs.SourceLocal {
			continue
		}
		switch row.Name {
		case lifecycle.OwnerProperty:
			if owner != "" {
				return false
			}
			owner = row.Value
		case lifecycle.LineageProperty:
			if lineage != "" {
				return false
			}
			lineage = row.Value
		}
	}
	return owner == installation && lifecycle.ValidID(lineage)
}

func actionableRoot(entry discovery.Entry, installation string) bool {
	owner := ""
	for _, row := range entry.Stored {
		if row.Dataset != entry.Dataset.Name || row.Name != lifecycle.OwnerProperty || row.Source != zfs.SourceLocal {
			continue
		}
		if owner != "" || !lifecycle.ValidID(row.Value) {
			return false
		}
		owner = row.Value
	}
	return owner == "" || owner == installation
}

func (r *Runtime) applyGeneration(generation *discovery.Generation) {
	if generation == nil {
		return
	}
	now := r.now().UTC()
	entries := generation.Entries()
	schedulable := slices.DeleteFunc(slices.Clone(entries), func(entry discovery.Entry) bool {
		return !actionableRoot(entry, r.installation)
	})
	active, _, err := r.scheduler.Update(schedulable, now)
	if err != nil {
		r.logger.Error("update snapshot schedules", "error", err)
		return
	}
	activeSet := make(map[string]bool, len(active))
	for _, name := range active {
		activeSet[name] = true
	}
	owned := make(map[string]discovery.Entry)
	for _, entry := range entries {
		if ownedRoot(entry, r.installation) {
			owned[entry.Dataset.Name] = entry
		}
	}
	r.mu.Lock()
	changed := make(map[string]bool)
	for _, name := range generation.Changed(r.generation) {
		changed[name] = true
	}
	r.generation = generation
	previousKnown := r.known
	previousActive := r.active
	r.known = make(map[string]bool)
	r.active = activeSet
	for _, name := range active {
		r.known[name] = true
		entry, exists := generation.Inspect(name)
		if exists {
			r.recursive[name] = entry.Policy.Send.Replicate
			r.policies[name] = entry.Policy.Clone()
		}
	}
	for name, entry := range owned {
		r.known[name] = true
		r.recursive[name] = entry.Policy.Send.Replicate
		r.policies[name] = entry.Policy.Clone()
	}
	r.mu.Unlock()
	for _, name := range active {
		if !previousActive[name] {
			_ = r.gate.SetEnabled(name, true)
		}
		if changed[name] {
			r.enqueueInactive(name, true)
		}
		if changed[name] {
			entry, _ := generation.Inspect(name)
			r.enqueueTransfers(name, entry.Policy, "")
		}
	}
	for name := range previousActive {
		if !activeSet[name] {
			r.deactivate(name)
		}
	}
	for name := range owned {
		if !activeSet[name] && (previousActive[name] || !previousKnown[name] || changed[name]) {
			if !previousActive[name] {
				r.deactivate(name)
			}
			r.enqueueInactive(name, false)
		}
	}
}

func (r *Runtime) deactivate(dataset string) {
	_ = r.gate.SetEnabled(dataset, false)
	r.management.RemoveScope(dataset)
	r.local.RemoveScope(dataset)
	r.remote.RemoveScope(dataset)
}

func (r *Runtime) enqueueInactive(dataset string, active bool) {
	id := fmt.Sprintf("inactive:%s:%t", dataset, active)
	_, err := r.management.Submit(Job{ID: id, Group: dataset, Scope: dataset, LockKey: dataset, StartState: "reconciling", Run: func(ctx context.Context) Outcome {
		if !active {
			if waitErr := r.gate.WaitQuiescent(ctx, dataset); waitErr != nil {
				return Outcome{State: "blocked", Reason: waitErr.Error()}
			}
		}
		service, serviceErr := r.service()
		if serviceErr != nil {
			return Outcome{State: "failed", Reason: serviceErr.Error()}
		}
		plan, reconcileErr := service.ReconcileInactive(ctx, dataset, active, r.now(), r.config.Daemon.InactiveGracePeriod.Duration, true)
		if reconcileErr != nil {
			return Outcome{State: "blocked", Reason: reconcileErr.Error()}
		}
		if !active && plan.Deadline != nil {
			if plan.Due {
				r.enqueueRetirement(dataset)
			} else {
				r.schedule("retire:"+dataset, *plan.Deadline, func() { r.enqueueRetirement(dataset) })
			}
		}
		return Outcome{State: "succeeded"}
	}})
	if err != nil {
		r.logger.Error("queue inactive reconciliation", "dataset", dataset, "error", err)
	}
}

func (r *Runtime) enqueueRetirement(dataset string) {
	r.mu.Lock()
	recursive := r.recursive[dataset]
	effective := r.policies[dataset].Clone()
	r.mu.Unlock()
	_, err := r.management.Submit(Job{ID: "retire:" + dataset, Group: dataset, Scope: dataset, LockKey: dataset, StartState: "retiring", Run: func(ctx context.Context) Outcome {
		service, serviceErr := r.service()
		if serviceErr != nil {
			return Outcome{State: "failed", Reason: serviceErr.Error()}
		}
		preview, retireErr := service.Retire(ctx, dataset, recursive, r.now(), r.config.Daemon.InactiveGracePeriod.Duration, false, r.safety)
		if retireErr == nil && preview.Eligible && len(preview.Clean.Blockers) == 0 {
			retireErr = r.retireTargets(ctx, dataset, recursive, effective)
		}
		plan := preview
		if retireErr == nil && preview.Eligible && len(preview.Clean.Blockers) == 0 {
			plan, retireErr = service.Retire(ctx, dataset, recursive, r.now(), r.config.Daemon.InactiveGracePeriod.Duration, true, r.safety)
		}
		if retireErr == nil && plan.Eligible && len(plan.Clean.Blockers) == 0 {
			r.mu.Lock()
			delete(r.retireFailures, dataset)
			delete(r.known, dataset)
			delete(r.recursive, dataset)
			delete(r.policies, dataset)
			r.mu.Unlock()
			r.scanner.Request()
			return Outcome{State: "succeeded"}
		}
		if retireErr == nil && !plan.Eligible {
			return Outcome{State: "scheduled", Reason: "retirement is not due"}
		}
		r.mu.Lock()
		r.retireFailures[dataset]++
		failures := r.retireFailures[dataset]
		r.mu.Unlock()
		delay, delayErr := transfer.DefaultRetryPolicy().Delay(failures, rand.Float64())
		if delayErr == nil {
			r.schedule("retire:"+dataset, r.now().Add(delay), func() { r.enqueueRetirement(dataset) })
		}
		reason := "retirement remains blocked"
		if retireErr != nil {
			reason = retireErr.Error()
		}
		return Outcome{State: "waiting-retry", Reason: reason}
	}})
	if err != nil {
		r.logger.Error("queue retirement", "dataset", dataset, "error", err)
	}
}

func (r *Runtime) enqueueSnapshot(schedule Schedule) {
	ticket, err := r.gate.Queue(context.Background(), schedule.Dataset, lifecycle.Management)
	if err != nil {
		return
	}
	dropped := func() {
		ticket.Finish()
		r.scheduler.Retry(schedule.Dataset, r.now().Add(time.Second))
	}
	job := Job{ID: "snapshot:" + schedule.Dataset, Group: schedule.Dataset, Scope: schedule.Dataset, LockKey: schedule.Dataset, StartState: "snapshotting", Drop: dropped}
	job.Run = func(context.Context) Outcome {
		defer ticket.Finish()
		if startErr := ticket.Start(); startErr != nil {
			return Outcome{State: "blocked", Reason: startErr.Error()}
		}
		deadline, deadlineErr := r.nextOwnedSnapshot(schedule.Dataset, schedule.Policy.Grid.Cadence())
		if deadlineErr != nil {
			r.scheduler.Retry(schedule.Dataset, r.now().Add(r.config.Daemon.ReconcileInterval.Duration))
			return Outcome{State: "failed", Reason: deadlineErr.Error()}
		}
		if !deadline.IsZero() && r.now().Before(deadline) {
			r.scheduler.Retry(schedule.Dataset, deadline)
			return Outcome{State: "scheduled", Reason: "existing owned snapshot sets the next deadline"}
		}
		service, serviceErr := r.service()
		if serviceErr != nil {
			return Outcome{State: "failed", Reason: serviceErr.Error()}
		}
		metadata, createErr := service.CreateSnapshot(ticket.Context(), schedule.Dataset, schedule.Policy.Send.Replicate, r.now(), schedule.Policy)
		if createErr != nil {
			r.scheduler.Retry(schedule.Dataset, r.now().Add(r.config.Daemon.ReconcileInterval.Duration))
			return Outcome{State: "failed", Reason: createErr.Error()}
		}
		completed := r.now()
		r.scheduler.Complete(schedule.Dataset, completed)
		r.enqueuePrune(schedule.Dataset, schedule.Policy)
		r.enqueueTransfers(schedule.Dataset, schedule.Policy, schedule.Dataset+"@"+metadata.Name())
		return Outcome{State: "succeeded"}
	}
	if added, submitErr := r.management.Submit(job); submitErr != nil || !added {
		dropped()
		if submitErr != nil {
			r.logger.Error("queue snapshot", "dataset", schedule.Dataset, "error", submitErr)
		}
	}
}

func (r *Runtime) nextOwnedSnapshot(dataset string, cadence time.Duration) (time.Time, error) {
	state, err := r.backend.InspectState(context.Background(), dataset, false)
	if err != nil {
		return time.Time{}, err
	}
	lineage, authoritative := rootLineage(state, dataset, r.installation)
	if !authoritative {
		// A fresh root has no authority until its first successful snapshot.
		return time.Time{}, nil
	}
	var latest time.Time
	for _, snapshot := range lifecycle.Snapshots(state, dataset) {
		metadata, ownershipErr := lifecycle.Ownership(snapshot, lineage)
		if ownershipErr == nil && metadata.Created.After(latest) {
			latest = metadata.Created
		}
	}
	if latest.IsZero() {
		return time.Time{}, nil
	}
	return latest.Add(cadence), nil
}

func rootLineage(state zfs.State, dataset, installation string) (string, bool) {
	lineage, err := lifecycle.RootAuthority(state, dataset, installation)
	return lineage, err == nil
}

func (r *Runtime) enqueuePrune(dataset string, effective policy.Effective) {
	ticket, err := r.gate.Queue(context.Background(), dataset, lifecycle.Management)
	if err != nil {
		return
	}
	job := Job{ID: "prune:" + dataset, Group: dataset, Scope: dataset, LockKey: dataset, StartState: "pruning", Drop: ticket.Finish}
	job.Run = func(context.Context) Outcome {
		defer ticket.Finish()
		if startErr := ticket.Start(); startErr != nil {
			return Outcome{State: "blocked", Reason: startErr.Error()}
		}
		service, serviceErr := r.service()
		if serviceErr != nil {
			return Outcome{State: "failed", Reason: serviceErr.Error()}
		}
		_, pruneErr := service.Prune(ticket.Context(), dataset, effective, true)
		if pruneErr != nil {
			return Outcome{State: "failed", Reason: pruneErr.Error()}
		}
		return Outcome{State: "succeeded"}
	}
	if added, submitErr := r.management.Submit(job); submitErr != nil || !added {
		ticket.Finish()
	}
}

func (r *Runtime) latestPending(dataset, snapshot string) (transfer.PendingSnapshot, error) {
	state, err := r.backend.InspectState(context.Background(), dataset, false)
	if err != nil {
		return transfer.PendingSnapshot{}, err
	}
	for _, object := range state.Objects {
		if object.Name == snapshot && object.Type == "snapshot" && object.CreateTXG > 0 {
			return transfer.PendingSnapshot{Name: snapshot, CreateTXG: object.CreateTXG}, nil
		}
	}
	return transfer.PendingSnapshot{}, fmt.Errorf("created snapshot is absent from source inventory")
}

func (r *Runtime) enqueueTransfers(dataset string, effective policy.Effective, snapshot string) {
	for _, target := range effective.Local {
		r.enqueueLocal(dataset, target, effective, snapshot)
	}
	for _, name := range effective.Remote {
		r.enqueueRemote(dataset, name, effective, snapshot)
	}
}

func (r *Runtime) enqueueLocal(dataset, target string, effective policy.Effective, snapshot string) {
	canonical := "local:" + target
	jobID := "local:" + dataset + ":" + target
	if snapshot != "" {
		pending, pendingErr := r.latestPending(dataset, snapshot)
		if pendingErr != nil {
			r.logger.Error("inventory new snapshot", "dataset", dataset, "error", pendingErr)
			return
		}
		if _, offerErr := r.pending.Offer(dataset, canonical, pending); offerErr != nil {
			r.logger.Error("coalesce local snapshot", "dataset", dataset, "target", target, "error", offerErr)
			return
		}
		r.markDirty(jobID)
	}
	ticket, err := r.gate.Queue(context.Background(), dataset, lifecycle.Transfer)
	if err != nil {
		return
	}
	job := Job{ID: jobID, Group: dataset, Scope: dataset, LockKey: canonical, StartState: "sending", Drop: ticket.Finish}
	job.Run = func(context.Context) Outcome {
		defer ticket.Finish()
		r.clearDirty(jobID)
		if startErr := ticket.Start(); startErr != nil {
			return Outcome{State: "blocked", Reason: startErr.Error()}
		}
		engine, engineErr := transfer.NewLocal(r.backend, r.localStream, r.installation)
		if engineErr != nil {
			return Outcome{State: "failed", Reason: engineErr.Error()}
		}
		requestSnapshot := ""
		pending, hasPending := r.pending.Peek(dataset, canonical)
		if hasPending {
			requestSnapshot = pending.Name
		}
		result, applyErr := engine.Apply(ticket.Context(), transfer.Request{Source: dataset, DestinationRoot: target, Snapshot: requestSnapshot, Policy: effective}, nil)
		if applyErr != nil {
			return Outcome{State: "blocked", Reason: applyErr.Error()}
		}
		if !result.Verified {
			return Outcome{State: "failed", Reason: "transfer was not verified"}
		}
		if hasPending && result.Plan.Snapshot == pending.Name {
			r.pending.Complete(dataset, canonical, pending)
		}
		r.enqueueDestinationPrune(dataset, effective, canonical)
		return Outcome{State: "succeeded"}
	}
	job.After = func(Outcome) {
		if r.isDirty(jobID) {
			r.enqueueLocal(dataset, target, effective, "")
		}
	}
	if added, submitErr := r.local.Submit(job); submitErr != nil || !added {
		ticket.Finish()
	}
}

func roadKey(dataset, remote string) string { return dataset + "\x00" + remote }

func (r *Runtime) road(dataset, remote string, effective policy.Effective) (roadState, error) {
	key := roadKey(dataset, remote)
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, found := r.roads[key]; found {
		if reflect.DeepEqual(existing.request.Policy, effective) {
			return existing, nil
		}
		delete(r.roads, key)
	}
	setting, found := r.config.Remotes[remote]
	if !found {
		return roadState{}, fmt.Errorf("remote %s is not configured", remote)
	}
	client := r.remotes[remote]
	request := transfer.Request{Source: dataset, DestinationRoot: setting.Root, Policy: effective.Clone(), Transport: "ssh", RemoteName: remote, CanonicalTarget: client.CanonicalTarget()}
	coordinator, err := transfer.NewRoadwarrior(remoteApplier{source: r.backend, client: client, setting: setting, installation: r.installation}, request, r.pending, transfer.DefaultRetryPolicy(), r.now, rand.Float64)
	if err != nil {
		return roadState{}, err
	}
	state := roadState{coordinator: coordinator, request: request}
	r.roads[key] = state
	return state, nil
}

func (r *Runtime) enqueueRemote(dataset, remote string, effective policy.Effective, snapshot string) {
	road, err := r.road(dataset, remote, effective)
	if err != nil {
		r.logger.Error("prepare remote recovery", "dataset", dataset, "remote", remote, "error", err)
		return
	}
	if snapshot != "" {
		pending, pendingErr := r.latestPending(dataset, snapshot)
		if pendingErr != nil {
			r.logger.Error("inventory new snapshot", "dataset", dataset, "error", pendingErr)
			return
		}
		if _, offerErr := road.coordinator.Offer(pending); offerErr != nil {
			r.logger.Error("coalesce remote snapshot", "dataset", dataset, "remote", remote, "error", offerErr)
			return
		}
		r.markDirty("remote:" + dataset + ":" + remote)
	}
	ticket, err := r.gate.Queue(context.Background(), dataset, lifecycle.Transfer)
	if err != nil {
		return
	}
	jobID := "remote:" + dataset + ":" + remote
	job := Job{ID: jobID, Group: dataset, Scope: dataset, LockKey: road.request.CanonicalTarget, StartState: "probing", Drop: ticket.Finish}
	job.Run = func(context.Context) Outcome {
		defer ticket.Finish()
		r.clearDirty(jobID)
		if startErr := ticket.Start(); startErr != nil {
			return Outcome{State: "blocked", Reason: startErr.Error()}
		}
		outcome, reconcileErr := road.coordinator.Reconcile(ticket.Context(), nil)
		if outcome.Status == "waiting-retry" && !outcome.NotBefore.IsZero() {
			r.schedule(jobID, outcome.NotBefore, func() { r.enqueueRemote(dataset, remote, effective, "") })
		}
		if reconcileErr != nil {
			return Outcome{State: outcome.Status, Reason: reconcileErr.Error()}
		}
		if outcome.Status == "succeeded" {
			r.enqueueDestinationPrune(dataset, effective, road.request.CanonicalTarget)
		}
		return Outcome{State: outcome.Status}
	}
	job.After = func(Outcome) {
		if r.isDirty(jobID) {
			r.enqueueRemote(dataset, remote, effective, "")
		}
	}
	if added, submitErr := r.remote.Submit(job); submitErr != nil || !added {
		ticket.Finish()
	}
}

func (r *Runtime) markDirty(key string) {
	r.mu.Lock()
	r.dirty[key] = true
	r.mu.Unlock()
}

func (r *Runtime) clearDirty(key string) {
	r.mu.Lock()
	delete(r.dirty, key)
	r.mu.Unlock()
}

func (r *Runtime) isDirty(key string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.dirty[key]
}

func (r *Runtime) schedule(key string, at time.Time, run func()) {
	r.mu.Lock()
	if r.shuttingDown || (!r.delayed[key].IsZero() && !at.Before(r.delayed[key])) {
		r.mu.Unlock()
		return
	}
	r.delayed[key] = at
	ctx := r.delayContext
	r.delayWait.Add(1)
	r.mu.Unlock()
	go func() {
		defer r.delayWait.Done()
		wait := time.Until(at)
		if wait < 0 {
			wait = 0
		}
		timer := time.NewTimer(wait)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		r.mu.Lock()
		if r.delayed[key] != at || r.shuttingDown {
			r.mu.Unlock()
			return
		}
		delete(r.delayed, key)
		r.mu.Unlock()
		run()
	}()
}

// Run serves until cancellation, then cancels streams, drops pending work, and
// lets already-running short management operations reconstruct their result.
func (r *Runtime) Run(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.delayContext, r.delayCancel = context.WithCancel(context.Background())
	managementCtx, managementCancel := context.WithCancel(context.Background())
	defer managementCancel()
	transferCtx, transferCancel := context.WithCancel(context.Background())
	defer transferCancel()
	if err := r.management.Start(managementCtx); err != nil {
		return err
	}
	if err := r.local.Start(transferCtx); err != nil {
		return err
	}
	if err := r.remote.Start(transferCtx); err != nil {
		return err
	}
	r.logger.Info("daemon started", "management_workers", r.config.Daemon.EffectiveManagementWorkers(), "local_transfer_workers", r.config.Daemon.LocalTransferWorkers, "remote_transfer_workers", r.config.Daemon.RemoteTransferWorkers)
	runCtx, runCancel := context.WithCancel(ctx)
	var loops sync.WaitGroup
	loops.Add(2)
	go func() {
		defer loops.Done()
		_ = r.scanner.Run(runCtx, r.config.Daemon.ReconcileInterval.Duration, r.retained, func(generation *discovery.Generation, scanErr error) {
			if scanErr != nil {
				if !errors.Is(scanErr, context.Canceled) {
					r.logger.Error("discovery failed", "error", scanErr)
				}
				return
			}
			r.logger.Info("discovery complete", "generation", generation.ID(), "datasets", len(generation.Entries()))
			r.applyGeneration(generation)
		})
	}()
	go func() {
		defer loops.Done()
		for {
			schedule, ok := r.scheduler.Next(runCtx)
			if !ok {
				return
			}
			r.enqueueSnapshot(schedule)
		}
	}()
	<-ctx.Done()
	r.mu.Lock()
	r.shuttingDown = true
	known := mapsKeys(r.known)
	r.mu.Unlock()
	r.delayCancel()
	runCancel()
	loops.Wait()
	for _, dataset := range known {
		_ = r.gate.SetEnabled(dataset, false)
	}
	r.management.DiscardPending()
	r.local.DiscardPending()
	r.remote.DiscardPending()
	transferCancel()
	r.management.Close()
	r.local.Close()
	r.remote.Close()
	r.local.Wait()
	r.remote.Wait()
	r.management.Wait()
	r.delayWait.Wait()
	r.logger.Info("daemon stopped")
	return nil
}

// Reconcile coalesces an explicit local discovery hint.
func (r *Runtime) Reconcile() { r.scanner.Request() }

// QueueStatus exposes pool pressure without implementing the Phase 7 API.
func (r *Runtime) QueueStatus() map[string]QueueSnapshot {
	return map[string]QueueSnapshot{"management": r.management.Snapshot(), "local_transfer": r.local.Snapshot(), "remote_transfer": r.remote.Snapshot()}
}

// Status returns the latest stable job transitions for control-plane consumers.
func (r *Runtime) Status() []Event { return r.status.Snapshot() }

// Ensure compile-time safety conformance.
var _ lifecycle.CleanSafety = (*Safety)(nil)
