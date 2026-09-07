package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"reflect"
	"slices"
	"strings"
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
	locks        *keyLocks
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
	lifecycleAdmin sync.Mutex
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
		remotes: clients, logger: logger, status: status, locks: sharedLocks, now: time.Now, known: make(map[string]bool), active: make(map[string]bool),
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

func (r *Runtime) enqueueSnapshot(schedule Schedule) bool {
	ticket, err := r.gate.Queue(context.Background(), schedule.Dataset, lifecycle.Management)
	if err != nil {
		return false
	}
	dropped := func() {
		ticket.Finish()
		if !schedule.Force {
			r.scheduler.Retry(schedule.Dataset, r.now().Add(time.Second))
		}
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
		if !schedule.Force && !deadline.IsZero() && r.now().Before(deadline) {
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
		if !r.enqueueTransfers(schedule.Dataset, schedule.Policy, schedule.Dataset+"@"+metadata.Name()) {
			return Outcome{State: "failed", Reason: "new snapshot could not be protected for every transfer target"}
		}
		r.enqueuePrune(schedule.Dataset, schedule.Policy)
		return Outcome{State: "succeeded"}
	}
	if added, submitErr := r.management.Submit(job); submitErr != nil || !added {
		dropped()
		if submitErr != nil {
			r.logger.Error("queue snapshot", "dataset", schedule.Dataset, "error", submitErr)
		}
		return false
	}
	return true
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

func (r *Runtime) protectAndCoalesce(dataset, snapshot, target string, recursive bool) error {
	pending, err := r.latestPending(dataset, snapshot)
	if err != nil {
		return err
	}
	service, err := r.service()
	if err != nil {
		return err
	}
	snapshots := []string{snapshot}
	if recursive {
		state, inspectErr := r.backend.InspectState(context.Background(), dataset, false)
		if inspectErr != nil {
			return inspectErr
		}
		_, component, found := strings.Cut(snapshot, "@")
		if !found {
			return fmt.Errorf("pending recursive snapshot has no component")
		}
		for _, object := range state.Objects {
			if object.Type == "snapshot" && strings.HasPrefix(object.Name, dataset+"/") && strings.HasSuffix(object.Name, "@"+component) {
				snapshots = append(snapshots, object.Name)
			}
		}
	}
	if _, err := service.ProtectSet(context.Background(), dataset, snapshots, target); err != nil {
		return err
	}
	coalesced, err := r.pending.Coalesce(dataset, target, pending)
	if err != nil {
		return err
	}
	if coalesced.ReleaseSuperseded {
		if err := r.releaseSupersededPending(dataset, target, coalesced.Superseded.Name); err != nil {
			r.logger.Warn("retain superseded pending recovery hold", "dataset", dataset, "target", target, "snapshot", coalesced.Superseded.Name, "error", err)
		}
	}
	return nil
}

func (r *Runtime) releaseSupersededPending(dataset, target, snapshot string) error {
	state, err := r.backend.InspectState(context.Background(), dataset, true)
	if err != nil {
		return err
	}
	lineage, err := lifecycle.RootAuthority(state, dataset, r.installation)
	if err != nil {
		return err
	}
	references, err := lifecycle.References(state, dataset, lineage)
	if err != nil {
		return err
	}
	for _, reference := range references {
		if reference.Target == target && reference.SnapshotName(dataset) == snapshot {
			service, serviceErr := r.service()
			if serviceErr != nil {
				return serviceErr
			}
			return service.ReleaseReference(context.Background(), dataset, reference)
		}
	}
	return fmt.Errorf("superseded pending recovery reference is missing")
}

func (r *Runtime) enqueueTransfers(dataset string, effective policy.Effective, snapshot string) bool {
	protected := true
	for _, target := range effective.Local {
		protected = r.enqueueLocal(dataset, target, effective, snapshot) && protected
	}
	for _, name := range effective.Remote {
		protected = r.enqueueRemote(dataset, name, effective, snapshot) && protected
	}
	return protected
}

func (r *Runtime) enqueueLocal(dataset, target string, effective policy.Effective, snapshot string) bool {
	canonical := "local:" + target
	jobID := "local:" + dataset + ":" + target
	if snapshot != "" {
		if pendingErr := r.protectAndCoalesce(dataset, snapshot, canonical, effective.Send.Replicate); pendingErr != nil {
			r.logger.Error("protect local pending snapshot", "dataset", dataset, "target", target, "error", pendingErr)
			return false
		}
		r.markDirty(jobID)
	}
	ticket, err := r.gate.Queue(context.Background(), dataset, lifecycle.Transfer)
	if err != nil {
		return snapshot == ""
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
		pending, hasPending := r.pending.Begin(dataset, canonical)
		completed := false
		if hasPending {
			defer func() { r.pending.End(dataset, canonical, pending, completed) }()
		}
		if hasPending {
			requestSnapshot = pending.Name
		}
		result, applyErr := engine.Apply(ticket.Context(), transfer.Request{Source: dataset, DestinationRoot: target, Snapshot: requestSnapshot, Policy: effective}, func(progress zfs.Progress) {
			r.recordProgress("transfer", jobID, dataset, canonical, progress)
		})
		if applyErr != nil {
			return Outcome{State: "blocked", Reason: applyErr.Error()}
		}
		if !result.Verified {
			return Outcome{State: "failed", Reason: "transfer was not verified"}
		}
		completed = hasPending && result.Plan.Snapshot == pending.Name
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
	return true
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

func (r *Runtime) enqueueRemote(dataset, remote string, effective policy.Effective, snapshot string) bool {
	road, err := r.road(dataset, remote, effective)
	if err != nil {
		r.logger.Error("prepare remote recovery", "dataset", dataset, "remote", remote, "error", err)
		return false
	}
	if snapshot != "" {
		if pendingErr := r.protectAndCoalesce(dataset, snapshot, road.request.CanonicalTarget, effective.Send.Replicate); pendingErr != nil {
			r.logger.Error("protect remote pending snapshot", "dataset", dataset, "remote", remote, "error", pendingErr)
			return false
		}
		r.markDirty("remote:" + dataset + ":" + remote)
	}
	ticket, err := r.gate.Queue(context.Background(), dataset, lifecycle.Transfer)
	if err != nil {
		return snapshot == ""
	}
	jobID := "remote:" + dataset + ":" + remote
	job := Job{ID: jobID, Group: dataset, Scope: dataset, LockKey: road.request.CanonicalTarget, StartState: "probing", Drop: ticket.Finish}
	job.Run = func(context.Context) Outcome {
		defer ticket.Finish()
		r.clearDirty(jobID)
		if startErr := ticket.Start(); startErr != nil {
			return Outcome{State: "blocked", Reason: startErr.Error()}
		}
		outcome, reconcileErr := road.coordinator.Reconcile(ticket.Context(), func(progress zfs.Progress) {
			r.recordProgress("transfer", jobID, dataset, road.request.CanonicalTarget, progress)
		})
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
	return true
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

func (r *Runtime) recordProgress(pool, job, dataset, target string, progress zfs.Progress) {
	event := Event{Pool: pool, Job: job, Scope: dataset, Target: target, State: "sending", At: r.now().UTC(), Bytes: progress.Bytes, TotalBytes: progress.Estimate.Bytes, BytesPerSecond: progress.BytesPerSecond, TotalKnown: progress.Estimate.Known}
	if progress.ETA != nil {
		event.ETA = *progress.ETA
	}
	r.status.Record(event)
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

// Trigger queues an immediate snapshot for each selected active root. Empty
// selection means every active root. Normal queue deduplication still applies.
func (r *Runtime) Trigger(datasets []string) ([]string, error) {
	r.mu.Lock()
	if r.shuttingDown {
		r.mu.Unlock()
		return nil, fmt.Errorf("daemon is shutting down")
	}
	if len(datasets) == 0 {
		datasets = mapsKeys(r.active)
	} else {
		datasets = slices.Clone(datasets)
	}
	r.mu.Unlock()
	slices.Sort(datasets)
	datasets = slices.Compact(datasets)
	schedules := make([]Schedule, 0, len(datasets))
	for _, dataset := range datasets {
		if err := zfs.ValidateDataset(dataset); err != nil {
			return nil, err
		}
		schedule, exists := r.scheduler.Lookup(dataset)
		if !exists {
			return nil, fmt.Errorf("dataset %s is not an active scheduling root", dataset)
		}
		schedule.Force = true
		schedules = append(schedules, schedule)
	}
	accepted := make([]string, 0, len(schedules))
	for _, schedule := range schedules {
		if r.enqueueSnapshot(schedule) {
			accepted = append(accepted, schedule.Dataset)
		}
	}
	return accepted, nil
}

// DatasetStatus is the detached daemon view used by the control API.
type DatasetStatus struct {
	Name         string
	Active       bool
	Recursive    bool
	NextSnapshot time.Time
}

// ControlSnapshot is one coherent-enough operational view. ZFS is not queried.
type ControlSnapshot struct {
	Revision   uint64
	Observed   time.Time
	Generation uint64
	Datasets   []DatasetStatus
	Queues     map[string]QueueSnapshot
	Jobs       []Event
}

// ControlStatus builds a cheap in-memory status snapshot.
func (r *Runtime) ControlStatus() ControlSnapshot {
	revision, jobs := r.status.SnapshotRevision()
	deadlines := r.scheduler.Entries()
	r.mu.Lock()
	names := mapsKeys(r.known)
	slices.Sort(names)
	result := ControlSnapshot{Revision: revision, Observed: r.now().UTC(), Queues: r.QueueStatus(), Jobs: jobs}
	if r.generation != nil {
		result.Generation = r.generation.ID()
	}
	for _, name := range names {
		result.Datasets = append(result.Datasets, DatasetStatus{Name: name, Active: r.active[name], Recursive: r.recursive[name], NextSnapshot: deadlines[name]})
	}
	r.mu.Unlock()
	return result
}

// WaitStatus waits for a worker transition after revision.
func (r *Runtime) WaitStatus(ctx context.Context, revision uint64) error {
	return r.status.Wait(ctx, revision)
}

// Clean runs explicit preview-first decommissioning inside the daemon's live
// lifecycle boundary. Apply never starts when any selected plan is blocked.
func (r *Runtime) Clean(ctx context.Context, names []string, recursive, all, destroy, apply bool) ([]lifecycle.CleanPlan, error) {
	r.lifecycleAdmin.Lock()
	defer r.lifecycleAdmin.Unlock()
	r.mu.Lock()
	shuttingDown := r.shuttingDown
	r.mu.Unlock()
	if shuttingDown {
		return nil, fmt.Errorf("daemon is shutting down")
	}
	if all && len(names) != 0 {
		return nil, fmt.Errorf("all cannot be combined with dataset names")
	}
	if !all && len(names) == 0 {
		return nil, fmt.Errorf("specify dataset names or explicit all")
	}
	if all {
		inventory, err := r.backend.ListDatasets(ctx)
		if err != nil {
			return nil, err
		}
		for _, dataset := range inventory {
			names = append(names, dataset.Name)
		}
		recursive = true
	}
	names = slices.Clone(names)
	slices.Sort(names)
	names = slices.Compact(names)
	var scopes []string
	for _, name := range names {
		if err := zfs.ValidateDataset(name); err != nil {
			return nil, err
		}
		covered := false
		if recursive {
			for _, root := range scopes {
				if strings.HasPrefix(name, root+"/") {
					covered = true
					break
				}
			}
		}
		if !covered {
			scopes = append(scopes, name)
		}
	}
	r.mu.Lock()
	lockNames := slices.Clone(scopes)
	for known := range r.known {
		for _, scope := range scopes {
			if strings.HasPrefix(known, scope+"/") || strings.HasPrefix(scope, known+"/") {
				lockNames = append(lockNames, known)
			}
		}
	}
	r.mu.Unlock()
	slices.Sort(lockNames)
	lockNames = slices.Compact(lockNames)
	releases := make([]func(), 0, len(lockNames))
	for _, name := range lockNames {
		release, lockErr := r.locks.acquire(ctx, name)
		if lockErr != nil {
			for index := len(releases) - 1; index >= 0; index-- {
				releases[index]()
			}
			return nil, lockErr
		}
		releases = append(releases, release)
	}
	defer func() {
		for index := len(releases) - 1; index >= 0; index-- {
			releases[index]()
		}
	}()
	service, err := lifecycle.NewCleanService(r.backend)
	if err != nil {
		return nil, err
	}
	options := lifecycle.CleanOptions{Recursive: recursive, DestroyOwnedSnapshots: destroy}
	plans := make([]lifecycle.CleanPlan, 0, len(scopes))
	blocked := false
	for _, name := range scopes {
		plan, planErr := service.Clean(ctx, name, options, false, r.safety)
		if planErr != nil {
			return plans, planErr
		}
		plans = append(plans, plan)
		blocked = blocked || len(plan.Blockers) != 0
	}
	if !apply {
		return plans, nil
	}
	if blocked {
		return plans, fmt.Errorf("clean blocked; no changes applied")
	}
	for index, name := range scopes {
		plan, planErr := service.Clean(ctx, name, options, true, r.safety)
		plans[index] = plan
		if planErr != nil {
			return plans, planErr
		}
	}
	r.Reconcile()
	return plans, nil
}

// QueueStatus exposes detached worker-pool pressure.
func (r *Runtime) QueueStatus() map[string]QueueSnapshot {
	return map[string]QueueSnapshot{"management": r.management.Snapshot(), "local_transfer": r.local.Snapshot(), "remote_transfer": r.remote.Snapshot()}
}

// Status returns the latest stable job transitions for control-plane consumers.
func (r *Runtime) Status() []Event { return r.status.Snapshot() }

// Ensure compile-time safety conformance.
var _ lifecycle.CleanSafety = (*Safety)(nil)
