package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math/rand/v2"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/pdf/boomerangz/internal/config"
	"github.com/pdf/boomerangz/internal/daemonstate"
	"github.com/pdf/boomerangz/internal/discovery"
	"github.com/pdf/boomerangz/internal/lifecycle"
	"github.com/pdf/boomerangz/internal/policy"
	"github.com/pdf/boomerangz/internal/transfer"
	"github.com/pdf/boomerangz/internal/zfs"
)

const (
	defaultQueueCapacity        = 1024
	transientTransferRetryDelay = time.Second
)

type backend interface {
	transfer.Backend
}

type remoteApplier struct {
	source       backend
	client       remoteClient
	installation string
	lifecycle    *lifecycle.Service
}

func (a remoteApplier) Apply(ctx context.Context, request transfer.Request, report func(zfs.Progress)) (transfer.Result, error) {
	endpoint, err := a.client.Open(ctx)
	if err != nil {
		return transfer.Result{}, err
	}
	engine, err := transfer.NewRemoteWithService(a.source, endpoint.executor, endpoint.stream, a.installation, a.lifecycle)
	if err != nil {
		return transfer.Result{}, errors.Join(err, endpoint.close())
	}
	result, applyErr := engine.Apply(ctx, request, report)
	return result, errors.Join(applyErr, endpoint.close())
}

type roadState struct {
	coordinator *transfer.Roadwarrior
	request     transfer.Request
}

func reportWorkerState(logger *slog.Logger, status *StatusStore, event Event) {
	status.Record(event)
	args := []any{"pool", event.Pool, "job", event.Job, "scope", event.Scope, "target", event.Target, "state", event.State, "reason", event.Reason, "pending", event.Pending}
	switch event.State {
	case "failed":
		logger.Error("worker state", args...)
	case "blocked", "waiting-retry":
		logger.Warn("worker state", args...)
	default:
		logger.Info("worker state", args...)
	}
}

func blockedOrCancelled(err error) Outcome {
	if errors.Is(err, context.Canceled) {
		return Outcome{State: "cancelled", Reason: err.Error()}
	}
	return Outcome{State: "blocked", Reason: err.Error()}
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
	remotes      map[string]remoteClient
	safety       *Safety
	locks        *keyLocks
	status       *StatusStore
	logger       *slog.Logger
	lifecycle    *lifecycle.Service
	now          func() time.Time
	reloadMu     sync.Mutex

	mu               sync.Mutex
	known            map[string]bool
	active           map[string]bool
	recursive        map[string]bool
	policies         map[string]policy.Effective
	generation       *discovery.Generation
	roads            map[string]roadState
	retireFailures   map[string]int
	delayed          map[string]time.Time
	dirty            map[string]bool
	delayContext     context.Context
	delayCancel      context.CancelFunc
	delayWait        sync.WaitGroup
	shuttingDown     bool
	configGeneration uint64
	lifecycleAdmin   sync.Mutex
}

// New constructs an operational daemon around typed local ZFS execution.
func New(cfg config.Config, source backend, installation string, logger *slog.Logger) (*Runtime, error) {
	stream, err := zfs.NewLocalStream("zfs")
	if err != nil {
		return nil, err
	}
	return NewWithLocalStream(cfg, source, installation, logger, stream)
}

// NewWithLocalStream constructs a daemon with an explicit local transfer
// stream. It permits embedders and guarded integration tests to wrap stream
// execution without changing the typed ZFS query backend.
func NewWithLocalStream(cfg config.Config, source backend, installation string, logger *slog.Logger, stream transfer.Stream) (*Runtime, error) {
	if source == nil || !lifecycle.ValidID(installation) {
		return nil, fmt.Errorf("daemon backend and installation identity are required")
	}
	if stream == nil {
		return nil, fmt.Errorf("daemon local transfer stream is required")
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	lifecycleService, err := lifecycle.NewService(source, installation)
	if err != nil {
		return nil, err
	}
	remoteNames := make([]string, 0, len(cfg.Remotes))
	clients, err := buildRemoteClients(cfg.Remotes, cfg.Paths.CredentialsDir)
	if err != nil {
		return nil, err
	}
	for name := range cfg.Remotes {
		remoteNames = append(remoteNames, name)
	}
	slices.Sort(remoteNames)
	scanner, err := discovery.New(source, discovery.Options{Remotes: remoteNames})
	if err != nil {
		return nil, err
	}
	status := &StatusStore{}
	report := func(event Event) {
		reportWorkerState(logger, status, event)
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
	liveConfig := cfg.Clone()
	runtime := &Runtime{
		config: liveConfig, backend: source, installation: installation,
		gate: gate, scanner: scanner, scheduler: NewScheduler(), management: management,
		local: local, remote: remote, localStream: stream, pending: &transfer.PendingSet{},
		remotes: clients, logger: logger, lifecycle: lifecycleService, status: status, locks: sharedLocks, now: time.Now, known: make(map[string]bool), active: make(map[string]bool),
		recursive: make(map[string]bool), policies: make(map[string]policy.Effective),
		roads: make(map[string]roadState), retireFailures: make(map[string]int), delayed: make(map[string]time.Time), dirty: make(map[string]bool), configGeneration: 1,
	}
	runtime.safety = newSafety(gate, source, clients, liveConfig.Remotes)
	return runtime, nil
}

// CurrentConfig returns a detached snapshot of the live configuration.
func (r *Runtime) CurrentConfig() config.Config {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.config.Clone()
}

func (r *Runtime) daemonConfig() config.DaemonConfig {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.config.Daemon
}

func configChanges(previous, next config.Config) (applied, restart []string) {
	fields := []struct {
		name    string
		old     any
		new     any
		restart bool
	}{
		{"daemon.reconcile_interval", previous.Daemon.ReconcileInterval, next.Daemon.ReconcileInterval, false},
		{"daemon.inactive_grace_period", previous.Daemon.InactiveGracePeriod, next.Daemon.InactiveGracePeriod, false},
		{"daemon.management_workers", previous.Daemon.ManagementWorkers, next.Daemon.ManagementWorkers, false},
		{"daemon.local_transfer_workers", previous.Daemon.LocalTransferWorkers, next.Daemon.LocalTransferWorkers, false},
		{"daemon.remote_transfer_workers", previous.Daemon.RemoteTransferWorkers, next.Daemon.RemoteTransferWorkers, false},
		{"paths.credentials_dir", previous.Paths.CredentialsDir, next.Paths.CredentialsDir, false},
		{"paths.identity_dir", previous.Paths.IdentityDir, next.Paths.IdentityDir, true},
		{"paths.socket_path", previous.Paths.SocketPath, next.Paths.SocketPath, false},
		{"ssh_shell.replication_roots", previous.SSHShell.ReplicationRoots, next.SSHShell.ReplicationRoots, false},
		{"remotes", previous.Remotes, next.Remotes, false},
		{"listeners", previous.Listeners, next.Listeners, false},
	}
	for _, field := range fields {
		if reflect.DeepEqual(field.old, field.new) {
			continue
		}
		if field.restart {
			restart = append(restart, field.name)
		} else {
			applied = append(applied, field.name)
		}
	}
	return applied, restart
}

// preparedConfig contains all fallible daemon-side reload work. Its contents
// are immutable after preparation and safe to commit after listeners are ready.
type preparedConfig struct {
	effective      config.Config
	clients        map[string]remoteClient
	remotes        []string
	applied        []string
	restart        []string
	refreshRemotes bool
}

// prepareConfig validates a candidate and constructs every remote client before
// any live daemon state changes.
func (r *Runtime) prepareConfig(next config.Config) (*preparedConfig, error) {
	if err := next.Validate(); err != nil {
		return nil, err
	}
	previous := r.CurrentConfig()
	applied, restart := configChanges(previous, next)
	effective := next.Clone()
	effective.Paths.IdentityDir = previous.Paths.IdentityDir
	clients, err := buildRemoteClients(effective.Remotes, effective.Paths.CredentialsDir)
	if err != nil {
		return nil, err
	}
	remoteNames := make([]string, 0, len(effective.Remotes))
	for name := range effective.Remotes {
		remoteNames = append(remoteNames, name)
	}
	slices.Sort(remoteNames)
	return &preparedConfig{
		effective:      effective,
		clients:        clients,
		remotes:        remoteNames,
		applied:        applied,
		restart:        restart,
		refreshRemotes: !reflect.DeepEqual(previous.Remotes, effective.Remotes) || previous.Paths.CredentialsDir != effective.Paths.CredentialsDir,
	}, nil
}

// commitConfig publishes a fully prepared configuration without fallible I/O.
func (r *Runtime) commitConfig(prepared *preparedConfig) daemonstate.ReloadResult {
	effective := prepared.effective
	_ = r.management.Resize(effective.Daemon.EffectiveManagementWorkers())
	_ = r.local.Resize(effective.Daemon.LocalTransferWorkers)
	_ = r.remote.Resize(effective.Daemon.RemoteTransferWorkers)
	_ = r.scanner.Reconfigure(effective.Daemon.ReconcileInterval.Duration, prepared.remotes)
	r.remote.DiscardPending()
	type remoteReconcile struct {
		dataset string
		remote  string
		policy  policy.Effective
	}
	var reconciles []remoteReconcile
	r.mu.Lock()
	r.config = effective.Clone()
	r.remotes = prepared.clients
	r.roads = make(map[string]roadState)
	if prepared.refreshRemotes {
		for key := range r.delayed {
			if strings.HasPrefix(key, "remote:") {
				delete(r.delayed, key)
			}
		}
		for key := range r.dirty {
			if strings.HasPrefix(key, "remote:") {
				delete(r.dirty, key)
			}
		}
		for dataset := range r.active {
			effectivePolicy := r.policies[dataset].Clone()
			for _, remote := range effectivePolicy.Remote {
				if prepared.clients[remote] == nil {
					continue
				}
				reconciles = append(reconciles, remoteReconcile{dataset: dataset, remote: remote, policy: effectivePolicy})
			}
		}
	}
	r.safety.setTargets(&TargetChecker{local: r.backend, remotes: prepared.clients, settings: effective.Remotes})
	r.configGeneration++
	generation := r.configGeneration
	r.mu.Unlock()
	for _, reconcile := range reconciles {
		jobID := "remote:" + reconcile.dataset + ":" + reconcile.remote
		r.markDirty(jobID)
		if !r.enqueueRemote(reconcile.dataset, reconcile.remote, reconcile.policy, "") {
			r.clearDirty(jobID)
		}
	}
	r.scanner.Request()
	r.status.Record(Event{Pool: "configuration", Job: "config:reload", State: "succeeded", At: r.now().UTC()})
	return daemonstate.ReloadResult{Generation: generation, Applied: slices.Clone(prepared.applied), RestartRequired: slices.Clone(prepared.restart)}
}

// ReloadConfig serializes preparation, listener replacement, and publication
// as one live configuration transaction.
func (r *Runtime) ReloadConfig(next config.Config, reloadListeners func(config.Config) error) (daemonstate.ReloadResult, error) {
	r.reloadMu.Lock()
	defer r.reloadMu.Unlock()
	r.mu.Lock()
	shuttingDown := r.shuttingDown
	r.mu.Unlock()
	if shuttingDown {
		return daemonstate.ReloadResult{}, fmt.Errorf("daemon is shutting down")
	}
	prepared, err := r.prepareConfig(next)
	if err != nil {
		return daemonstate.ReloadResult{}, err
	}
	if reloadListeners != nil {
		if err := reloadListeners(prepared.effective.Clone()); err != nil {
			return daemonstate.ReloadResult{}, err
		}
	}
	return r.commitConfig(prepared), nil
}

// ApplyConfig validates and atomically publishes daemon-owned live settings.
// identity_dir is retained and reported because changing the installation and
// authorization identity is an administrative migration, not a live reload.
func (r *Runtime) ApplyConfig(next config.Config) (daemonstate.ReloadResult, error) {
	return r.ReloadConfig(next, nil)
}

func (r *Runtime) service() (*lifecycle.Service, error) {
	return r.lifecycle, nil
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

func discoveryChangeRequiresReconcile(current discovery.Entry, currentExists bool, previous discovery.Entry, previousExists bool) bool {
	if !currentExists || !previousExists {
		return true
	}
	stripReferences := func(entry discovery.Entry) discovery.Entry {
		entry.Stored = slices.DeleteFunc(slices.Clone(entry.Stored), func(property zfs.Property) bool {
			return strings.HasPrefix(property.Name, lifecycle.ReferencePrefix)
		})
		return entry
	}
	return !reflect.DeepEqual(stripReferences(current), stripReferences(previous))
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
	// Enable lifecycle admission before Update publishes immediately due work
	// to the scheduler loop. Queue failures remain retryable as a backstop.
	for _, entry := range schedulable {
		if isSchedulable(entry) {
			_ = r.gate.SetEnabled(entry.Dataset.Name, true)
		}
	}
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
		current, currentExists := generation.Inspect(name)
		var previous discovery.Entry
		previousExists := false
		if r.generation != nil {
			previous, previousExists = r.generation.Inspect(name)
		}
		if discoveryChangeRequiresReconcile(current, currentExists, previous, previousExists) {
			changed[name] = true
		}
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
		if changed[name] {
			r.enqueueInactive(name, true)
		}
		if changed[name] {
			entry, _ := generation.Inspect(name)
			deadline, deadlineErr := r.nextOwnedSnapshot(name, entry.Policy.Grid.Cadence())
			if deadlineErr != nil {
				r.logger.Error("inspect existing transfer work", "dataset", name, "error", deadlineErr)
			} else if !deadline.IsZero() {
				r.enqueueTransfers(name, entry.Policy, "")
			}
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
			if waitErr := r.gate.WaitScopeQuiescent(ctx, dataset); waitErr != nil {
				return Outcome{State: "blocked", Reason: waitErr.Error()}
			}
		}
		service, serviceErr := r.service()
		if serviceErr != nil {
			return Outcome{State: "failed", Reason: serviceErr.Error()}
		}
		plan, reconcileErr := service.ReconcileInactive(ctx, dataset, active, r.now(), r.daemonConfig().InactiveGracePeriod.Duration, true)
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
		grace := r.daemonConfig().InactiveGracePeriod.Duration
		preview, retireErr := service.Retire(ctx, dataset, recursive, r.now(), grace, false, r.safety)
		if retireErr == nil && preview.Eligible && len(preview.Clean.Blockers) == 0 {
			retireErr = r.retireTargets(ctx, dataset, recursive, effective)
		}
		plan := preview
		if retireErr == nil && preview.Eligible && len(preview.Clean.Blockers) == 0 {
			plan, retireErr = service.Retire(ctx, dataset, recursive, r.now(), grace, true, r.safety)
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
		if !schedule.Force {
			r.scheduler.Retry(schedule.Dataset, r.now().Add(r.daemonConfig().ReconcileInterval.Duration))
		}
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
			return blockedOrCancelled(startErr)
		}
		deadline, deadlineErr := r.nextOwnedSnapshot(schedule.Dataset, schedule.Policy.Grid.Cadence())
		if deadlineErr != nil {
			r.scheduler.Retry(schedule.Dataset, r.now().Add(r.daemonConfig().ReconcileInterval.Duration))
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
			r.scheduler.Retry(schedule.Dataset, r.now().Add(r.daemonConfig().ReconcileInterval.Duration))
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
			return blockedOrCancelled(startErr)
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

func nearestReceiveLockScope(root, mapped string, inventory []zfs.Dataset) string {
	nearest := ""
	for _, dataset := range inventory {
		insideRoot := dataset.Name == root || strings.HasPrefix(dataset.Name, root+"/")
		containsMapped := mapped == dataset.Name || strings.HasPrefix(mapped, dataset.Name+"/")
		if insideRoot && containsMapped && len(dataset.Name) > len(nearest) {
			nearest = dataset.Name
		}
	}
	return nearest
}

func missingMappedAncestor(dataset, target, mapped string, inventory []zfs.Dataset, active map[string]bool, policies map[string]policy.Effective) string {
	exists := make(map[string]bool, len(inventory))
	for _, entry := range inventory {
		exists[entry.Name] = true
	}
	for ancestor := dataset; strings.Contains(ancestor, "/"); {
		ancestor = ancestor[:strings.LastIndexByte(ancestor, '/')]
		if !active[ancestor] {
			continue
		}
		ancestorPolicy, ok := policies[ancestor]
		if !ok || !slices.Contains(ancestorPolicy.Local, target) {
			continue
		}
		ancestorMapped, err := zfs.MapReceiveDataset(ancestor, target, zfs.ReceiveDiscard(ancestorPolicy.Discard))
		if err != nil || ancestorMapped == mapped || !strings.HasPrefix(mapped, ancestorMapped+"/") {
			continue
		}
		if !exists[ancestorMapped] {
			return ancestorMapped
		}
	}
	return ""
}

func (r *Runtime) missingLocalDestinationAncestor(ctx context.Context, dataset, target, mapped string) (string, error) {
	inventory, err := r.backend.ListDatasets(ctx)
	if err != nil {
		return "", err
	}
	r.mu.Lock()
	active := maps.Clone(r.active)
	policies := maps.Clone(r.policies)
	r.mu.Unlock()
	return missingMappedAncestor(dataset, target, mapped, inventory, active, policies), nil
}

func (r *Runtime) enqueueLocal(dataset, target string, effective policy.Effective, snapshot string) bool {
	canonical := "local:" + target
	jobID := "local:" + dataset + ":" + target
	mapped, mapErr := zfs.MapReceiveDataset(dataset, target, zfs.ReceiveDiscard(effective.Discard))
	if mapErr != nil {
		r.logger.Error("map local transfer lock scope", "dataset", dataset, "target", target, "error", mapErr)
		return false
	}
	lockScope := ""
	if inventory, inventoryErr := r.backend.ListDatasets(context.Background()); inventoryErr != nil {
		r.logger.Warn("use exclusive local target lock", "dataset", dataset, "target", target, "error", inventoryErr)
	} else {
		lockScope = nearestReceiveLockScope(target, mapped, inventory)
	}
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
	job := Job{ID: jobID, Group: dataset, Scope: dataset, LockKey: canonical, LockScope: lockScope, StartState: "sending", Drop: ticket.Finish}
	job.Run = func(context.Context) Outcome {
		defer ticket.Finish()
		r.clearDirty(jobID)
		if startErr := ticket.Start(); startErr != nil {
			return blockedOrCancelled(startErr)
		}
		missingAncestor, ancestorErr := r.missingLocalDestinationAncestor(ticket.Context(), dataset, target, mapped)
		if ancestorErr != nil {
			return Outcome{State: "waiting-retry", Reason: "inspect destination hierarchy: " + ancestorErr.Error()}
		}
		if missingAncestor != "" {
			return Outcome{State: "waiting-retry", Reason: "waiting for destination ancestor " + missingAncestor}
		}
		engine, engineErr := transfer.NewLocalWithService(r.backend, r.localStream, r.installation, r.lifecycle)
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
			if errors.Is(applyErr, context.Canceled) {
				return blockedOrCancelled(applyErr)
			}
			var temporary interface{ Temporary() bool }
			if errors.As(applyErr, &temporary) && temporary.Temporary() {
				return Outcome{State: "waiting-retry", Reason: applyErr.Error()}
			}
			return Outcome{State: "blocked", Reason: applyErr.Error()}
		}
		if !result.Verified {
			return Outcome{State: "failed", Reason: "transfer was not verified"}
		}
		completed = hasPending && result.Plan.Snapshot == pending.Name
		r.enqueueDestinationPrune(dataset, effective, canonical, result.Plan.Destination)
		return Outcome{State: "succeeded"}
	}
	job.After = func(outcome Outcome) {
		if r.isDirty(jobID) {
			r.enqueueLocal(dataset, target, effective, "")
			return
		}
		if outcome.State == "waiting-retry" {
			r.schedule("retry:"+jobID, r.now().Add(transientTransferRetryDelay), func() {
				r.enqueueLocal(dataset, target, effective, "")
			})
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
	request := transfer.Request{Source: dataset, DestinationRoot: setting.Root, Policy: effective.Clone(), Transport: client.Transport(), RemoteName: remote, CanonicalTarget: client.CanonicalTarget()}
	coordinator, err := transfer.NewRoadwarrior(remoteApplier{source: r.backend, client: client, installation: r.installation, lifecycle: r.lifecycle}, request, r.pending, transfer.DefaultRetryPolicy(), r.now, rand.Float64)
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
	// Inside a backoff window an attempt would short-circuit on the deadline
	// without touching the transport. Running it anyway costs a transfer
	// worker and two status transitions every reconcile, and the last real
	// attempt already scheduled the retry for the deadline itself. Any newly
	// due snapshot is protected and coalesced above, so that retry carries it.
	if r.now().Before(road.coordinator.NotBefore()) {
		return true
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
			return blockedOrCancelled(startErr)
		}
		outcome, reconcileErr := road.coordinator.Reconcile(ticket.Context(), func(progress zfs.Progress) {
			r.recordProgress("transfer", jobID, dataset, road.request.CanonicalTarget, progress)
		})
		if outcome.Status == "waiting-retry" && !outcome.NotBefore.IsZero() {
			r.schedule(jobID, outcome.NotBefore, func() { r.enqueueRemote(dataset, remote, effective, "") })
		}
		if reconcileErr != nil {
			if errors.Is(reconcileErr, context.Canceled) {
				return blockedOrCancelled(reconcileErr)
			}
			return Outcome{State: outcome.Status, Reason: reconcileErr.Error()}
		}
		if outcome.Status == "succeeded" {
			r.enqueueDestinationPrune(dataset, effective, road.request.CanonicalTarget, "")
		}
		if outcome.Status == "waiting-retry" {
			// The backoff deadline has not passed, so nothing was attempted.
			// The state the last real attempt reported still stands.
			return Outcome{State: outcome.Status, Reason: outcome.Reason, Silent: true}
		}
		return Outcome{State: outcome.Status, Reason: outcome.Reason}
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
	settings := r.daemonConfig()
	r.logger.Info("daemon started", "management_workers", settings.EffectiveManagementWorkers(), "local_transfer_workers", settings.LocalTransferWorkers, "remote_transfer_workers", settings.RemoteTransferWorkers)
	runCtx, runCancel := context.WithCancel(ctx)
	var loops sync.WaitGroup
	loops.Add(2)
	go func() {
		defer loops.Done()
		_ = r.scanner.Run(runCtx, settings.ReconcileInterval.Duration, r.retained, func(generation *discovery.Generation, scanErr error) {
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

// DatasetStatus is the public control-plane view of one managed dataset.
type DatasetStatus = daemonstate.DatasetStatus

// ControlSnapshot is an immutable daemon status generation.
type ControlSnapshot = daemonstate.ControlSnapshot

// ControlStatus builds a cheap in-memory status snapshot.
func (r *Runtime) ControlStatus() ControlSnapshot {
	revision, jobs := r.status.SnapshotRevision()
	deadlines := r.scheduler.Entries()
	r.mu.Lock()
	names := mapsKeys(r.known)
	slices.Sort(names)
	result := ControlSnapshot{Revision: revision, Observed: r.now().UTC(), Queues: r.QueueStatus(), Jobs: jobs, ConfigGeneration: r.configGeneration}
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
