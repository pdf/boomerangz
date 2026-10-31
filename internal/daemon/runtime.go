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
	"sync/atomic"
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
	// hostLocal is told when the remote turns out to receive into one of this
	// host's own pools.
	hostLocal func()
}

func (a remoteApplier) Apply(ctx context.Context, request transfer.Request, report transfer.Reporter) (transfer.Result, error) {
	endpoint, err := a.client.Open(ctx)
	if err != nil {
		return transfer.Result{}, err
	}
	engine, err := transfer.NewRemoteWithService(a.source, endpoint.executor, endpoint.stream, a.installation, a.lifecycle)
	if err != nil {
		return transfer.Result{}, errors.Join(err, endpoint.close())
	}
	result, applyErr := engine.Apply(ctx, request, report)
	var loopback *transfer.HostLocalError
	if errors.As(applyErr, &loopback) && a.hostLocal != nil {
		a.hostLocal()
	}
	return result, errors.Join(applyErr, endpoint.close())
}

type roadState struct {
	coordinator *transfer.Roadwarrior
	request     transfer.Request
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
	status       *Status
	sends        atomic.Uint64 // numbers each sending transition; see transferReporter
	logger       *slog.Logger
	lifecycle    *lifecycle.Service
	now          func() time.Time
	reloadMu     sync.Mutex

	mu         sync.Mutex
	known      map[string]bool
	active     map[string]bool
	recursive  map[string]bool
	policies   map[string]policy.Effective
	generation *discovery.Generation
	roads      map[string]roadState
	// hostLocal names the remotes found to receive into this host's own
	// pools. It is learned from transfers and forgotten when remotes change.
	hostLocal        map[string]bool
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
	scanner, err := discovery.New(source, discovery.Options{Remotes: remoteNames, Destinations: remoteDestinations(clients, nil)})
	if err != nil {
		return nil, err
	}
	// The log subscribes before any producer exists, so it holds every
	// transition from here on.
	status := NewStatus(time.Now)
	status.subscribeLog(logger)
	management, err := newStatusPool("management", "management", cfg.Daemon.EffectiveManagementWorkers(), defaultQueueCapacity, status)
	if err != nil {
		return nil, err
	}
	local, err := newStatusPool("transfer", "local_transfer", cfg.Daemon.LocalTransferWorkers, defaultQueueCapacity, status)
	if err != nil {
		return nil, err
	}
	remote, err := newStatusPool("transfer", "remote_transfer", cfg.Daemon.RemoteTransferWorkers, defaultQueueCapacity, status)
	if err != nil {
		return nil, err
	}
	sharedLocks := &keyLocks{}
	management.locks, local.locks, remote.locks = sharedLocks, sharedLocks, sharedLocks
	gate := &lifecycle.Gate{}
	liveConfig := cfg.Clone()
	scheduler := NewScheduler()
	scheduler.status = status
	runtime := &Runtime{
		config: liveConfig, backend: source, installation: installation,
		gate: gate, scanner: scanner, scheduler: scheduler, management: management,
		local: local, remote: remote, localStream: stream, pending: &transfer.PendingSet{},
		remotes: clients, logger: logger, lifecycle: lifecycleService, status: status, locks: sharedLocks, now: time.Now, known: make(map[string]bool), active: make(map[string]bool),
		recursive: make(map[string]bool), policies: make(map[string]policy.Effective),
		roads: make(map[string]roadState), hostLocal: make(map[string]bool), retireFailures: make(map[string]int), delayed: make(map[string]time.Time), dirty: make(map[string]bool), configGeneration: 1,
	}
	runtime.safety = newSafety(gate, source, clients, liveConfig.Remotes)
	for _, pool := range []*Pool{management, local, remote} {
		pool.queue.mu.Lock()
		pool.queue.observeLocked(queueChange{})
		pool.queue.mu.Unlock()
	}
	runtime.mu.Lock()
	runtime.publishRuntimeLocked()
	runtime.mu.Unlock()
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
	r.mu.Lock()
	unchanged := sameRemoteClients(r.remotes, clients)
	r.mu.Unlock()
	return &preparedConfig{
		effective:      effective,
		clients:        clients,
		remotes:        remoteNames,
		applied:        applied,
		restart:        restart,
		refreshRemotes: !unchanged,
	}, nil
}

// commitConfig publishes a fully prepared configuration without fallible I/O.
func (r *Runtime) commitConfig(prepared *preparedConfig) daemonstate.ReloadResult {
	effective := prepared.effective
	_ = r.management.Resize(effective.Daemon.EffectiveManagementWorkers())
	_ = r.local.Resize(effective.Daemon.LocalTransferWorkers)
	_ = r.remote.Resize(effective.Daemon.RemoteTransferWorkers)
	r.mu.Lock()
	if prepared.refreshRemotes {
		r.hostLocal = make(map[string]bool)
	}
	hostLocal := maps.Clone(r.hostLocal)
	r.mu.Unlock()
	_ = r.scanner.Reconfigure(effective.Daemon.ReconcileInterval.Duration, prepared.remotes, remoteDestinations(prepared.clients, hostLocal))
	// Queued remote work, and each remote's recovery coordinator with its
	// retry state, is bound to the client it was built with. A reload that
	// leaves every remote as it was keeps both; one that changes a remote
	// discards them and requeues every active dataset's remote work against
	// the new clients below.
	if prepared.refreshRemotes {
		r.remote.DiscardPending("remote configuration changed")
	}
	type remoteReconcile struct {
		dataset string
		remote  string
		policy  policy.Effective
	}
	var reconciles []remoteReconcile
	r.mu.Lock()
	r.config = effective.Clone()
	if prepared.refreshRemotes {
		r.remotes = prepared.clients
		r.roads = make(map[string]roadState)
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
	r.safety.setTargets(&TargetChecker{local: r.backend, remotes: r.remotes, settings: effective.Remotes})
	r.configGeneration++
	generation := r.configGeneration
	r.publishRuntimeLocked()
	r.mu.Unlock()
	for _, reconcile := range reconciles {
		jobID := "remote:" + reconcile.dataset + ":" + reconcile.remote
		r.markDirty(jobID)
		if !r.enqueueRemote(reconcile.dataset, reconcile.remote, reconcile.policy, nil) {
			r.clearDirty(jobID)
		}
	}
	r.scanner.Request()
	r.status.record(Event{Kind: EventTransition, RunID: nextRunID(), Pool: "configuration", Job: "config:reload", State: "succeeded", At: r.now().UTC(), Identity: daemonstate.Identity{ConfigGeneration: generation}})
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
	r.publishRuntimeLocked()
	r.mu.Unlock()
	for _, name := range active {
		if changed[name] {
			r.enqueueInactive(name, true)
		}
		if changed[name] {
			entry, _ := generation.Inspect(name)
			owned, ownedErr := r.hasOwnedSnapshot(name)
			if ownedErr != nil {
				r.logger.Error("inspect existing transfer work", "dataset", name, "error", ownedErr)
			} else if owned {
				r.enqueueTransfers(name, entry.Policy, nil)
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
	r.management.RemoveScope(dataset, "dataset deactivated")
	r.local.RemoveScope(dataset, "dataset deactivated")
	r.remote.RemoveScope(dataset, "dataset deactivated")
}

func (r *Runtime) enqueueInactive(dataset string, active bool) {
	id := fmt.Sprintf("inactive:%s:%t", dataset, active)
	_, err := r.management.Submit(Job{RunID: nextRunID(), ID: id, Group: dataset, Scope: dataset, LockKey: dataset, StartState: "reconciling", Run: func(ctx context.Context) Outcome {
		if !active {
			if waitErr := r.gate.WaitScopeQuiescent(ctx, dataset); waitErr != nil {
				return Outcome{State: "blocked", Reason: waitErr.Error()}
			}
		}
		service, serviceErr := r.service()
		if serviceErr != nil {
			return Outcome{State: "failed", Reason: serviceErr.Error()}
		}
		// The marker's time starts the grace period, so a concurrent change
		// is re-planned around at once rather than after a delay. A source
		// that keeps changing is retried later: a refusal on a state change
		// wrote nothing, and nothing else would run the job again.
		plan, reconcileErr := replan(func() (lifecycle.InactivePlan, error) {
			return service.ReconcileInactive(ctx, dataset, active, r.now(), r.daemonConfig().InactiveGracePeriod.Duration, true)
		})
		var changed *lifecycle.StateChangedError
		if errors.As(reconcileErr, &changed) {
			if delay, delayErr := transfer.DefaultRetryPolicy().Delay(1, rand.Float64()); delayErr == nil {
				r.schedule(id, r.now().Add(delay), func() { r.retryInactive(dataset, active) })
			}
			return Outcome{State: "waiting-retry", Reason: reconcileErr.Error()}
		}
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
		return Outcome{State: "succeeded", Identity: daemonstate.Identity{Marker: markerAction(plan)}}
	}})
	if err != nil {
		r.logger.Error("queue inactive reconciliation", "dataset", dataset, "error", err)
	}
}

// retryInactive runs an inactive reconciliation again, unless the dataset's
// activation has changed since: that change queued its own reconciliation,
// which a retry of the old one must not undo.
func (r *Runtime) retryInactive(dataset string, active bool) {
	r.mu.Lock()
	current := r.active[dataset]
	r.mu.Unlock()
	if current == active {
		r.enqueueInactive(dataset, active)
	}
}

// markerAction names what an inactive reconciliation did to the marker.
func markerAction(plan lifecycle.InactivePlan) string {
	switch {
	case !plan.Applied:
		return "none"
	case plan.Action == "set-inactive-marker":
		return "set"
	case plan.Action == "clear-inactive-marker":
		return "cleared"
	default:
		return plan.Action
	}
}

// destroyedSnapshots names the snapshots a clean plan destroyed.
func destroyedSnapshots(plan lifecycle.CleanPlan) daemonstate.Identity {
	var names []string
	for _, action := range plan.Actions {
		if action.Operation == "destroy-snapshot" {
			names = append(names, action.Object)
		}
	}
	return daemonstate.DestroyedIdentity(names)
}

func (r *Runtime) enqueueRetirement(dataset string) {
	r.mu.Lock()
	effective := r.policies[dataset].Clone()
	r.mu.Unlock()
	// Members hold snapshots of the root's lineage whatever the wire format,
	// so retiring a root always covers its whole subtree.
	const recursive = true
	_, err := r.management.Submit(Job{RunID: nextRunID(), ID: "retire:" + dataset, Group: dataset, Scope: dataset, LockKey: dataset, StartState: "retiring", Run: func(ctx context.Context) Outcome {
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
			r.publishRuntimeLocked()
			r.mu.Unlock()
			r.scanner.Request()
			return Outcome{State: "succeeded", Identity: destroyedSnapshots(plan.Clean)}
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
	job := Job{RunID: nextRunID(), ID: "snapshot:" + schedule.Dataset, Group: schedule.Dataset, Scope: schedule.Dataset, LockKey: schedule.Dataset, StartState: "snapshotting", Drop: dropped}
	job.Run = func(context.Context) Outcome {
		defer ticket.Finish()
		if startErr := ticket.Start(); startErr != nil {
			return blockedOrCancelled(startErr)
		}
		satisfied, latest, deadlineErr := r.satisfiedMembers(schedule)
		if deadlineErr != nil {
			r.scheduler.Retry(schedule.Dataset, r.now().Add(r.daemonConfig().ReconcileInterval.Duration))
			return Outcome{State: "failed", Reason: deadlineErr.Error()}
		}
		var due []string
		for _, name := range schedule.Due {
			if _, deferred := satisfied[name]; !deferred {
				due = append(due, name)
			}
		}
		if len(due) == 0 {
			r.scheduler.Complete(schedule.Dataset, nil, r.now(), satisfied)
			return Outcome{State: "scheduled", Reason: "existing owned snapshot sets the next deadline", Identity: daemonstate.Identity{Snapshot: latest}}
		}
		service, serviceErr := r.service()
		if serviceErr != nil {
			return Outcome{State: "failed", Reason: serviceErr.Error()}
		}
		retentions := memberRetentions(schedule, due)
		if schedule.Policy.Send.Replicate {
			// One -R stream needs the snapshot on every dataset ZFS holds
			// beneath the root now, including any created since discovery.
			subtree, subtreeErr := r.subtree(schedule.Dataset)
			if subtreeErr != nil {
				r.scheduler.Retry(schedule.Dataset, r.now().Add(r.daemonConfig().ReconcileInterval.Duration))
				return Outcome{State: "failed", Reason: subtreeErr.Error()}
			}
			due, retentions = subtree, nil
			for _, name := range subtree {
				retentions = append(retentions, lifecycle.Retention{Dataset: name, Grid: schedule.Policy.Grid})
			}
		}
		metadata, createErr := service.CreateSnapshot(ticket.Context(), schedule.Dataset, due, r.now(), schedule.Policy)
		if createErr != nil {
			r.scheduler.Retry(schedule.Dataset, r.now().Add(r.daemonConfig().ReconcileInterval.Duration))
			return Outcome{State: "failed", Reason: createErr.Error()}
		}
		r.scheduler.Complete(schedule.Dataset, due, r.now(), satisfied)
		snapshots := make([]string, 0, len(due))
		for _, name := range due {
			snapshots = append(snapshots, name+"@"+metadata.Name())
		}
		created := daemonstate.Identity{Snapshot: snapshots[0]}
		if slices.Contains(due, schedule.Dataset) {
			created.Snapshot = schedule.Dataset + "@" + metadata.Name()
			// Datasets excluded from the root are never snapshotted again, so
			// their remaining snapshots only need pruning on the root's own
			// cadence, not on every member's.
			retentions = append(retentions, r.excludedRetentions(schedule.Dataset)...)
		}
		if !r.enqueueTransfers(schedule.Dataset, schedule.Policy, snapshots) {
			return Outcome{State: "failed", Reason: "new snapshot could not be protected for every transfer target", Identity: created}
		}
		r.enqueuePrune(schedule.Dataset, schedule.Policy, retentions)
		return Outcome{State: "succeeded", Identity: created}
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

// subtree names the root and every filesystem and volume ZFS holds beneath it.
func (r *Runtime) subtree(root string) ([]string, error) {
	state, err := r.backend.InspectState(context.Background(), root, true)
	if err != nil {
		return nil, err
	}
	return lifecycle.Subtree(state, root), nil
}

// memberRetentions pairs each named member with its own grid.
func memberRetentions(schedule Schedule, names []string) []lifecycle.Retention {
	var retentions []lifecycle.Retention
	for _, member := range schedule.Members {
		if slices.Contains(names, member.Dataset) {
			retentions = append(retentions, member.Retention())
		}
	}
	return retentions
}

// excludedRetentions names the datasets inside root that are not members, with
// the grid each still resolves. They may hold snapshots of the root's lineage
// from before they were excluded.
func (r *Runtime) excludedRetentions(root string) []lifecycle.Retention {
	r.mu.Lock()
	generation := r.generation
	r.mu.Unlock()
	if generation == nil {
		return nil
	}
	var retentions []lifecycle.Retention
	for _, entry := range generation.Entries() {
		if entry.CoveredBy == root && !entry.Member && entry.Inspected && entry.Policy.Grid.Cadence() > 0 {
			retentions = append(retentions, lifecycle.Retention{Dataset: entry.Dataset.Name, Grid: entry.Policy.Grid})
		}
	}
	return retentions
}

// satisfiedMembers maps each due member whose newest owned snapshot already
// covers its own cadence to the deadline that snapshot sets, and names the
// newest owned snapshot of the root itself, or failing that of any member.
// Deadlines live only in memory, so this is what stops a restart from
// snapshotting every member at once. A forced schedule satisfies nothing.
func (r *Runtime) satisfiedMembers(schedule Schedule) (map[string]time.Time, string, error) {
	if schedule.Force {
		return nil, "", nil
	}
	state, err := r.backend.InspectState(context.Background(), schedule.Dataset, true)
	if err != nil {
		return nil, "", err
	}
	lineage, authoritative := rootLineage(state, schedule.Dataset, r.installation)
	if !authoritative {
		// A fresh root has no authority until its first successful snapshot.
		return nil, "", nil
	}
	newest := func(dataset string) (time.Time, string) {
		var (
			latest time.Time
			name   string
		)
		for _, snapshot := range lifecycle.Snapshots(state, dataset) {
			metadata, ownershipErr := lifecycle.Ownership(snapshot, lineage)
			if ownershipErr == nil && metadata.Created.After(latest) {
				latest, name = metadata.Created, snapshot.Name
			}
		}
		return latest, name
	}
	now := r.now()
	satisfied := make(map[string]time.Time)
	var (
		rootName, anyName string
		anyCreated        time.Time
	)
	for _, member := range schedule.Members {
		created, name := newest(member.Dataset)
		if created.IsZero() {
			continue
		}
		if member.Dataset == schedule.Dataset {
			rootName = name
		}
		if created.After(anyCreated) {
			anyCreated, anyName = created, name
		}
		if deadline := created.Add(member.Grid.Cadence()); now.Before(deadline) {
			satisfied[member.Dataset] = deadline
		}
	}
	if schedule.Policy.Send.Replicate {
		// One cadence, the root's, governs the whole -R subtree.
		if deadline, found := satisfied[schedule.Dataset]; found {
			for _, name := range schedule.Due {
				satisfied[name] = deadline
			}
		} else {
			clear(satisfied)
		}
	}
	if rootName == "" {
		rootName = anyName
	}
	return satisfied, rootName, nil
}

// hasOwnedSnapshot reports whether anything in the root's subtree already holds
// a snapshot of its lineage, which is what makes catch-up transfer work
// meaningful.
func (r *Runtime) hasOwnedSnapshot(root string) (bool, error) {
	state, err := r.backend.InspectState(context.Background(), root, true)
	if err != nil {
		return false, err
	}
	lineage, authoritative := rootLineage(state, root, r.installation)
	if !authoritative {
		return false, nil
	}
	for _, dataset := range lifecycle.Subtree(state, root) {
		for _, snapshot := range lifecycle.Snapshots(state, dataset) {
			if _, ownershipErr := lifecycle.Ownership(snapshot, lineage); ownershipErr == nil {
				return true, nil
			}
		}
	}
	return false, nil
}

func rootLineage(state zfs.State, dataset, installation string) (string, bool) {
	lineage, err := lifecycle.RootAuthority(state, dataset, installation)
	return lineage, err == nil
}

// enqueuePrune prunes the named datasets of one root in a single job, each
// against its own grid. They share the root's lock, so one job rather than one
// per member avoids the root contending with itself.
func (r *Runtime) enqueuePrune(dataset string, effective policy.Effective, retentions []lifecycle.Retention) {
	if len(retentions) == 0 {
		return
	}
	ticket, err := r.gate.Queue(context.Background(), dataset, lifecycle.Management)
	if err != nil {
		return
	}
	job := Job{RunID: nextRunID(), ID: "prune:" + dataset, Group: dataset, Scope: dataset, LockKey: dataset, StartState: "pruning", Drop: ticket.Finish}
	job.Run = func(context.Context) Outcome {
		defer ticket.Finish()
		if startErr := ticket.Start(); startErr != nil {
			return blockedOrCancelled(startErr)
		}
		service, serviceErr := r.service()
		if serviceErr != nil {
			return Outcome{State: "failed", Reason: serviceErr.Error()}
		}
		var destroyed []string
		for _, retention := range retentions {
			decisions, pruneErr := service.Prune(ticket.Context(), dataset, effective, retention, true)
			if pruneErr != nil {
				// A failed pass returns its plan, not what it destroyed, so
				// only completed passes are reported.
				return Outcome{State: "failed", Reason: pruneErr.Error(), Identity: daemonstate.DestroyedIdentity(destroyed)}
			}
			for _, decision := range decisions {
				if decision.Destroy {
					destroyed = append(destroyed, decision.Snapshot)
				}
			}
		}
		return Outcome{State: "succeeded", Identity: daemonstate.DestroyedIdentity(destroyed)}
	}
	if added, submitErr := r.management.Submit(job); submitErr != nil || !added {
		ticket.Finish()
	}
}

func (r *Runtime) latestPending(dataset, snapshot string) (transfer.PendingSnapshot, error) {
	// A tick may snapshot a member without the root, so the snapshot can sit
	// anywhere in the root's subtree.
	state, err := r.backend.InspectState(context.Background(), dataset, true)
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

// protectAndCoalesce holds a newly created snapshot for target under its own
// stream's proof and makes it that stream's pending work. A replication root's
// stream carries its whole subtree, so it holds the subtree's snapshots too.
func (r *Runtime) protectAndCoalesce(root, snapshot, target string, recursive bool) error {
	pending, err := r.latestPending(root, snapshot)
	if err != nil {
		return err
	}
	service, err := r.service()
	if err != nil {
		return err
	}
	stream, component, found := strings.Cut(snapshot, "@")
	if !found {
		return fmt.Errorf("pending snapshot has no component")
	}
	snapshots := []string{snapshot}
	if recursive {
		state, inspectErr := r.backend.InspectState(context.Background(), root, true)
		if inspectErr != nil {
			return inspectErr
		}
		for _, object := range state.Objects {
			if object.Type == "snapshot" && strings.HasPrefix(object.Name, root+"/") && strings.HasSuffix(object.Name, "@"+component) {
				snapshots = append(snapshots, object.Name)
			}
		}
	}
	if err := protectSet(service, root, stream, snapshots, target); err != nil {
		return err
	}
	coalesced, err := r.pending.Coalesce(stream, target, pending)
	if err != nil {
		return err
	}
	if coalesced.ReleaseSuperseded {
		if err := r.releaseSupersededPending(root, stream, target, coalesced.Superseded.Name); err != nil {
			r.logger.Warn("retain superseded pending recovery hold", "dataset", stream, "target", target, "snapshot", coalesced.Superseded.Name, "error", err)
		}
	}
	return nil
}

// replanAttempts bounds how many times a lifecycle operation re-plans around
// concurrent changes to its source. The daemon's own transfers write their
// source without the lifecycle lock - a first transfer records its target
// binding - so an operation can find the source changed between its read and
// its write, which refuses with a StateChangedError. Each attempt re-reads the
// source, and none waits.
const replanAttempts = 5

// replan runs operation until it does anything but refuse on a state change,
// at most replanAttempts times, and returns its last result.
func replan[T any](operation func() (T, error)) (T, error) {
	var (
		result T
		err    error
	)
	for range replanAttempts {
		var changed *lifecycle.StateChangedError
		if result, err = operation(); !errors.As(err, &changed) {
			return result, err
		}
	}
	return result, err
}

// protectSet places a reference and holds on snapshots for target, re-planning
// around concurrent changes. Nothing reschedules a new snapshot's protection,
// so leaving it to a later attempt would leave the snapshot unprotected.
func protectSet(service *lifecycle.Service, dataset, stream string, snapshots []string, target string) error {
	_, err := replan(func() (lifecycle.Reference, error) {
		return service.ProtectSet(context.Background(), dataset, stream, snapshots, target)
	})
	return err
}

func (r *Runtime) releaseSupersededPending(dataset, stream, target, snapshot string) error {
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
		if reference.Target == target && reference.Dataset == stream && reference.SnapshotName() == snapshot {
			service, serviceErr := r.service()
			if serviceErr != nil {
				return serviceErr
			}
			return service.ReleaseReference(context.Background(), dataset, reference)
		}
	}
	return fmt.Errorf("superseded pending recovery reference is missing")
}

// transferStreams lists, for each target of a root, the members that send
// there with their own policies, parents first. A target only the root sends
// to carries no member list, and neither does any target of a replication
// root: the root's own stream is then the whole transfer.
func (r *Runtime) transferStreams(root string, effective policy.Effective) (local, remote map[string][]transfer.Member) {
	local, remote = map[string][]transfer.Member{}, map[string][]transfer.Member{}
	add := func(member transfer.Member) {
		for _, target := range member.Policy.Local {
			local[target] = append(local[target], member)
		}
		for _, name := range member.Policy.Remote {
			remote[name] = append(remote[name], member)
		}
	}
	add(transfer.Member{Dataset: root, Policy: effective.Clone()})
	r.mu.Lock()
	generation := r.generation
	r.mu.Unlock()
	if !effective.Send.Replicate && generation != nil {
		if entry, found := generation.Inspect(root); found {
			for _, name := range entry.Members {
				if member, exists := generation.Inspect(name); exists && member.Member && member.CoveredBy == root {
					add(transfer.Member{Dataset: name, Policy: member.Policy.Clone()})
				}
			}
		}
	}
	for _, set := range []map[string][]transfer.Member{local, remote} {
		for target, members := range set {
			if len(members) == 1 && members[0].Dataset == root {
				set[target] = nil
			}
		}
	}
	return local, remote
}

// streamDatasets names the datasets a target's transfer sends from.
func streamDatasets(root string, members []transfer.Member) []string {
	if len(members) == 0 {
		return []string{root}
	}
	names := make([]string, 0, len(members))
	for _, member := range members {
		names = append(names, member.Dataset)
	}
	return names
}

// protectCreated protects each newly created snapshot that one of a target's
// streams sends from. A replication root's stream is protected through its
// root snapshot, which holds the subtree.
func (r *Runtime) protectCreated(root string, effective policy.Effective, members []transfer.Member, created []string, target string) error {
	streams := streamDatasets(root, members)
	for _, snapshot := range created {
		stream, _, _ := strings.Cut(snapshot, "@")
		if !slices.Contains(streams, stream) {
			continue
		}
		if err := r.protectAndCoalesce(root, snapshot, target, effective.Send.Replicate); err != nil {
			return err
		}
	}
	return nil
}

// enqueueTransfers queues each of a root's targets, after protecting the
// snapshots this tick created for the streams that send there.
func (r *Runtime) enqueueTransfers(dataset string, effective policy.Effective, created []string) bool {
	local, remote := r.transferStreams(dataset, effective)
	protected := true
	for _, target := range slices.Sorted(maps.Keys(local)) {
		protected = r.enqueueLocal(dataset, target, effective, created) && protected
	}
	for _, name := range slices.Sorted(maps.Keys(remote)) {
		protected = r.enqueueRemote(dataset, name, effective, created) && protected
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

func (r *Runtime) enqueueLocal(dataset, target string, effective policy.Effective, created []string) bool {
	canonical := "local:" + target
	local, _ := r.transferStreams(dataset, effective)
	members, configured := local[target]
	if !configured {
		return true
	}
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
	if len(created) > 0 {
		if pendingErr := r.protectCreated(dataset, effective, members, created, canonical); pendingErr != nil {
			r.logger.Error("protect local pending snapshot", "dataset", dataset, "target", target, "error", pendingErr)
			return false
		}
		r.markDirty(jobID)
	}
	ticket, err := r.gate.Queue(context.Background(), dataset, lifecycle.Transfer)
	if err != nil {
		return len(created) == 0
	}
	job := Job{RunID: nextRunID(), ID: jobID, Group: dataset, Scope: dataset, LockKey: canonical, LockScope: lockScope, StartState: "planning", Drop: ticket.Finish}
	job.Run = func(context.Context) Outcome {
		defer ticket.Finish()
		r.clearDirty(jobID)
		return r.withPending(dataset, canonical, r.runLocal(ticket, dataset, target, canonical, mapped, effective, members, job))
	}
	job.After = func(outcome Outcome) {
		if r.isDirty(jobID) {
			r.enqueueLocal(dataset, target, effective, nil)
			return
		}
		if outcome.State == "waiting-retry" {
			r.schedule("retry:"+jobID, r.now().Add(transientTransferRetryDelay), func() {
				r.enqueueLocal(dataset, target, effective, nil)
			})
		}
	}
	if added, submitErr := r.local.Submit(job); submitErr != nil || !added {
		ticket.Finish()
	}
	return true
}

// runLocal runs one local transfer of a root's streams to target. An outcome
// that follows a transfer names the snapshot it carried; withPending names the
// pending one for any other.
func (r *Runtime) runLocal(ticket *lifecycle.Ticket, dataset, target, canonical, mapped string, effective policy.Effective, members []transfer.Member, job Job) Outcome {
	if startErr := ticket.Start(); startErr != nil {
		return blockedOrCancelled(startErr)
	}
	engine, engineErr := transfer.NewLocalWithService(r.backend, r.localStream, r.installation, r.lifecycle)
	if engineErr != nil {
		return Outcome{State: "failed", Reason: engineErr.Error()}
	}
	request, reserved, beginErr := r.pending.BeginStreams(transfer.Request{Source: dataset, DestinationRoot: target, Policy: effective, Members: members}, canonical)
	if beginErr != nil {
		return Outcome{State: "blocked", Reason: beginErr.Error()}
	}
	var result transfer.Result
	defer func() { r.pending.FinishStreams(canonical, reserved, result) }()
	carried := daemonstate.Identity{}
	if len(reserved) > 0 {
		carried.Snapshot = reserved[0].Snapshot.Name
	}
	result, applyErr := engine.Apply(ticket.Context(), request, r.transferReporter(r.local, job))
	// Each stream that verified advanced its own proof whether or not the
	// others did, so its replica is pruned either way.
	if slices.ContainsFunc(transfer.StreamResults(result), func(stream transfer.Result) bool { return stream.Verified }) {
		r.enqueueDestinationPrune(dataset, effective, canonical, mapped, members)
	}
	if applyErr != nil {
		if errors.Is(applyErr, context.Canceled) {
			outcome := blockedOrCancelled(applyErr)
			outcome.Identity = carried
			return outcome
		}
		var temporary interface{ Temporary() bool }
		if errors.As(applyErr, &temporary) && temporary.Temporary() {
			return Outcome{State: "waiting-retry", Reason: applyErr.Error(), Identity: carried}
		}
		return Outcome{State: "blocked", Reason: applyErr.Error(), Identity: carried}
	}
	if !result.Verified {
		return Outcome{State: "failed", Reason: "transfer was not verified", Identity: carried}
	}
	return Outcome{State: "succeeded", Identity: daemonstate.Identity{Snapshot: result.Plan.Snapshot, Destination: result.Plan.Destination}}
}

// withPending names, on a waiting-retry, blocked, or cancelled outcome that
// carries no snapshot, the snapshot pending for the target when the run ended.
// It runs on the worker after the run, holding no pool lock. A job removed
// from its queue before running names none; see the design's chunk J.
func (r *Runtime) withPending(dataset, canonical string, outcome Outcome) Outcome {
	if (outcome.State == "waiting-retry" || outcome.State == "blocked" || outcome.State == "cancelled") && outcome.Identity.Snapshot == "" {
		if pending, found := r.pending.PeekScope(dataset, canonical); found {
			outcome.Identity.Snapshot = pending.Name
		}
	}
	return outcome
}

func roadKey(dataset, remote string) string { return dataset + "\x00" + remote }

func (r *Runtime) road(dataset, remote string, effective policy.Effective, members []transfer.Member) (roadState, error) {
	key := roadKey(dataset, remote)
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, found := r.roads[key]; found {
		if reflect.DeepEqual(existing.request.Policy, effective) && reflect.DeepEqual(existing.request.Members, members) && existing.request.HostLocal == r.hostLocal[remote] {
			return existing, nil
		}
		delete(r.roads, key)
	}
	setting, found := r.config.Remotes[remote]
	if !found {
		return roadState{}, fmt.Errorf("remote %s is not configured", remote)
	}
	client := r.remotes[remote]
	request := transfer.Request{Source: dataset, DestinationRoot: setting.Root, Policy: effective.Clone(), Members: slices.Clone(members), Transport: client.Transport(), RemoteName: remote, CanonicalTarget: client.CanonicalTarget(), HostLocal: r.hostLocal[remote]}
	applier := remoteApplier{source: r.backend, client: client, installation: r.installation, lifecycle: r.lifecycle, hostLocal: func() { r.learnHostLocal(remote, client) }}
	coordinator, err := transfer.NewRoadwarrior(applier, request, r.pending, transfer.DefaultRetryPolicy(), r.now, rand.Float64)
	if err != nil {
		return roadState{}, err
	}
	state := roadState{coordinator: coordinator, request: request}
	r.roads[key] = state
	return state, nil
}

// learnHostLocal records that remote receives into one of this host's own
// pools and rescans with its replicas in the local namespace. A root whose
// replica there would overlap this host's datasets becomes invalid; any other
// is retried with the request marked as validated. Knowledge is dropped if the
// remote has since been replaced.
func (r *Runtime) learnHostLocal(remote string, client remoteClient) {
	r.mu.Lock()
	if r.remotes[remote] != client || r.hostLocal[remote] {
		r.mu.Unlock()
		return
	}
	r.hostLocal[remote] = true
	destinations := remoteDestinations(r.remotes, r.hostLocal)
	r.mu.Unlock()
	r.logger.Info("remote receives into this host's own pools; revalidating its replicas as local", "remote", remote)
	r.scanner.SetDestinations(destinations)
}

func (r *Runtime) enqueueRemote(dataset, remote string, effective policy.Effective, created []string) bool {
	_, remotes := r.transferStreams(dataset, effective)
	members, configured := remotes[remote]
	if !configured {
		return true
	}
	road, err := r.road(dataset, remote, effective, members)
	if err != nil {
		r.logger.Error("prepare remote recovery", "dataset", dataset, "remote", remote, "error", err)
		return false
	}
	if len(created) > 0 {
		if pendingErr := r.protectCreated(dataset, effective, members, created, road.request.CanonicalTarget); pendingErr != nil {
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
		return len(created) == 0
	}
	jobID := "remote:" + dataset + ":" + remote
	job := Job{RunID: nextRunID(), ID: jobID, Group: dataset, Scope: dataset, LockKey: road.request.CanonicalTarget, StartState: "probing", Drop: ticket.Finish}
	job.Run = func(context.Context) Outcome {
		defer ticket.Finish()
		r.clearDirty(jobID)
		if startErr := ticket.Start(); startErr != nil {
			return r.withPending(dataset, road.request.CanonicalTarget, blockedOrCancelled(startErr))
		}
		outcome, reconcileErr := road.coordinator.Reconcile(ticket.Context(), r.transferReporter(r.remote, job))
		if outcome.Status == "waiting-retry" && !outcome.NotBefore.IsZero() {
			r.schedule(jobID, outcome.NotBefore, func() { r.enqueueRemote(dataset, remote, effective, nil) })
		}
		carried := daemonstate.Identity{Snapshot: outcome.Pending}
		if reconcileErr != nil {
			if errors.Is(reconcileErr, context.Canceled) {
				cancelled := blockedOrCancelled(reconcileErr)
				cancelled.Identity = carried
				return r.withPending(dataset, road.request.CanonicalTarget, cancelled)
			}
			return r.withPending(dataset, road.request.CanonicalTarget, Outcome{State: outcome.Status, Reason: reconcileErr.Error(), Identity: carried})
		}
		if outcome.Status == "succeeded" {
			r.enqueueDestinationPrune(dataset, effective, road.request.CanonicalTarget, "", members)
			last := outcome.Results[len(outcome.Results)-1].Plan
			return Outcome{State: outcome.Status, Reason: outcome.Reason, Identity: daemonstate.Identity{Snapshot: last.Snapshot, Destination: last.Destination}}
		}
		if outcome.Status == "waiting-retry" {
			// The backoff deadline has not passed, so nothing was attempted.
			// The state the last real attempt reported still stands.
			return Outcome{State: outcome.Status, Reason: outcome.Reason, Silent: true, Identity: carried}
		}
		return r.withPending(dataset, road.request.CanonicalTarget, Outcome{State: outcome.Status, Reason: outcome.Reason, Identity: carried})
	}
	job.After = func(Outcome) {
		if r.isDirty(jobID) {
			r.enqueueRemote(dataset, remote, effective, nil)
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

// transferReporter turns one running job's transfer reports into status. A
// phase change is a transition the pool emits for the job, as it emits the
// job's start state and outcome; a progress sample updates the job's progress.
//
// Each sending transition takes a new number, carried by the samples that
// follow it, so the status owner can tell a late sample from an earlier stream
// - a resume's first pass - from one belonging to the current stream.
func (r *Runtime) transferReporter(pool *Pool, job Job) transfer.Reporter {
	var send uint64
	return func(report transfer.Report) {
		switch {
		case report.Phase != "":
			event := pool.event(job, string(report.Phase), "")
			if report.Phase == transfer.PhaseSending {
				send = r.sends.Add(1)
				event.Send = send
				if report.Plan != nil {
					event.Identity = daemonstate.Identity{Snapshot: report.Plan.Snapshot, Base: report.Plan.Base, Mode: report.Plan.Mode}
				}
			}
			if pool.report != nil {
				pool.report(event)
			}
		case report.Progress != nil:
			r.recordProgress(pool.name, job, send, *report.Progress)
		}
	}
}

// recordProgress offers a progress sample. It never changes the job's state:
// the status owner keeps it only while the job's latest transition is the
// sending transition numbered send.
func (r *Runtime) recordProgress(pool string, job Job, send uint64, progress zfs.Progress) {
	event := Event{Kind: EventProgress, Send: send, Pool: pool, Job: job.ID, Scope: job.Scope, Target: job.LockKey, At: r.now().UTC(), Bytes: progress.Bytes, TotalBytes: progress.Estimate.Bytes, BytesPerSecond: progress.BytesPerSecond, TotalKnown: progress.Estimate.Known}
	if progress.ETA != nil {
		event.ETA = *progress.ETA
	}
	r.status.record(event)
}

// publishRuntimeLocked sends the runtime's dataset view to the status owner.
// Callers hold r.mu, which orders the views.
func (r *Runtime) publishRuntimeLocked() {
	names := mapsKeys(r.known)
	slices.Sort(names)
	view := runtimeView{configGeneration: r.configGeneration, datasets: make([]DatasetStatus, 0, len(names))}
	if r.generation != nil {
		view.generation, view.entries = r.generation.ID(), len(r.generation.Entries())
	}
	for _, name := range names {
		view.datasets = append(view.datasets, DatasetStatus{Name: name, Active: r.active[name], Recursive: r.recursive[name]})
	}
	r.status.publishRuntime(view)
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
	r.management.DiscardPending("daemon shutting down")
	r.local.DiscardPending("daemon shutting down")
	r.remote.DiscardPending("daemon shutting down")
	transferCancel()
	r.management.Close()
	r.local.Close()
	r.remote.Close()
	r.local.Wait()
	r.remote.Wait()
	r.management.Wait()
	r.delayWait.Wait()
	// Transitions reach the log on its own goroutine; let the shutdown's
	// transitions land before the line that says the daemon stopped.
	flushCtx, flushCancel := context.WithTimeout(context.Background(), 5*time.Second)
	r.status.flush(flushCtx)
	flushCancel()
	r.logger.Info("daemon stopped", "status_backlog_peak", r.status.BacklogPeak())
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
			r.mu.Lock()
			generation := r.generation
			r.mu.Unlock()
			if generation != nil {
				if entry, found := generation.Inspect(dataset); found {
					if coveredErr := entry.ErrCovered(); coveredErr != nil {
						return nil, coveredErr
					}
				}
			}
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

// ControlStatus returns the status owner's current state.
func (r *Runtime) ControlStatus() ControlSnapshot { return r.status.Snapshot() }

// SubscribeStatus subscribes to the daemon's status; see Status.Subscribe.
func (r *Runtime) SubscribeStatus(ctx context.Context) (daemonstate.Subscription, error) {
	subscription, err := r.status.Subscribe(ctx)
	if err != nil {
		return nil, err
	}
	return subscription, nil
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

// Status returns the latest transition for each job, with the newest progress
// of every running transfer.
func (r *Runtime) Status() []Event { return r.status.Snapshot().Jobs }

// Ensure compile-time safety conformance.
var _ lifecycle.CleanSafety = (*Safety)(nil)
