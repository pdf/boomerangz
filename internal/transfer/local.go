package transfer

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/pdf/boomerangz/internal/lifecycle"
	"github.com/pdf/boomerangz/internal/policy"
	"github.com/pdf/boomerangz/internal/zfs"
)

// Backend provides typed queries and lifecycle operations; it cannot run shells.
type Backend interface {
	zfs.Executor
	EstimateSend(context.Context, zfs.SendOptions) (zfs.Estimate, error)
}

// Stream is the transport boundary; SSH can implement a separate backend later.
type Stream interface {
	Run(context.Context, zfs.SendOptions, zfs.ReceiveOptions, zfs.Estimate, func(zfs.Progress)) (zfs.Progress, error)
}

// Phase names a stage of a transfer that its caller reports as a state change.
type Phase string

const (
	// PhaseSending starts when a plan has newer state to send: the stream is
	// estimated and run.
	PhaseSending Phase = "sending"
	// PhaseVerifying starts when the stream has completed: received GUIDs,
	// the target binding, and destination properties are checked.
	PhaseVerifying Phase = "verifying"
)

// Report is one ordered observation from a transfer: a phase change or a
// progress sample. Exactly one of Phase and Progress is set.
type Report struct {
	Phase Phase // set on a phase change
	// Plan is set with PhaseSending: the plan the stream carries out, naming
	// the snapshot sent, its base, and the mode.
	Plan     *Plan
	Progress *zfs.Progress // set on a sample
}

// Reporter receives a transfer's reports in the order they happened. A
// stream's samples are emitted inside its run, so every sample precedes the
// phase that follows the stream.
type Reporter func(Report)

func (r Reporter) phase(phase Phase, plan *Plan) {
	if r != nil {
		r(Report{Phase: phase, Plan: plan})
	}
}

func (r Reporter) progress() func(zfs.Progress) {
	if r == nil {
		return nil
	}
	return func(progress zfs.Progress) { r(Report{Progress: &progress}) }
}

// Result distinguishes successful byte transport from verified replication.
// For a request with members, Streams holds one result per stream attempted,
// and the top-level fields describe the root's stream when it ran, otherwise
// the last stream attempted; Verified then means every stream verified.
type Result struct {
	Plan           Plan         `json:"plan"`
	Estimate       zfs.Estimate `json:"estimate"`
	Progress       zfs.Progress `json:"progress"`
	Verified       bool         `json:"verified"`
	ResumeDatasets []string     `json:"resume_datasets,omitempty"`
	// Resumed is set when the stream completed an interrupted receive before
	// planning again; ResumedSnapshot names the endpoint that receive was for.
	Resumed         bool     `json:"resumed,omitempty"`
	ResumedSnapshot string   `json:"resumed_snapshot,omitempty"`
	Streams         []Result `json:"streams,omitempty"`
}

func destinationPermissions(plan Plan) []string {
	return union(receivePermissions, propertyPermissions(plan.Receive.Set, plan.Receive.Exclude))
}

// DestinationUnavailableError reports that a remote destination has no
// currently visible anchor. The pool may be temporarily unimported, so remote
// recovery must retry rather than permanently block the target.
type DestinationUnavailableError struct {
	Root string
}

func (e *DestinationUnavailableError) Error() string {
	return fmt.Sprintf("remote destination %s has no existing ancestor", e.Root)
}

// Temporary permits bounded roadwarrior retry while the remote pool is absent.
func (*DestinationUnavailableError) Temporary() bool { return true }

// PreparationChangedError reports concurrent inventory changes that prevented a
// stable preflight. Recovery state is retained and the complete attempt is safe
// to retry.
type PreparationChangedError struct {
	Reason string
}

func (e *PreparationChangedError) Error() string { return e.Reason }

// Temporary permits local and remote coordinators to retry ordinary churn.
func (*PreparationChangedError) Temporary() bool { return true }

// PlaceholderBusyError reports a placeholder that cannot be replaced yet,
// because a receive beneath it is still resumable. It defers only its own
// stream: the streams beneath it run, finish their receives, and the next
// attempt replaces it.
type PlaceholderBusyError struct {
	Dataset string
}

func (e *PlaceholderBusyError) Error() string {
	return "placeholder " + e.Dataset + " has a resumable receive beneath it; it is replaced once that completes"
}

// Temporary lets the job retry once the receives beneath have finished.
func (*PlaceholderBusyError) Temporary() bool { return true }

// StreamError is one member stream's failure.
type StreamError struct {
	Dataset string
	Err     error
}

func (e StreamError) Error() string { return e.Dataset + ": " + e.Err.Error() }

// StreamsError reports the member streams that failed in one transfer. Streams
// beneath a failed one were skipped rather than attempted, and are not listed.
type StreamsError struct {
	Failed []StreamError
}

func (e *StreamsError) Error() string {
	parts := make([]string, 0, len(e.Failed))
	for _, failure := range e.Failed {
		parts = append(parts, failure.Error())
	}
	return "member streams failed: " + strings.Join(parts, "; ")
}

// Temporary holds only when every failure is temporary, so one permanent
// failure is never retried as churn behind a temporary one.
func (e *StreamsError) Temporary() bool {
	for _, failure := range e.Failed {
		var temporary interface{ Temporary() bool }
		if !errors.As(failure.Err, &temporary) || !temporary.Temporary() {
			return false
		}
	}
	return len(e.Failed) > 0
}

func (e *StreamsError) Unwrap() []error {
	errs := make([]error, 0, len(e.Failed))
	for _, failure := range e.Failed {
		errs = append(errs, failure.Err)
	}
	return errs
}

// Local serializes administrative jobs through this instance. Callers must also
// coordinate other processes and retain a stable effective policy generation.
type Local struct {
	backend      Backend
	target       zfs.Executor
	stream       Stream
	lifecycle    *lifecycle.Service
	installation string
	sameHost     bool
	mu           sync.Mutex
}

// NewLocal constructs a local transfer engine bound to one installation identity.
func NewLocal(backend Backend, stream Stream, installation string) (*Local, error) {
	service, err := lifecycle.NewService(backend, installation)
	if err != nil {
		return nil, err
	}
	return NewLocalWithService(backend, stream, installation, service)
}

// NewLocalWithService constructs a local transfer engine using the daemon's
// shared lifecycle coordinator.
func NewLocalWithService(backend Backend, stream Stream, installation string, service *lifecycle.Service) (*Local, error) {
	if backend == nil || stream == nil {
		return nil, fmt.Errorf("local transfer backend and stream are required")
	}
	if !lifecycle.ValidID(installation) || service == nil {
		return nil, fmt.Errorf("valid installation identity required")
	}
	return &Local{backend: backend, target: backend, stream: stream, lifecycle: service, installation: installation, sameHost: true}, nil
}

// NewRemote constructs an SSH-capable transfer engine. Source lifecycle writes
// remain local; destination inspection and reconciliation use the remote typed
// executor. The stream is responsible for the authenticated transport.
func NewRemote(source Backend, destination zfs.Executor, stream Stream, installation string) (*Local, error) {
	service, err := lifecycle.NewService(source, installation)
	if err != nil {
		return nil, err
	}
	return NewRemoteWithService(source, destination, stream, installation, service)
}

// NewRemoteWithService constructs a remote transfer engine using the daemon's
// shared source lifecycle coordinator.
func NewRemoteWithService(source Backend, destination zfs.Executor, stream Stream, installation string, service *lifecycle.Service) (*Local, error) {
	if source == nil || destination == nil || stream == nil {
		return nil, fmt.Errorf("source, remote destination, and stream are required")
	}
	if !lifecycle.ValidID(installation) || service == nil {
		return nil, fmt.Errorf("valid installation identity required")
	}
	return &Local{backend: source, target: destination, stream: stream, lifecycle: service, installation: installation}, nil
}

// ownTokens keeps only the resume tokens that belong to a stream received at
// destination. A recursive stream owns every token beneath its destination; any
// other stream owns only its own, since a token on a descendant belongs to that
// member's stream and is resumed and verified there.
func ownTokens(state zfs.State, destination string, recursive bool) zfs.State {
	if recursive || len(state.ResumeTokens) == 0 {
		return state
	}
	tokens := make(map[string]string)
	if token, found := state.ResumeTokens[destination]; found {
		tokens[destination] = token
	}
	if len(tokens) == 0 {
		tokens = nil
	}
	state.ResumeTokens = tokens
	return state
}

// nearestExisting returns name or its nearest ancestor present in inventory.
func nearestExisting(name string, inventory []zfs.Dataset) string {
	nearest := ""
	for _, dataset := range inventory {
		if inside(name, dataset.Name) && len(dataset.Name) > len(nearest) {
			nearest = dataset.Name
		}
	}
	return nearest
}

// refuseHostLocal applies checkHostLocal to the destination's configured
// root, or its nearest existing ancestor. A destination with none is left to
// load, which reports it unavailable.
func (l *Local) refuseHostLocal(ctx context.Context, request Request) error {
	if l.sameHost || requestTransport(request) == "local" || request.HostLocal {
		return nil
	}
	inventory, err := l.target.ListDatasets(ctx)
	if err != nil {
		return err
	}
	nearest := nearestExisting(request.DestinationRoot, inventory)
	if nearest == "" {
		return nil
	}
	identity, err := l.target.InspectDatasetIdentity(ctx, nearest)
	if err != nil {
		return err
	}
	return checkHostLocal(ctx, l.backend, request, identity)
}

// load inventories both ends for one stream. The source view is the root's own
// state, which carries authority, binding and references, plus the stream's;
// a replication root's stream inspects the whole subtree it carries.
func (l *Local) load(ctx context.Context, request Request, stream string) (View, error) {
	view := View{Stream: stream}
	if err := zfs.ValidateDataset(request.Source); err != nil {
		return view, err
	}
	mappedRoot, target, err := request.mapped(stream)
	if err != nil {
		return view, err
	}
	view.Inventory, err = l.backend.ListDatasets(ctx)
	if err != nil {
		return view, err
	}
	if l.sameHost {
		view.DestinationInventory = view.Inventory
	} else {
		view.DestinationInventory, err = l.target.ListDatasets(ctx)
		if err != nil {
			return view, err
		}
	}
	view.Source, err = l.backend.InspectState(ctx, request.Source, request.Policy.Send.Replicate)
	if err != nil {
		return view, err
	}
	if stream != request.Source {
		member, inspectErr := l.backend.InspectState(ctx, stream, false)
		if inspectErr != nil {
			return view, inspectErr
		}
		view.Source = zfs.MergeStates(view.Source, member)
	}
	// Inherited public properties may be carried by property streams, and changes
	// to their local ancestor values must be visible to preflight revalidation.
	// The root and the stream are already inspected; everything above the
	// stream besides them is read for its stored values alone.
	var ancestors []string
	for name := stream; strings.Contains(name, "/"); {
		name = name[:strings.LastIndexByte(name, '/')]
		if name != request.Source {
			ancestors = append(ancestors, name)
		}
	}
	if len(ancestors) > 0 {
		rows, err := l.backend.GetStoredProperties(ctx, ancestors)
		if err != nil {
			return view, err
		}
		view.Source.Properties = append(view.Source.Properties, rows...)
	}
	bound, err := storedTargetBinding(view.Source, request.Source, canonicalTarget(request))
	if err != nil {
		return view, err
	}
	if bound != nil {
		view.BindingIdentity, err = l.target.InspectDatasetIdentity(ctx, bound.Anchor)
		if err != nil {
			return view, err
		}
	}
	identify := func(mapped string) (bool, zfs.DatasetIdentity, error) {
		nearest := nearestExisting(mapped, view.DestinationInventory)
		if nearest == "" {
			if !l.sameHost {
				return false, zfs.DatasetIdentity{}, &DestinationUnavailableError{Root: request.DestinationRoot}
			}
			return false, zfs.DatasetIdentity{}, fmt.Errorf("destination has no existing ancestor")
		}
		identity, identityErr := l.target.InspectDatasetIdentity(ctx, nearest)
		return nearest == mapped, identity, identityErr
	}
	view.DestinationExists, view.DestinationIdentity, err = identify(target)
	if err != nil {
		return view, err
	}
	if view.DestinationExists {
		view.Destination, err = l.target.InspectState(ctx, target, true)
		if err != nil {
			return view, err
		}
		view.Destination = ownTokens(view.Destination, target, request.Policy.Send.Replicate)
	}
	// The binding proves the root's replica, and a placeholder is never one:
	// the root's identity is that of the nearest real dataset, so a
	// placeholder standing in for the root's replica reads as absent.
	anchor, err := nearestReal(ctx, l.target, mappedRoot, request.DestinationRoot, view.DestinationInventory)
	if err != nil {
		return view, err
	}
	if anchor.Name == "" {
		if !l.sameHost {
			return view, &DestinationUnavailableError{Root: request.DestinationRoot}
		}
		return view, fmt.Errorf("destination has no existing ancestor")
	}
	view.RootDestinationExists = anchor.Name == mappedRoot
	view.RootDestinationIdentity = anchor
	return view, checkHostLocal(ctx, l.backend, request, view.RootDestinationIdentity)
}

// Preview inventories both ends without creating holds or changing properties.
// A request with members previews its first stream.
func (l *Local) Preview(ctx context.Context, request Request) (Plan, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	streams, err := request.Streams()
	if err != nil {
		return Plan{}, err
	}
	view, err := l.load(ctx, request, streams[0].Dataset)
	if err != nil {
		return Plan{}, err
	}
	if len(view.Destination.ResumeTokens) > 0 {
		plan, _, buildErr := BuildResume(request, view, l.installation)
		if buildErr != nil {
			return plan, buildErr
		}
		return plan, l.checkPermissions(ctx, request, view, plan)
	}
	plan, err := Build(request, view, l.installation)
	if err != nil {
		return plan, err
	}
	return plan, l.checkPermissions(ctx, request, view, plan)
}

func (l *Local) checkPermissions(ctx context.Context, request Request, view View, plan Plan) error {
	sent := plan.Source
	if sent == "" {
		sent = request.Source
	}
	if err := l.backend.CheckPermissions(ctx, sent, slices.Clone(sendPermissions)); err != nil {
		return fmt.Errorf("source permission preflight: %w", err)
	}
	anchor := request.DestinationRoot
	found := slices.ContainsFunc(view.DestinationInventory, func(dataset zfs.Dataset) bool { return dataset.Name == anchor })
	if !found {
		anchor = view.DestinationIdentity.Name
	}
	if anchor == "" {
		return fmt.Errorf("destination permission preflight: existing destination anchor is missing")
	}
	if err := l.target.CheckPermissions(ctx, anchor, destinationPermissions(plan)); err != nil {
		return fmt.Errorf("destination permission preflight: %w", err)
	}
	return nil
}

func sourceStable(a, b zfs.State) bool {
	// Snapshot and bookmark churn is permitted here. Build immediately revalidates
	// the selected base and endpoints by name and GUID after this control-plane
	// check. Dataset identities and effective policy must remain stable.
	objects := func(state zfs.State) []zfs.Object {
		var rows []zfs.Object
		for _, object := range state.Objects {
			if object.Type == "filesystem" || object.Type == "volume" {
				rows = append(rows, object)
			}
		}
		return rows
	}
	properties := func(state zfs.State) []zfs.Property {
		var rows []zfs.Property
		for _, p := range state.Properties {
			if strings.ContainsAny(p.Dataset, "@#") {
				continue
			}
			if strings.HasPrefix(p.Name, lifecycle.ReferencePrefix) || strings.HasPrefix(p.Name, targetBindingPrefix) || (p.Name == lifecycle.LineageProperty && !strings.Contains(p.Dataset, "@")) {
				continue
			}
			rows = append(rows, p)
		}
		return rows
	}
	return reflect.DeepEqual(objects(a), objects(b)) && reflect.DeepEqual(properties(a), properties(b)) && reflect.DeepEqual(a.ResumeTokens, b.ResumeTokens)
}

func destinationStable(a, b zfs.State) bool {
	objects := func(state zfs.State) []zfs.Object {
		var rows []zfs.Object
		for _, object := range state.Objects {
			if object.Type == "filesystem" || object.Type == "volume" {
				rows = append(rows, object)
			}
		}
		return rows
	}
	properties := func(state zfs.State) []zfs.Property {
		return slices.DeleteFunc(slices.Clone(state.Properties), func(property zfs.Property) bool {
			return strings.ContainsAny(property.Dataset, "@#")
		})
	}
	return reflect.DeepEqual(objects(a), objects(b)) && reflect.DeepEqual(properties(a), properties(b)) && reflect.DeepEqual(a.ResumeTokens, b.ResumeTokens)
}

func missingReceiveParents(root, target string, inventory []zfs.Dataset) ([]string, error) {
	if !inside(target, root) {
		return nil, fmt.Errorf("mapped destination is outside configured root")
	}
	if target == root {
		return nil, nil
	}
	existing := make(map[string]zfs.Dataset, len(inventory))
	for _, dataset := range inventory {
		existing[dataset.Name] = dataset
	}
	configured, exists := existing[root]
	if !exists || configured.Type != zfs.Filesystem {
		return nil, fmt.Errorf("receive root must be an existing filesystem")
	}
	parentIndex := strings.LastIndexByte(target, '/')
	if parentIndex < len(root) {
		return nil, nil
	}
	parent := target[:parentIndex]
	if parent == root {
		return nil, nil
	}
	relative := strings.TrimPrefix(parent, root+"/")
	current := root
	var missing []string
	for _, component := range strings.Split(relative, "/") {
		current += "/" + component
		if dataset, found := existing[current]; found {
			if dataset.Type != zfs.Filesystem {
				return nil, fmt.Errorf("receive ancestor is not a filesystem: %s", current)
			}
			continue
		}
		missing = append(missing, current)
	}
	return missing, nil
}

// prepareReceiveParents creates the missing datasets between the configured
// root and a full receive's destination. For a member these may include the
// root's own replica and holes left by excluded datasets: nothing is sent for
// them, but a deeper member cannot be received without them. A replica the
// root's mapping makes the configured root itself is created from its parent.
func (l *Local) prepareReceiveParents(ctx context.Context, request Request, view View, plan Plan) (View, Plan, error) {
	if plan.Mode != "full" {
		return view, plan, nil
	}
	base := request.DestinationRoot
	if request.Policy.Discard == policy.DiscardOff {
		index := strings.LastIndexByte(base, '/')
		if index < 0 {
			return view, plan, fmt.Errorf("cannot create a replica in place of a pool root")
		}
		base = base[:index]
	}
	parents, err := missingReceiveParents(base, plan.Destination, view.DestinationInventory)
	if err != nil {
		return view, plan, err
	}
	for _, parent := range parents {
		if err := l.target.CreateReceiveParent(ctx, parent); err != nil {
			return view, plan, fmt.Errorf("prepare receive parent %s: %w", parent, err)
		}
	}
	if len(parents) == 0 {
		return view, plan, nil
	}
	prepared, err := l.load(ctx, request, view.Stream)
	if err != nil {
		return view, plan, err
	}
	if !sourceStable(view.Source, prepared.Source) {
		return view, plan, &PreparationChangedError{Reason: "source state changed while preparing receive ancestors"}
	}
	fresh, err := Build(request, prepared, l.installation)
	if err != nil {
		return view, plan, err
	}
	if fresh.Snapshot != plan.Snapshot || fresh.Base != plan.Base || !reflect.DeepEqual(fresh.Expected, plan.Expected) {
		return view, plan, &PreparationChangedError{Reason: "transfer history changed while preparing receive ancestors"}
	}
	return prepared, fresh, nil
}

// Apply sends each of the request's streams in order, parents first. A stream
// that fails leaves every stream beneath it unattempted: they would be received
// into placeholders that block the failed stream's own later full receive.
// Unrelated streams still run, and each verified stream advances its own proof.
//
// Each stream rebuilds its plan, pins owned source endpoints and snapshot bases,
// sends, verifies every expected GUID, and only then advances/relinquishes its
// references. Failures retain references and any receive token; no implicit
// reseed or abort. Phases are reported only for work that happens:
// PhaseSending before a stream and PhaseVerifying after it completes, so an
// up-to-date stream reports none.
func (l *Local) Apply(ctx context.Context, request Request, report Reporter) (Result, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	streams, err := request.Streams()
	if err != nil {
		return Result{}, err
	}
	// Replacing a placeholder renames on the destination before any stream
	// loads, so a remote on this host's own pools is refused up front.
	if err := l.refuseHostLocal(ctx, request); err != nil {
		return Result{}, err
	}
	if len(request.Members) == 0 {
		return l.applyStream(ctx, request, streams[0].Dataset, report)
	}
	var (
		result  Result
		failed  StreamsError
		blocked []string
	)
	result.Verified = true
	for _, stream := range streams {
		if slices.ContainsFunc(blocked, func(failure string) bool { return inside(stream.Dataset, failure) }) {
			result.Verified = false
			continue
		}
		streamResult, streamErr := l.applyStream(ctx, request, stream.Dataset, report)
		result.Streams = append(result.Streams, streamResult)
		result.ResumeDatasets = append(result.ResumeDatasets, streamResult.ResumeDatasets...)
		if stream.Dataset == request.Source || result.Plan.Source != request.Source {
			result.Plan, result.Estimate, result.Progress = streamResult.Plan, streamResult.Estimate, streamResult.Progress
		}
		result.Verified = result.Verified && streamResult.Verified
		if streamErr != nil {
			if errors.Is(streamErr, context.Canceled) || errors.Is(streamErr, context.DeadlineExceeded) {
				return result, streamErr
			}
			failed.Failed = append(failed.Failed, StreamError{Dataset: stream.Dataset, Err: streamErr})
			// A deferred placeholder is the one failure whose descendants must
			// run: their pending receives are what it is waiting for.
			var busy *PlaceholderBusyError
			if !errors.As(streamErr, &busy) {
				blocked = append(blocked, stream.Dataset)
			}
		}
	}
	if len(failed.Failed) > 0 {
		return result, &failed
	}
	return result, nil
}

// applyStream sends one stream. A stream left resumable by an interrupted
// receive resumes first and then plans again, so it ends at its newest endpoint.
func (l *Local) applyStream(ctx context.Context, request Request, stream string, report Reporter) (Result, error) {
	result, err := l.applyOnce(ctx, request, stream, report)
	if err != nil || result.Plan.Mode != "resume" {
		return result, err
	}
	resumed := result.Plan.Snapshot
	result, err = l.applyOnce(ctx, request, stream, report)
	result.Resumed, result.ResumedSnapshot = true, resumed
	return result, err
}

func (l *Local) applyOnce(ctx context.Context, request Request, stream string, report Reporter) (Result, error) {
	member, err := request.stream(stream)
	if err != nil {
		return Result{}, err
	}
	streamPolicy := member.Policy
	if stream == request.Source {
		streamPolicy = request.Policy
	}
	if err := l.setAsidePlaceholder(ctx, request, stream); err != nil {
		return Result{}, err
	}
	var result Result
	before, err := l.load(ctx, request, stream)
	if err != nil {
		return result, err
	}
	resuming := len(before.Destination.ResumeTokens) > 0
	var plan Plan
	var ref lifecycle.Reference
	if resuming {
		plan, ref, err = BuildResume(request, before, l.installation)
		if err != nil {
			// The token stays, so every later attempt would fail the same way;
			// discarding it is the operator's call.
			target := request.DestinationRoot
			if requestTransport(request) != "local" {
				target = request.RemoteName
			}
			err = fmt.Errorf("interrupted receive cannot be resumed: %w; to discard it and send again, run boomerangz dataset abort-receive %s %s", err, request.Source, target)
		}
	} else {
		plan, err = Build(request, before, l.installation)
	}
	result.Plan = plan
	if err != nil {
		return result, err
	}
	if err := l.checkPermissions(ctx, request, before, plan); err != nil {
		return result, err
	}
	if !resuming {
		before, plan, err = l.prepareReceiveParents(ctx, request, before, plan)
		if err != nil {
			return result, err
		}
	}
	result.Plan = plan
	if !resuming {
		if plan.BindingNew {
			value, encodeErr := encodeBinding(plan.TargetBinding)
			if encodeErr != nil {
				return result, encodeErr
			}
			if err := l.backend.SetProperties(ctx, request.Source, map[string]string{targetBindingProperty(plan.TargetBinding.CanonicalTarget): value}); err != nil {
				return result, err
			}
		}
		targetID := plan.TargetBinding.CanonicalTarget
		endpointSnapshots := make([]string, 0, len(plan.Endpoints))
		for _, endpoint := range plan.Endpoints {
			endpointSnapshots = append(endpointSnapshots, endpoint.Source)
		}
		ref, err = l.lifecycle.ProtectSet(ctx, request.Source, stream, endpointSnapshots, targetID)
		if err != nil {
			return result, err
		}
		if strings.Contains(plan.Base, "@") {
			baseSnapshots := []string{plan.Base}
			if plan.Send.Recursive {
				_, component, _ := strings.Cut(plan.Base, "@")
				objects := objectMap(before.Source)
				for _, endpoint := range plan.Endpoints {
					dataset := datasetOf(endpoint.Source)
					candidate := dataset + "@" + component
					if candidate != plan.Base {
						if object, exists := objects[candidate]; exists && object.Type == "snapshot" {
							baseSnapshots = append(baseSnapshots, candidate)
						}
					}
				}
			}
			if _, err := l.lifecycle.ProtectSet(ctx, request.Source, stream, baseSnapshots, targetID); err != nil {
				return result, err
			}
		}
		prepared, loadErr := l.load(ctx, request, stream)
		if loadErr != nil {
			return result, loadErr
		}
		if !sourceStable(before.Source, prepared.Source) || !destinationStable(before.Destination, prepared.Destination) || before.DestinationExists != prepared.DestinationExists {
			return result, &PreparationChangedError{Reason: "transfer state changed during preparation; recovery holds retained"}
		}
		fresh, buildErr := Build(request, prepared, l.installation)
		if buildErr != nil {
			return result, buildErr
		}
		if fresh.Snapshot != plan.Snapshot || fresh.Base != plan.Base || !reflect.DeepEqual(fresh.Expected, plan.Expected) || fresh.TargetBinding != plan.TargetBinding {
			return result, &PreparationChangedError{Reason: "transfer history changed during preparation; recovery holds retained"}
		}
		// Newly written recovery records must also be excluded from property streams.
		plan = fresh
		result.Plan = plan
	} else {
		prepared, loadErr := l.load(ctx, request, stream)
		if loadErr != nil {
			return result, loadErr
		}
		if !reflect.DeepEqual(before, prepared) {
			return result, fmt.Errorf("resumable transfer state changed during preflight")
		}
		fresh, freshRef, buildErr := BuildResume(request, prepared, l.installation)
		if buildErr != nil || !reflect.DeepEqual(freshRef, ref) || !reflect.DeepEqual(fresh, plan) {
			if buildErr != nil {
				return result, buildErr
			}
			return result, fmt.Errorf("resumable transfer proof changed during preflight")
		}
	}
	targetID := plan.TargetBinding.CanonicalTarget
	if plan.Mode != "up-to-date" {
		sending := plan
		report.phase(PhaseSending, &sending)
		result.Estimate, err = l.backend.EstimateSend(ctx, plan.Send)
		if err != nil {
			return result, err
		}
		result.Progress, err = l.stream.Run(ctx, plan.Send, plan.Receive, result.Estimate, report.progress())
		if err != nil {
			probeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			if state, probeErr := l.target.InspectState(probeCtx, plan.Destination, true); probeErr == nil {
				state = ownTokens(state, plan.Destination, plan.Send.Recursive)
				for name := range state.ResumeTokens {
					result.ResumeDatasets = append(result.ResumeDatasets, name)
				}
				slices.Sort(result.ResumeDatasets)
			}
			return result, fmt.Errorf("transfer failed; source recovery references retained: %w", err)
		}
		report.phase(PhaseVerifying, nil)
	}
	destination, err := l.target.InspectState(ctx, plan.Destination, true)
	if err != nil {
		return result, err
	}
	destination = ownTokens(destination, plan.Destination, plan.Send.Recursive)
	if err := verifyGUIDs(plan, destination); err != nil {
		return result, err
	}
	// Authority, the binding and every proof are the root's own properties.
	sourceAfter, err := l.backend.InspectState(ctx, request.Source, false)
	if err != nil {
		return result, err
	}
	if _, err := lifecycle.RootAuthority(sourceAfter, request.Source, l.installation); err != nil {
		return result, err
	}
	bound, err := storedTargetBinding(sourceAfter, request.Source, plan.TargetBinding.CanonicalTarget)
	if err != nil || bound == nil || *bound != plan.TargetBinding {
		return result, fmt.Errorf("source target binding changed during transfer")
	}
	preparedRefs, err := lifecycle.References(sourceAfter, request.Source, plan.Lineage)
	if err != nil || !slices.ContainsFunc(preparedRefs, func(candidate lifecycle.Reference) bool { return reflect.DeepEqual(candidate, ref) }) {
		return result, fmt.Errorf("source recovery proof changed during transfer")
	}
	// The binding proves the root's replica, which every stream is received
	// beneath, so the first stream to bring it into existence promotes the
	// bootstrap anchor to it.
	mappedRoot := plan.TargetBinding.MappedDataset
	// A placeholder is never an anchor: until the root's own replica replaces
	// it, the binding stays on the nearest real ancestor.
	_, rootPlaceholder, rootErr := placeholderAt(ctx, l.target, mappedRoot)
	if rootErr != nil {
		return result, rootErr
	}
	if plan.TargetBinding.Anchor != mappedRoot && !rootPlaceholder {
		resolved, identityErr := l.target.InspectDatasetIdentity(ctx, mappedRoot)
		if identityErr != nil {
			return result, identityErr
		}
		transport := requestTransport(request)
		canonical := request.CanonicalTarget
		if transport == "local" && canonical == "" {
			canonical = canonicalLocalTarget(request.DestinationRoot)
		}
		promoted, bindingErr := bindingForTarget(request, mappedRoot, resolved, transport, canonical)
		if bindingErr != nil || promoted.PoolGUID != plan.TargetBinding.PoolGUID {
			return result, fmt.Errorf("received destination identity could not be anchored")
		}
		value, encodeErr := encodeBinding(promoted)
		if encodeErr != nil {
			return result, encodeErr
		}
		if err := l.backend.SetProperties(ctx, request.Source, map[string]string{targetBindingProperty(promoted.CanonicalTarget): value}); err != nil {
			return result, err
		}
		plan.TargetBinding = promoted
		plan.BindingNew = false
		result.Plan = plan
	}
	if err := l.reconcile(ctx, plan, destination); err != nil {
		return result, err
	}
	verified, err := l.target.InspectState(ctx, plan.Destination, true)
	if err != nil {
		return result, err
	}
	verified = ownTokens(verified, plan.Destination, plan.Send.Recursive)
	if err := verifyGUIDs(plan, verified); err != nil {
		return result, err
	}
	for _, expected := range plan.Expected {
		if expected.Metadata == nil {
			continue
		}
		o := objectMap(verified)[expected.Destination]
		metadata, err := ownership(verified, o, plan.Lineage)
		if err != nil || metadata != *expected.Metadata {
			return result, fmt.Errorf("received ownership metadata did not verify: %s", expected.Destination)
		}
	}
	verifiedGUIDs := make(map[string]uint64, len(plan.Endpoints))
	for _, endpoint := range plan.Endpoints {
		verifiedGUIDs[endpoint.Source] = endpoint.GUID
	}
	if err := l.lifecycle.CheckpointSet(ctx, request.Source, ref, verifiedGUIDs); err != nil {
		return result, err
	}
	// An intermediary send requires its base to remain a snapshot. Keep the
	// latest verified endpoint protected until a newer transfer succeeds;
	// ReleaseReference below then retires the previous endpoint. Latest-only
	// sends can recover from the verified bookmark and do not need this hold.
	if canResumeFromBookmark(streamPolicy) {
		if err := l.lifecycle.ReleaseCompletedHold(ctx, request.Source, ref); err != nil {
			return result, err
		}
	}
	current, err := l.backend.InspectState(ctx, request.Source, false)
	if err != nil {
		return result, err
	}
	old, err := lifecycle.References(current, request.Source, plan.Lineage)
	if err != nil {
		return result, err
	}
	// Only this stream's older proofs are retired: every other member's base
	// on this target is its own, at its own instant.
	for _, prior := range old {
		if prior.Target == targetID && prior.Dataset == stream && prior.Metadata.Created.Before(ref.Metadata.Created) {
			if err := l.lifecycle.ReleaseReference(ctx, request.Source, prior); err != nil {
				return result, err
			}
		}
	}
	if err := l.adoptPlaceholderChildren(ctx, request, stream); err != nil {
		return result, err
	}
	result.Verified = true
	return result, nil
}

// placeholderAside names where a placeholder waits while its dataset's own
// replica is received in its place: a sibling, so it stays inside every
// destination scope the placeholder was in.
func placeholderAside(dataset string) string { return dataset + ".boomerangz-placeholder" }

// setAsidePlaceholder makes room for a stream whose destination is still the
// placeholder that once stood in for it, as a hole between its root and a
// deeper member, or as the root's replica when only members sent here. A full
// receive cannot replace an existing dataset without -F, which boomerangz
// never uses, so the placeholder and the replicas beneath it move to a
// sibling first; adoptPlaceholderChildren moves them back beneath the received
// replica once it verifies.
//
// Every step leaves a state the next attempt recognises. The placeholder
// moved aside with its destination absent is an ordinary full receive with
// children waiting; with its destination received, the children are moved
// across and the placeholder removed. Nothing is destroyed until the
// placeholder is empty.
func (l *Local) setAsidePlaceholder(ctx context.Context, request Request, stream string) error {
	_, destination, err := request.mapped(stream)
	if err != nil {
		return err
	}
	inventory, err := l.target.ListDatasets(ctx)
	if err != nil {
		return err
	}
	exists := func(name string) bool {
		return slices.ContainsFunc(inventory, func(dataset zfs.Dataset) bool { return dataset.Name == name })
	}
	aside := placeholderAside(destination)
	if !exists(destination) {
		return nil
	}
	if _, placeholder, err := placeholderAt(ctx, l.target, destination); err != nil || !placeholder {
		return err
	}
	// The busy check below needs any resume token beneath it too.
	state, err := l.target.InspectState(ctx, destination, true)
	if err != nil {
		return err
	}
	if exists(aside) {
		return fmt.Errorf("placeholder %s cannot be set aside: %s already exists", destination, aside)
	}
	if len(state.ResumeTokens) > 0 {
		return &PlaceholderBusyError{Dataset: destination}
	}
	replicas, ok := l.target.(zfs.ReplicaExecutor)
	if !ok {
		return fmt.Errorf("destination cannot replace placeholder %s with its replica", destination)
	}
	parent := destination[:strings.LastIndexByte(destination, '/')]
	if err := l.target.CheckPermissions(ctx, parent, slices.Clone(placeholderPermissions)); err != nil {
		return fmt.Errorf("placeholder replacement permission preflight: %w", err)
	}
	return replicas.RenameDataset(ctx, destination, aside)
}

// adoptPlaceholderChildren finishes a placeholder replacement once the stream's
// replica has verified: every child of the set-aside placeholder moves beneath
// the replica, and the then empty placeholder is removed.
func (l *Local) adoptPlaceholderChildren(ctx context.Context, request Request, stream string) error {
	_, destination, err := request.mapped(stream)
	if err != nil {
		return err
	}
	aside := placeholderAside(destination)
	inventory, err := l.target.ListDatasets(ctx)
	if err != nil {
		return err
	}
	if !slices.ContainsFunc(inventory, func(dataset zfs.Dataset) bool { return dataset.Name == aside }) {
		return nil
	}
	state, err := l.target.InspectState(ctx, aside, false)
	if err != nil {
		return err
	}
	if !isPlaceholder(state, aside) {
		return fmt.Errorf("%s is not a placeholder boomerangz set aside; refusing to adopt its children", aside)
	}
	replicas, ok := l.target.(zfs.ReplicaExecutor)
	if !ok {
		return fmt.Errorf("destination cannot replace placeholder %s with its replica", destination)
	}
	for _, dataset := range inventory {
		child, found := strings.CutPrefix(dataset.Name, aside+"/")
		if !found || strings.Contains(child, "/") {
			continue
		}
		target := destination + "/" + child
		if slices.ContainsFunc(inventory, func(existing zfs.Dataset) bool { return existing.Name == target }) {
			return fmt.Errorf("cannot move %s beneath the replica: %s already exists", dataset.Name, target)
		}
		if err := replicas.RenameDataset(ctx, dataset.Name, target); err != nil {
			return fmt.Errorf("move %s beneath its replica: %w", dataset.Name, err)
		}
	}
	return replicas.DestroyDataset(ctx, aside, false)
}

func verifyGUIDs(plan Plan, state zfs.State) error {
	if len(state.ResumeTokens) > 0 {
		return fmt.Errorf("receive remains incomplete; recovery references retained")
	}
	objects := objectMap(state)
	for _, expected := range plan.Expected {
		received, exists := objects[expected.Destination]
		if !exists || received.Type != "snapshot" || received.GUID != expected.GUID {
			return fmt.Errorf("received GUID verification failed for %s", expected.Destination)
		}
	}
	return nil
}

func (l *Local) reconcile(ctx context.Context, plan Plan, state zfs.State) error {
	for _, property := range state.Properties {
		if property.Source == zfs.SourceReceived && (policy.IsPublic(property.Name) || strings.HasPrefix(property.Name, lifecycle.ReferencePrefix) || strings.HasPrefix(property.Name, targetBindingPrefix)) {
			if err := l.target.InheritProperty(ctx, property.Dataset, property.Name); err != nil {
				return err
			}
		}
	}
	for _, endpoint := range plan.Endpoints {
		dataset := datasetOf(endpoint.Destination)
		lineage, err := lifecycle.DatasetLineage(state, dataset)
		if err != nil {
			return err
		}
		if lineage != "" && lineage != plan.Lineage {
			return fmt.Errorf("received dataset lineage conflicts with source")
		}
		if lineage == "" {
			if err := l.target.SetProperties(ctx, dataset, map[string]string{lifecycle.LineageProperty: plan.Lineage}); err != nil {
				return err
			}
		}
	}
	for _, expected := range plan.Expected {
		if expected.Metadata == nil {
			continue
		}
		properties := expected.Metadata.Properties()
		for _, p := range state.Properties {
			if p.Dataset != expected.Destination {
				continue
			}
			if wanted, exists := properties[p.Name]; exists && wanted != p.Value {
				return fmt.Errorf("received snapshot metadata conflicts with source: %s", expected.Destination)
			}
		}
		if err := l.target.SetProperties(ctx, expected.Destination, properties); err != nil {
			return err
		}
	}
	return nil
}
