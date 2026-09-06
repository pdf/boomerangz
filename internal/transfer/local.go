package transfer

import (
	"context"
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

// Result distinguishes successful byte transport from verified replication.
type Result struct {
	Plan           Plan         `json:"plan"`
	Estimate       zfs.Estimate `json:"estimate"`
	Progress       zfs.Progress `json:"progress"`
	Verified       bool         `json:"verified"`
	ResumeDatasets []string     `json:"resume_datasets,omitempty"`
}

// Local serializes administrative jobs through this instance. Callers must also
// coordinate other processes and retain a stable effective policy generation.
type Local struct {
	backend      Backend
	stream       Stream
	lifecycle    *lifecycle.Service
	installation string
	mu           sync.Mutex
}

// NewLocal constructs a local transfer engine bound to one installation identity.
func NewLocal(backend Backend, stream Stream, installation string) (*Local, error) {
	if backend == nil || stream == nil {
		return nil, fmt.Errorf("local transfer backend and stream are required")
	}
	if !lifecycle.ValidID(installation) {
		return nil, fmt.Errorf("valid installation identity required")
	}
	service, err := lifecycle.NewService(backend, installation)
	if err != nil {
		return nil, err
	}
	return &Local{backend: backend, stream: stream, lifecycle: service, installation: installation}, nil
}

func (l *Local) load(ctx context.Context, request Request) (View, error) {
	var view View
	if err := zfs.ValidateDataset(request.Source); err != nil {
		return view, err
	}
	target, err := zfs.MapReceiveDataset(request.Source, request.DestinationRoot, zfs.ReceiveDiscard(request.Policy.Discard))
	if err != nil {
		return view, err
	}
	view.Inventory, err = l.backend.ListDatasets(ctx)
	if err != nil {
		return view, err
	}
	view.Source, err = l.backend.InspectState(ctx, request.Source, request.Policy.Send.Replicate)
	if err != nil {
		return view, err
	}
	// Inherited public properties may be carried by property streams, and changes
	// to their local ancestor values must be visible to preflight revalidation.
	var ancestors []string
	for name := request.Source; strings.Contains(name, "/"); {
		name = name[:strings.LastIndexByte(name, '/')]
		ancestors = append(ancestors, name)
	}
	if len(ancestors) > 0 {
		rows, err := l.backend.GetStoredProperties(ctx, ancestors)
		if err != nil {
			return view, err
		}
		view.Source.Properties = append(view.Source.Properties, rows...)
	}
	for _, dataset := range view.Inventory {
		if dataset.Name == target {
			view.DestinationExists = true
			break
		}
	}
	if view.DestinationExists {
		view.Destination, err = l.backend.InspectState(ctx, target, true)
		if err == nil {
			view.DestinationIdentity, err = l.backend.InspectDatasetIdentity(ctx, target)
		}
	} else {
		ancestor := ""
		for _, dataset := range view.Inventory {
			if inside(target, dataset.Name) && len(dataset.Name) > len(ancestor) {
				ancestor = dataset.Name
			}
		}
		if ancestor == "" {
			return view, fmt.Errorf("destination has no existing ancestor")
		}
		view.DestinationIdentity, err = l.backend.InspectDatasetIdentity(ctx, ancestor)
	}
	return view, err
}

// Preview inventories both ends without creating holds or changing properties.
func (l *Local) Preview(ctx context.Context, request Request) (Plan, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	view, err := l.load(ctx, request)
	if err != nil {
		return Plan{}, err
	}
	return Build(request, view, l.installation)
}

func sourceStable(a, b zfs.State) bool {
	// Preparation adds only dataset lineage and reference records/holds. Everything
	// else, including snapshot GUIDs and policy values on ancestors, must agree.
	properties := func(state zfs.State) []zfs.Property {
		var rows []zfs.Property
		for _, p := range state.Properties {
			if strings.HasPrefix(p.Name, lifecycle.ReferencePrefix) || strings.HasPrefix(p.Name, targetBindingPrefix) || (p.Name == lifecycle.LineageProperty && !strings.Contains(p.Dataset, "@")) {
				continue
			}
			rows = append(rows, p)
		}
		return rows
	}
	return reflect.DeepEqual(a.Objects, b.Objects) && reflect.DeepEqual(properties(a), properties(b)) && reflect.DeepEqual(a.ResumeTokens, b.ResumeTokens)
}

// Apply rebuilds the plan, pins owned source endpoints and snapshot bases, sends,
// verifies every expected GUID, and only then advances/relinquishes references.
// Failures retain references and any receive token; no implicit reseed or abort.
func (l *Local) Apply(ctx context.Context, request Request, report func(zfs.Progress)) (Result, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	var result Result
	before, err := l.load(ctx, request)
	if err != nil {
		return result, err
	}
	plan, err := Build(request, before, l.installation)
	result.Plan = plan
	if err != nil {
		return result, err
	}
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
	ref, err := l.lifecycle.ProtectSet(ctx, request.Source, endpointSnapshots, targetID)
	if err != nil {
		return result, err
	}
	if strings.Contains(plan.Base, "@") {
		if _, err := l.lifecycle.Protect(ctx, request.Source, plan.Base, targetID); err != nil {
			return result, err
		}
	}
	prepared, err := l.load(ctx, request)
	if err != nil {
		return result, err
	}
	if !sourceStable(before.Source, prepared.Source) || !reflect.DeepEqual(before.Destination, prepared.Destination) || before.DestinationExists != prepared.DestinationExists {
		return result, fmt.Errorf("transfer state changed during preparation; recovery holds retained")
	}
	fresh, err := Build(request, prepared, l.installation)
	if err != nil {
		return result, err
	}
	if fresh.Snapshot != plan.Snapshot || fresh.Base != plan.Base || !reflect.DeepEqual(fresh.Expected, plan.Expected) || fresh.TargetBinding != plan.TargetBinding {
		return result, fmt.Errorf("transfer history changed during preparation")
	}
	// Newly written recovery records must also be excluded from property streams.
	plan = fresh
	result.Plan = plan
	if plan.Mode != "up-to-date" {
		result.Estimate, err = l.backend.EstimateSend(ctx, plan.Send)
		if err != nil {
			return result, err
		}
		result.Progress, err = l.stream.Run(ctx, plan.Send, plan.Receive, result.Estimate, report)
		if err != nil {
			probeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			if state, probeErr := l.backend.InspectState(probeCtx, plan.Destination, true); probeErr == nil {
				for name := range state.ResumeTokens {
					result.ResumeDatasets = append(result.ResumeDatasets, name)
				}
				slices.Sort(result.ResumeDatasets)
			}
			return result, fmt.Errorf("transfer failed; source recovery references retained: %w", err)
		}
	}
	destination, err := l.backend.InspectState(ctx, plan.Destination, true)
	if err != nil {
		return result, err
	}
	if err := verifyGUIDs(plan, destination); err != nil {
		return result, err
	}
	sourceAfter, err := l.backend.InspectState(ctx, request.Source, true)
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
	if plan.TargetBinding.Anchor != plan.Destination {
		resolved, identityErr := l.backend.InspectDatasetIdentity(ctx, plan.Destination)
		if identityErr != nil {
			return result, identityErr
		}
		promoted, bindingErr := bindingFor(request, plan.Destination, resolved)
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
	verified, err := l.backend.InspectState(ctx, plan.Destination, true)
	if err != nil {
		return result, err
	}
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
	if err := l.lifecycle.ReleaseCompletedHold(ctx, request.Source, ref); err != nil {
		return result, err
	}
	current, err := l.backend.InspectState(ctx, request.Source, true)
	if err != nil {
		return result, err
	}
	old, err := lifecycle.References(current, request.Source, plan.Lineage)
	if err != nil {
		return result, err
	}
	for _, prior := range old {
		if prior.Target == targetID && !reflect.DeepEqual(prior, ref) {
			if err := l.lifecycle.ReleaseReference(ctx, request.Source, prior); err != nil {
				return result, err
			}
		}
	}
	result.Verified = true
	return result, nil
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
			if err := l.backend.InheritProperty(ctx, property.Dataset, property.Name); err != nil {
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
			if err := l.backend.SetProperties(ctx, dataset, map[string]string{lifecycle.LineageProperty: plan.Lineage}); err != nil {
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
		if err := l.backend.SetProperties(ctx, expected.Destination, properties); err != nil {
			return err
		}
	}
	return nil
}
