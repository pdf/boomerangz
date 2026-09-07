package daemon

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/pdf/boomerangz/internal/lifecycle"
	"github.com/pdf/boomerangz/internal/policy"
	"github.com/pdf/boomerangz/internal/transfer"
	"github.com/pdf/boomerangz/internal/zfs"
)

type retirementEndpoint struct {
	executor zfs.Executor
	request  transfer.Request
	close    func() error
}

func (r *Runtime) retirementEndpoint(ctx context.Context, source string, effective policy.Effective, canonical string) (retirementEndpoint, error) {
	for _, root := range effective.Local {
		if "local:"+root == canonical {
			return retirementEndpoint{executor: r.backend, request: transfer.Request{Source: source, DestinationRoot: root, Policy: effective}, close: func() error { return nil }}, nil
		}
	}
	for _, name := range effective.Remote {
		client := r.remotes[name]
		if client == nil || client.CanonicalTarget() != canonical {
			continue
		}
		setting := r.config.Remotes[name]
		endpoint, err := client.Open(ctx)
		if err != nil {
			return retirementEndpoint{}, err
		}
		request := transfer.Request{Source: source, DestinationRoot: setting.Root, Policy: effective, Transport: client.Transport(), RemoteName: name, CanonicalTarget: canonical}
		return retirementEndpoint{executor: endpoint.executor, request: request, close: endpoint.close}, nil
	}
	return retirementEndpoint{}, fmt.Errorf("recorded target is not configured")
}

func mappedSnapshot(sourceRoot, mappedRoot string, member lifecycle.ReferenceSource) string {
	suffix := strings.TrimPrefix(member.Dataset, sourceRoot)
	return mappedRoot + suffix + "@" + member.Metadata.Name()
}

func verifyReplica(state zfs.State, name, lineage string, guid uint64, expected lifecycle.Metadata) error {
	for _, snapshot := range lifecycle.Snapshots(state, strings.Split(name, "@")[0]) {
		if snapshot.Name != name {
			continue
		}
		metadata, err := lifecycle.Ownership(snapshot, lineage)
		if err != nil || metadata != expected || snapshot.GUID != guid || len(snapshot.Holds) > 0 || len(snapshot.Clones) > 0 || snapshot.ResumeRequired {
			return fmt.Errorf("replica snapshot proof or dependencies changed: %s", name)
		}
		return nil
	}
	// A prior partial retirement may already have removed this exact replica.
	return nil
}

func ownedReplicas(state zfs.State, lineage string) ([]string, []string, error) {
	var datasets, snapshots []string
	for _, object := range state.Objects {
		if object.Type != "filesystem" && object.Type != "volume" {
			continue
		}
		datasets = append(datasets, object.Name)
		for _, snapshot := range lifecycle.Snapshots(state, object.Name) {
			_, err := lifecycle.Ownership(snapshot, lineage)
			if err == nil {
				if len(snapshot.Holds) > 0 || len(snapshot.Clones) > 0 || snapshot.ResumeRequired {
					return nil, nil, fmt.Errorf("dependent owned replica prevents retirement: %s", snapshot.Name)
				}
				snapshots = append(snapshots, snapshot.Name)
				continue
			}
			for _, property := range snapshot.Properties {
				if property.Name == lifecycle.LineageProperty || property.Name == lifecycle.SnapshotProperty || property.Name == lifecycle.CreatedProperty {
					return nil, nil, fmt.Errorf("ambiguous replica ownership metadata: %s", snapshot.Name)
				}
			}
		}
	}
	slices.Sort(datasets)
	slices.Sort(snapshots)
	return datasets, snapshots, nil
}

func (r *Runtime) retireReference(ctx context.Context, source string, effective policy.Effective, sourceState zfs.State, lineage string, reference lifecycle.Reference) (resultErr error) {
	endpoint, err := r.retirementEndpoint(ctx, source, effective, reference.Target)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, endpoint.close()) }()
	inspection, err := transfer.InspectTarget(ctx, endpoint.executor, endpoint.request, sourceState)
	if err != nil || inspection.Status != "verified" || inspection.MappedDataset == "" {
		return errors.Join(fmt.Errorf("target binding is not verified"), err)
	}
	state, err := endpoint.executor.InspectState(ctx, inspection.MappedDataset, true)
	if err != nil {
		return err
	}
	if len(state.ResumeTokens) > 0 {
		return fmt.Errorf("target has resumable receive state")
	}
	members := make(map[string]lifecycle.ReferenceSource)
	for _, member := range reference.Members() {
		name := mappedSnapshot(source, inspection.MappedDataset, member)
		if err := verifyReplica(state, name, lineage, member.GUID, member.Metadata); err != nil {
			return err
		}
		members[name] = member
	}
	datasets, snapshots, err := ownedReplicas(state, lineage)
	if err != nil {
		return err
	}
	// Descendants go first so recursive package members are removed exactly.
	slices.Reverse(snapshots)
	for _, name := range snapshots {
		current, inspectErr := endpoint.executor.InspectState(ctx, strings.Split(name, "@")[0], false)
		if inspectErr != nil {
			return inspectErr
		}
		if member, exists := members[name]; exists {
			if err := verifyReplica(current, name, lineage, member.GUID, member.Metadata); err != nil {
				return err
			}
		} else {
			_, currentOwned, ownedErr := ownedReplicas(current, lineage)
			if ownedErr != nil || !slices.Contains(currentOwned, name) {
				return fmt.Errorf("replica ownership changed: %s", name)
			}
		}
		if err := endpoint.executor.DestroySnapshot(ctx, name); err != nil {
			return err
		}
	}
	remaining, err := endpoint.executor.InspectState(ctx, inspection.MappedDataset, true)
	if err != nil {
		return err
	}
	_, remainingSnapshots, err := ownedReplicas(remaining, lineage)
	if err != nil {
		return err
	}
	if len(remainingSnapshots) > 0 {
		return fmt.Errorf("owned replicas appeared during retirement")
	}
	slices.Reverse(datasets)
	for _, dataset := range datasets {
		for _, property := range remaining.Properties {
			if property.Dataset == dataset && property.Name == lifecycle.LineageProperty && property.Source == zfs.SourceLocal && property.Value == lineage {
				if err := endpoint.executor.InheritProperty(ctx, dataset, lifecycle.LineageProperty); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (r *Runtime) retireTargets(ctx context.Context, dataset string, recursive bool, effective policy.Effective) error {
	state, err := r.backend.InspectState(ctx, dataset, recursive)
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
		if err := r.retireReference(ctx, dataset, effective, state, lineage, reference); err != nil {
			return fmt.Errorf("retire target %s: %w", reference.Target, err)
		}
	}
	return nil
}

func (r *Runtime) pruneDestination(ctx context.Context, dataset string, effective policy.Effective, canonical string) (resultErr error) {
	endpoint, err := r.retirementEndpoint(ctx, dataset, effective, canonical)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, endpoint.close()) }()
	source, err := r.backend.InspectState(ctx, dataset, effective.Send.Replicate)
	if err != nil {
		return err
	}
	lineage, err := lifecycle.RootAuthority(source, dataset, r.installation)
	if err != nil {
		return err
	}
	inspection, err := transfer.InspectTarget(ctx, endpoint.executor, endpoint.request, source)
	if err != nil || inspection.Status != "verified" {
		return errors.Join(fmt.Errorf("target binding is not verified"), err)
	}
	state, err := endpoint.executor.InspectState(ctx, inspection.MappedDataset, effective.Send.Replicate)
	if err != nil {
		return err
	}
	for _, object := range state.Objects {
		if object.Type != "filesystem" && object.Type != "volume" {
			continue
		}
		decisions, planErr := lifecycle.PlanPrune(object.Name, lineage, effective.Grid, lifecycle.Snapshots(state, object.Name))
		if planErr != nil {
			return planErr
		}
		for _, decision := range decisions {
			if !decision.Destroy {
				continue
			}
			current, inspectErr := endpoint.executor.InspectState(ctx, object.Name, false)
			if inspectErr != nil {
				return inspectErr
			}
			found := false
			for _, snapshot := range lifecycle.Snapshots(current, object.Name) {
				if snapshot.Name != decision.Snapshot {
					continue
				}
				_, ownershipErr := lifecycle.Ownership(snapshot, lineage)
				if ownershipErr != nil || snapshot.GUID != decision.GUID || len(snapshot.Holds) > 0 || len(snapshot.Clones) > 0 || snapshot.ResumeRequired {
					return fmt.Errorf("destination prune proof changed: %s", decision.Snapshot)
				}
				found = true
			}
			if !found {
				return fmt.Errorf("destination prune candidate disappeared: %s", decision.Snapshot)
			}
			if err := endpoint.executor.DestroySnapshot(ctx, decision.Snapshot); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *Runtime) enqueueDestinationPrune(dataset string, effective policy.Effective, canonical string) {
	ticket, err := r.gate.Queue(context.Background(), dataset, lifecycle.Management)
	if err != nil {
		return
	}
	job := Job{ID: "destination-prune:" + dataset + ":" + lifecycle.TargetID(canonical), Group: dataset, Scope: dataset, LockKey: canonical, StartState: "pruning", Drop: ticket.Finish}
	job.Run = func(context.Context) Outcome {
		defer ticket.Finish()
		if startErr := ticket.Start(); startErr != nil {
			return Outcome{State: "blocked", Reason: startErr.Error()}
		}
		if pruneErr := r.pruneDestination(ticket.Context(), dataset, effective, canonical); pruneErr != nil {
			return Outcome{State: "failed", Reason: pruneErr.Error()}
		}
		return Outcome{State: "succeeded"}
	}
	if added, submitErr := r.management.Submit(job); submitErr != nil || !added {
		ticket.Finish()
	}
}
