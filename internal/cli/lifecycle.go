package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"syscall"

	"github.com/pdf/boomerangz/internal/config"
	"github.com/pdf/boomerangz/internal/daemon"
	"github.com/pdf/boomerangz/internal/discovery"
	"github.com/pdf/boomerangz/internal/identity"
	"github.com/pdf/boomerangz/internal/lifecycle"
	"github.com/pdf/boomerangz/internal/policy"
	replicationssh "github.com/pdf/boomerangz/internal/replication/ssh"
	"github.com/pdf/boomerangz/internal/transfer"
	"github.com/pdf/boomerangz/internal/zfs"
)

// Standalone commands fail closed around daemon coordination.
// The daemon holds the same lock for its lifetime before accepting work.
type standaloneSafety struct {
	socket  string
	targets *daemon.TargetChecker
}

func (s standaloneSafety) Quiescent(ctx context.Context, _ []string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err := os.Lstat(s.socket)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	return fmt.Errorf("control socket exists; stop the daemon before this lifecycle operation")
}
func (s standaloneSafety) CheckTarget(ctx context.Context, source, target string) error {
	if s.targets == nil {
		return fmt.Errorf("target verification is unavailable for %s", target)
	}
	return s.targets.CheckTarget(ctx, source, target)
}

func lifecycleLock(cfg config.Config) (*os.File, error) {
	path := cfg.Paths.SocketPath + ".lifecycle.lock"
	if err := os.MkdirAll(filepath.Dir(path), 0750); err != nil {
		return nil, err
	}
	fd, err := syscall.Open(path, syscall.O_CREAT|syscall.O_RDWR|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), path)
	if err := syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return nil, errors.Join(fmt.Errorf("another lifecycle operation is running: %w", err), file.Close())
	}
	return file, nil
}

func runAdopt(ctx context.Context, out io.Writer, cfg config.Config, executor zfs.Executor, dataset string, apply bool) (resultErr error) {
	if err := zfs.ValidateDataset(dataset); err != nil {
		return err
	}
	safety := standaloneSafety{socket: cfg.Paths.SocketPath}
	if apply {
		lock, err := lifecycleLock(cfg)
		if err != nil {
			return err
		}
		defer func() { resultErr = errors.Join(resultErr, lock.Close()) }()
	}
	if err := safety.Quiescent(ctx, []string{dataset}); err != nil {
		return err
	}
	installation, err := identity.LoadOrCreate(cfg.Paths.IdentityDir)
	if err != nil {
		return err
	}
	state, err := executor.InspectState(ctx, dataset, false)
	if err != nil {
		return err
	}
	lineage, err := lifecycle.AdoptionLineage(state, dataset)
	if err != nil {
		return err
	}
	oldOwner, err := localMarker(state.Properties, dataset, lifecycle.OwnerProperty)
	if err != nil {
		return err
	}
	if oldOwner != "" && !identity.Valid(oldOwner) {
		return fmt.Errorf("existing local owner is invalid")
	}
	ownedLineage, authorityErr := lifecycle.RootAuthority(state, dataset, installation)
	alreadyOwned := authorityErr == nil && ownedLineage == lineage
	var remoteNames []string
	for name := range cfg.Remotes {
		remoteNames = append(remoteNames, name)
	}
	scanner, err := discovery.New(executor, discovery.Options{Remotes: remoteNames})
	if err != nil {
		return err
	}
	generation, err := scanner.Scan(ctx, []string{dataset})
	if err != nil {
		return err
	}
	entry, exists := generation.Inspect(dataset)
	if !exists || !entry.Inspected {
		return fmt.Errorf("dataset policy inspection is incomplete")
	}
	effective := entry.Policy
	if err := lifecycle.ActiveRoot(effective, dataset); err != nil {
		return err
	}
	inspectLocal := func() ([]transfer.LocalTargetInspection, []string) {
		var inspections []transfer.LocalTargetInspection
		var blockers []string
		for _, target := range effective.Local {
			inspection, inspectErr := transfer.InspectLocalTarget(ctx, executor, transfer.Request{Source: dataset, DestinationRoot: target, Policy: effective}, state)
			inspections = append(inspections, inspection)
			if inspectErr != nil {
				blockers = append(blockers, fmt.Sprintf("local target %s: %v", target, inspectErr))
			}
		}
		return inspections, blockers
	}
	localTargets, blockers := inspectLocal()
	inspectRemote := func() ([]transfer.LocalTargetInspection, []string) {
		var inspections []transfer.LocalTargetInspection
		var remoteBlockers []string
		for _, name := range effective.Remote {
			remote := cfg.Remotes[name]
			client, clientErr := replicationssh.New("ssh", replicationssh.Config{Host: remote.Host, Port: remote.Port, User: remote.User, Root: remote.Root, IdentityFile: remote.IdentityFile, ShellPath: remote.SSHShellPath, ConnectTimeout: remote.ConnectTimeout.Duration})
			if clientErr != nil {
				remoteBlockers = append(remoteBlockers, fmt.Sprintf("remote target %s: %v", name, clientErr))
				continue
			}
			inspection := transfer.LocalTargetInspection{ConfiguredName: name, Transport: "ssh", CanonicalEndpoint: client.CanonicalTarget(), DestinationRoot: remote.Root, EndpointMode: remote.Endpoint, Status: "unavailable"}
			endpoint, openErr := replicationssh.OpenEndpoint(ctx, client, "zfs", remote.Endpoint)
			if openErr != nil {
				if replicationssh.IsUnavailable(openErr) {
					inspection.Status = "unverified-suspended"
				} else {
					remoteBlockers = append(remoteBlockers, fmt.Sprintf("remote target %s: %v", name, openErr))
				}
				inspections = append(inspections, inspection)
				continue
			}
			inspection.EndpointMode = endpoint.Mode
			request := transfer.Request{Source: dataset, DestinationRoot: remote.Root, Policy: effective, Transport: "ssh", RemoteName: name, CanonicalTarget: client.CanonicalTarget()}
			resolved, inspectErr := transfer.InspectTarget(ctx, endpoint.Executor, request, state)
			resolved.EndpointMode = endpoint.Mode
			inspection = resolved
			closeErr := endpoint.Close()
			if inspectErr != nil {
				if replicationssh.IsUnavailable(inspectErr) {
					inspection.Status = "unverified-suspended"
				} else {
					remoteBlockers = append(remoteBlockers, fmt.Sprintf("remote target %s: %v", name, inspectErr))
				}
			}
			if closeErr != nil {
				remoteBlockers = append(remoteBlockers, fmt.Sprintf("remote target %s close: %v", name, closeErr))
			}
			inspections = append(inspections, inspection)
		}
		return inspections, remoteBlockers
	}
	remoteTargets, remoteBlockers := inspectRemote()
	blockers = append(blockers, remoteBlockers...)
	suspendedTargets := make(map[string]bool)
	for _, remote := range remoteTargets {
		if remote.CanonicalEndpoint == "" {
			continue
		}
		suspended, suspendedErr := transfer.TargetSuspended(state, dataset, remote.CanonicalEndpoint)
		if suspendedErr != nil {
			blockers = append(blockers, fmt.Sprintf("remote target %s: %v", remote.ConfiguredName, suspendedErr))
			continue
		}
		suspendedTargets[remote.CanonicalEndpoint] = suspended
	}
	type adoptionResult struct {
		Dataset       string                           `json:"dataset"`
		Lineage       string                           `json:"lineage"`
		OldOwner      string                           `json:"old_owner,omitempty"`
		NewOwner      string                           `json:"new_owner"`
		Policy        policy.Effective                 `json:"policy"`
		LocalTargets  []transfer.LocalTargetInspection `json:"local_targets,omitempty"`
		RemoteTargets []transfer.LocalTargetInspection `json:"remote_targets,omitempty"`
		Blockers      []string                         `json:"blockers,omitempty"`
		Warning       string                           `json:"warning"`
		Applied       bool                             `json:"applied"`
	}
	result := adoptionResult{Dataset: dataset, Lineage: lineage, OldOwner: oldOwner, NewOwner: installation, Policy: effective, LocalTargets: localTargets, RemoteTargets: remoteTargets, Blockers: blockers, Warning: "transferred policy may name absent, unrelated, or inappropriate destinations on this host"}
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	if len(blockers) > 0 {
		if !apply {
			return encoder.Encode(result)
		}
		return errors.Join(fmt.Errorf("adoption blocked; no owner change applied"), encoder.Encode(result))
	}
	if apply {
		revalidated, revalidationBlockers := inspectLocal()
		revalidatedRemote, remoteRevalidationBlockers := inspectRemote()
		if len(revalidationBlockers) > 0 || len(remoteRevalidationBlockers) > 0 || !reflect.DeepEqual(localTargets, revalidated) || !reflect.DeepEqual(remoteTargets, revalidatedRemote) {
			result.Blockers = append(result.Blockers, "target identity changed during adoption; retry preview")
			return errors.Join(fmt.Errorf("adoption blocked; no owner change applied"), encoder.Encode(result))
		}
		if alreadyOwned {
			current, inspectErr := executor.InspectState(ctx, dataset, false)
			if inspectErr != nil || !reflect.DeepEqual(state, current) {
				result.Blockers = append(result.Blockers, "source state changed during target revalidation; retry preview")
				return errors.Join(fmt.Errorf("target revalidation blocked; no changes applied"), inspectErr, encoder.Encode(result))
			}
		}
		for _, remote := range revalidatedRemote {
			if remote.Status == "unverified-suspended" {
				if err := transfer.SetTargetSuspended(ctx, executor, dataset, remote.CanonicalEndpoint, true); err != nil {
					return err
				}
			}
		}
		if !alreadyOwned {
			service, serviceErr := lifecycle.NewService(executor, installation)
			if serviceErr != nil {
				return serviceErr
			}
			if _, serviceErr = service.AdoptDataset(ctx, dataset, effective); serviceErr != nil {
				return serviceErr
			}
		}
		for _, remote := range revalidatedRemote {
			if remote.Status != "unverified-suspended" && suspendedTargets[remote.CanonicalEndpoint] {
				if err := transfer.SetTargetSuspended(ctx, executor, dataset, remote.CanonicalEndpoint, false); err != nil {
					return err
				}
			}
		}
		result.Applied = true
	}
	return encoder.Encode(result)
}

func cleanScopes(ctx context.Context, executor zfs.Executor, names []string, recursive, all bool) ([]string, bool, error) {
	if all && len(names) > 0 {
		return nil, false, fmt.Errorf("--all cannot be combined with dataset names")
	}
	if !all && len(names) == 0 {
		return nil, false, fmt.Errorf("specify dataset names or explicit --all")
	}
	if all {
		inventory, err := executor.ListDatasets(ctx)
		if err != nil {
			return nil, false, err
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
			return nil, false, err
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
	return scopes, recursive, nil
}

func runClean(ctx context.Context, out io.Writer, cfg config.Config, executor zfs.Executor, names []string, recursive, all, destroy, apply bool) (resultErr error) {
	if handled, err := runDaemonClean(ctx, out, cfg, names, recursive, all, destroy, apply); handled {
		return err
	}
	scopes, recursive, err := cleanScopes(ctx, executor, names, recursive, all)
	if err != nil {
		return err
	}
	if apply {
		lock, err := lifecycleLock(cfg)
		if err != nil {
			return err
		}
		defer func() { resultErr = errors.Join(resultErr, lock.Close()) }()
	}
	service, err := lifecycle.NewCleanService(executor)
	if err != nil {
		return err
	}
	targets, err := daemon.NewTargetChecker(executor, cfg.Remotes, cfg.Paths.CredentialsDir)
	if err != nil {
		return err
	}
	safety := standaloneSafety{socket: cfg.Paths.SocketPath, targets: targets}
	options := lifecycle.CleanOptions{Recursive: recursive, DestroyOwnedSnapshots: destroy}
	var plans []lifecycle.CleanPlan
	blocked := false
	for _, name := range scopes {
		plan, err := service.Clean(ctx, name, options, false, safety)
		if err != nil {
			return err
		}
		plans = append(plans, plan)
		blocked = blocked || len(plan.Blockers) > 0
	}
	if apply && !blocked {
		for i, name := range scopes {
			plan, err := service.Clean(ctx, name, options, true, safety)
			plans[i] = plan
			if err != nil {
				resultErr = err
				break
			}
		}
	} else if apply && blocked {
		resultErr = fmt.Errorf("clean blocked; no changes applied")
	}
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	return errors.Join(resultErr, encoder.Encode(plans))
}
