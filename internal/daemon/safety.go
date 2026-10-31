package daemon

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/pdf/boomerangz/internal/config"
	"github.com/pdf/boomerangz/internal/discovery"
	"github.com/pdf/boomerangz/internal/lifecycle"
	"github.com/pdf/boomerangz/internal/policy"
	"github.com/pdf/boomerangz/internal/transfer"
	"github.com/pdf/boomerangz/internal/zfs"
)

// Safety combines live daemon quiescence with reusable target verification.
type Safety struct {
	gate    *lifecycle.Gate
	mu      sync.RWMutex
	targets *TargetChecker
}

// TargetChecker performs read-only just-in-time checks against configured targets.
type TargetChecker struct {
	local    zfs.Executor
	remotes  map[string]remoteClient
	settings map[string]config.RemoteConfig
}

// TargetEndpoint is one configured remote destination opened for an explicit
// standalone administrative operation.
type TargetEndpoint struct {
	Executor        zfs.Executor
	Transport       string
	CanonicalTarget string
	Root            string
	close           func() error
}

// Close releases the remote endpoint.
func (e *TargetEndpoint) Close() error {
	if e == nil || e.close == nil {
		return nil
	}
	return e.close()
}

func newSafety(gate *lifecycle.Gate, local zfs.Executor, remotes map[string]remoteClient, settings map[string]config.RemoteConfig) *Safety {
	return &Safety{gate: gate, targets: &TargetChecker{local: local, remotes: remotes, settings: settings}}
}

// NewTargetChecker builds the same target verifier for standalone administration.
func NewTargetChecker(local zfs.Executor, settings map[string]config.RemoteConfig, credentialsDir ...string) (*TargetChecker, error) {
	if local == nil {
		return nil, fmt.Errorf("local target executor is required")
	}
	directory := config.DefaultCredentialsDir
	if len(credentialsDir) != 0 {
		directory = credentialsDir[0]
	}
	clients, err := buildRemoteClients(settings, directory)
	if err != nil {
		return nil, err
	}
	return &TargetChecker{local: local, remotes: clients, settings: settings}, nil
}

// Quiescent verifies that every selected scheduling scope is disabled and idle.
func (s *Safety) Quiescent(ctx context.Context, datasets []string) error {
	if s == nil || s.gate == nil {
		return fmt.Errorf("live daemon quiescence is unavailable")
	}
	for _, dataset := range datasets {
		if err := s.gate.WaitQuiescent(ctx, dataset); err != nil {
			return fmt.Errorf("dataset %s is not quiescent: %w", dataset, err)
		}
	}
	return nil
}

func checkResumeState(ctx context.Context, executor zfs.Executor, root string) error {
	state, err := executor.InspectState(ctx, root, true)
	if err != nil {
		return err
	}
	if len(state.ResumeTokens) > 0 {
		return fmt.Errorf("target has resumable receive state")
	}
	return nil
}

// CheckTarget probes a recorded canonical target just in time and refuses
// inaccessible, identity-mismatched, or resumable destinations without changing
// either endpoint.
func (s *Safety) CheckTarget(ctx context.Context, source, target string) error {
	if s == nil {
		return fmt.Errorf("live target verification is unavailable")
	}
	s.mu.RLock()
	targets := s.targets
	s.mu.RUnlock()
	if targets == nil {
		return fmt.Errorf("live target verification is unavailable")
	}
	return targets.CheckTarget(ctx, source, target)
}

// TargetID resolves an operator's name for a target, as TargetChecker does.
func (s *Safety) TargetID(name string) (string, error) {
	if s == nil {
		return "", fmt.Errorf("target resolution is unavailable")
	}
	s.mu.RLock()
	targets := s.targets
	s.mu.RUnlock()
	return targets.TargetID(name)
}

func (s *Safety) setTargets(targets *TargetChecker) {
	s.mu.Lock()
	s.targets = targets
	s.mu.Unlock()
}

// CheckTarget verifies one recorded target without changing either endpoint.
// A recovery proof records only the hash of its target's canonical identity, so
// targetID is matched against each configured remote and each local target the
// source's policy, or a member's, configures. A target no longer configured
// cannot be checked. The binding is rebuilt from the source's policy, which
// says where the replica is mapped.
func (s *TargetChecker) CheckTarget(ctx context.Context, source, targetID string) error {
	if s == nil || s.local == nil {
		return fmt.Errorf("target verification is unavailable")
	}
	sourceState, err := s.local.InspectState(ctx, source, false)
	if err != nil {
		return err
	}
	effective, locals, err := s.sourcePolicy(ctx, source)
	if err != nil {
		return err
	}
	for name, client := range s.remotes {
		target := client.CanonicalTarget()
		if lifecycle.TargetID(target) != targetID {
			continue
		}
		setting := s.settings[name]
		endpoint, err := client.Open(ctx)
		if err != nil {
			return err
		}
		request := transfer.Request{Source: source, DestinationRoot: setting.Root, Policy: effective, Transport: client.Transport(), RemoteName: name, CanonicalTarget: target}
		binding, bindingErr := transfer.VerifyTargetBinding(ctx, endpoint.executor, sourceState, request)
		checkErr := bindingErr
		if bindingErr == nil {
			checkErr = checkResumeState(ctx, endpoint.executor, binding.MappedDataset)
		}
		if closeErr := endpoint.close(); closeErr != nil {
			return errors.Join(checkErr, fmt.Errorf("target close: %w", closeErr))
		}
		return checkErr
	}
	for _, root := range locals {
		if lifecycle.TargetID("local:"+root) != targetID {
			continue
		}
		binding, err := transfer.VerifyTargetBinding(ctx, s.local, sourceState, transfer.Request{Source: source, DestinationRoot: root, Policy: effective})
		if err != nil {
			return err
		}
		return checkResumeState(ctx, s.local, binding.MappedDataset)
	}
	return fmt.Errorf("recorded target is not configured")
}

// TargetID resolves an operator's name for a target to the ID its state is
// recorded under, without needing the target to be reachable: a target ID is
// taken as it is, a configured remote's name is its canonical target's ID, and
// any other dataset name is a local destination root, configured or not. A
// remote no longer configured can only be named by its ID, which a clean
// blocker for it reports.
func (s *TargetChecker) TargetID(name string) (string, error) {
	if lifecycle.ValidTargetID(name) {
		return name, nil
	}
	if s != nil {
		if client, ok := s.remotes[name]; ok {
			return lifecycle.TargetID(client.CanonicalTarget()), nil
		}
	}
	if err := zfs.ValidateDataset(name); err != nil {
		return "", fmt.Errorf("target %q is neither a target ID, a configured remote nor a local destination root", name)
	}
	return lifecycle.TargetID("local:" + name), nil
}

// sourcePolicy resolves source's policy, and the local destinations it and
// its members configure. A root's members may send where the root itself does
// not, and their proofs are on the root.
func (s *TargetChecker) sourcePolicy(ctx context.Context, source string) (policy.Effective, []string, error) {
	reader, ok := s.local.(discovery.Reader)
	if !ok {
		return policy.Effective{}, nil, fmt.Errorf("source policy is unavailable")
	}
	var remotes []string
	for name := range s.settings {
		remotes = append(remotes, name)
	}
	scanner, err := discovery.New(reader, discovery.Options{Remotes: remotes})
	if err != nil {
		return policy.Effective{}, nil, err
	}
	generation, err := scanner.Scan(ctx, []string{source})
	if err != nil {
		return policy.Effective{}, nil, err
	}
	entry, exists := generation.Inspect(source)
	if !exists || !entry.Inspected {
		return policy.Effective{}, nil, fmt.Errorf("source policy inspection is incomplete")
	}
	locals := slices.Clone(entry.Policy.Local)
	for _, member := range entry.Members {
		if memberEntry, exists := generation.Inspect(member); exists {
			locals = append(locals, memberEntry.Policy.Local...)
		}
	}
	slices.Sort(locals)
	return entry.Policy, slices.Compact(locals), nil
}

// OpenConfigured opens one named remote using the same transport construction
// as the daemon.
func (s *TargetChecker) OpenConfigured(ctx context.Context, name string) (*TargetEndpoint, error) {
	if s == nil {
		return nil, fmt.Errorf("target verification is unavailable")
	}
	client, exists := s.remotes[name]
	if !exists {
		return nil, fmt.Errorf("remote target %q is not configured", name)
	}
	setting := s.settings[name]
	opened, err := client.Open(ctx)
	if err != nil {
		return nil, err
	}
	return &TargetEndpoint{Executor: opened.executor, Transport: client.Transport(), CanonicalTarget: client.CanonicalTarget(), Root: setting.Root, close: opened.close}, nil
}
