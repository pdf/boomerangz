package daemon

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/pdf/boomerangz/internal/config"
	"github.com/pdf/boomerangz/internal/lifecycle"
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

func (s *Safety) setTargets(targets *TargetChecker) {
	s.mu.Lock()
	s.targets = targets
	s.mu.Unlock()
}

// CheckTarget verifies one recorded target without changing either endpoint.
func (s *TargetChecker) CheckTarget(ctx context.Context, source, target string) error {
	if s == nil || s.local == nil {
		return fmt.Errorf("target verification is unavailable")
	}
	sourceState, err := s.local.InspectState(ctx, source, false)
	if err != nil {
		return err
	}
	if root, local := strings.CutPrefix(target, "local:"); local {
		if err := zfs.ValidateDataset(root); err != nil {
			return err
		}
		binding, err := transfer.VerifyTargetBinding(ctx, s.local, sourceState, source, target, root)
		if err != nil {
			return err
		}
		return checkResumeState(ctx, s.local, binding.MappedDataset)
	}
	for name, client := range s.remotes {
		if client.CanonicalTarget() != target {
			continue
		}
		setting := s.settings[name]
		endpoint, err := client.Open(ctx)
		if err != nil {
			return err
		}
		binding, bindingErr := transfer.VerifyTargetBinding(ctx, endpoint.executor, sourceState, source, target, setting.Root)
		checkErr := bindingErr
		if bindingErr == nil {
			checkErr = checkResumeState(ctx, endpoint.executor, binding.MappedDataset)
		}
		if closeErr := endpoint.close(); closeErr != nil {
			return errors.Join(checkErr, fmt.Errorf("target close: %w", closeErr))
		}
		return checkErr
	}
	return fmt.Errorf("recorded target is not configured")
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
