package daemon

import (
	"context"
	"fmt"
	"strings"

	"github.com/pdf/boomerangz/internal/config"
	"github.com/pdf/boomerangz/internal/lifecycle"
	replicationssh "github.com/pdf/boomerangz/internal/replication/ssh"
	"github.com/pdf/boomerangz/internal/zfs"
)

// Safety is the live daemon implementation shared by automatic retirement and
// the future daemon-coordinated explicit clean control method.
type Safety struct {
	gate     *lifecycle.Gate
	local    zfs.Executor
	remotes  map[string]*replicationssh.Client
	settings map[string]config.RemoteConfig
}

func newSafety(gate *lifecycle.Gate, local zfs.Executor, remotes map[string]*replicationssh.Client, settings map[string]config.RemoteConfig) *Safety {
	return &Safety{gate: gate, local: local, remotes: remotes, settings: settings}
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
// inaccessible or resumable destinations without changing either endpoint.
func (s *Safety) CheckTarget(ctx context.Context, target string) error {
	if s == nil || s.local == nil {
		return fmt.Errorf("live target verification is unavailable")
	}
	if root, local := strings.CutPrefix(target, "local:"); local {
		if err := zfs.ValidateDataset(root); err != nil {
			return err
		}
		return checkResumeState(ctx, s.local, root)
	}
	for name, client := range s.remotes {
		if client.CanonicalTarget() != target {
			continue
		}
		setting := s.settings[name]
		endpoint, err := replicationssh.OpenEndpoint(ctx, client, "zfs", setting.Endpoint)
		if err != nil {
			return err
		}
		checkErr := checkResumeState(ctx, endpoint.Executor, setting.Root)
		if closeErr := endpoint.Close(); closeErr != nil {
			return fmt.Errorf("target close: %w", closeErr)
		}
		return checkErr
	}
	return fmt.Errorf("recorded target is not configured")
}
