package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"

	"github.com/pdf/boomerangz/internal/config"
	"github.com/pdf/boomerangz/internal/lifecycle"
	"github.com/pdf/boomerangz/internal/zfs"
)

// Standalone commands fail closed around daemon coordination and target probes.
// The daemon phase must hold the same lock for its lifetime before accepting work.
type standaloneSafety struct{ socket string }

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
	return fmt.Errorf("control socket exists; daemon-coordinated lifecycle operations are not yet available")
}
func (standaloneSafety) CheckTarget(_ context.Context, target string) error {
	return fmt.Errorf("target verification is not yet available for %s; retaining recovery references", target)
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
	state, err := executor.InspectState(ctx, dataset, false)
	if err != nil {
		return err
	}
	lineage, err := lifecycle.AdoptionLineage(state, dataset)
	if err != nil {
		return err
	}
	if apply {
		service, err := lifecycle.NewService(executor)
		if err != nil {
			return err
		}
		lineage, err = service.AdoptDataset(ctx, dataset)
		if err != nil {
			return err
		}
	}
	return json.NewEncoder(out).Encode(struct {
		Dataset string `json:"dataset"`
		Lineage string `json:"lineage"`
		Applied bool   `json:"applied"`
	}{dataset, lineage, apply})
}

func cleanupScopes(ctx context.Context, executor zfs.Executor, names []string, recursive, all bool) ([]string, bool, error) {
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

func runCleanup(ctx context.Context, out io.Writer, cfg config.Config, executor zfs.Executor, names []string, recursive, all, destroy, apply bool) (resultErr error) {
	scopes, recursive, err := cleanupScopes(ctx, executor, names, recursive, all)
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
	service, err := lifecycle.NewService(executor)
	if err != nil {
		return err
	}
	safety := standaloneSafety{socket: cfg.Paths.SocketPath}
	options := lifecycle.CleanupOptions{Recursive: recursive, DestroyOwnedSnapshots: destroy}
	var plans []lifecycle.CleanupPlan
	blocked := false
	for _, name := range scopes {
		plan, err := service.Cleanup(ctx, name, options, false, safety)
		if err != nil {
			return err
		}
		plans = append(plans, plan)
		blocked = blocked || len(plan.Blockers) > 0
	}
	if apply && !blocked {
		for i, name := range scopes {
			plan, err := service.Cleanup(ctx, name, options, true, safety)
			plans[i] = plan
			if err != nil {
				resultErr = err
				break
			}
		}
	} else if apply && blocked {
		resultErr = fmt.Errorf("cleanup blocked; no changes applied")
	}
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	return errors.Join(resultErr, encoder.Encode(plans))
}
