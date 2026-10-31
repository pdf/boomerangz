package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"

	"github.com/pdf/boomerangz/internal/config"
	"github.com/pdf/boomerangz/internal/migrate"
	"github.com/pdf/boomerangz/internal/zfs"
)

// runMigrate previews, and with apply performs, the migration of each
// selected root. Preview may run beside the daemon; apply may not, since the
// daemon refuses the roots it would be rewriting, and nothing is applied
// while any selected root is blocked.
func runMigrate(ctx context.Context, out io.Writer, cfg config.Config, executor zfs.Executor, names []string, all, apply bool) (resultErr error) {
	if all && len(names) > 0 {
		return fmt.Errorf("--all cannot be combined with dataset names")
	}
	if !all && len(names) == 0 {
		return fmt.Errorf("specify dataset names or explicit --all")
	}
	if all {
		roots, err := migrate.Roots(ctx, executor)
		if err != nil {
			return err
		}
		names = roots
	}
	names = slices.Clone(names)
	slices.Sort(names)
	names = slices.Compact(names)
	for _, name := range names {
		if err := zfs.ValidateDataset(name); err != nil {
			return err
		}
	}
	if apply {
		if err := (standaloneSafety{socket: cfg.Paths.SocketPath}).Quiescent(ctx, names); err != nil {
			return err
		}
		lock, err := lifecycleLock(cfg)
		if err != nil {
			return err
		}
		defer func() { resultErr = errors.Join(resultErr, lock.Close()) }()
	}
	plans := make([]migrate.Plan, 0, len(names))
	blocked := false
	for _, name := range names {
		plan, err := migrate.Preview(ctx, executor, name)
		if err != nil {
			return err
		}
		plans = append(plans, plan)
		blocked = blocked || plan.Status == migrate.StatusBlocked
	}
	switch {
	case apply && blocked:
		resultErr = fmt.Errorf("migration blocked; no changes applied")
	case apply:
		for i, plan := range plans {
			if plan.Status != migrate.StatusPending {
				continue
			}
			applied, err := migrate.Apply(ctx, executor, plan.Dataset)
			plans[i] = applied
			if err != nil {
				resultErr = err
				break
			}
		}
	}
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	return errors.Join(resultErr, encoder.Encode(plans))
}
