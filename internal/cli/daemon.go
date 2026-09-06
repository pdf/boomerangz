package cli

import (
	"context"

	"github.com/pdf/boomerangz/internal/zfs"
)

// daemonBackend includes stream estimation in addition to the typed executor.
// Keeping it local prevents administrative readers from gaining mutation APIs.
type daemonBackend interface {
	zfs.Executor
	EstimateSend(context.Context, zfs.SendOptions) (zfs.Estimate, error)
}
