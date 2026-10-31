package transfer

import (
	"context"
	"fmt"
	"slices"

	"github.com/pdf/boomerangz/internal/zfs"
)

// HostLocalError reports a remote whose destination pool is one of this host's
// own pools, such as an SSH remote addressed back at this host. Discovery checks
// a remote's replicas only against other replicas reaching the same remote
// address, so nothing has yet checked this one against the actor roots and
// local replicas it shares a pool with. The caller re-validates with the remote
// counted as local and retries with Request.HostLocal set.
type HostLocalError struct {
	Remote string
	Pool   string
}

func (e *HostLocalError) Error() string {
	return fmt.Sprintf("remote %s receives into pool %s on this host; its replicas must be validated against this host's datasets first", e.Remote, e.Pool)
}

// Temporary lets the caller retry once it has revalidated the remote.
func (*HostLocalError) Temporary() bool { return true }

// checkHostLocal refuses a remote destination on a pool this host holds unless
// the request records that its replicas were validated as local ones. Pool
// GUIDs identify the pool itself, so a remote is recognised however it is
// addressed.
func checkHostLocal(ctx context.Context, local zfs.Executor, request Request, destination zfs.DatasetIdentity) error {
	if requestTransport(request) == "local" || request.HostLocal {
		return nil
	}
	inventory, err := local.ListDatasets(ctx)
	if err != nil {
		return err
	}
	if !slices.ContainsFunc(inventory, func(d zfs.Dataset) bool { return d.Name == destination.Pool }) {
		return nil
	}
	pool, err := local.InspectDatasetIdentity(ctx, destination.Pool)
	if err != nil {
		return err
	}
	if pool.PoolGUID != destination.PoolGUID {
		return nil
	}
	return &HostLocalError{Remote: request.RemoteName, Pool: destination.Pool}
}
