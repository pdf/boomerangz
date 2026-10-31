package daemon

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/pdf/boomerangz/internal/daemonstate"
	"github.com/pdf/boomerangz/internal/discovery"
	"github.com/pdf/boomerangz/internal/policy"
	"github.com/pdf/boomerangz/internal/transfer"
	"github.com/pdf/boomerangz/internal/zfs"
)

// destinationKey names a destination: a local destination root or, with
// remote, a configured remote.
type destinationKey struct {
	name   string
	remote bool
}

// destinationNeed is what one destination must grant: the delegated
// permissions every stream sent there may need, checked at root.
type destinationNeed struct {
	root     string
	required []string
}

type permissionCheck struct {
	need destinationNeed
	// stale asks for a recheck the next time the destination is reached,
	// even though what it must grant has not changed.
	stale  bool
	result *daemonstate.DestinationPermissions // nil until a check completes
}

// permissionChecks holds the latest delegated-permission check of each
// destination, so an installation missing a permission that only a rare
// operation exercises hears of it when the daemon first reaches the
// destination, not when that operation fails.
//
// A destination is checked when it is first reached and again when what it
// must grant changes, when the configuration is reloaded, or, for a remote,
// when its endpoint opens after failing to. Nothing is checked on a timer. A
// finding is a warning: transfers go on, and each operation's own preflight
// still stops the one that needs a missing permission.
type permissionChecks struct {
	status *Status
	logger *slog.Logger
	now    func() time.Time

	mu     sync.Mutex
	checks map[destinationKey]*permissionCheck
}

func newPermissionChecks(status *Status, logger *slog.Logger, now func() time.Time) *permissionChecks {
	return &permissionChecks{status: status, logger: logger, now: now, checks: make(map[destinationKey]*permissionCheck)}
}

// destinationNeeds returns what each destination the generation sends to
// must grant. A remote is included only when settings configures it.
func destinationNeeds(generation *discovery.Generation, installation string, remoteRoots map[string]string) map[destinationKey]destinationNeed {
	policies := make(map[destinationKey][]policy.Effective)
	if generation != nil {
		for _, entry := range generation.Entries() {
			if !entry.Policy.Enabled || !entry.Policy.Valid() || !actionableRoot(entry, installation) {
				continue
			}
			for _, root := range entry.Policy.Local {
				key := destinationKey{name: root}
				policies[key] = append(policies[key], entry.Policy)
			}
			for _, name := range entry.Policy.Remote {
				if _, configured := remoteRoots[name]; configured {
					key := destinationKey{name: name, remote: true}
					policies[key] = append(policies[key], entry.Policy)
				}
			}
		}
	}
	needs := make(map[destinationKey]destinationNeed, len(policies))
	for key, effective := range policies {
		root := key.name
		if key.remote {
			root = remoteRoots[key.name]
		}
		needs[key] = destinationNeed{root: root, required: transfer.DestinationPermissions(effective...)}
	}
	return needs
}

// update makes the checks follow needs: a destination nothing sends to any
// more is forgotten, and one whose need changed is due again.
func (p *permissionChecks) update(needs map[destinationKey]destinationNeed) {
	p.mu.Lock()
	defer p.mu.Unlock()
	changed := false
	for key, check := range p.checks {
		if _, needed := needs[key]; !needed {
			delete(p.checks, key)
			changed = changed || check.result != nil
		}
	}
	if changed {
		p.publishLocked()
	}
}

// invalidate makes every destination's check due again, or only remote
// ones.
func (p *permissionChecks) invalidate(remoteOnly bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for key, check := range p.checks {
		if key.remote || !remoteOnly {
			check.stale = true
		}
	}
}

// unreached records that a remote's endpoint failed to open, so the open that
// next succeeds checks it again.
func (p *permissionChecks) unreached(name string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if check, found := p.checks[destinationKey{name: name, remote: true}]; found {
		check.stale = true
	}
}

// begin reports whether key is due a check against need, and if so claims it,
// so concurrent reaches of one destination check it once.
func (p *permissionChecks) begin(key destinationKey, need destinationNeed) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	check, found := p.checks[key]
	if found && !check.stale && check.need.root == need.root && slices.Equal(check.need.required, need.required) {
		return false
	}
	if !found {
		check = &permissionCheck{}
		p.checks[key] = check
	}
	check.need, check.stale = need, false
	return true
}

// check runs a due check of key through executor and records it.
func (p *permissionChecks) check(ctx context.Context, key destinationKey, need destinationNeed, executor zfs.Executor) {
	if !p.begin(key, need) {
		return
	}
	result := checkDestination(ctx, executor, need)
	result.Destination, result.Remote, result.Checked = key.name, key.remote, p.now().UTC()
	p.mu.Lock()
	defer p.mu.Unlock()
	check, found := p.checks[key]
	if !found || check.need.root != need.root || !slices.Equal(check.need.required, need.required) {
		// Forgotten or superseded while it ran.
		return
	}
	if ctx.Err() != nil {
		// The work reaching the destination ended; the next reach checks.
		check.stale = true
		return
	}
	previous := check.result
	check.result = &result
	p.log(previous, result)
	p.publishLocked()
}

func (p *permissionChecks) log(previous *daemonstate.DestinationPermissions, result daemonstate.DestinationPermissions) {
	if previous != nil && previous.Dataset == result.Dataset && previous.Account == result.Account && slices.Equal(previous.Missing, result.Missing) && previous.Error == result.Error {
		return
	}
	attrs := []any{"destination", result.Destination, "remote", result.Remote}
	switch {
	case len(result.Missing) > 0:
		p.logger.Warn("destination lacks delegated ZFS permissions boomerangz may need; operations that need them will fail", append(attrs, "dataset", result.Dataset, "account", result.Account, "missing", strings.Join(result.Missing, ","))...)
	case result.Error != "":
		p.logger.Warn("could not check destination ZFS permissions", append(attrs, "error", result.Error)...)
	case previous != nil:
		p.logger.Info("destination grants every delegated ZFS permission boomerangz may need", append(attrs, "dataset", result.Dataset)...)
	}
}

// publishLocked sends every completed check to the status owner, local
// destinations first and each kind by name. Callers hold p.mu.
func (p *permissionChecks) publishLocked() {
	results := make([]daemonstate.DestinationPermissions, 0, len(p.checks))
	for _, key := range slices.SortedFunc(maps.Keys(p.checks), func(a, b destinationKey) int {
		if a.remote != b.remote {
			if a.remote {
				return 1
			}
			return -1
		}
		return cmp.Compare(a.name, b.name)
	}) {
		if result := p.checks[key].result; result != nil {
			result := *result
			result.Missing = slices.Clone(result.Missing)
			results = append(results, result)
		}
	}
	p.status.publishDestinations(results)
}

// checkDestination checks need at its root or, while the root is absent, at
// its nearest existing ancestor: the anchor a transfer's own preflight checks.
func checkDestination(ctx context.Context, executor zfs.Executor, need destinationNeed) daemonstate.DestinationPermissions {
	inventory, err := executor.ListDatasets(ctx)
	if err != nil {
		return daemonstate.DestinationPermissions{Error: fmt.Sprintf("list destination datasets: %v", err)}
	}
	anchor := ""
	for _, dataset := range inventory {
		if (need.root == dataset.Name || strings.HasPrefix(need.root, dataset.Name+"/")) && len(dataset.Name) > len(anchor) {
			anchor = dataset.Name
		}
	}
	if anchor == "" {
		return daemonstate.DestinationPermissions{Error: fmt.Sprintf("destination root %s has no existing dataset at or above it", need.root)}
	}
	result := daemonstate.DestinationPermissions{Dataset: anchor}
	err = executor.CheckPermissions(ctx, anchor, slices.Clone(need.required))
	var missing *zfs.MissingPermissionsError
	switch {
	case errors.As(err, &missing):
		result.Account, result.Missing = missing.Account, slices.Clone(missing.Missing)
	case err != nil:
		result.Error = err.Error()
	}
	return result
}
