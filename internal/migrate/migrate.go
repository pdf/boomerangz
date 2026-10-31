// Package migrate moves a source root's state from the format it was written
// in to the one this build reads (lifecycle.CurrentVersion), one format at a
// time.
//
// Each step knows two formats and nothing else: it decodes the records of the
// format it starts from and encodes those of the next, with its own frozen
// copies of both. It must never call the live encoders, which write whatever
// format is current: once a later format exists, a step that did would write
// that format and stamp it as its own. Only the runner checks the end of the
// chain against the live decoders. Steps are kept for as long as roots in
// their format may exist, which is indefinitely.
package migrate

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/pdf/boomerangz/internal/lifecycle"
	"github.com/pdf/boomerangz/internal/zfs"
)

// Action rewrites one property of a root. Previous is the value it replaces,
// empty when there is none; apply refuses a property that no longer holds it.
type Action struct {
	Property string `json:"property"`
	Previous string `json:"previous,omitempty"`
	Value    string `json:"value"`
}

// Plan statuses.
const (
	// StatusUnmanaged is a dataset that keeps no state of its own: it has no
	// format, and migration never gives it one.
	StatusUnmanaged = "unmanaged"
	// StatusCurrent is a root already in the current format.
	StatusCurrent = "current"
	// StatusPending is a root that Apply would migrate.
	StatusPending = "pending"
	// StatusBlocked is a root that cannot be migrated as it stands.
	StatusBlocked = "blocked"
	// StatusMigrated is a root Apply migrated.
	StatusMigrated = "migrated"
)

// Plan migrates one root. A root already in the current format, or keeping
// no state, has no actions. No change is made while any blocker remains.
type Plan struct {
	Dataset  string   `json:"dataset"`
	Status   string   `json:"status"`
	From     int      `json:"from"`
	To       int      `json:"to"`
	Actions  []Action `json:"actions"`
	Blockers []string `json:"blockers"`
	Applied  int      `json:"applied"`
}

// step moves a root from format from to from+1. plan reads the root's state
// in format from and returns the property writes that rewrite it in from+1,
// or why it cannot. It never writes the version marker, which the runner
// writes once every step is done, and it must leave records already in from+1
// alone, so that a run interrupted part way through converges when repeated.
type step struct {
	from int
	plan func(state zfs.State, root string) ([]Action, []string)
}

// steps are ordered by from, with one for every format before the current.
var steps = []step{
	{from: 1, plan: v1ToV2},
}

// Backend is what migration reads and writes.
type Backend interface {
	InspectState(context.Context, string, bool) (zfs.State, error)
	SetProperties(context.Context, string, map[string]string) error
}

// Preview plans the migration of root without changing anything.
func Preview(ctx context.Context, backend Backend, root string) (Plan, error) {
	plan, _, err := planRoot(ctx, backend, root)
	return plan, err
}

// Apply plans the migration of root again, refuses it if anything blocks it
// or the plan differs from a second reading, and then writes each property,
// checking before every write that the property still holds the value the
// plan read. The version marker is written last, so a root whose migration
// was interrupted is still in its old format and a repeated run finishes it.
func Apply(ctx context.Context, backend Backend, root string) (Plan, error) {
	plan, _, err := planRoot(ctx, backend, root)
	if err != nil || len(plan.Blockers) > 0 {
		if err == nil {
			err = fmt.Errorf("migration blocked")
		}
		return plan, err
	}
	again, _, err := planRoot(ctx, backend, root)
	if err != nil {
		return plan, err
	}
	if !slices.EqualFunc(plan.Actions, again.Actions, func(a, b Action) bool { return a == b }) || len(again.Blockers) > 0 || plan.From != again.From {
		return plan, fmt.Errorf("state on %s changed while migration was planned; preview again", root)
	}
	for _, action := range plan.Actions {
		if err := holds(ctx, backend, root, action.Property, action.Previous); err != nil {
			return plan, err
		}
		if err := backend.SetProperties(ctx, root, map[string]string{action.Property: action.Value}); err != nil {
			return plan, err
		}
		if err := holds(ctx, backend, root, action.Property, action.Value); err != nil {
			return plan, err
		}
		plan.Applied++
	}
	plan.Status = StatusMigrated
	return plan, nil
}

// holds checks that root's local value of property is value, or that it has
// none when value is empty.
func holds(ctx context.Context, backend Backend, root, property, value string) error {
	state, err := backend.InspectState(ctx, root, false)
	if err != nil {
		return err
	}
	actual := local(state, root, property)
	if actual != value {
		return fmt.Errorf("%s on %s changed during migration; preview again", property, root)
	}
	return nil
}

// planRoot inspects root and its subtree, whose snapshots, holds and bookmarks
// the proofs on root describe, and plans every step from root's format.
func planRoot(ctx context.Context, backend Backend, root string) (Plan, zfs.State, error) {
	plan, state, err := planSteps(ctx, backend, root)
	if err == nil && plan.Status == "" {
		switch {
		case len(plan.Blockers) > 0:
			plan.Status = StatusBlocked
		case len(plan.Actions) > 0:
			plan.Status = StatusPending
		default:
			plan.Status = StatusCurrent
		}
	}
	return plan, state, err
}

func planSteps(ctx context.Context, backend Backend, root string) (Plan, zfs.State, error) {
	plan := Plan{Dataset: root, To: lifecycle.CurrentVersion}
	if err := zfs.ValidateDataset(root); err != nil {
		return plan, zfs.State{}, err
	}
	state, err := backend.InspectState(ctx, root, true)
	if err != nil {
		return plan, state, err
	}
	version, err := lifecycle.StateVersion(state, root)
	if err != nil {
		plan.Blockers = append(plan.Blockers, err.Error())
		return plan, state, nil
	}
	plan.From = version
	switch {
	case version > lifecycle.CurrentVersion:
		plan.Blockers = append(plan.Blockers, fmt.Sprintf("state on %s is format v%d, newer than the v%d this build knows; upgrade boomerangz", root, version, lifecycle.CurrentVersion))
		return plan, state, nil
	case version == lifecycle.CurrentVersion:
		return plan, state, nil
	case !managed(state, root):
		// Nothing to migrate, and a marker would make the dataset look managed.
		plan.From, plan.Status = 0, StatusUnmanaged
		return plan, state, nil
	}
	current := state
	for _, s := range steps {
		if s.from < version {
			continue
		}
		actions, blockers := s.plan(current, root)
		plan.Blockers = append(plan.Blockers, blockers...)
		plan.Actions = append(plan.Actions, actions...)
		current = applied(current, root, actions)
	}
	if len(plan.Blockers) == 0 {
		// The end of the chain must read as the current format does.
		marker := Action{Property: lifecycle.VersionProperty, Value: strconv.Itoa(lifecycle.CurrentVersion)}
		current = applied(current, root, []Action{marker})
		if _, err := lifecycle.References(current, root, local(current, root, lifecycle.LineageProperty)); err != nil {
			plan.Blockers = append(plan.Blockers, fmt.Sprintf("migrated proofs do not read as format v%d: %v", lifecycle.CurrentVersion, err))
		}
		plan.Actions = append(plan.Actions, marker)
	}
	slices.Sort(plan.Blockers)
	plan.Blockers = slices.Compact(plan.Blockers)
	return plan, state, nil
}

// Lister finds the datasets that keep state of their own.
type Lister interface {
	ListDatasets(context.Context) ([]zfs.Dataset, error)
	GetStoredProperties(context.Context, []string) ([]zfs.Property, error)
}

// Roots lists, sorted, every dataset that keeps state of its own, and so has
// a format: what migrating everything covers. It reads only stored
// properties, so it costs one query rather than an inspection per dataset.
func Roots(ctx context.Context, lister Lister) ([]string, error) {
	inventory, err := lister.ListDatasets(ctx)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(inventory))
	for _, dataset := range inventory {
		names = append(names, dataset.Name)
	}
	rows, err := lister.GetStoredProperties(ctx, names)
	if err != nil {
		return nil, err
	}
	var roots []string
	for _, row := range rows {
		if managed(zfs.State{Properties: []zfs.Property{row}}, row.Dataset) {
			roots = append(roots, row.Dataset)
		}
	}
	slices.Sort(roots)
	return slices.Compact(roots), nil
}

// managed reports whether root keeps state of its own: an owner, or records.
// A member, or a dataset boomerangz has never managed, has no format.
func managed(state zfs.State, root string) bool {
	for _, p := range state.Properties {
		if p.Dataset == root && p.Source == zfs.SourceLocal && (p.Name == lifecycle.OwnerProperty || strings.HasPrefix(p.Name, lifecycle.ReferencePrefix) || strings.HasPrefix(p.Name, lifecycle.TargetStatePrefix)) {
			return true
		}
	}
	return false
}

// local is root's local value of property, or empty.
func local(state zfs.State, root, property string) string {
	for _, p := range state.Properties {
		if p.Dataset == root && p.Name == property && p.Source == zfs.SourceLocal {
			return p.Value
		}
	}
	return ""
}

// applied is state as it reads once actions are written on root.
func applied(state zfs.State, root string, actions []Action) zfs.State {
	values := map[string]string{}
	for _, action := range actions {
		values[action.Property] = action.Value
	}
	next := state
	next.Properties = nil
	for _, p := range state.Properties {
		if p.Dataset == root && p.Source == zfs.SourceLocal {
			if _, rewritten := values[p.Name]; rewritten {
				continue
			}
		}
		next.Properties = append(next.Properties, p)
	}
	for _, name := range slices.Sorted(maps.Keys(values)) {
		next.Properties = append(next.Properties, zfs.Property{Dataset: root, Name: name, Value: values[name], Source: zfs.SourceLocal})
	}
	return next
}
