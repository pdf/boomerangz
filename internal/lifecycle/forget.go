package lifecycle

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/pdf/boomerangz/internal/policy"
	"github.com/pdf/boomerangz/internal/zfs"
)

// TargetStatePrefix names a source root's state for one target other than
// its proofs: the target binding, state:target:<target-id>, and its
// suspension marker, state:target:<target-id>:suspended.
const TargetStatePrefix = policy.StateNamespace + "target:"

// ValidTargetID reports whether id is a TargetID: what a proof, a hold tag and
// a binding key name a target by.
func ValidTargetID(id string) bool {
	if len(id) != 64 {
		return false
	}
	for _, c := range id {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// forgetPlan removes the state a source root keeps for each target in
// options.ForgetTargets, from the root and its subtree: the target's proofs,
// the holds and bookmarks they record, its binding and its suspension marker.
// The targets are not checked, which is the point: it is how an operator lets
// go of a target that is gone for good, or whose mapping has changed, without
// cleaning everything else. Ownership, the other targets' state and the
// snapshots all stay, and the next transfer to a forgotten target seeds it.
func (s *Service) forgetPlan(ctx context.Context, dataset string, options CleanOptions, safety CleanSafety) (CleanPlan, zfs.State, error) {
	plan := CleanPlan{Dataset: dataset, Options: options}
	if options.Recursive || options.DestroyOwnedSnapshots {
		return plan, zfs.State{}, fmt.Errorf("forgetting a target cannot be combined with recursive clean or snapshot destruction")
	}
	forget := map[string]bool{}
	for _, id := range options.ForgetTargets {
		if !ValidTargetID(id) {
			return plan, zfs.State{}, fmt.Errorf("invalid target ID %q", id)
		}
		forget[id] = true
	}
	if err := zfs.ValidateDataset(dataset); err != nil {
		return plan, zfs.State{}, err
	}
	// Proofs are the root's, and the holds and bookmarks they record are on
	// its members' snapshots too.
	state, err := s.backend.InspectState(ctx, dataset, true)
	if err != nil {
		return plan, state, err
	}
	if err := scopeReady(state, dataset); err != nil {
		plan.Blockers = append(plan.Blockers, err.Error())
	}
	if err := recordsReadable(state, dataset); err != nil {
		plan.Blockers = append(plan.Blockers, err.Error())
	}
	var datasets []string
	for _, object := range datasetObjects(state) {
		datasets = append(datasets, object.Name)
	}
	slices.Sort(datasets)
	if safety == nil {
		plan.Blockers = append(plan.Blockers, "quiescence has not been established")
	} else if err := safety.Quiescent(ctx, datasets); err != nil {
		plan.Blockers = append(plan.Blockers, err.Error())
	}
	found := map[string]bool{}
	provedHolds := map[string]bool{}
	provedBookmarks := map[string]bool{}
	lineage, err := storedLineage(state, dataset)
	if err != nil {
		plan.Blockers = append(plan.Blockers, err.Error())
	} else if lineage == "" {
		plan.Blockers = append(plan.Blockers, "forgetting a target needs a source root with a lineage: "+dataset)
	} else if refs, err := referenceRecords(state, dataset, lineage); err != nil {
		plan.Blockers = append(plan.Blockers, err.Error())
	} else {
		for _, r := range refs {
			if !forget[r.TargetID] {
				continue
			}
			found[r.TargetID] = true
			plan.Actions = append(plan.Actions, CleanAction{Operation: "inherit", Object: dataset, Property: r.property()})
			sources, err := r.Sources(state)
			if err != nil {
				plan.Blockers = append(plan.Blockers, fmt.Sprintf("reference proof %s: %v", r.property(), err))
				continue
			}
			for _, source := range sources {
				snapshot := r.sourceSnapshot(source)
				if slices.Contains(state.Holds[snapshot], r.hold()) {
					owned, metadata, err := findOwned(state, source.Dataset, snapshot, lineage)
					if err != nil || owned.GUID != source.GUID || metadata != r.Metadata {
						plan.Blockers = append(plan.Blockers, "held snapshot ownership changed: "+snapshot)
					} else {
						provedHolds[snapshot+"\x00"+r.hold()] = true
						plan.Actions = append(plan.Actions, CleanAction{Operation: "release", Object: snapshot, Property: r.hold(), GUID: source.GUID})
					}
				}
				if bookmark := r.sourceBookmark(source); hasObject(state, bookmark) {
					provedBookmarks[bookmark] = true
					plan.Actions = append(plan.Actions, CleanAction{Operation: "destroy-bookmark", Object: bookmark, GUID: source.GUID})
				}
			}
		}
	}
	for _, p := range state.Properties {
		rest, isTarget := strings.CutPrefix(p.Name, TargetStatePrefix)
		id, _, _ := strings.Cut(rest, ":")
		// A received binding is the sending installation's, not this root's.
		if p.Dataset != dataset || !isTarget || p.Source != zfs.SourceLocal || !forget[id] {
			continue
		}
		found[id] = true
		plan.Actions = append(plan.Actions, CleanAction{Operation: "inherit", Object: dataset, Property: p.Name})
	}
	// Anything else tagged for a forgotten target is not this root's to
	// remove on its word: no proof accounts for it.
	for object, tags := range state.Holds {
		for _, tag := range tags {
			if id, ours := strings.CutPrefix(tag, SnapshotPrefix); ours && forget[id] && !provedHolds[object+"\x00"+tag] {
				plan.Blockers = append(plan.Blockers, "unproven hold: "+object+" "+tag)
			}
		}
	}
	for _, o := range state.Objects {
		if o.Type != "bookmark" || provedBookmarks[o.Name] {
			continue
		}
		_, name, _ := strings.Cut(o.Name, "#")
		for id := range forget {
			if strings.HasPrefix(name, SnapshotPrefix+id+"-") {
				plan.Blockers = append(plan.Blockers, "unproven bookmark: "+o.Name)
			}
		}
	}
	for _, id := range options.ForgetTargets {
		if !found[id] {
			plan.Blockers = append(plan.Blockers, fmt.Sprintf("%s keeps no state for target %s", dataset, id))
		}
	}
	sortCleanActions(plan.Actions)
	slices.Sort(plan.Blockers)
	plan.Blockers = slices.Compact(plan.Blockers)
	return plan, state, nil
}
