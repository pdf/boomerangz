package lifecycle

import (
	"context"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/pdf/boomerangz/internal/policy"
	"github.com/pdf/boomerangz/internal/zfs"
)

// CleanSafety must establish quiescence for the duration of apply, and reject
// inaccessible, identity-mismatched, or resumable targets. Neither check may
// abandon a receive or release a hold as a side effect. Preview uses the same
// checks.
type CleanSafety interface {
	Quiescent(context.Context, []string) error
	CheckTarget(context.Context, string, string) error
}

type cleanBackend interface {
	referenceBackend
	GetActivationProperties(context.Context) ([]zfs.Property, error)
}

// CleanOptions always select an exact local dataset unless Recursive is set.
// DestroyOwnedSnapshots is independent of scope selection and defaults false.
type CleanOptions struct {
	Recursive             bool `json:"recursive"`
	DestroyOwnedSnapshots bool `json:"destroy_owned_snapshots"`
}

// CleanAction is informational, never accepted as a raw mutation request.
type CleanAction struct {
	Operation string `json:"operation"`
	Object    string `json:"object"`
	Property  string `json:"property,omitempty"`
	GUID      uint64 `json:"guid,omitempty"`
}

// CleanPlan includes all blockers. No mutation occurs if any blocker remains.
// Hidden received values cannot be exhaustively enumerated or erased by the CLI.
type CleanPlan struct {
	Dataset  string        `json:"dataset"`
	Options  CleanOptions  `json:"options"`
	Actions  []CleanAction `json:"actions"`
	Blockers []string      `json:"blockers"`
	Warnings []string      `json:"warnings"`
	Applied  int           `json:"applied"`
}

type cleanMode struct {
	retirement bool
	now        time.Time
	grace      time.Duration
}

func (s *Service) cleanPlan(ctx context.Context, dataset string, options CleanOptions, safety CleanSafety, mode cleanMode) (CleanPlan, zfs.State, error) {
	plan := CleanPlan{Dataset: dataset, Options: options, Warnings: []string{"inherit masks received properties; hidden received values may remain and can be restored externally"}}
	backend, ok := s.backend.(cleanBackend)
	if !ok {
		return plan, zfs.State{}, fmt.Errorf("clean operations unavailable")
	}
	if err := zfs.ValidateDataset(dataset); err != nil {
		return plan, zfs.State{}, err
	}
	state, err := s.backend.InspectState(ctx, dataset, options.Recursive)
	if err != nil {
		return plan, state, err
	}
	if err := scopeReady(state, dataset); err != nil {
		plan.Blockers = append(plan.Blockers, err.Error())
	}
	activation, err := backend.GetActivationProperties(ctx)
	if err != nil {
		return plan, state, err
	}
	if mode.retirement {
		active, activationErr := locallyActive(activation, dataset)
		if activationErr != nil {
			plan.Blockers = append(plan.Blockers, activationErr.Error())
		} else if active {
			plan.Blockers = append(plan.Blockers, "retirement requires an inactive source root")
		}
		inactive, inactiveErr := inactivePlan(state, dataset, s.installation, active, mode.now, mode.grace)
		if inactiveErr != nil {
			plan.Blockers = append(plan.Blockers, inactiveErr.Error())
		} else if inactive.Action != "" {
			plan.Blockers = append(plan.Blockers, "inactive marker transition must complete before retirement")
		} else if !inactive.Automatic {
			plan.Blockers = append(plan.Blockers, "automatic retirement is disabled")
		} else if !inactive.Due {
			plan.Blockers = append(plan.Blockers, "inactive grace period has not elapsed")
		}
	}
	selected := map[string]bool{}
	var datasets []string
	for _, o := range state.Objects {
		if o.Type == "filesystem" || o.Type == "volume" {
			selected[o.Name] = true
			datasets = append(datasets, o.Name)
		}
	}
	slices.Sort(datasets)
	if safety == nil {
		plan.Blockers = append(plan.Blockers, "quiescence has not been established")
	} else if err := safety.Quiescent(ctx, datasets); err != nil {
		plan.Blockers = append(plan.Blockers, err.Error())
	}
	for _, name := range datasets {
		if mode.retirement {
			continue
		}
		// Find the nearest activation ancestor outside the clean scope. A local
		// off between this dataset and an on ancestor prevents reactivation.
		nearest := ""
		value := ""
		for _, p := range activation {
			if p.Name == policy.Namespace+"enabled" && p.Source == zfs.SourceLocal && !selected[p.Dataset] && strings.HasPrefix(name, p.Dataset+"/") && len(p.Dataset) > len(nearest) {
				nearest = p.Dataset
				value = p.Value
			}
		}
		if value == "on" {
			plan.Blockers = append(plan.Blockers, fmt.Sprintf("cleaning %s would expose enabled=on from %s", name, nearest))
		}
	}
	rootLineage, err := storedLineage(state, dataset)
	if err != nil {
		plan.Blockers = append(plan.Blockers, err.Error())
	}
	provedHolds := map[string]bool{}
	provedBookmarks := map[string]bool{}
	lineages := map[string]string{}
	for _, name := range datasets {
		lineage, err := storedLineage(state, name)
		if err != nil {
			plan.Blockers = append(plan.Blockers, err.Error())
			continue
		}
		if lineage == "" && name != dataset {
			lineage = rootLineage
		}
		lineages[name] = lineage
		candidates := map[string]bool{}
		for _, snapshot := range snapshotsIn(state, name) {
			for _, p := range snapshot.Properties {
				if p.Name != LineageProperty {
					continue
				}
				if _, err := Ownership(snapshot, p.Value); err == nil {
					candidates[p.Value] = true
				}
			}
		}
		if len(candidates) > 1 || (lineage != "" && len(candidates) > 0 && !candidates[lineage]) {
			plan.Blockers = append(plan.Blockers, "conflicting snapshot lineages on "+name)
		}
		if options.DestroyOwnedSnapshots && lineage == "" && len(candidates) > 0 {
			plan.Blockers = append(plan.Blockers, "adopt missing lineage before destroying snapshots on "+name)
		}
		refs, err := referenceRecords(state, name, lineage)
		if err != nil {
			plan.Blockers = append(plan.Blockers, err.Error())
			continue
		}
		for _, r := range refs {
			if safety == nil {
				plan.Blockers = append(plan.Blockers, "target not verified: "+r.Target)
			} else if err := safety.CheckTarget(ctx, name, r.Target); err != nil {
				plan.Blockers = append(plan.Blockers, fmt.Sprintf("target %s: %v", r.Target, err))
			}
			for _, source := range r.sources() {
				snapshot := r.sourceSnapshot(source)
				if slices.Contains(state.Holds[snapshot], r.hold()) {
					owned, metadata, err := findOwned(state, source.Dataset, snapshot, lineage)
					if err != nil || owned.GUID != source.GUID || metadata != source.Metadata {
						plan.Blockers = append(plan.Blockers, "held snapshot ownership changed: "+snapshot)
					} else {
						provedHolds[snapshot+"\x00"+r.hold()] = true
						plan.Actions = append(plan.Actions, CleanAction{Operation: "release", Object: snapshot, Property: r.hold(), GUID: source.GUID})
					}
				}
				for _, o := range state.Objects {
					if o.Name == r.sourceBookmark(source) {
						if o.Type != "bookmark" || o.GUID != source.GUID {
							plan.Blockers = append(plan.Blockers, "bookmark ownership changed: "+o.Name)
						} else {
							provedBookmarks[o.Name] = true
							plan.Actions = append(plan.Actions, CleanAction{Operation: "destroy-bookmark", Object: o.Name, GUID: o.GUID})
						}
					}
				}
			}
		}
	}
	for object, tags := range state.Holds {
		for _, tag := range tags {
			if strings.HasPrefix(tag, SnapshotPrefix) && !provedHolds[object+"\x00"+tag] {
				plan.Blockers = append(plan.Blockers, "unproven hold: "+object+" "+tag)
			}
		}
	}
	for _, o := range state.Objects {
		if o.Type == "bookmark" && strings.HasPrefix(o.Name, strings.Split(o.Name, "#")[0]+"#"+SnapshotPrefix) && !provedBookmarks[o.Name] {
			plan.Blockers = append(plan.Blockers, "unproven bookmark: "+o.Name)
		}
	}
	destroyed := map[string]bool{}
	if options.DestroyOwnedSnapshots {
		for _, name := range datasets {
			for _, snapshot := range snapshotsIn(state, name) {
				if _, err := Ownership(snapshot, lineages[name]); err != nil {
					continue
				}
				blocked := len(snapshot.Clones) > 0 || snapshot.ResumeRequired
				for _, tag := range snapshot.Holds {
					if !provedHolds[snapshot.Name+"\x00"+tag] {
						blocked = true
					}
				}
				if blocked {
					if mode.retirement {
						plan.Blockers = append(plan.Blockers, "dependent owned snapshot prevents retirement: "+snapshot.Name)
					} else {
						plan.Warnings = append(plan.Warnings, "retaining dependent snapshot: "+snapshot.Name)
					}
					continue
				}
				destroyed[snapshot.Name] = true
				plan.Actions = append(plan.Actions, CleanAction{Operation: "destroy-snapshot", Object: snapshot.Name, GUID: snapshot.GUID})
			}
		}
	}
	keys := map[string]bool{}
	for _, p := range state.Properties {
		if mode.retirement && (policy.IsPublic(p.Name) || strings.Contains(p.Dataset, "@") || strings.Contains(p.Dataset, "#")) {
			continue
		}
		if strings.HasPrefix(p.Name, policy.Namespace) && !destroyed[p.Dataset] {
			keys[p.Dataset+"\x00"+p.Name] = true
		}
	}
	for object, values := range state.Received {
		for key := range values {
			if mode.retirement && (policy.IsPublic(key) || strings.Contains(object, "@") || strings.Contains(object, "#")) {
				continue
			}
			if strings.HasPrefix(key, policy.Namespace) && !destroyed[object] {
				keys[object+"\x00"+key] = true
			}
		}
	}
	for key := range keys {
		object, property, _ := strings.Cut(key, "\x00")
		plan.Actions = append(plan.Actions, CleanAction{Operation: "inherit", Object: object, Property: property})
	}
	// Drop references before destroying snapshots, clear snapshot metadata before
	// dataset-level proofs, and clear dataset lineage last. Exact commands only.
	rank := func(a CleanAction) int {
		switch a.Operation {
		case "destroy-bookmark":
			return 0
		case "release":
			return 1
		case "destroy-snapshot":
			return 2
		}
		if strings.Contains(a.Object, "@") {
			return 3
		}
		if a.Property == LineageProperty {
			return 5
		}
		return 4
	}
	slices.SortFunc(plan.Actions, func(a, b CleanAction) int {
		if rank(a) != rank(b) {
			return rank(a) - rank(b)
		}
		if c := strings.Compare(a.Object, b.Object); c != 0 {
			return c
		}
		return strings.Compare(a.Property, b.Property)
	})
	slices.Sort(plan.Blockers)
	plan.Blockers = slices.Compact(plan.Blockers)
	slices.Sort(plan.Warnings)
	return plan, state, nil
}

// Clean previews by default. Apply reconstructs and compares the entire plan
// before the first write and checks the remaining inventory before every write.
// Partial progress is reported; it never rolls back, aborts receives, or uses -S.
func (s *Service) Clean(ctx context.Context, dataset string, options CleanOptions, apply bool, safety CleanSafety) (CleanPlan, error) {
	unlock, err := s.lock(ctx, dataset)
	if err != nil {
		return CleanPlan{}, err
	}
	defer unlock()
	return s.clean(ctx, dataset, options, apply, safety, cleanMode{})
}

func (s *Service) clean(ctx context.Context, dataset string, options CleanOptions, apply bool, safety CleanSafety, mode cleanMode) (CleanPlan, error) {
	plan, state, err := s.cleanPlan(ctx, dataset, options, safety, mode)
	if err != nil || !apply {
		return plan, err
	}
	if len(plan.Blockers) > 0 {
		return plan, fmt.Errorf("clean blocked")
	}
	fresh, current, err := s.cleanPlan(ctx, dataset, options, safety, mode)
	if err != nil {
		return plan, err
	}
	if !reflect.DeepEqual(plan, fresh) || !reflect.DeepEqual(state, current) {
		return plan, fmt.Errorf("clean state changed; preview again")
	}
	backend := s.backend.(cleanBackend)
	for _, action := range plan.Actions {
		if err := s.unchanged(ctx, dataset, options.Recursive, current); err != nil {
			return plan, err
		}
		switch action.Operation {
		case "destroy-bookmark":
			err = backend.DestroyBookmark(ctx, action.Object)
		case "release":
			err = backend.Release(ctx, action.Property, action.Object)
		case "destroy-snapshot":
			err = s.backend.DestroySnapshot(ctx, action.Object)
		case "inherit":
			err = backend.InheritProperty(ctx, action.Object, action.Property)
		default:
			err = fmt.Errorf("unknown clean operation")
		}
		if err != nil {
			return plan, err
		}
		plan.Applied++
		after, readErr := s.backend.InspectState(ctx, dataset, options.Recursive)
		err = readErr
		if err != nil {
			return plan, err
		}
		if !cleanTransition(current, after, action) {
			return plan, fmt.Errorf("unexpected state change after clean action on %s", action.Object)
		}
		current = after
		// Unexpected dependencies appearing during apply always stop further work.
		if len(current.ResumeTokens) > 0 {
			return plan, fmt.Errorf("resume state appeared during clean")
		}
	}
	return plan, nil
}

// Only the exact intended effect may become the baseline for the next command.
// Received keys may disappear from CLI enumeration after inheritance, but may
// not appear or change value unnoticed.
func cleanTransition(before, after zfs.State, action CleanAction) bool {
	expected := before
	expected.Objects = slices.Clone(before.Objects)
	expected.Properties = slices.Clone(before.Properties)
	expected.Holds = maps.Clone(before.Holds)
	expected.Clones = maps.Clone(before.Clones)
	expected.ResumeTokens = maps.Clone(before.ResumeTokens)
	switch action.Operation {
	case "destroy-snapshot", "destroy-bookmark":
		expected.Objects = slices.DeleteFunc(expected.Objects, func(o zfs.Object) bool { return o.Name == action.Object })
		expected.Properties = slices.DeleteFunc(expected.Properties, func(p zfs.Property) bool { return p.Dataset == action.Object })
		delete(expected.Holds, action.Object)
		delete(expected.Clones, action.Object)
		delete(expected.ResumeTokens, action.Object)
	case "release":
		expected.Holds[action.Object] = slices.DeleteFunc(slices.Clone(expected.Holds[action.Object]), func(tag string) bool { return tag == action.Property })
	case "inherit":
		expected.Properties = slices.DeleteFunc(expected.Properties, func(p zfs.Property) bool { return p.Dataset == action.Object && p.Name == action.Property })
	}
	for object, values := range after.Received {
		for key, value := range values {
			if before.Received[object][key] != value {
				return false
			}
		}
	}
	expected.Received = nil
	after.Received = nil
	// Treat empty collections equally, since the CLI need not emit empty rows.
	normalize := func(state *zfs.State) {
		if len(state.Properties) == 0 {
			state.Properties = nil
		}
		state.Holds = maps.Clone(state.Holds)
		state.Clones = maps.Clone(state.Clones)
		for key, value := range state.Holds {
			if len(value) == 0 {
				delete(state.Holds, key)
			}
		}
		for key, value := range state.Clones {
			if len(value) == 0 {
				delete(state.Clones, key)
			}
		}
		if len(state.Holds) == 0 {
			state.Holds = nil
		}
		if len(state.Clones) == 0 {
			state.Clones = nil
		}
		if len(state.ResumeTokens) == 0 {
			state.ResumeTokens = nil
		}
	}
	normalize(&expected)
	normalize(&after)
	return reflect.DeepEqual(expected, after)
}
