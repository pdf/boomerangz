package transfer

import (
	"context"
	"fmt"
	"reflect"
	"slices"

	"github.com/pdf/boomerangz/internal/lifecycle"
	"github.com/pdf/boomerangz/internal/policy"
	"github.com/pdf/boomerangz/internal/zfs"
)

// ReseedPlan is the complete destructive scope of one source-target reset.
type ReseedPlan struct {
	Dataset            string        `json:"dataset"`
	Target             string        `json:"target"`
	Binding            TargetBinding `json:"binding"`
	BindingStored      bool          `json:"binding_stored"`
	ReceiveAnchor      string        `json:"receive_anchor"`
	DestinationExists  bool          `json:"destination_exists"`
	DestinationObjects []string      `json:"destination_objects,omitempty"`
	// SetAside is the root replica's placeholder, moved aside while the root's
	// own replica replaces it, with the member replicas waiting beneath it.
	// It lies beside the mapped replica rather than inside it, so it is
	// destroyed with it explicitly.
	SetAside       string                `json:"set_aside,omitempty"`
	ResumeDatasets []string              `json:"resume_datasets,omitempty"`
	References     []lifecycle.Reference `json:"references,omitempty"`
	Suspended      bool                  `json:"suspended,omitempty"`
	Warning        string                `json:"warning"`
	Applied        bool                  `json:"applied"`
}

// ReseedService coordinates destructive destination cleanup with release of
// the corresponding source-side recovery state.
type ReseedService struct {
	source       zfs.Executor
	destination  zfs.ReseedExecutor
	installation string
}

// NewReseedService constructs an explicit target-reseed administrator.
func NewReseedService(source zfs.Executor, destination zfs.ReseedExecutor, installation string) (*ReseedService, error) {
	if source == nil || destination == nil || !lifecycle.ValidID(installation) {
		return nil, fmt.Errorf("source, reseed-capable destination, and valid installation identity required")
	}
	return &ReseedService{source: source, destination: destination, installation: installation}, nil
}

func sameBoundIdentity(binding TargetBinding, identity zfs.DatasetIdentity) bool {
	return identity.Name == binding.Anchor && identity.GUID == binding.AnchorGUID && identity.Pool == binding.Pool && identity.PoolGUID == binding.PoolGUID
}

func (s *ReseedService) plan(ctx context.Context, request Request) (ReseedPlan, error) {
	var plan ReseedPlan
	if err := zfs.ValidateDataset(request.Source); err != nil {
		return plan, err
	}
	canonical := canonicalTarget(request)
	if canonical == "" {
		return plan, fmt.Errorf("canonical target identity is required")
	}
	source, err := s.source.InspectState(ctx, request.Source, true)
	if err != nil {
		return plan, err
	}
	lineage, err := lifecycle.RootAuthority(source, request.Source, s.installation)
	if err != nil {
		return plan, err
	}
	binding, err := storedTargetBinding(source, request.Source, canonical)
	if err != nil {
		return plan, err
	}
	bindingStored := binding != nil
	if binding != nil {
		if err := validateBinding(*binding); err != nil {
			return plan, err
		}
	}
	mapped, _, err := request.mapped(request.Source)
	if err != nil {
		return plan, err
	}
	if binding != nil && (binding.Transport != requestTransport(request) || binding.CanonicalTarget != canonical || binding.DestinationRoot != request.DestinationRoot || binding.MappedDataset != mapped) {
		return plan, fmt.Errorf("configured target mapping differs from persistent binding; refusing reseed")
	}
	inventory, err := s.destination.ListDatasets(ctx)
	if err != nil {
		return plan, err
	}
	datasets := make(map[string]bool, len(inventory))
	nearest := ""
	receiveAnchor := ""
	for _, dataset := range inventory {
		datasets[dataset.Name] = true
		if inside(mapped, dataset.Name) && len(dataset.Name) > len(nearest) {
			nearest = dataset.Name
		}
		if dataset.Name != mapped && inside(mapped, dataset.Name) && len(dataset.Name) > len(receiveAnchor) {
			receiveAnchor = dataset.Name
		}
	}
	identityDataset := nearest
	if binding != nil && datasets[binding.Anchor] {
		identityDataset = binding.Anchor
	}
	if identityDataset == "" {
		return plan, fmt.Errorf("destination pool is unavailable")
	}
	if receiveAnchor == "" {
		return plan, fmt.Errorf("destination has no existing parent for a fresh receive")
	}
	identity, err := s.destination.InspectDatasetIdentity(ctx, identityDataset)
	if err != nil {
		return plan, err
	}
	if err := checkHostLocal(ctx, s.source, request, identity); err != nil {
		return plan, err
	}
	if binding != nil {
		if identityDataset == binding.Anchor && !sameBoundIdentity(*binding, identity) {
			return plan, fmt.Errorf("destination anchor identity differs from persistent binding")
		} else if identityDataset != binding.Anchor && (identity.Pool != binding.Pool || identity.PoolGUID != binding.PoolGUID) {
			return plan, fmt.Errorf("destination pool identity differs from persistent binding")
		}
	} else {
		resolved, resolveErr := bindingForTarget(request, mapped, identity, requestTransport(request), canonical)
		if resolveErr != nil {
			return plan, resolveErr
		}
		binding = &resolved
	}
	plan = ReseedPlan{
		Dataset:           request.Source,
		Target:            canonical,
		Binding:           *binding,
		BindingStored:     bindingStored,
		ReceiveAnchor:     receiveAnchor,
		DestinationExists: datasets[mapped],
		Warning:           "applying this plan permanently destroys the mapped destination replica and resets its replication history",
	}
	if plan.DestinationExists {
		destination, inspectErr := s.destination.InspectState(ctx, binding.MappedDataset, true)
		if inspectErr != nil {
			return ReseedPlan{}, inspectErr
		}
		for _, object := range destination.Objects {
			plan.DestinationObjects = append(plan.DestinationObjects, object.Name)
		}
		for dataset := range destination.ResumeTokens {
			plan.ResumeDatasets = append(plan.ResumeDatasets, dataset)
		}
		slices.Sort(plan.DestinationObjects)
		slices.Sort(plan.ResumeDatasets)
	}
	if aside := placeholderAside(binding.MappedDataset); datasets[aside] {
		state, inspectErr := s.destination.InspectState(ctx, aside, true)
		if inspectErr != nil {
			return ReseedPlan{}, inspectErr
		}
		if !isPlaceholder(state, aside) {
			return ReseedPlan{}, fmt.Errorf("%s is not a placeholder boomerangz set aside; refusing reseed", aside)
		}
		plan.SetAside = aside
		for _, object := range state.Objects {
			plan.DestinationObjects = append(plan.DestinationObjects, object.Name)
		}
		for dataset := range state.ResumeTokens {
			plan.ResumeDatasets = append(plan.ResumeDatasets, dataset)
		}
		slices.Sort(plan.DestinationObjects)
		slices.Sort(plan.ResumeDatasets)
	}
	references, err := lifecycle.References(source, request.Source, lineage)
	if err != nil {
		return ReseedPlan{}, err
	}
	for _, reference := range references {
		if reference.TargetID == lifecycle.TargetID(canonical) {
			plan.References = append(plan.References, reference)
		}
	}
	plan.Suspended, err = TargetSuspended(source, request.Source, canonical)
	return plan, err
}

// Plan returns a read-only, revalidated description of a reseed.
func (s *ReseedService) Plan(ctx context.Context, request Request) (ReseedPlan, error) {
	return s.plan(ctx, request)
}

// Apply revalidates and executes exactly the previewed source-target reset.
func (s *ReseedService) Apply(ctx context.Context, request Request) (ReseedPlan, error) {
	plan, err := s.plan(ctx, request)
	if err != nil {
		return plan, err
	}
	if err := s.source.CheckPermissions(ctx, request.Source, slices.Clone(reseedSourcePermissions)); err != nil {
		return plan, fmt.Errorf("source permission preflight: %w", err)
	}
	if plan.DestinationExists {
		permissions := slices.Clone(reseedCleanupPermissions)
		if len(plan.ResumeDatasets) > 0 {
			permissions = union(permissions, abortPermissions)
		}
		if err := s.destination.CheckPermissions(ctx, plan.Binding.MappedDataset, permissions); err != nil {
			return plan, fmt.Errorf("destination cleanup permission preflight: %w", err)
		}
	}
	if plan.SetAside != "" {
		if err := s.destination.CheckPermissions(ctx, plan.SetAside, slices.Clone(reseedCleanupPermissions)); err != nil {
			return plan, fmt.Errorf("set-aside placeholder cleanup permission preflight: %w", err)
		}
	}
	if err := s.destination.CheckPermissions(ctx, plan.ReceiveAnchor, reseedReceivePermissions(request)); err != nil {
		return plan, fmt.Errorf("fresh receive permission preflight: %w", err)
	}
	fresh, err := s.plan(ctx, request)
	if err != nil {
		return plan, err
	}
	if !reflect.DeepEqual(plan, fresh) {
		return plan, fmt.Errorf("reseed state changed during preflight; retry preview")
	}
	for i := len(plan.ResumeDatasets) - 1; i >= 0; i-- {
		if err := s.destination.AbortReceive(ctx, plan.ResumeDatasets[i]); err != nil {
			return plan, fmt.Errorf("abandon receive on %s: %w", plan.ResumeDatasets[i], err)
		}
	}
	if plan.DestinationExists {
		if err := s.destination.DestroyDataset(ctx, plan.Binding.MappedDataset, true); err != nil {
			return plan, fmt.Errorf("destroy mapped destination %s: %w", plan.Binding.MappedDataset, err)
		}
	}
	if plan.SetAside != "" {
		if err := s.destination.DestroyDataset(ctx, plan.SetAside, true); err != nil {
			return plan, fmt.Errorf("destroy set-aside placeholder %s: %w", plan.SetAside, err)
		}
	}
	references := slices.Clone(plan.References)
	slices.SortFunc(references, func(a, b lifecycle.Reference) int {
		return a.Metadata.Created.Compare(b.Metadata.Created)
	})
	lifecycleService, err := lifecycle.NewService(s.source, s.installation)
	if err != nil {
		return plan, err
	}
	for _, reference := range references {
		if err := lifecycleService.ReleaseReference(ctx, request.Source, reference); err != nil {
			return plan, fmt.Errorf("release source recovery reference: %w", err)
		}
	}
	if plan.Suspended {
		if err := SetTargetSuspended(ctx, s.source, request.Source, plan.Target, false); err != nil {
			return plan, err
		}
	}
	if plan.BindingStored {
		if err := s.source.InheritProperty(ctx, request.Source, targetBindingProperty(plan.Target)); err != nil {
			return plan, fmt.Errorf("remove target binding: %w", err)
		}
	}
	plan.Applied = true
	return plan, nil
}

// AbortPlan describes discarding the interrupted receives one target holds for
// an actor root's streams. It is the operator's way out when resuming a stream
// keeps failing, which transfers never resolve on their own: nothing else on
// the destination is touched, the binding and every proof stay, and the next
// transfer plans afresh from what the destination still holds.
type AbortPlan struct {
	Dataset  string   `json:"dataset"`
	Target   string   `json:"target"`
	Receives []string `json:"receives"`
	Warning  string   `json:"warning"`
	Applied  bool     `json:"applied"`
}

func abortPlan(plan ReseedPlan) AbortPlan {
	return AbortPlan{
		Dataset:  plan.Dataset,
		Target:   plan.Target,
		Receives: slices.Clone(plan.ResumeDatasets),
		Warning:  "applying this plan discards the partially received data of each listed receive; the next transfer sends it again",
	}
}

// PlanAbort returns a read-only, revalidated description of an abort. The
// target is resolved and checked exactly as for a reseed.
func (s *ReseedService) PlanAbort(ctx context.Context, request Request) (AbortPlan, error) {
	plan, err := s.plan(ctx, request)
	if err != nil {
		return AbortPlan{}, err
	}
	return abortPlan(plan), nil
}

// ApplyAbort revalidates and discards exactly the previewed receives, deepest
// first.
func (s *ReseedService) ApplyAbort(ctx context.Context, request Request) (AbortPlan, error) {
	reseed, err := s.plan(ctx, request)
	if err != nil {
		return AbortPlan{}, err
	}
	plan := abortPlan(reseed)
	for _, dataset := range plan.Receives {
		if err := s.destination.CheckPermissions(ctx, dataset, slices.Clone(abortPermissions)); err != nil {
			return plan, fmt.Errorf("abort permission preflight: %w", err)
		}
	}
	fresh, err := s.plan(ctx, request)
	if err != nil {
		return plan, err
	}
	if !reflect.DeepEqual(plan, abortPlan(fresh)) {
		return plan, fmt.Errorf("receive state changed during preflight; retry preview")
	}
	for i := len(plan.Receives) - 1; i >= 0; i-- {
		if err := s.destination.AbortReceive(ctx, plan.Receives[i]); err != nil {
			return plan, fmt.Errorf("abort receive on %s: %w", plan.Receives[i], err)
		}
	}
	plan.Applied = true
	return plan, nil
}

// reseedReceivePermissions covers the fresh receives that follow a reseed: the
// root's own and each member stream's, whose receive overrides are their own.
func reseedReceivePermissions(request Request) []string {
	permissions := union(receivePermissions, filesystemReceivePermissions)
	policies := []policy.Effective{request.Policy}
	for _, member := range request.Members {
		policies = append(policies, member.Policy)
	}
	for _, p := range policies {
		permissions = union(permissions, propertyPermissions(p.SetProperties, p.IgnoreProperties))
	}
	return permissions
}
