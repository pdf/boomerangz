package transfer

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/pdf/boomerangz/internal/lifecycle"
	"github.com/pdf/boomerangz/internal/zfs"
)

// ReseedPlan is the complete destructive scope of one source-target reset.
type ReseedPlan struct {
	Dataset            string                `json:"dataset"`
	Target             string                `json:"target"`
	Binding            TargetBinding         `json:"binding"`
	BindingStored      bool                  `json:"binding_stored"`
	ReceiveAnchor      string                `json:"receive_anchor"`
	DestinationExists  bool                  `json:"destination_exists"`
	DestinationObjects []string              `json:"destination_objects,omitempty"`
	ResumeDatasets     []string              `json:"resume_datasets,omitempty"`
	References         []lifecycle.Reference `json:"references,omitempty"`
	Suspended          bool                  `json:"suspended,omitempty"`
	Warning            string                `json:"warning"`
	Applied            bool                  `json:"applied"`
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
	mapped, err := zfs.MapReceiveDataset(request.Source, request.DestinationRoot, zfs.ReceiveDiscard(request.Policy.Discard))
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
	references, err := lifecycle.References(source, request.Source, lineage)
	if err != nil {
		return ReseedPlan{}, err
	}
	for _, reference := range references {
		if reference.Target == canonical {
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
	if err := s.source.CheckPermissions(ctx, request.Source, []string{"destroy", "release", "userprop"}); err != nil {
		return plan, fmt.Errorf("source permission preflight: %w", err)
	}
	if plan.DestinationExists {
		permissions := []string{"destroy", "mount"}
		if len(plan.ResumeDatasets) > 0 {
			permissions = append(permissions, "receive:append")
		}
		if err := s.destination.CheckPermissions(ctx, plan.Binding.MappedDataset, permissions); err != nil {
			return plan, fmt.Errorf("destination cleanup permission preflight: %w", err)
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
	references := slices.Clone(plan.References)
	slices.SortFunc(references, func(a, b lifecycle.Reference) int {
		return strings.Compare(a.Metadata.Created.String(), b.Metadata.Created.String())
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

func reseedReceivePermissions(request Request) []string {
	permissions := []string{"canmount", "create", "destroy", "mount", "receive:append", "userprop"}
	for property := range request.Policy.SetProperties {
		if !strings.Contains(property, ":") {
			permissions = append(permissions, property)
		}
	}
	for _, property := range request.Policy.IgnoreProperties {
		if !strings.Contains(property, ":") {
			permissions = append(permissions, property)
		}
	}
	slices.Sort(permissions)
	return slices.Compact(permissions)
}
