package transfer

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strings"

	"github.com/pdf/boomerangz/internal/lifecycle"
	"github.com/pdf/boomerangz/internal/policy"
	"github.com/pdf/boomerangz/internal/zfs"
)

const (
	targetBindingVersion = 1
	targetBindingPrefix  = policy.StateNamespace + "target:"
)

// TargetBinding is the persistent source-side identity of one configured
// destination. Anchor identifies either the existing destination dataset or,
// for a bootstrap, its nearest existing ancestor.
type TargetBinding struct {
	Version         int    `json:"version"`
	Transport       string `json:"transport"`
	CanonicalTarget string `json:"canonical_target"`
	DestinationRoot string `json:"destination_root"`
	MappedDataset   string `json:"mapped_dataset"`
	Pool            string `json:"pool"`
	PoolGUID        uint64 `json:"pool_guid"`
	Anchor          string `json:"anchor"`
	AnchorGUID      uint64 `json:"anchor_guid"`
	RelativePath    string `json:"relative_path"`
}

// LocalTargetInspection is the adoption-time identity view of one configured
// local destination. Unbound is safe only because dataset adopt --apply is the
// operator's explicit acceptance of the displayed GUIDs and mapping.
type LocalTargetInspection struct {
	ConfiguredName    string         `json:"configured_name"`
	Transport         string         `json:"transport"`
	CanonicalEndpoint string         `json:"canonical_endpoint"`
	DestinationRoot   string         `json:"destination_root"`
	MappedDataset     string         `json:"mapped_dataset"`
	Stored            *TargetBinding `json:"stored_binding,omitempty"`
	Resolved          *TargetBinding `json:"resolved_identity,omitempty"`
	Status            string         `json:"status"`
}

type localIdentityReader interface {
	ListDatasets(context.Context) ([]zfs.Dataset, error)
	InspectDatasetIdentity(context.Context, string) (zfs.DatasetIdentity, error)
}

// InspectLocalTarget resolves the exact dataset or nearest existing ancestor
// and compares it with any persistent source-root binding.
func InspectLocalTarget(ctx context.Context, reader localIdentityReader, request Request, source zfs.State) (LocalTargetInspection, error) {
	inspection := LocalTargetInspection{ConfiguredName: request.DestinationRoot, Transport: "local", CanonicalEndpoint: canonicalLocalTarget(request.DestinationRoot), DestinationRoot: request.DestinationRoot}
	mapped, err := zfs.MapReceiveDataset(request.Source, request.DestinationRoot, zfs.ReceiveDiscard(request.Policy.Discard))
	if err != nil {
		inspection.Status = "invalid-mapping"
		return inspection, err
	}
	inspection.MappedDataset = mapped
	inventory, err := reader.ListDatasets(ctx)
	if err != nil {
		inspection.Status = "unavailable"
		return inspection, err
	}
	anchor := ""
	for _, dataset := range inventory {
		if inside(mapped, dataset.Name) && len(dataset.Name) > len(anchor) {
			anchor = dataset.Name
		}
	}
	if anchor == "" {
		inspection.Status = "unavailable"
		return inspection, fmt.Errorf("destination has no existing ancestor")
	}
	identity, err := reader.InspectDatasetIdentity(ctx, anchor)
	if err != nil {
		inspection.Status = "unavailable"
		return inspection, err
	}
	resolved, err := bindingFor(request, mapped, identity)
	if err != nil {
		inspection.Status = "invalid-mapping"
		return inspection, err
	}
	inspection.Resolved = &resolved
	stored, err := storedTargetBinding(source, request.Source, resolved.CanonicalTarget)
	if err != nil {
		inspection.Status = "invalid-binding"
		return inspection, err
	}
	inspection.Stored = stored
	if stored == nil {
		inspection.Status = "unbound"
		return inspection, nil
	}
	if err := validateBinding(*stored); err != nil {
		inspection.Status = "invalid-binding"
		return inspection, err
	}
	if *stored != resolved {
		inspection.Status = "mismatch"
		return inspection, fmt.Errorf("target identity or mapping differs from persistent binding")
	}
	inspection.Status = "verified"
	return inspection, nil
}

func canonicalLocalTarget(root string) string { return "local:" + root }

func targetBindingProperty(canonical string) string {
	return targetBindingPrefix + lifecycle.TargetID(canonical)
}

func bindingFor(request Request, mapped string, identity zfs.DatasetIdentity) (TargetBinding, error) {
	binding := TargetBinding{
		Version:         targetBindingVersion,
		Transport:       "local",
		CanonicalTarget: canonicalLocalTarget(request.DestinationRoot),
		DestinationRoot: request.DestinationRoot,
		MappedDataset:   mapped,
		Pool:            identity.Pool,
		PoolGUID:        identity.PoolGUID,
		Anchor:          identity.Name,
		AnchorGUID:      identity.GUID,
	}
	if identity.Name != mapped {
		if !inside(mapped, identity.Name) {
			return TargetBinding{}, fmt.Errorf("destination anchor does not contain mapped dataset")
		}
		binding.RelativePath = strings.TrimPrefix(mapped, identity.Name+"/")
		if binding.RelativePath == "" || path.Clean(binding.RelativePath) != binding.RelativePath {
			return TargetBinding{}, fmt.Errorf("invalid destination path relative to anchor")
		}
	}
	return binding, nil
}

func storedTargetBinding(state zfs.State, root, canonical string) (*TargetBinding, error) {
	property := targetBindingProperty(canonical)
	var binding *TargetBinding
	for _, row := range state.Properties {
		if row.Dataset != root || row.Name != property {
			continue
		}
		if row.Source != zfs.SourceLocal || binding != nil {
			return nil, fmt.Errorf("target binding must be unambiguous and explicitly local")
		}
		var decoded TargetBinding
		if err := json.Unmarshal([]byte(row.Value), &decoded); err != nil {
			return nil, fmt.Errorf("invalid target binding: %w", err)
		}
		binding = &decoded
	}
	if state.Received[root][property] != "" {
		return nil, fmt.Errorf("hidden received target binding requires explicit resolution")
	}
	return binding, nil
}

func validateBinding(binding TargetBinding) error {
	if binding.Version != targetBindingVersion || binding.Transport != "local" || binding.CanonicalTarget == "" || binding.DestinationRoot == "" || binding.MappedDataset == "" || binding.Pool == "" || binding.PoolGUID == 0 || binding.Anchor == "" || binding.AnchorGUID == 0 {
		return fmt.Errorf("target binding is incomplete or unsupported")
	}
	if targetBindingProperty(binding.CanonicalTarget) == targetBindingPrefix {
		return fmt.Errorf("target binding identity is invalid")
	}
	return nil
}

func encodeBinding(binding TargetBinding) (string, error) {
	if err := validateBinding(binding); err != nil {
		return "", err
	}
	data, err := json.Marshal(binding)
	return string(data), err
}
