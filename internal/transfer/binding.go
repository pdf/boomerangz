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
// destination. The historical name is retained for API stability.
type LocalTargetInspection struct {
	ConfiguredName    string         `json:"configured_name"`
	Transport         string         `json:"transport"`
	CanonicalEndpoint string         `json:"canonical_endpoint"`
	DestinationRoot   string         `json:"destination_root"`
	MappedDataset     string         `json:"mapped_dataset"`
	Stored            *TargetBinding `json:"stored_binding,omitempty"`
	Resolved          *TargetBinding `json:"resolved_identity,omitempty"`
	Status            string         `json:"status"`
	EndpointMode      string         `json:"endpoint_mode,omitempty"`
}

type localIdentityReader interface {
	ListDatasets(context.Context) ([]zfs.Dataset, error)
	InspectDatasetIdentity(context.Context, string) (zfs.DatasetIdentity, error)
}

// InspectTarget resolves the exact dataset or nearest existing ancestor and
// compares it with any persistent source-root binding for local or SSH targets.
func InspectTarget(ctx context.Context, reader localIdentityReader, request Request, source zfs.State) (LocalTargetInspection, error) {
	transport, canonical := requestTransport(request), canonicalTarget(request)
	configured := request.DestinationRoot
	if transport == "ssh" {
		configured = request.RemoteName
	}
	inspection := LocalTargetInspection{ConfiguredName: configured, Transport: transport, CanonicalEndpoint: canonical, DestinationRoot: request.DestinationRoot}
	if (transport != "local" && transport != "ssh") || canonical == "" {
		inspection.Status = "invalid-mapping"
		return inspection, fmt.Errorf("unsupported or incomplete target transport")
	}
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
	resolved, err := bindingForTarget(request, mapped, identity, transport, canonical)
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

// InspectLocalTarget preserves the Phase 4 local-target API.
func InspectLocalTarget(ctx context.Context, reader localIdentityReader, request Request, source zfs.State) (LocalTargetInspection, error) {
	return InspectTarget(ctx, reader, request, source)
}

func canonicalLocalTarget(root string) string { return "local:" + root }

func targetBindingProperty(canonical string) string {
	return targetBindingPrefix + lifecycle.TargetID(canonical)
}

func targetSuspendedProperty(canonical string) string {
	return targetBindingProperty(canonical) + ":suspended"
}

// TargetSuspended reports whether adoption has gated a remote pending explicit
// identity revalidation.
func TargetSuspended(state zfs.State, root, canonical string) (bool, error) {
	property := targetSuspendedProperty(canonical)
	found := false
	for _, row := range state.Properties {
		if row.Dataset != root || row.Name != property {
			continue
		}
		if row.Source != zfs.SourceLocal || found || row.Value != "unverified" {
			return false, fmt.Errorf("target suspension marker is invalid")
		}
		found = true
	}
	if state.Received[root][property] != "" {
		return false, fmt.Errorf("hidden received target suspension marker requires explicit resolution")
	}
	return found, nil
}

// SetTargetSuspended records or clears an adoption-time remote safety gate.
func SetTargetSuspended(ctx context.Context, executor zfs.Executor, source, canonical string, suspended bool) error {
	if executor == nil || canonical == "" || strings.ContainsAny(canonical, "\x00\r\n") {
		return fmt.Errorf("valid target suspension request required")
	}
	property := targetSuspendedProperty(canonical)
	if suspended {
		return executor.SetProperties(ctx, source, map[string]string{property: "unverified"})
	}
	return executor.InheritProperty(ctx, source, property)
}

func bindingFor(request Request, mapped string, identity zfs.DatasetIdentity) (TargetBinding, error) {
	return bindingForTarget(request, mapped, identity, "local", canonicalLocalTarget(request.DestinationRoot))
}

func bindingForTarget(request Request, mapped string, identity zfs.DatasetIdentity, transport, canonical string) (TargetBinding, error) {
	binding := TargetBinding{
		Version:         targetBindingVersion,
		Transport:       transport,
		CanonicalTarget: canonical,
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
	if binding.Version != targetBindingVersion || (binding.Transport != "local" && binding.Transport != "ssh") || binding.CanonicalTarget == "" || binding.DestinationRoot == "" || binding.MappedDataset == "" || binding.Pool == "" || binding.PoolGUID == 0 || binding.Anchor == "" || binding.AnchorGUID == 0 {
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
