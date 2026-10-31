package transfer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strings"

	"github.com/pdf/boomerangz/internal/lifecycle"
	"github.com/pdf/boomerangz/internal/zfs"
)

const (
	targetBindingVersion = 2
	targetBindingPrefix  = lifecycle.TargetStatePrefix
)

// TargetBinding is the persistent source-side identity of one configured
// destination. Anchor identifies either the existing destination dataset or,
// for a bootstrap, its nearest existing ancestor.
//
// A binding is checked by rebuilding it from configuration and the live
// identity of its anchor and comparing the two, so only the identity and the
// mapped dataset it was made for are stored (see bindingRecord). Transport,
// CanonicalTarget and DestinationRoot are what configuration says about the
// target, and the property key hashes CanonicalTarget; Pool and RelativePath
// follow from Anchor.
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

// bindingRecord is a TargetBinding's stored value. Mapped is the dataset the
// binding was made for. Configuration names the mapped dataset too, but a
// changed discard or destination moves that, and only the stored name tells
// the replica's old place from the new one. Anchor is stored only while it is
// an ancestor of the mapped dataset: after the first receive the binding moves
// to the replica itself, which Mapped already names.
type bindingRecord struct {
	Version    int    `json:"version"`
	PoolGUID   uint64 `json:"pool_guid"`
	Mapped     string `json:"mapped"`
	AnchorGUID uint64 `json:"anchor_guid"`
	Anchor     string `json:"anchor,omitempty"`
}

// MappingChangedError reports a binding made for another mapped dataset than
// the one configuration now names. Neither transfer nor reseed may proceed: a
// replica, if there is one, is at Stored, and the binding cannot follow it to
// Configured.
type MappingChangedError struct {
	Stored, Configured string
}

func (e *MappingChangedError) Error() string {
	return fmt.Sprintf("target mapping changed from %s to %s since the binding was recorded; restore the previous mapping, or forget the target with boomerangz dataset clean --forget-target and let the next transfer seed it afresh", e.Stored, e.Configured)
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
	InspectState(context.Context, string, bool) (zfs.State, error)
}

// placeholderAt reports whether dataset is a placeholder, with the identity it
// read to decide. The identity says whether the marker is set, in one query;
// only a marked dataset is inspected in full, since a state inspection costs
// commands per snapshot.
func placeholderAt(ctx context.Context, reader localIdentityReader, dataset string) (zfs.DatasetIdentity, bool, error) {
	identity, err := reader.InspectDatasetIdentity(ctx, dataset)
	if err != nil || !identity.Placeholder {
		return identity, false, err
	}
	state, err := reader.InspectState(ctx, dataset, false)
	if err != nil {
		return identity, false, err
	}
	return identity, isPlaceholder(state, dataset), nil
}

// isPlaceholder reports whether state shows dataset as a placeholder: marked
// locally when boomerangz created it as a receive parent, and never given a
// snapshot or bookmark since. Anything else is treated as a real dataset.
func isPlaceholder(state zfs.State, dataset string) bool {
	marked := false
	for _, property := range state.Properties {
		if property.Dataset == dataset && property.Name == zfs.PlaceholderProperty && property.Source == zfs.SourceLocal && property.Value == "on" {
			marked = true
		}
	}
	if !marked || state.ResumeTokens[dataset] != "" {
		return false
	}
	for _, object := range state.Objects {
		if (object.Type == "snapshot" || object.Type == "bookmark") && datasetOf(object.Name) == dataset {
			return false
		}
	}
	return true
}

// nearestReal returns name, or its nearest ancestor present in inventory that
// is not a placeholder. A placeholder is never a replica, so it can never
// anchor a target binding or stand for a replica's existence: anchoring a
// binding on one would block the replica it stood in for from ever replacing
// it. Only datasets inside the configured root can be placeholders, so nothing
// above it is inspected.
func nearestReal(ctx context.Context, reader localIdentityReader, name, configuredRoot string, inventory []zfs.Dataset) (zfs.DatasetIdentity, error) {
	for candidate := nearestExisting(name, inventory); candidate != ""; {
		if !inside(candidate, configuredRoot) {
			return reader.InspectDatasetIdentity(ctx, candidate)
		}
		identity, placeholder, err := placeholderAt(ctx, reader, candidate)
		if err != nil {
			return zfs.DatasetIdentity{}, err
		}
		if !placeholder {
			return identity, nil
		}
		index := strings.LastIndexByte(candidate, '/')
		if index < 0 {
			return zfs.DatasetIdentity{}, nil
		}
		candidate = nearestExisting(candidate[:index], inventory)
	}
	return zfs.DatasetIdentity{}, nil
}

// InspectTarget resolves an unbound target through its nearest existing ancestor.
// A bound target revalidates its stored anchor until the exact dataset exists.
func InspectTarget(ctx context.Context, reader localIdentityReader, request Request, source zfs.State) (LocalTargetInspection, error) {
	transport, canonical := requestTransport(request), canonicalTarget(request)
	configured := request.DestinationRoot
	if transport == "ssh" || transport == "native" {
		configured = request.RemoteName
	}
	inspection := LocalTargetInspection{ConfiguredName: configured, Transport: transport, CanonicalEndpoint: canonical, DestinationRoot: request.DestinationRoot}
	if (transport != "local" && transport != "ssh" && transport != "native") || canonical == "" {
		inspection.Status = "invalid-mapping"
		return inspection, fmt.Errorf("unsupported or incomplete target transport")
	}
	mapped, _, err := request.mapped(request.Source)
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
	nearest, err := nearestReal(ctx, reader, mapped, request.DestinationRoot, inventory)
	if err != nil {
		inspection.Status = "unavailable"
		return inspection, err
	}
	anchor := nearest.Name
	if anchor == "" {
		inspection.Status = "unavailable"
		return inspection, fmt.Errorf("destination has no existing ancestor")
	}
	stored, err := storedTargetBinding(source, request.Source, bindingTemplate(request, mapped, transport, canonical))
	if changed := (*MappingChangedError)(nil); errors.As(err, &changed) {
		inspection.Status = "mismatch"
		return inspection, err
	}
	if err != nil {
		inspection.Status = "invalid-binding"
		return inspection, err
	}
	inspection.Stored = stored
	if stored != nil {
		if err := validateBinding(*stored); err != nil {
			inspection.Status = "invalid-binding"
			return inspection, err
		}
		// New intermediate datasets may appear beneath a verified bootstrap
		// anchor as related sources populate the destination namespace. Until
		// this source's exact mapped dataset exists, revalidate the stored anchor
		// rather than treating the nearer ancestor as a replacement.
		if anchor != mapped {
			anchor = stored.Anchor
		}
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
	if stored == nil {
		inspection.Status = "unbound"
		return inspection, nil
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

// VerifyTargetBinding re-resolves the exact stored anchor before lifecycle
// administration relies on a previously recorded destination. request names
// the source root, its policy and the configured target.
func VerifyTargetBinding(ctx context.Context, reader localIdentityReader, source zfs.State, request Request) (TargetBinding, error) {
	target, err := bindingTarget(request)
	if err != nil {
		return TargetBinding{}, err
	}
	stored, err := storedTargetBinding(source, request.Source, target)
	if err != nil {
		return TargetBinding{}, err
	}
	if stored == nil {
		return TargetBinding{}, fmt.Errorf("target has no persistent binding")
	}
	if err := validateBinding(*stored); err != nil {
		return TargetBinding{}, err
	}
	identity, err := reader.InspectDatasetIdentity(ctx, stored.Anchor)
	if err != nil {
		return TargetBinding{}, err
	}
	resolved, err := target.anchored(identity.Name, identity.PoolGUID, identity.GUID)
	if err != nil {
		return TargetBinding{}, err
	}
	if resolved != *stored {
		return TargetBinding{}, fmt.Errorf("target identity differs from persistent binding")
	}
	return *stored, nil
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
		// A received marker is the sending installation's gate, not this one's.
		if row.Dataset != root || row.Name != property || row.Source != zfs.SourceLocal {
			continue
		}
		if found || row.Value != "unverified" {
			return false, fmt.Errorf("target suspension marker is invalid")
		}
		found = true
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
	return bindingTemplate(request, mapped, transport, canonical).anchored(identity.Name, identity.PoolGUID, identity.GUID)
}

// bindingTemplate is what configuration says about a target: the part of a
// binding that is never stored, which a stored record is completed from.
func bindingTemplate(request Request, mapped, transport, canonical string) TargetBinding {
	return TargetBinding{Version: targetBindingVersion, Transport: transport, CanonicalTarget: canonical, DestinationRoot: request.DestinationRoot, MappedDataset: mapped}
}

// bindingTarget is bindingTemplate for request's own root and target.
func bindingTarget(request Request) (TargetBinding, error) {
	mapped, _, err := request.mapped(request.Source)
	if err != nil {
		return TargetBinding{}, err
	}
	return bindingTemplate(request, mapped, requestTransport(request), canonicalTarget(request)), nil
}

// anchored completes target with the identity of the dataset it is anchored
// on: the mapped dataset itself, or for a bootstrap its nearest existing
// ancestor.
func (b TargetBinding) anchored(anchor string, poolGUID, anchorGUID uint64) (TargetBinding, error) {
	b.Pool, _, _ = strings.Cut(anchor, "/")
	b.PoolGUID, b.Anchor, b.AnchorGUID, b.RelativePath = poolGUID, anchor, anchorGUID, ""
	if anchor != b.MappedDataset {
		if !inside(b.MappedDataset, anchor) {
			return TargetBinding{}, fmt.Errorf("destination anchor does not contain mapped dataset")
		}
		b.RelativePath = strings.TrimPrefix(b.MappedDataset, anchor+"/")
		if b.RelativePath == "" || path.Clean(b.RelativePath) != b.RelativePath {
			return TargetBinding{}, fmt.Errorf("invalid destination path relative to anchor")
		}
	}
	return b, nil
}

// storedTargetBinding reads root's binding for target, completing the stored
// identity with what configuration says about the target.
func storedTargetBinding(state zfs.State, root string, target TargetBinding) (*TargetBinding, error) {
	property := targetBindingProperty(target.CanonicalTarget)
	var binding *TargetBinding
	for _, row := range state.Properties {
		// A received binding names the sending installation's destination,
		// which says nothing about where this root replicates.
		if row.Dataset != root || row.Name != property || row.Source != zfs.SourceLocal {
			continue
		}
		if binding != nil {
			return nil, fmt.Errorf("target binding must be unambiguous")
		}
		var record bindingRecord
		decoder := json.NewDecoder(strings.NewReader(row.Value))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&record); err != nil || decoder.More() {
			return nil, fmt.Errorf("invalid target binding")
		}
		if record.Version != targetBindingVersion {
			return nil, fmt.Errorf("target binding version %d is unsupported", record.Version)
		}
		if zfs.ValidateDataset(record.Mapped) != nil {
			return nil, fmt.Errorf("invalid target binding mapping")
		}
		if record.Mapped != target.MappedDataset {
			return nil, &MappingChangedError{Stored: record.Mapped, Configured: target.MappedDataset}
		}
		anchor := record.Anchor
		if anchor == "" {
			anchor = record.Mapped
		} else if anchor == record.Mapped || zfs.ValidateDataset(anchor) != nil {
			return nil, fmt.Errorf("invalid target binding anchor")
		}
		decoded, err := target.anchored(anchor, record.PoolGUID, record.AnchorGUID)
		if err != nil {
			return nil, fmt.Errorf("invalid target binding: %w", err)
		}
		binding = &decoded
	}
	return binding, nil
}

func validateBinding(binding TargetBinding) error {
	if binding.Version != targetBindingVersion || (binding.Transport != "local" && binding.Transport != "ssh" && binding.Transport != "native") || binding.CanonicalTarget == "" || binding.DestinationRoot == "" || binding.MappedDataset == "" || binding.Pool == "" || binding.PoolGUID == 0 || binding.Anchor == "" || binding.AnchorGUID == 0 {
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
	record := bindingRecord{Version: binding.Version, PoolGUID: binding.PoolGUID, Mapped: binding.MappedDataset, AnchorGUID: binding.AnchorGUID}
	if binding.Anchor != binding.MappedDataset {
		record.Anchor = binding.Anchor
	}
	data, err := json.Marshal(record)
	return string(data), err
}
