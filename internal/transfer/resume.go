package transfer

import (
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"

	"github.com/pdf/boomerangz/internal/lifecycle"
	"github.com/pdf/boomerangz/internal/zfs"
)

// BuildResume reconstructs an interrupted receive of the stream view describes
// exclusively from durable ZFS state. It never guesses which source snapshot
// belongs to an opaque token: the token must lie inside the stream's mapped
// destination and match a proof of that stream alone. Other members' streams
// play no part, since a token can only belong to the one stream that wrote it.
func BuildResume(request Request, view View, installation string) (Plan, lifecycle.Reference, error) {
	plan := Plan{Source: request.Source}
	var zero lifecycle.Reference
	if err := zfs.ValidateDataset(request.Source); err != nil {
		return plan, zero, err
	}
	rootPolicy := request.Policy.Clone()
	if !rootPolicy.Enabled || !rootPolicy.Valid() {
		return plan, zero, fmt.Errorf("source policy must be enabled and valid")
	}
	if err := lifecycle.ActiveRoot(rootPolicy, request.Source); err != nil {
		return plan, zero, err
	}
	streamName := view.Stream
	if streamName == "" {
		streamName = request.Source
	}
	stream, err := request.stream(streamName)
	if err != nil {
		return plan, zero, err
	}
	plan.Source = stream.Dataset
	p, err := streamPolicy(request, stream)
	if err != nil {
		return plan, zero, err
	}
	transport, canonical := requestTransport(request), canonicalTarget(request)
	switch transport {
	case "local":
		if (stream.Dataset == request.Source && !slices.Contains(rootPolicy.Local, request.DestinationRoot)) || canonical != canonicalLocalTarget(request.DestinationRoot) {
			return plan, zero, fmt.Errorf("destination is not configured by the source local property")
		}
	case "ssh", "native":
		if request.RemoteName == "" || (stream.Dataset == request.Source && !slices.Contains(rootPolicy.Remote, request.RemoteName)) || canonical == "" || strings.ContainsAny(canonical, "\x00\r\n") {
			return plan, zero, fmt.Errorf("remote is not configured with a canonical target")
		}
	default:
		return plan, zero, fmt.Errorf("unsupported transfer transport %q", transport)
	}
	mappedRoot, target, err := request.mapped(stream.Dataset)
	if err != nil {
		return plan, zero, err
	}
	plan.Destination = target
	if transport == "local" && (inside(mappedRoot, request.Source) || inside(request.Source, mappedRoot)) {
		return plan, zero, fmt.Errorf("source and destination scopes overlap")
	}
	if len(view.Source.ResumeTokens) != 0 || len(view.Destination.ResumeTokens) != 1 {
		return plan, zero, fmt.Errorf("exactly one destination resume token is required")
	}
	var tokenDataset, token string
	for dataset, value := range view.Destination.ResumeTokens {
		tokenDataset, token = dataset, value
		break
	}
	if !inside(tokenDataset, target) || token == "" {
		return plan, zero, fmt.Errorf("resume token escapes the mapped destination")
	}
	if !view.DestinationExists {
		return plan, zero, fmt.Errorf("resumable destination root is missing")
	}
	for _, object := range view.Source.Objects {
		if !inside(datasetOf(object.Name), request.Source) {
			return plan, zero, fmt.Errorf("source inventory escapes selected scope")
		}
	}
	for _, object := range view.Destination.Objects {
		if !inside(datasetOf(object.Name), target) {
			return plan, zero, fmt.Errorf("destination inventory escapes selected scope")
		}
	}
	inventory := make(map[string]zfs.Dataset, len(view.Inventory))
	for _, dataset := range view.Inventory {
		if _, duplicate := inventory[dataset.Name]; duplicate {
			return plan, zero, fmt.Errorf("duplicate dataset in source transfer inventory: %s", dataset.Name)
		}
		inventory[dataset.Name] = dataset
	}
	sent, exists := inventory[stream.Dataset]
	if !exists || (sent.Type != zfs.Filesystem && sent.Type != zfs.Volume) || sent.EncryptionRoot == "" {
		return plan, zero, fmt.Errorf("source dataset lacks complete matching inventory")
	}
	destinationDatasets := make(map[string]zfs.Dataset)
	for _, dataset := range destinationInventory(request, view) {
		if _, duplicate := destinationDatasets[dataset.Name]; duplicate {
			return plan, zero, fmt.Errorf("duplicate dataset in destination transfer inventory: %s", dataset.Name)
		}
		destinationDatasets[dataset.Name] = dataset
	}
	if view.DestinationIdentity.Name != target || view.DestinationIdentity.GUID == 0 || destinationDatasets[target].Type != view.DestinationIdentity.Type {
		return plan, zero, fmt.Errorf("destination identity does not match resumable root")
	}
	lineage, err := lifecycle.RootAuthority(view.Source, request.Source, installation)
	if err != nil {
		return plan, zero, err
	}
	plan.Lineage = lineage
	suspended, err := TargetSuspended(view.Source, request.Source, canonical)
	if err != nil {
		return plan, zero, err
	}
	if suspended {
		return plan, zero, fmt.Errorf("target is suspended pending explicit remote revalidation")
	}
	stored, err := storedTargetBinding(view.Source, request.Source, canonical)
	if err != nil || stored == nil {
		return plan, zero, fmt.Errorf("resumable transfer lacks a valid target binding")
	}
	if err := validateBinding(*stored); err != nil {
		return plan, zero, err
	}
	if view.BindingIdentity.Name == "" {
		return plan, zero, fmt.Errorf("bound destination identity is missing")
	}
	wanted, err := bindingForTarget(request, mappedRoot, view.BindingIdentity, transport, canonical)
	if err != nil || *stored != wanted {
		return plan, zero, fmt.Errorf("resumable target differs from persistent binding")
	}
	plan.TargetBinding = *stored
	refs, err := lifecycle.References(view.Source, request.Source, lineage)
	if err != nil {
		return plan, zero, err
	}
	sourceObjects, destinationObjects := objectMap(view.Source), objectMap(view.Destination)
	var candidate lifecycle.Reference
	var candidateTXG uint64
	tied := false
	for _, ref := range refs {
		if ref.Target != canonical || ref.Dataset != stream.Dataset {
			continue
		}
		valid := true
		for _, member := range ref.Sources {
			name := member.Dataset + "@" + member.Metadata.Name()
			object, exists := sourceObjects[name]
			if !exists || object.Type != "snapshot" || object.GUID != member.GUID || !slices.Contains(view.Source.Holds[name], ref.HoldName()) {
				valid = false
				break
			}
			metadata, ownershipErr := ownership(view.Source, object, lineage)
			if ownershipErr != nil || !reflect.DeepEqual(metadata, member.Metadata) {
				valid = false
				break
			}
		}
		if valid {
			txg := sourceObjects[ref.SnapshotName()].CreateTXG
			if txg == 0 {
				continue
			}
			if txg > candidateTXG {
				candidate, candidateTXG, tied = ref, txg, false
			} else if txg == candidateTXG {
				tied = true
			}
		}
	}
	if candidateTXG == 0 || tied {
		return plan, zero, fmt.Errorf("resume token does not have an unambiguous newest held source proof")
	}
	ref := candidate
	tokenCovered := false
	for _, member := range ref.Sources {
		mappedDataset := mappedRoot + strings.TrimPrefix(member.Dataset, request.Source)
		if tokenDataset == mappedDataset {
			tokenCovered = true
		}
		if existing, exists := destinationObjects[mappedDataset]; exists {
			sourceDataset := sourceObjects[member.Dataset]
			if existing.Type != sourceDataset.Type {
				return plan, zero, fmt.Errorf("source/destination dataset types differ")
			}
			localLineage, lineageErr := lifecycle.DatasetLineage(view.Destination, mappedDataset)
			if lineageErr != nil || (localLineage != "" && localLineage != lineage) {
				return plan, zero, fmt.Errorf("destination has a conflicting lineage")
			}
		}
		metadata := member.Metadata
		expected := Expected{Source: member.Dataset + "@" + member.Metadata.Name(), Destination: mappedDataset + "@" + member.Metadata.Name(), GUID: member.GUID, Metadata: &metadata}
		plan.Expected = append(plan.Expected, expected)
		plan.Endpoints = append(plan.Endpoints, expected)
		if member.Dataset == stream.Dataset {
			plan.Snapshot = expected.Source
		}
	}
	if !tokenCovered || plan.Snapshot == "" {
		return plan, zero, fmt.Errorf("resume token is not covered by its held source proof")
	}
	plan.Mode = "resume"
	plan.Warnings = slices.Clone(p.Warnings)
	plan.Send = zfs.SendOptions{Source: stream.Dataset, ResumeToken: token}
	// OpenZFS requires a resumed stream to be received into the exact dataset
	// that owns the token, including a descendant interrupted by recursive send.
	plan.Receive = zfs.ReceiveOptions{Root: tokenDataset, Discard: zfs.ReceiveExact, Set: maps.Clone(p.SetProperties), Exclude: slices.Clone(p.IgnoreProperties)}
	completeReceiveExclusions(&plan.Receive, p, view.Source)
	return plan, ref, nil
}
