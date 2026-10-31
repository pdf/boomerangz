// Package transfer plans and executes ownership-checked replication jobs.
package transfer

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/pdf/boomerangz/internal/lifecycle"
	"github.com/pdf/boomerangz/internal/policy"
	"github.com/pdf/boomerangz/internal/zfs"
)

// Request selects an explicitly configured local destination and an optional
// owned snapshot. Empty Snapshot chooses the newest proven owned snapshot.
//
// Source is the actor root and Policy its effective policy: they carry the
// authority, lineage, target binding and receive mapping every stream uses.
type Request struct {
	Source          string
	DestinationRoot string
	Snapshot        string
	Policy          policy.Effective
	// Members are the datasets this target receives as streams of their own,
	// each with the send options of its own policy. Empty sends the root
	// alone, which is also how a replication root sends its whole subtree.
	Members []Member
	// Transport defaults to local. RemoteName and CanonicalTarget are required
	// for ssh and are derived from validated global configuration by the caller.
	Transport       string
	RemoteName      string
	CanonicalTarget string
	// HostLocal records that the caller validated this remote's replicas
	// against this host's own datasets, because the remote's destination pool
	// is one of this host's pools. Without it such a remote is refused with a
	// HostLocalError before anything is received or destroyed.
	HostLocal bool
}

// Member is one dataset an actor root sends to a target as its own stream.
// Policy is the member's own effective policy, whose send options, incremental
// mode and receive overrides the stream uses. Snapshot pins the endpoint; empty
// selects the newest snapshot of the root's lineage on the member.
type Member struct {
	Dataset  string           `json:"dataset"`
	Policy   policy.Effective `json:"policy"`
	Snapshot string           `json:"snapshot,omitempty"`
}

// Streams lists what the request sends: its members parents first, or the root
// alone with the request's own snapshot.
func (r Request) Streams() ([]Member, error) {
	if len(r.Members) == 0 {
		return []Member{{Dataset: r.Source, Policy: r.Policy, Snapshot: r.Snapshot}}, nil
	}
	if r.Policy.Send.Replicate {
		return nil, fmt.Errorf("a replication root sends one stream, not one per member")
	}
	members := slices.Clone(r.Members)
	// Lexical order puts every parent before its children.
	slices.SortFunc(members, func(a, b Member) int { return strings.Compare(a.Dataset, b.Dataset) })
	for i, member := range members {
		if err := zfs.ValidateDataset(member.Dataset); err != nil {
			return nil, err
		}
		if !inside(member.Dataset, r.Source) {
			return nil, fmt.Errorf("member %s is outside root %s", member.Dataset, r.Source)
		}
		if i > 0 && members[i-1].Dataset == member.Dataset {
			return nil, fmt.Errorf("member %s is repeated", member.Dataset)
		}
	}
	return members, nil
}

// stream returns the member the request names, or the root.
func (r Request) stream(dataset string) (Member, error) {
	streams, err := r.Streams()
	if err != nil {
		return Member{}, err
	}
	for _, member := range streams {
		if member.Dataset == dataset {
			return member, nil
		}
	}
	return Member{}, fmt.Errorf("%s is not a stream of this request", dataset)
}

// mapped returns the root's mapped destination and the stream's, which is the
// root's plus the stream's path relative to the root: the root's mapping places
// every member's replica, so discard never re-derives it from the member's name.
func (r Request) mapped(stream string) (string, string, error) {
	root, err := zfs.MapReceiveDataset(r.Source, r.DestinationRoot, zfs.ReceiveDiscard(r.Policy.Discard))
	if err != nil {
		return "", "", err
	}
	if !inside(stream, r.Source) {
		return "", "", fmt.Errorf("stream %s is outside root %s", stream, r.Source)
	}
	return root, root + strings.TrimPrefix(stream, r.Source), nil
}

// View is a complete preflight inventory for one stream. Destination includes
// descendants even for nonrecursive sends, so a descendant resume token is never
// overlooked. Source holds the root's own state and the stream's.
//
// Destination describes the stream's mapped dataset; RootDestination describes
// the root's, which is what the target binding proves. They are the same for
// the root's own stream.
type View struct {
	Stream                  string
	Inventory               []zfs.Dataset
	DestinationInventory    []zfs.Dataset
	Source                  zfs.State
	Destination             zfs.State
	DestinationExists       bool
	DestinationIdentity     zfs.DatasetIdentity
	RootDestinationExists   bool
	RootDestinationIdentity zfs.DatasetIdentity
	BindingIdentity         zfs.DatasetIdentity
}

func requestTransport(request Request) string {
	if request.Transport == "" {
		return "local"
	}
	return request.Transport
}

func destinationInventory(request Request, view View) []zfs.Dataset {
	if requestTransport(request) == "local" || view.DestinationInventory == nil {
		return view.Inventory
	}
	return view.DestinationInventory
}

func canonicalTarget(request Request) string {
	if requestTransport(request) == "local" && request.CanonicalTarget == "" {
		return canonicalLocalTarget(request.DestinationRoot)
	}
	return request.CanonicalTarget
}

func canResumeFromBookmark(effective policy.Effective) bool {
	return effective.Incremental == "latest" && !effective.Send.Replicate
}

// streamPolicy checks a member stream's own policy against the root's: the
// member must itself send to this target, and must share the root's mapping.
func streamPolicy(request Request, stream Member) (policy.Effective, error) {
	if stream.Dataset == request.Source {
		return request.Policy.Clone(), nil
	}
	p := stream.Policy.Clone()
	if !p.Enabled || !p.Valid() {
		return p, fmt.Errorf("member %s policy must be enabled and valid", stream.Dataset)
	}
	if p.Send.Replicate {
		return p, fmt.Errorf("member %s cannot send recursively", stream.Dataset)
	}
	if p.Discard != request.Policy.Discard {
		return p, fmt.Errorf("member %s does not share the root's receive mapping", stream.Dataset)
	}
	configured := slices.Contains(p.Local, request.DestinationRoot)
	if requestTransport(request) != "local" {
		configured = slices.Contains(p.Remote, request.RemoteName)
	}
	if !configured {
		return p, fmt.Errorf("member %s does not send to this target", stream.Dataset)
	}
	return p, nil
}

// Expected records the exact received snapshot and source GUID to verify.
type Expected struct {
	Source      string              `json:"source"`
	Destination string              `json:"destination"`
	GUID        uint64              `json:"guid"`
	Metadata    *lifecycle.Metadata `json:"metadata,omitempty"`
}

// Plan is an inspection result, not an authorization to execute raw arguments.
type Plan struct {
	Source        string             `json:"source"`
	Destination   string             `json:"destination"`
	Snapshot      string             `json:"snapshot"`
	Base          string             `json:"base,omitempty"`
	Lineage       string             `json:"lineage"`
	Mode          string             `json:"mode"`
	Send          zfs.SendOptions    `json:"send"`
	Receive       zfs.ReceiveOptions `json:"receive"`
	Expected      []Expected         `json:"expected"`
	Endpoints     []Expected         `json:"endpoints"`
	Warnings      []string           `json:"warnings,omitempty"`
	TargetBinding TargetBinding      `json:"target_binding"`
	BindingNew    bool               `json:"binding_new"`
}

func datasetOf(name string) string {
	base, _, _ := strings.Cut(name, "@")
	base, _, _ = strings.Cut(base, "#")
	return base
}
func inside(name, root string) bool { return name == root || strings.HasPrefix(name, root+"/") }
func objectMap(state zfs.State) map[string]zfs.Object {
	result := map[string]zfs.Object{}
	for _, o := range state.Objects {
		result[o.Name] = o
	}
	return result
}
func ownership(state zfs.State, object zfs.Object, lineage string) (lifecycle.Metadata, error) {
	snapshot := lifecycle.Snapshot{Name: object.Name, GUID: object.GUID}
	for _, p := range state.Properties {
		if p.Dataset == object.Name {
			snapshot.Properties = append(snapshot.Properties, p)
		}
	}
	return lifecycle.Ownership(snapshot, lineage)
}

func completeReceiveExclusions(options *zfs.ReceiveOptions, p policy.Effective, source zfs.State) {
	for key := range p.Values {
		if policy.IsPublic(key) {
			options.Exclude = append(options.Exclude, key)
		}
	}
	for _, property := range source.Properties {
		if policy.IsPublic(property.Name) || strings.HasPrefix(property.Name, lifecycle.ReferencePrefix) || strings.HasPrefix(property.Name, targetBindingPrefix) {
			options.Exclude = append(options.Exclude, property.Name)
		}
	}
	slices.Sort(options.Exclude)
	options.Exclude = slices.Compact(options.Exclude)
}

// Build validates mapping, lineage, history and receive isolation for the
// stream view describes, without writes.
func Build(request Request, view View, installation string) (Plan, error) {
	plan := Plan{Source: request.Source}
	if err := zfs.ValidateDataset(request.Source); err != nil {
		return plan, err
	}
	rootPolicy := request.Policy.Clone()
	if !rootPolicy.Enabled || !rootPolicy.Valid() {
		return plan, fmt.Errorf("source policy must be enabled and valid")
	}
	if err := lifecycle.ActiveRoot(rootPolicy, request.Source); err != nil {
		return plan, err
	}
	streamName := view.Stream
	if streamName == "" {
		streamName = request.Source
	}
	stream, err := request.stream(streamName)
	if err != nil {
		return plan, err
	}
	plan.Source = stream.Dataset
	p, err := streamPolicy(request, stream)
	if err != nil {
		return plan, err
	}
	transport := requestTransport(request)
	canonical := request.CanonicalTarget
	switch transport {
	case "local":
		if !slices.Contains(rootPolicy.Local, request.DestinationRoot) && stream.Dataset == request.Source {
			return plan, fmt.Errorf("destination is not configured by the source local property")
		}
		if canonical == "" {
			canonical = canonicalLocalTarget(request.DestinationRoot)
		}
		if canonical != canonicalLocalTarget(request.DestinationRoot) {
			return plan, fmt.Errorf("local canonical target does not match destination")
		}
	case "ssh", "native":
		if request.RemoteName == "" || (stream.Dataset == request.Source && !slices.Contains(rootPolicy.Remote, request.RemoteName)) {
			return plan, fmt.Errorf("remote is not configured by the source remote property")
		}
		if canonical == "" || strings.ContainsAny(canonical, "\x00\r\n") {
			return plan, fmt.Errorf("canonical remote target is required")
		}
	default:
		return plan, fmt.Errorf("unsupported transfer transport %q", transport)
	}
	mappedRoot, target, err := request.mapped(stream.Dataset)
	if err != nil {
		return plan, err
	}
	plan.Destination = target
	if transport == "local" && (inside(mappedRoot, request.Source) || inside(request.Source, mappedRoot)) {
		return plan, fmt.Errorf("source and destination scopes overlap")
	}
	source := objectMap(view.Source)
	dest := objectMap(view.Destination)
	if root, ok := source[request.Source]; !ok || root.GUID == 0 || (root.Type != "filesystem" && root.Type != "volume") {
		return plan, fmt.Errorf("source dataset is missing")
	}
	sent, ok := source[stream.Dataset]
	if !ok || sent.GUID == 0 || (sent.Type != "filesystem" && sent.Type != "volume") {
		return plan, fmt.Errorf("member dataset is missing: %s", stream.Dataset)
	}
	for _, o := range view.Source.Objects {
		if !inside(datasetOf(o.Name), request.Source) {
			return plan, fmt.Errorf("source inventory escapes selected scope")
		}
	}
	for _, o := range view.Destination.Objects {
		if !inside(datasetOf(o.Name), target) {
			return plan, fmt.Errorf("destination inventory escapes selected scope")
		}
	}
	if len(view.Source.ResumeTokens) > 0 || len(view.Destination.ResumeTokens) > 0 {
		return plan, fmt.Errorf("resumable receive state requires recovery before a new stream")
	}
	inventory := map[string]zfs.Dataset{}
	for _, d := range view.Inventory {
		if _, duplicate := inventory[d.Name]; duplicate {
			return plan, fmt.Errorf("duplicate dataset in transfer inventory: %s", d.Name)
		}
		inventory[d.Name] = d
	}
	for _, object := range view.Source.Objects {
		if object.Type != "filesystem" && object.Type != "volume" {
			continue
		}
		dataset, exists := inventory[object.Name]
		if !exists || string(dataset.Type) != object.Type || dataset.EncryptionRoot == "" {
			return plan, fmt.Errorf("source dataset lacks complete matching inventory: %s", object.Name)
		}
	}
	destinationRows := destinationInventory(request, view)
	destinationDatasets := map[string]zfs.Dataset{}
	for _, d := range destinationRows {
		if _, duplicate := destinationDatasets[d.Name]; duplicate {
			return plan, fmt.Errorf("duplicate dataset in destination transfer inventory: %s", d.Name)
		}
		destinationDatasets[d.Name] = d
	}
	if view.DestinationIdentity.Name == "" || destinationDatasets[view.DestinationIdentity.Name].Name == "" || destinationDatasets[view.DestinationIdentity.Name].Type != view.DestinationIdentity.Type {
		return plan, fmt.Errorf("destination identity does not match sparse inventory")
	}
	rootIdentity, rootExists := view.RootDestinationIdentity, view.RootDestinationExists
	if rootIdentity.Name == "" || destinationDatasets[rootIdentity.Name].Name == "" || destinationDatasets[rootIdentity.Name].Type != rootIdentity.Type || (rootExists && rootIdentity.Name != mappedRoot) || (!rootExists && !inside(mappedRoot, rootIdentity.Name)) {
		return plan, fmt.Errorf("root destination identity does not match sparse inventory")
	}
	if view.DestinationExists {
		destinationRoot, exists := dest[target]
		if !exists || view.DestinationIdentity.Name != target || destinationRoot.GUID != view.DestinationIdentity.GUID || destinationRoot.Type != string(view.DestinationIdentity.Type) {
			return plan, fmt.Errorf("destination identity does not match exact destination root")
		}
	} else {
		nearest := ""
		for name := range destinationDatasets {
			if inside(target, name) && len(name) > len(nearest) {
				nearest = name
			}
		}
		if nearest != view.DestinationIdentity.Name {
			return plan, fmt.Errorf("destination identity is not the nearest existing ancestor")
		}
	}
	encrypted := inventory[stream.Dataset].EncryptionRoot != "" && inventory[stream.Dataset].EncryptionRoot != "-"
	descendant := ""
	if p.Send.Replicate {
		for _, d := range view.Inventory {
			if d.Name != request.Source && inside(d.Name, request.Source) && d.EncryptionRoot != "" && d.EncryptionRoot != "-" {
				descendant = d.Name
				break
			}
		}
	}
	if p.Send.Replicate {
		p = p.ForReplicationScope(encrypted, descendant)
	}
	if !p.Valid() {
		return plan, fmt.Errorf("replication-scope policy is invalid: %s", strings.Join(p.Errors, "; "))
	}
	if encrypted && p.Send.Props && !p.Send.Raw {
		return plan, fmt.Errorf("encrypted property streams require raw mode")
	}
	lineage, err := lifecycle.RootAuthority(view.Source, request.Source, installation)
	if err != nil {
		return plan, err
	}
	plan.Lineage = lineage
	suspended, err := TargetSuspended(view.Source, request.Source, canonical)
	if err != nil {
		return plan, err
	}
	if suspended {
		return plan, fmt.Errorf("target is suspended pending explicit remote revalidation")
	}
	if rootIdentity.Name == "" {
		return plan, fmt.Errorf("destination identity is missing")
	}
	// The binding proves the root's mapped destination, whichever stream is
	// being planned: every member's replica is placed beneath it.
	wantedBinding, err := bindingForTarget(request, mappedRoot, rootIdentity, transport, canonical)
	if err != nil {
		return plan, err
	}
	storedBinding, err := storedTargetBinding(view.Source, request.Source, wantedBinding.CanonicalTarget)
	if err != nil {
		return plan, err
	}
	if storedBinding == nil {
		plan.TargetBinding, plan.BindingNew = wantedBinding, true
	} else {
		if err := validateBinding(*storedBinding); err != nil {
			return plan, err
		}
		verifiedBinding := wantedBinding
		if !rootExists {
			if view.BindingIdentity.Name == "" {
				return plan, fmt.Errorf("bound destination identity is missing")
			}
			verifiedBinding, err = bindingForTarget(request, mappedRoot, view.BindingIdentity, transport, canonical)
			if err != nil {
				return plan, err
			}
		}
		if *storedBinding != verifiedBinding {
			return plan, fmt.Errorf("target identity or mapping differs from persistent binding; run boomerangz dataset reseed for this source and target")
		}
		plan.TargetBinding = *storedBinding
	}
	// A send -R stream is incremental only from a replica of the whole
	// subtree. Proofs for member streams mean this target was last replicated
	// one stream per member, so its holes and excluded datasets were never
	// sent and the recursive stream has no common base to start from.
	if p.Send.Replicate {
		refs, err := lifecycle.References(view.Source, request.Source, lineage)
		if err != nil {
			return plan, err
		}
		for _, r := range refs {
			if r.Target == plan.TargetBinding.CanonicalTarget && r.Dataset != request.Source {
				return plan, fmt.Errorf("target was last replicated one stream per member, and replicate=on needs a replica of the whole subtree; run boomerangz dataset reseed for this source and target")
			}
		}
	}
	var endpoint zfs.Object
	for _, o := range view.Source.Objects {
		if o.Type != "snapshot" || datasetOf(o.Name) != stream.Dataset {
			continue
		}
		if stream.Snapshot != "" && o.Name != stream.Snapshot {
			continue
		}
		if _, err := ownership(view.Source, o, lineage); err != nil {
			continue
		}
		if o.CreateTXG == 0 {
			return plan, fmt.Errorf("snapshot creation transaction is missing")
		}
		if o.CreateTXG > endpoint.CreateTXG {
			endpoint = o
		}
	}
	if endpoint.Name == "" {
		return plan, fmt.Errorf("no selected owned snapshot exists")
	}
	plan.Snapshot = endpoint.Name
	component := strings.TrimPrefix(endpoint.Name, stream.Dataset+"@")
	for _, o := range view.Source.Objects {
		if o.Type != "filesystem" && o.Type != "volume" {
			continue
		}
		if !p.Send.Replicate && o.Name != stream.Dataset {
			continue
		}
		childLineage, err := lifecycle.DatasetLineage(view.Source, o.Name)
		if err != nil {
			return plan, err
		}
		if childLineage != "" && childLineage != lineage {
			return plan, fmt.Errorf("conflicting source lineage on %s", o.Name)
		}
		snap, found := source[o.Name+"@"+component]
		if !found {
			return plan, fmt.Errorf("replication endpoint missing on %s", o.Name)
		}
		metadata, err := ownership(view.Source, snap, lineage)
		if err != nil {
			return plan, err
		}
		mapped := mappedRoot + strings.TrimPrefix(o.Name, request.Source)
		if existing, found := dest[mapped]; found {
			if existing.Type != o.Type {
				return plan, fmt.Errorf("source/destination dataset types differ")
			}
			localLineage, err := lifecycle.DatasetLineage(view.Destination, mapped)
			if err != nil {
				return plan, err
			}
			if localLineage != "" && localLineage != lineage {
				return plan, fmt.Errorf("destination has a conflicting lineage")
			}
		}
		plan.Endpoints = append(plan.Endpoints, Expected{Source: snap.Name, Destination: mapped + "@" + component, GUID: snap.GUID, Metadata: &metadata})
	}
	plan.Warnings = slices.Clone(p.Warnings)
	plan.Send = zfs.SendOptions{Source: stream.Dataset, Snapshot: endpoint.Name, Recursive: p.Send.Replicate, LargeBlocks: p.Send.LargeBlocks, Compressed: p.Send.Compressed, EmbeddedData: p.Send.EmbeddedData, Raw: p.Send.Raw, Properties: p.Send.Props}
	receiveProperties := maps.Clone(p.SetProperties)
	if receiveProperties == nil {
		receiveProperties = map[string]string{}
	}
	if sent.Type == "filesystem" {
		if _, configured := receiveProperties["canmount"]; !configured {
			receiveProperties["canmount"] = "noauto"
		}
	}
	// Every stream receives into exactly the destination planned above. Letting
	// receive derive it from the stream's own name with -d or -e would be a
	// second mapping rule that must agree with the root's, and for a member
	// under discard=all it does not: the member would land beside the root's
	// replica rather than beneath it.
	plan.Receive = zfs.ReceiveOptions{Root: target, Discard: zfs.ReceiveExact, Set: receiveProperties, Exclude: slices.Clone(p.IgnoreProperties)}
	already := view.DestinationExists
	for _, expected := range plan.Endpoints {
		o, found := dest[expected.Destination]
		if found && o.GUID != expected.GUID {
			return plan, fmt.Errorf("destination snapshot name has a different GUID")
		}
		already = already && found && o.GUID == expected.GUID
	}
	var baseTXG uint64
	if already {
		plan.Mode = "up-to-date"
	} else if !view.DestinationExists {
		plan.Mode = "full"
		// Missing receive parents are created from a base that must exist:
		// the configured root, or its parent when the root's replica is the
		// configured root itself. A remote never creates its configured root
		// as a receive parent, so there only the root's own stream may
		// receive it; a member's stream needs it to exist already.
		parent := request.DestinationRoot
		remoteMember := requestTransport(request) != "local" && stream.Dataset != request.Source
		if p.Discard == policy.DiscardOff && !remoteMember {
			index := strings.LastIndexByte(parent, '/')
			if index < 0 {
				return plan, fmt.Errorf("cannot bootstrap into an absent pool root")
			}
			parent = parent[:index]
		}
		if _, exists := destinationDatasets[parent]; !exists {
			if p.Discard == policy.DiscardOff && remoteMember {
				return plan, fmt.Errorf("remote %s has no configured root %s, which %s's stream receives beneath and only %s's own stream would create; create it on the remote, or send %s to this remote too", request.RemoteName, parent, stream.Dataset, request.Source, request.Source)
			}
			return plan, fmt.Errorf("receive parent %s does not exist", parent)
		}
	} else {
		var latest zfs.Object
		for _, o := range view.Destination.Objects {
			if o.Type == "snapshot" && datasetOf(o.Name) == target && o.CreateTXG > latest.CreateTXG {
				latest = o
			}
		}
		if latest.Name == "" {
			return plan, fmt.Errorf("existing destination has no common base; run boomerangz dataset reseed for this source and target")
		}
		for _, o := range view.Source.Objects {
			if datasetOf(o.Name) != stream.Dataset || o.GUID != latest.GUID || o.CreateTXG >= endpoint.CreateTXG || o.Type != "snapshot" {
				continue
			}
			if _, err := ownership(view.Source, o, lineage); err == nil {
				plan.Base = o.Name
				baseTXG = o.CreateTXG
				break
			}
		}
		if plan.Base == "" && canResumeFromBookmark(p) {
			refs, err := lifecycle.References(view.Source, request.Source, lineage)
			if err != nil {
				return plan, err
			}
			for _, r := range refs {
				if r.Target != plan.TargetBinding.CanonicalTarget || r.Dataset != stream.Dataset || r.GUID() != latest.GUID {
					continue
				}
				o, exists := source[r.BookmarkName()]
				if exists && o.Type == "bookmark" && o.GUID == r.GUID() && o.CreateTXG > 0 && o.CreateTXG < endpoint.CreateTXG {
					plan.Base = o.Name
					baseTXG = o.CreateTXG
					break
				}
			}
		}
		if plan.Base == "" {
			return plan, fmt.Errorf("destination latest snapshot has no eligible common source base; run boomerangz dataset reseed for this source and target")
		}
		plan.Mode = "incremental-latest"
		if p.Incremental == "all" {
			plan.Mode = "incremental-all"
			plan.Send.Intermediates = true
		}
		plan.Send.Base = plan.Base
	}
	if plan.Send.Recursive {
		plan.Warnings = append(plan.Warnings, "recursive replication uses native package semantics and may affect foreign destination snapshots")
	}
	for _, o := range view.Source.Objects {
		if o.Type != "snapshot" || o.CreateTXG == 0 || o.CreateTXG > endpoint.CreateTXG {
			continue
		}
		if !plan.Send.Recursive && datasetOf(o.Name) != stream.Dataset {
			continue
		}
		include := o.Name == endpoint.Name
		if plan.Send.Recursive {
			include = plan.Mode == "full" || (o.CreateTXG > baseTXG && strings.HasSuffix(o.Name, "@"+component))
		}
		if plan.Send.Intermediates {
			include = o.CreateTXG > baseTXG
		}
		if plan.Mode == "up-to-date" {
			include = false
		}
		if !include {
			continue
		}
		mapped := mappedRoot + strings.TrimPrefix(o.Name, request.Source)
		expected := Expected{Source: o.Name, Destination: mapped, GUID: o.GUID}
		if metadata, err := ownership(view.Source, o, lineage); err == nil {
			expected.Metadata = &metadata
		} else {
			plan.Warnings = append(plan.Warnings, "stream includes foreign snapshot: "+o.Name)
		}
		plan.Expected = append(plan.Expected, expected)
	}
	if plan.Mode == "up-to-date" {
		plan.Expected = slices.Clone(plan.Endpoints)
	}
	// Include all known public keys and source recovery references, but retain the
	// three minimal ownership keys. No prefix wildcard exists in receive -x.
	completeReceiveExclusions(&plan.Receive, p, view.Source)
	slices.SortFunc(plan.Expected, func(a, b Expected) int { return strings.Compare(a.Source, b.Source) })
	slices.SortFunc(plan.Endpoints, func(a, b Expected) int { return strings.Compare(a.Source, b.Source) })
	slices.Sort(plan.Warnings)
	plan.Warnings = slices.Compact(plan.Warnings)
	return plan, nil
}
