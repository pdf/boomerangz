package lifecycle

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/pdf/boomerangz/internal/policy"
	"github.com/pdf/boomerangz/internal/zfs"
)

// ReferencePrefix identifies per-target, per-snapshot recovery proofs. Keep
// old records until their corresponding hold and bookmark have been removed.
const ReferencePrefix = policy.StateNamespace + "reference:"

// Reference proves one stream's endpoint on one target. Dataset is the stream's
// source: a member of the actor root, or the root itself, whose -R stream then
// carries its whole subtree. Sources are exactly the snapshots that endpoint
// consists of, sorted by dataset with the stream's own first, each held under
// the target's tag and checkpointed by bookmark once the destination verifies.
//
// The record lives on the actor root, keyed by target, stream and snapshot
// instant. Members keep their own cadences, so two members' newest snapshots
// are usually different instants; keying by stream lets each member's proof
// advance on its own. Target is a canonical non-secret destination identity
// supplied by the transport planner, not a credential or arbitrary command.
type Reference struct {
	Target   string            `json:"target"`
	Dataset  string            `json:"dataset"`
	Metadata Metadata          `json:"metadata"`
	Sources  []ReferenceSource `json:"sources"`
}

// ReferenceSource is one snapshot of a stream's endpoint.
type ReferenceSource struct {
	Dataset  string   `json:"dataset"`
	GUID     uint64   `json:"guid"`
	Metadata Metadata `json:"metadata"`
}

// TargetID is deterministic and does not put arbitrary target text in ZFS names.
func TargetID(target string) string {
	sum := sha256.Sum256([]byte(target))
	return hex.EncodeToString(sum[:])
}

// streamID names a stream in a property name without putting arbitrary
// dataset text there, and keeps the name well inside ZFS's limit.
func streamID(dataset string) string {
	sum := sha256.Sum256([]byte(dataset))
	return hex.EncodeToString(sum[:16])
}
func (r Reference) property() string {
	return ReferencePrefix + TargetID(r.Target) + ":" + streamID(r.Dataset) + ":" + r.Metadata.Snapshot
}
func (r Reference) hold() string { return SnapshotPrefix + TargetID(r.Target) }
func (r Reference) bookmark() string {
	return r.Dataset + "#" + r.hold() + "-" + r.Metadata.Snapshot
}
func (r Reference) snapshot() string { return r.Dataset + "@" + r.Metadata.Name() }

func (r Reference) sources() []ReferenceSource { return r.Sources }

// GUID is the GUID of the stream's own snapshot.
func (r Reference) GUID() uint64 {
	for _, source := range r.Sources {
		if source.Dataset == r.Dataset {
			return source.GUID
		}
	}
	return 0
}

func (r Reference) sourceSnapshot(source ReferenceSource) string {
	return source.Dataset + "@" + source.Metadata.Name()
}

func (r Reference) sourceBookmark(source ReferenceSource) string {
	return source.Dataset + "#" + r.hold() + "-" + source.Metadata.Snapshot
}

type referenceBackend interface {
	Hold(context.Context, string, string) error
	Release(context.Context, string, string) error
	Bookmark(context.Context, string, string) error
	DestroyBookmark(context.Context, string) error
	InheritProperty(context.Context, string, string) error
	ListDatasets(context.Context) ([]zfs.Dataset, error)
}

func referenceRecords(state zfs.State, dataset, lineage string) ([]Reference, error) {
	var refs []Reference
	seen := map[string]bool{}
	for _, p := range state.Properties {
		if p.Dataset != dataset || !strings.HasPrefix(p.Name, ReferencePrefix) {
			continue
		}
		// A received proof describes another source's targets. It is clean
		// metadata, not authority to claim local holds or bookmarks.
		if p.Source != zfs.SourceLocal {
			continue
		}
		var r Reference
		if json.Unmarshal([]byte(p.Value), &r) != nil || r.Target == "" || r.Metadata.Lineage != lineage || !ValidID(r.Metadata.Snapshot) || r.Metadata.Created.IsZero() || zfs.ValidateDataset(r.Dataset) != nil || r.property() != p.Name || seen[p.Name] {
			return nil, fmt.Errorf("invalid reference proof %s", p.Name)
		}
		if err := validSources(r, dataset); err != nil {
			return nil, fmt.Errorf("invalid reference proof %s: %w", p.Name, err)
		}
		seen[p.Name] = true
		refs = append(refs, r)
	}
	return refs, nil
}

// validSources requires a stream inside root whose sources are its own
// snapshot followed by snapshots of its descendants, all of one instant and in
// canonical order.
func validSources(r Reference, root string) error {
	if r.Dataset != root && !strings.HasPrefix(r.Dataset, root+"/") {
		return fmt.Errorf("stream is outside root")
	}
	if len(r.Sources) == 0 || r.Sources[0].Dataset != r.Dataset {
		return fmt.Errorf("stream snapshot must lead its sources")
	}
	for i, source := range r.Sources {
		if zfs.ValidateDataset(source.Dataset) != nil || (i > 0 && !strings.HasPrefix(source.Dataset, r.Dataset+"/")) || source.GUID == 0 || source.Metadata != r.Metadata {
			return fmt.Errorf("source %s does not belong to the stream's endpoint", source.Dataset)
		}
		if i > 0 && strings.Compare(r.Sources[i-1].Dataset, source.Dataset) >= 0 {
			return fmt.Errorf("sources are not in canonical order")
		}
	}
	return nil
}

func findOwned(state zfs.State, dataset, name, lineage string) (Snapshot, Metadata, error) {
	for _, snapshot := range snapshotsIn(state, dataset) {
		if snapshot.Name != name {
			continue
		}
		metadata, err := Ownership(snapshot, lineage)
		return snapshot, metadata, err
	}
	return Snapshot{}, Metadata{}, fmt.Errorf("source snapshot not found")
}

// covers reports whether prior already proves every source wanted names, with
// identical GUIDs and metadata. A stream that sent its whole subtree with
// send -R proves the root's own snapshot among the rest, so a root stream that
// later sends members one at a time inherits that proof rather than
// contradicting it.
func covers(prior, wanted Reference) bool {
	if prior.Target != wanted.Target || prior.Dataset != wanted.Dataset || prior.Metadata != wanted.Metadata {
		return false
	}
	for _, source := range wanted.Sources {
		if !slices.Contains(prior.Sources, source) {
			return false
		}
	}
	return true
}

// proofScope inspects what a recovery proof involves: the actor root, which
// holds authority and every proof, and each dataset its snapshots are on that
// still exists. Nothing else beneath the root bears on a proof, and reading the
// whole subtree would cost every member's snapshots for each stream a root
// sends. A dataset that is gone contributes nothing, so a proof for it can
// still be released.
func (s *Service) proofScope(ctx context.Context, root string, datasets []string) (zfs.State, error) {
	state, err := s.backend.InspectState(ctx, root, false)
	if err != nil {
		return zfs.State{}, err
	}
	var wanted []string
	for _, dataset := range datasets {
		if dataset != root && !slices.Contains(wanted, dataset) {
			wanted = append(wanted, dataset)
		}
	}
	if len(wanted) == 0 {
		return state, nil
	}
	lister, ok := s.backend.(referenceBackend)
	if !ok {
		return zfs.State{}, fmt.Errorf("reference operations unavailable")
	}
	inventory, err := lister.ListDatasets(ctx)
	if err != nil {
		return zfs.State{}, err
	}
	slices.Sort(wanted)
	for _, dataset := range wanted {
		if !slices.ContainsFunc(inventory, func(d zfs.Dataset) bool { return d.Name == dataset }) {
			continue
		}
		member, err := s.backend.InspectState(ctx, dataset, false)
		if err != nil {
			return zfs.State{}, err
		}
		state = zfs.MergeStates(state, member)
	}
	return state, nil
}

// proofUnchanged re-reads proofScope and requires it to equal expected.
func (s *Service) proofUnchanged(ctx context.Context, root string, datasets []string, expected zfs.State) error {
	current, err := s.proofScope(ctx, root, datasets)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(expected, current) {
		return &StateChangedError{}
	}
	return nil
}

// sourceDatasets names the datasets a proof's snapshots are on.
func sourceDatasets(r Reference) []string {
	var datasets []string
	for _, source := range r.sources() {
		datasets = append(datasets, source.Dataset)
	}
	return datasets
}

func containsReference(refs []Reference, wanted Reference) bool {
	return slices.ContainsFunc(refs, func(candidate Reference) bool { return reflect.DeepEqual(candidate, wanted) })
}

// Protect records a recovery proof for one snapshot's own stream before
// placing the target hold. If interrupted between writes, the record remains
// reconstructable and a retry is idempotent.
func (s *Service) Protect(ctx context.Context, dataset, snapshot, target string) (Reference, error) {
	stream, _, _ := strings.Cut(snapshot, "@")
	return s.ProtectSet(ctx, dataset, stream, []string{snapshot}, target)
}

// ProtectSet records the proof for one stream's endpoint on the actor root
// dataset and holds every snapshot it consists of: the stream's own, and under
// -R every descendant's at the same instant.
func (s *Service) ProtectSet(ctx context.Context, dataset, stream string, snapshots []string, target string) (Reference, error) {
	unlock, err := s.lock(ctx, dataset)
	if err != nil {
		return Reference{}, err
	}
	defer unlock()
	backend, ok := s.backend.(referenceBackend)
	if !ok {
		return Reference{}, fmt.Errorf("reference operations unavailable")
	}
	if err := zfs.ValidateDataset(dataset); err != nil {
		return Reference{}, err
	}
	if strings.TrimSpace(target) == "" || strings.ContainsAny(target, "\x00\r\n") {
		return Reference{}, fmt.Errorf("canonical non-secret target identity required")
	}
	if len(snapshots) == 0 {
		return Reference{}, fmt.Errorf("at least one source snapshot is required")
	}
	var snapshotDatasets []string
	for _, snapshot := range snapshots {
		name, _, _ := strings.Cut(snapshot, "@")
		snapshotDatasets = append(snapshotDatasets, name)
	}
	state, err := s.proofScope(ctx, dataset, snapshotDatasets)
	if err != nil {
		return Reference{}, err
	}
	if err := scopeReady(state, dataset); err != nil {
		return Reference{}, err
	}
	lineage, err := RootAuthority(state, dataset, s.installation)
	if err != nil {
		return Reference{}, err
	}
	if err := zfs.ValidateDataset(stream); err != nil {
		return Reference{}, err
	}
	var sources []ReferenceSource
	seenSnapshots := make(map[string]bool)
	for _, snapshot := range snapshots {
		if seenSnapshots[snapshot] {
			return Reference{}, fmt.Errorf("duplicate source snapshot")
		}
		seenSnapshots[snapshot] = true
		snapshotDataset, _, _ := strings.Cut(snapshot, "@")
		if snapshotDataset != stream && !strings.HasPrefix(snapshotDataset, stream+"/") {
			return Reference{}, fmt.Errorf("source snapshot %s is outside stream %s", snapshot, stream)
		}
		owned, metadata, findErr := findOwned(state, snapshotDataset, snapshot, lineage)
		if findErr != nil {
			return Reference{}, findErr
		}
		sources = append(sources, ReferenceSource{Dataset: snapshotDataset, GUID: owned.GUID, Metadata: metadata})
	}
	slices.SortFunc(sources, func(a, b ReferenceSource) int { return strings.Compare(a.Dataset, b.Dataset) })
	r := Reference{Target: target, Dataset: stream, Metadata: sources[0].Metadata, Sources: sources}
	if err := validSources(r, dataset); err != nil {
		return Reference{}, err
	}
	refs, err := referenceRecords(state, dataset, lineage)
	if err != nil {
		return Reference{}, err
	}
	exists := false
	for _, prior := range refs {
		if prior.property() == r.property() {
			if !reflect.DeepEqual(prior, r) && !covers(prior, r) {
				return Reference{}, fmt.Errorf("conflicting reference proof")
			}
			// The stored proof stands, including when it covers a whole
			// send -R subtree and this stream now asks for part of it.
			r, exists = prior, true
		}
	}
	// A stored proof may span more datasets than this stream asked for, and
	// all of their holds must be read before any is placed.
	scope := snapshotDatasets
	if exists {
		scope = sourceDatasets(r)
		if state, err = s.proofScope(ctx, dataset, scope); err != nil {
			return Reference{}, err
		}
		if refs, err = referenceRecords(state, dataset, lineage); err != nil {
			return Reference{}, err
		}
		if !containsReference(refs, r) {
			return Reference{}, &StateChangedError{}
		}
	}
	if state.Received[dataset][r.property()] != "" {
		return Reference{}, fmt.Errorf("received reference proof requires explicit resolution")
	}
	if !exists {
		for _, source := range r.sources() {
			if slices.Contains(state.Holds[r.sourceSnapshot(source)], r.hold()) {
				return Reference{}, fmt.Errorf("unproven existing target hold")
			}
		}
	}
	if err := s.proofUnchanged(ctx, dataset, scope, state); err != nil {
		return Reference{}, err
	}
	if !exists {
		value, err := json.Marshal(r)
		if err != nil {
			return Reference{}, err
		}
		if err := s.backend.SetProperties(ctx, dataset, map[string]string{r.property(): string(value)}); err != nil {
			return Reference{}, err
		}
	}
	for _, source := range r.sources() {
		snapshot := r.sourceSnapshot(source)
		if !slices.Contains(state.Holds[snapshot], r.hold()) {
			if err := backend.Hold(ctx, r.hold(), snapshot); err != nil {
				return Reference{}, err
			}
		}
	}
	return r, nil
}

// Checkpoint creates a versioned bookmark only after the caller supplies the
// destination snapshot GUID obtained by a successful receive verification.
// It deliberately retains the hold and previous checkpoints; the transfer layer
// releases obsolete references only once recovery no longer needs them.
func (s *Service) Checkpoint(ctx context.Context, dataset string, r Reference, verifiedGUID uint64) error {
	return s.CheckpointSet(ctx, dataset, r, map[string]uint64{r.snapshot(): verifiedGUID})
}

// CheckpointSet creates every versioned bookmark only after the caller supplies
// the verified destination GUID for every protected source snapshot.
func (s *Service) CheckpointSet(ctx context.Context, dataset string, r Reference, verifiedGUIDs map[string]uint64) error {
	unlock, err := s.lock(ctx, dataset)
	if err != nil {
		return err
	}
	defer unlock()
	backend, ok := s.backend.(referenceBackend)
	if !ok {
		return fmt.Errorf("reference operations unavailable")
	}
	if err := zfs.ValidateDataset(dataset); err != nil {
		return err
	}
	state, err := s.proofScope(ctx, dataset, sourceDatasets(r))
	if err != nil {
		return err
	}
	if err := scopeReady(state, dataset); err != nil {
		return err
	}
	lineage, err := RootAuthority(state, dataset, s.installation)
	if err != nil {
		return err
	}
	refs, err := referenceRecords(state, dataset, lineage)
	if err != nil {
		return err
	}
	if !containsReference(refs, r) {
		return fmt.Errorf("reference proof is missing or changed")
	}
	missing := make([]ReferenceSource, 0, len(r.sources()))
	for _, source := range r.sources() {
		snapshotName := r.sourceSnapshot(source)
		found := false
		for _, object := range state.Objects {
			if object.Name == r.sourceBookmark(source) {
				if object.Type != "bookmark" || object.GUID != source.GUID {
					return fmt.Errorf("conflicting bookmark")
				}
				found = true
			}
		}
		verified, supplied := verifiedGUIDs[snapshotName]
		if supplied && verified != source.GUID {
			return fmt.Errorf("destination snapshot GUID does not match source: %s", snapshotName)
		}
		// A checkpoint bookmark exists only once an earlier transfer verified
		// that source, so a stream inheriting a wider proof need not verify
		// again the sources it did not send.
		if !found && (!supplied || verified == 0) {
			return fmt.Errorf("destination snapshot GUID does not match source: %s", snapshotName)
		}
		owned, metadata, findErr := findOwned(state, source.Dataset, snapshotName, lineage)
		if findErr != nil {
			return findErr
		}
		if owned.GUID != source.GUID || metadata != source.Metadata || !slices.Contains(owned.Holds, r.hold()) {
			return fmt.Errorf("source GUID or protecting hold changed")
		}
		if !found {
			missing = append(missing, source)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	if err := s.proofUnchanged(ctx, dataset, sourceDatasets(r), state); err != nil {
		return err
	}
	for _, source := range missing {
		if err := backend.Bookmark(ctx, r.sourceSnapshot(source), r.sourceBookmark(source)); err != nil {
			return err
		}
	}
	return nil
}

// ReleaseReference removes only exactly proven references. Resume state blocks
// release. Callers must also establish that no active transfer needs the hold.
func (s *Service) ReleaseReference(ctx context.Context, dataset string, r Reference) error {
	unlock, err := s.lock(ctx, dataset)
	if err != nil {
		return err
	}
	defer unlock()
	backend, ok := s.backend.(referenceBackend)
	if !ok {
		return fmt.Errorf("reference operations unavailable")
	}
	if err := zfs.ValidateDataset(dataset); err != nil {
		return err
	}
	state, err := s.proofScope(ctx, dataset, sourceDatasets(r))
	if err != nil {
		return err
	}
	if err := scopeReady(state, dataset); err != nil {
		return err
	}
	lineage, err := RootAuthority(state, dataset, s.installation)
	if err != nil {
		return err
	}
	refs, err := referenceRecords(state, dataset, lineage)
	if err != nil {
		return err
	}
	if !containsReference(refs, r) {
		return fmt.Errorf("reference proof is missing or changed")
	}
	type sourceState struct {
		source      ReferenceSource
		hasBookmark bool
		hasHold     bool
	}
	var found []sourceState
	for _, source := range r.sources() {
		item := sourceState{source: source, hasHold: slices.Contains(state.Holds[r.sourceSnapshot(source)], r.hold())}
		for _, object := range state.Objects {
			if object.Name == r.sourceBookmark(source) {
				if object.Type != "bookmark" || object.GUID != source.GUID {
					return fmt.Errorf("bookmark GUID changed")
				}
				item.hasBookmark = true
			}
		}
		if item.hasHold {
			owned, metadata, findErr := findOwned(state, source.Dataset, r.sourceSnapshot(source), lineage)
			if findErr != nil || owned.GUID != source.GUID || metadata != source.Metadata {
				return fmt.Errorf("held snapshot ownership changed")
			}
		}
		found = append(found, item)
	}
	if err := s.proofUnchanged(ctx, dataset, sourceDatasets(r), state); err != nil {
		return err
	}
	for _, item := range found {
		if item.hasBookmark {
			if err := backend.DestroyBookmark(ctx, r.sourceBookmark(item.source)); err != nil {
				return err
			}
		}
		if item.hasHold {
			if err := backend.Release(ctx, r.hold(), r.sourceSnapshot(item.source)); err != nil {
				return err
			}
		}
	}
	if err := backend.InheritProperty(ctx, dataset, r.property()); err != nil {
		return err
	}
	return nil
}
