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
	"time"

	"github.com/pdf/boomerangz/internal/policy"
	"github.com/pdf/boomerangz/internal/zfs"
)

// ReferencePrefix identifies per-target, per-snapshot recovery proofs. Keep
// old records until their corresponding hold and bookmark have been removed.
const ReferencePrefix = policy.StateNamespace + "reference:"

// Reference proves one stream's endpoint on one target. Dataset is the stream's
// source: a member of the actor root, or the root itself. A recursive stream
// carries its whole subtree with send -R, so its endpoint is a snapshot of the
// same instant on the stream and on each descendant that has one.
//
// The record lives on the actor root, keyed by target, stream and snapshot
// UUID. Members keep their own cadences, so two members' newest snapshots are
// usually different instants; keying by stream lets each member's proof
// advance on its own.
//
// The record stores only what ZFS cannot say: where the stream is relative to
// the root, when the snapshot was taken, which the snapshot name needs and
// which a bookmark does not carry, and whether the stream is recursive. The
// lineage is the root's own, and TargetID and the snapshot UUID are in the
// key. Which snapshots the endpoint consists of, and their GUIDs, are read
// from ZFS: see Sources. The record is therefore the same size however many
// datasets a recursive stream carries.
type Reference struct {
	TargetID  string   `json:"target_id"`
	Dataset   string   `json:"dataset"`
	Metadata  Metadata `json:"metadata"`
	Recursive bool     `json:"recursive,omitempty"`
}

// referenceRecord is the stored value of a Reference. Stream is relative to
// the root, and empty for the root's own stream.
type referenceRecord struct {
	Stream    string    `json:"stream,omitempty"`
	Created   time.Time `json:"created"`
	Recursive bool      `json:"recursive,omitempty"`
}

// ReferenceSource is one snapshot of a stream's endpoint, named by the
// reference's Metadata.
type ReferenceSource struct {
	Dataset string `json:"dataset"`
	GUID    uint64 `json:"guid"`
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
	return ReferencePrefix + r.TargetID + ":" + streamID(r.Dataset) + ":" + r.Metadata.Snapshot
}
func (r Reference) hold() string { return SnapshotPrefix + r.TargetID }
func (r Reference) bookmark() string {
	return r.Dataset + "#" + r.hold() + "-" + r.Metadata.Snapshot
}
func (r Reference) snapshot() string { return r.Dataset + "@" + r.Metadata.Name() }

func (r Reference) sourceSnapshot(source ReferenceSource) string {
	return source.Dataset + "@" + r.Metadata.Name()
}

func (r Reference) sourceBookmark(source ReferenceSource) string {
	return source.Dataset + "#" + r.hold() + "-" + r.Metadata.Snapshot
}

// subtree is the stream whose descendants the proof can involve, or empty
// when it involves the stream alone.
func (r Reference) subtree() string {
	if r.Recursive {
		return r.Dataset
	}
	return ""
}

// Sources derives the snapshots the endpoint consists of from state, which
// must cover the stream and, for a recursive proof, its descendants. A
// snapshot belongs to the endpoint while it carries the target's hold, and
// afterwards while its checkpoint bookmark remains: a proof is written before
// any hold, and released only once both are gone. A source's GUID is its
// snapshot's, or once the snapshot is pruned its bookmark's, which keeps the
// snapshot's GUID; where both exist they must agree. Sources are sorted by
// dataset, so the stream's own comes first.
func (r Reference) Sources(state zfs.State) ([]ReferenceSource, error) {
	objects := make(map[string]zfs.Object, len(state.Objects))
	var candidates []string
	for _, object := range state.Objects {
		objects[object.Name] = object
		if (object.Type == "filesystem" || object.Type == "volume") && (object.Name == r.Dataset || (r.Recursive && strings.HasPrefix(object.Name, r.Dataset+"/"))) {
			candidates = append(candidates, object.Name)
		}
	}
	slices.Sort(candidates)
	var sources []ReferenceSource
	for _, dataset := range slices.Compact(candidates) {
		source := ReferenceSource{Dataset: dataset}
		snapshot, hasSnapshot := objects[r.sourceSnapshot(source)]
		bookmark, hasBookmark := objects[r.sourceBookmark(source)]
		held := slices.Contains(state.Holds[r.sourceSnapshot(source)], r.hold())
		if !held && !hasBookmark {
			continue
		}
		if hasBookmark && (bookmark.Type != "bookmark" || bookmark.GUID == 0 || (hasSnapshot && bookmark.GUID != snapshot.GUID)) {
			return nil, fmt.Errorf("checkpoint bookmark %s does not match its snapshot", bookmark.Name)
		}
		if hasSnapshot {
			source.GUID = snapshot.GUID
		} else {
			source.GUID = bookmark.GUID
		}
		if source.GUID == 0 {
			return nil, fmt.Errorf("source %s has no GUID", dataset)
		}
		sources = append(sources, source)
	}
	return sources, nil
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
	// A proof in an older format is not misread as an invalid one.
	if err := recordsReadable(state, dataset); err != nil {
		return nil, err
	}
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
		r, err := decodeReference(p, dataset, lineage)
		if err != nil || seen[p.Name] {
			return nil, fmt.Errorf("invalid reference proof %s", p.Name)
		}
		seen[p.Name] = true
		refs = append(refs, r)
	}
	return refs, nil
}

// decodeReference reads one stored proof on root. The key names the target,
// the stream and the snapshot UUID; the value must name the same stream.
func decodeReference(p zfs.Property, root, lineage string) (Reference, error) {
	parts := strings.Split(strings.TrimPrefix(p.Name, ReferencePrefix), ":")
	if !ValidID(lineage) || len(parts) != 3 || len(parts[0]) != sha256.Size*2 || !ValidID(parts[2]) {
		return Reference{}, fmt.Errorf("invalid reference key")
	}
	if _, err := hex.DecodeString(parts[0]); err != nil {
		return Reference{}, err
	}
	var record referenceRecord
	decoder := json.NewDecoder(strings.NewReader(p.Value))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&record); err != nil || decoder.More() {
		return Reference{}, fmt.Errorf("invalid reference record")
	}
	stream := root
	if record.Stream != "" {
		stream = root + "/" + record.Stream
	}
	created := record.Created.UTC()
	if zfs.ValidateDataset(stream) != nil || created.IsZero() || created.Year() < 1 || created.Year() > 9999 {
		return Reference{}, fmt.Errorf("invalid reference record")
	}
	r := Reference{TargetID: parts[0], Dataset: stream, Metadata: Metadata{Lineage: lineage, Snapshot: parts[2], Created: created}, Recursive: record.Recursive}
	if r.property() != p.Name {
		return Reference{}, fmt.Errorf("reference record names another stream")
	}
	return r, nil
}

func encodeReference(r Reference, root string) (string, error) {
	record := referenceRecord{Created: r.Metadata.Created.UTC(), Recursive: r.Recursive}
	if r.Dataset != root {
		record.Stream = strings.TrimPrefix(r.Dataset, root+"/")
	}
	value, err := json.Marshal(record)
	return string(value), err
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

// proofScope inspects what a recovery proof involves: the actor root, which
// holds authority and every proof, each named dataset that still exists, and,
// when subtree is set, that stream's whole subtree. Nothing else beneath the
// root bears on a proof, and reading the whole subtree would cost every
// member's snapshots for each stream a root sends. A dataset that is gone
// contributes nothing, so a proof for it can still be released.
func (s *Service) proofScope(ctx context.Context, root string, datasets []string, subtree string) (zfs.State, error) {
	state, err := s.backend.InspectState(ctx, root, root == subtree)
	if err != nil {
		return zfs.State{}, err
	}
	var wanted []string
	for _, dataset := range datasets {
		if dataset != root && !slices.Contains(wanted, dataset) && (subtree == "" || !inside(dataset, subtree)) {
			wanted = append(wanted, dataset)
		}
	}
	if subtree != "" && subtree != root {
		wanted = append(wanted, subtree)
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
		member, err := s.backend.InspectState(ctx, dataset, dataset == subtree)
		if err != nil {
			return zfs.State{}, err
		}
		state = zfs.MergeStates(state, member)
	}
	return state, nil
}

func hasObject(state zfs.State, name string) bool {
	return slices.ContainsFunc(state.Objects, func(object zfs.Object) bool { return object.Name == name })
}

func inside(dataset, root string) bool {
	return dataset == root || strings.HasPrefix(dataset, root+"/")
}

// proofUnchanged re-reads proofScope and requires it to equal expected.
func (s *Service) proofUnchanged(ctx context.Context, root string, datasets []string, subtree string, expected zfs.State) error {
	current, err := s.proofScope(ctx, root, datasets, subtree)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(expected, current) {
		return &StateChangedError{}
	}
	return nil
}

// referenceState inspects what one stored proof involves and requires the
// proof to be stored there unchanged, under the root's authority.
func (s *Service) referenceState(ctx context.Context, dataset string, r Reference) (zfs.State, string, []ReferenceSource, error) {
	if err := zfs.ValidateDataset(dataset); err != nil {
		return zfs.State{}, "", nil, err
	}
	state, err := s.proofScope(ctx, dataset, []string{r.Dataset}, r.subtree())
	if err != nil {
		return zfs.State{}, "", nil, err
	}
	if err := scopeReady(state, dataset); err != nil {
		return zfs.State{}, "", nil, err
	}
	lineage, err := RootAuthority(state, dataset, s.installation)
	if err != nil {
		return zfs.State{}, "", nil, err
	}
	refs, err := referenceRecords(state, dataset, lineage)
	if err != nil {
		return zfs.State{}, "", nil, err
	}
	if !slices.Contains(refs, r) {
		return zfs.State{}, "", nil, fmt.Errorf("reference proof is missing or changed")
	}
	sources, err := r.Sources(state)
	if err != nil {
		return zfs.State{}, "", nil, err
	}
	return state, lineage, sources, nil
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
//
// The proof is written before any hold, so a hold under the target's tag is
// always backed by one. A stored proof for the same snapshot stands: a
// recursive proof covers a later stream that asks for part of its subtree, as
// when a root that sent its whole subtree with send -R later sends its members
// one at a time, and only the snapshots asked for are held again.
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
	if err := zfs.ValidateDataset(stream); err != nil {
		return Reference{}, err
	}
	if !inside(stream, dataset) {
		return Reference{}, fmt.Errorf("stream %s is outside root %s", stream, dataset)
	}
	var snapshotDatasets []string
	for _, snapshot := range snapshots {
		name, _, _ := strings.Cut(snapshot, "@")
		snapshotDatasets = append(snapshotDatasets, name)
	}
	state, err := s.proofScope(ctx, dataset, snapshotDatasets, "")
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
	var instant Metadata
	recursive, own := false, false
	seenSnapshots := make(map[string]bool)
	for i, snapshot := range snapshots {
		if seenSnapshots[snapshot] {
			return Reference{}, fmt.Errorf("duplicate source snapshot")
		}
		seenSnapshots[snapshot] = true
		snapshotDataset, _, _ := strings.Cut(snapshot, "@")
		if !inside(snapshotDataset, stream) {
			return Reference{}, fmt.Errorf("source snapshot %s is outside stream %s", snapshot, stream)
		}
		if _, metadata, findErr := findOwned(state, snapshotDataset, snapshot, lineage); findErr != nil {
			return Reference{}, findErr
		} else if i == 0 {
			instant = metadata
		} else if metadata != instant {
			return Reference{}, fmt.Errorf("source snapshot %s is not of the stream's instant", snapshot)
		}
		own = own || snapshotDataset == stream
		recursive = recursive || snapshotDataset != stream
	}
	if !own {
		return Reference{}, fmt.Errorf("stream snapshot must be among its sources")
	}
	r := Reference{TargetID: TargetID(target), Dataset: stream, Metadata: instant, Recursive: recursive}
	refs, err := referenceRecords(state, dataset, lineage)
	if err != nil {
		return Reference{}, err
	}
	exists := false
	for _, prior := range refs {
		if prior.property() != r.property() {
			continue
		}
		if prior != r && (prior.Metadata != r.Metadata || prior.Dataset != r.Dataset || !prior.Recursive) {
			return Reference{}, fmt.Errorf("conflicting reference proof")
		}
		r, exists = prior, true
	}
	if !exists {
		for _, snapshot := range snapshots {
			if slices.Contains(state.Holds[snapshot], r.hold()) {
				return Reference{}, fmt.Errorf("unproven existing target hold")
			}
		}
	}
	if err := s.proofUnchanged(ctx, dataset, snapshotDatasets, "", state); err != nil {
		return Reference{}, err
	}
	if !exists {
		value, err := encodeReference(r, dataset)
		if err != nil {
			return Reference{}, err
		}
		// The record no longer grows with the stream, but ZFS still bounds it.
		if len(value) > zfs.MaxUserPropertyValue {
			return Reference{}, fmt.Errorf("recovery proof for stream %s needs %d bytes, over the ZFS property limit of %d", stream, len(value), zfs.MaxUserPropertyValue)
		}
		if err := s.backend.SetProperties(ctx, dataset, map[string]string{r.property(): value}); err != nil {
			return Reference{}, err
		}
	}
	for _, snapshot := range snapshots {
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
	state, lineage, sources, err := s.referenceState(ctx, dataset, r)
	if err != nil {
		return err
	}
	if len(sources) == 0 || sources[0].Dataset != r.Dataset {
		return fmt.Errorf("stream snapshot is not protected: %s", r.snapshot())
	}
	for snapshot := range verifiedGUIDs {
		if !slices.ContainsFunc(sources, func(source ReferenceSource) bool { return r.sourceSnapshot(source) == snapshot }) {
			return fmt.Errorf("reference proof does not protect %s", snapshot)
		}
	}
	var missing []ReferenceSource
	for _, source := range sources {
		snapshotName := r.sourceSnapshot(source)
		verified, supplied := verifiedGUIDs[snapshotName]
		if supplied && verified != source.GUID {
			return fmt.Errorf("destination snapshot GUID does not match source: %s", snapshotName)
		}
		// A checkpoint bookmark exists only once an earlier transfer verified
		// that source, so a stream inheriting a wider proof need not verify
		// again the sources it did not send.
		if hasObject(state, r.sourceBookmark(source)) {
			continue
		}
		if !supplied || verified == 0 {
			return fmt.Errorf("destination snapshot GUID does not match source: %s", snapshotName)
		}
		owned, metadata, findErr := findOwned(state, source.Dataset, snapshotName, lineage)
		if findErr != nil {
			return findErr
		}
		if owned.GUID != source.GUID || metadata != r.Metadata || !slices.Contains(owned.Holds, r.hold()) {
			return fmt.Errorf("source GUID or protecting hold changed")
		}
		missing = append(missing, source)
	}
	if len(missing) == 0 {
		return nil
	}
	if err := s.proofUnchanged(ctx, dataset, []string{r.Dataset}, r.subtree(), state); err != nil {
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
	state, lineage, sources, err := s.referenceState(ctx, dataset, r)
	if err != nil {
		return err
	}
	type sourceState struct {
		source      ReferenceSource
		hasBookmark bool
		hasHold     bool
	}
	var found []sourceState
	for _, source := range sources {
		hasBookmark := hasObject(state, r.sourceBookmark(source))
		item := sourceState{source: source, hasBookmark: hasBookmark, hasHold: slices.Contains(state.Holds[r.sourceSnapshot(source)], r.hold())}
		if item.hasHold {
			owned, metadata, findErr := findOwned(state, source.Dataset, r.sourceSnapshot(source), lineage)
			if findErr != nil || owned.GUID != source.GUID || metadata != r.Metadata {
				return fmt.Errorf("held snapshot ownership changed")
			}
		}
		found = append(found, item)
	}
	if err := s.proofUnchanged(ctx, dataset, []string{r.Dataset}, r.subtree(), state); err != nil {
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
