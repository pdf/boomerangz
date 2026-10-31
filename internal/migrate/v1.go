package migrate

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/pdf/boomerangz/internal/identity"
	"github.com/pdf/boomerangz/internal/zfs"
)

// Format v1 is the state written before roots carried a version marker.
// Formats v1 and v2 name every record, hold and bookmark alike; what changed
// is the values of two records:
//
//   - A recovery proof listed every snapshot of its endpoint with its GUID
//     and a copy of the proof's metadata. In v2 the endpoint is derived from
//     the holds and bookmarks those names already give, and the proof keeps
//     the stream relative to the root, its creation time, and whether it is
//     recursive, which a v1 proof was exactly when it listed more than its
//     stream's own snapshot.
//   - A target binding stored everything it was compared against. In v2 it
//     keeps the pool GUID, the mapped dataset, and the anchor and its GUID,
//     and the anchor only while it differs from the mapped dataset.
//
// Snapshots in v1 also carried state:created, which v2 reads from the
// snapshot name instead. It is left where it is: v2 never reads it, and the
// snapshots carrying it age out under retention.

// The names both formats share, frozen here.
const (
	v1Namespace      = "org.boomerangz:state:"
	v1ReferenceKey   = v1Namespace + "reference:"
	v1TargetKey      = v1Namespace + "target:"
	v1LineageKey     = v1Namespace + "lineage"
	v1SnapshotPrefix = "boomerangz-"
	v1NameTime       = "20060102T150405.000000000Z"
)

type v1Metadata struct {
	Lineage  string    `json:"lineage"`
	Snapshot string    `json:"snapshot"`
	Created  time.Time `json:"created"`
}

func (m v1Metadata) name() string {
	return v1SnapshotPrefix + m.Created.UTC().Format(v1NameTime) + "-" + m.Snapshot[:8]
}

type v1Source struct {
	Dataset  string     `json:"dataset"`
	GUID     uint64     `json:"guid"`
	Metadata v1Metadata `json:"metadata"`
}

type v1Reference struct {
	Target   string     `json:"target"`
	Dataset  string     `json:"dataset"`
	Metadata v1Metadata `json:"metadata"`
	Sources  []v1Source `json:"sources"`
}

type v1Binding struct {
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

type v2Reference struct {
	Stream    string    `json:"stream,omitempty"`
	Created   time.Time `json:"created"`
	Recursive bool      `json:"recursive,omitempty"`
}

type v2Binding struct {
	PoolGUID   uint64 `json:"pool_guid"`
	Mapped     string `json:"mapped"`
	AnchorGUID uint64 `json:"anchor_guid"`
	Anchor     string `json:"anchor,omitempty"`
}

func v1TargetID(target string) string {
	sum := sha256.Sum256([]byte(target))
	return hex.EncodeToString(sum[:])
}

func v1StreamID(dataset string) string {
	sum := sha256.Sum256([]byte(dataset))
	return hex.EncodeToString(sum[:16])
}

// strict decodes value into record, refusing unknown fields and trailing data,
// which is what tells one format's record from another's.
func strict(value string, record any) bool {
	decoder := json.NewDecoder(strings.NewReader(value))
	decoder.DisallowUnknownFields()
	if decoder.Decode(record) != nil || decoder.More() {
		return false
	}
	return true
}

func within(dataset, root string) bool {
	return dataset == root || strings.HasPrefix(dataset, root+"/")
}

func v1ToV2(state zfs.State, root string) ([]Action, []string) {
	var actions []Action
	var blockers []string
	lineage := local(state, root, v1LineageKey)
	objects := map[string]zfs.Object{}
	for _, object := range state.Objects {
		objects[object.Name] = object
	}
	for _, p := range state.Properties {
		// A received record is the sending installation's, and no format of
		// this root's.
		if p.Dataset != root || p.Source != zfs.SourceLocal {
			continue
		}
		var value string
		var err error
		switch {
		case strings.HasPrefix(p.Name, v1ReferenceKey):
			value, err = v1Proof(p, root, lineage, objects)
		case strings.HasPrefix(p.Name, v1TargetKey) && !strings.Contains(strings.TrimPrefix(p.Name, v1TargetKey), ":"):
			// The suspension marker, target:<id>:suspended, is the same in both.
			value, err = v1BindingRecord(p)
		default:
			continue
		}
		if err != nil {
			blockers = append(blockers, fmt.Sprintf("%s on %s: %v", p.Name, root, err))
		} else if value != "" {
			actions = append(actions, Action{Property: p.Name, Previous: p.Value, Value: value})
		}
	}
	slices.SortFunc(actions, func(a, b Action) int { return strings.Compare(a.Property, b.Property) })
	return actions, blockers
}

// v1Proof rewrites one v1 proof, or returns "" for one already in v2.
func v1Proof(p zfs.Property, root, lineage string, objects map[string]zfs.Object) (string, error) {
	var old v1Reference
	if !strict(p.Value, &old) {
		var current v2Reference
		if strict(p.Value, &current) {
			return "", nil
		}
		return "", fmt.Errorf("proof is in neither format v1 nor v2")
	}
	m := old.Metadata
	if lineage == "" || m.Lineage != lineage {
		return "", fmt.Errorf("proof is not of the root's lineage")
	}
	if old.Target == "" || !identity.Valid(m.Snapshot) || m.Created.IsZero() || zfs.ValidateDataset(old.Dataset) != nil || !within(old.Dataset, root) {
		return "", fmt.Errorf("proof is incomplete")
	}
	if p.Name != v1ReferenceKey+v1TargetID(old.Target)+":"+v1StreamID(old.Dataset)+":"+m.Snapshot {
		return "", fmt.Errorf("proof does not match its key")
	}
	if len(old.Sources) == 0 || old.Sources[0].Dataset != old.Dataset {
		return "", fmt.Errorf("proof does not lead with its stream's snapshot")
	}
	hold := v1SnapshotPrefix + v1TargetID(old.Target)
	for _, source := range old.Sources {
		if source.GUID == 0 || source.Metadata != m || !within(source.Dataset, old.Dataset) {
			return "", fmt.Errorf("source %s is not of the proof's endpoint", source.Dataset)
		}
		// v2 reads each GUID from the snapshot, or once it is pruned from its
		// bookmark, so both must still agree with what v1 recorded.
		for _, name := range []string{source.Dataset + "@" + m.name(), source.Dataset + "#" + hold + "-" + m.Snapshot} {
			if object, ok := objects[name]; ok && object.GUID != source.GUID {
				return "", fmt.Errorf("%s has GUID %d, and the proof recorded %d", name, object.GUID, source.GUID)
			}
		}
	}
	record := v2Reference{Created: m.Created.UTC(), Recursive: len(old.Sources) > 1}
	if old.Dataset != root {
		record.Stream = strings.TrimPrefix(old.Dataset, root+"/")
	}
	return encode(record)
}

// v1BindingRecord rewrites one v1 binding, or returns "" for one already in v2.
func v1BindingRecord(p zfs.Property) (string, error) {
	var old v1Binding
	if !strict(p.Value, &old) {
		var current v2Binding
		if strict(p.Value, &current) {
			return "", nil
		}
		return "", fmt.Errorf("binding is in neither format v1 nor v2")
	}
	if old.Version != 1 {
		return "", fmt.Errorf("binding version %d is not format v1", old.Version)
	}
	if p.Name != v1TargetKey+v1TargetID(old.CanonicalTarget) {
		return "", fmt.Errorf("binding does not match its key")
	}
	if old.PoolGUID == 0 || old.AnchorGUID == 0 || zfs.ValidateDataset(old.MappedDataset) != nil || zfs.ValidateDataset(old.Anchor) != nil || !within(old.MappedDataset, old.Anchor) {
		return "", fmt.Errorf("binding is incomplete")
	}
	record := v2Binding{PoolGUID: old.PoolGUID, Mapped: old.MappedDataset, AnchorGUID: old.AnchorGUID}
	if old.Anchor != old.MappedDataset {
		record.Anchor = old.Anchor
	}
	return encode(record)
}

func encode(record any) (string, error) {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(record); err != nil {
		return "", err
	}
	return strings.TrimSuffix(buffer.String(), "\n"), nil
}
