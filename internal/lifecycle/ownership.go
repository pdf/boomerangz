// Package lifecycle plans ownership-checked snapshot lifecycle operations.
package lifecycle

import (
	"fmt"
	"strings"
	"time"

	"github.com/pdf/boomerangz/internal/identity"
	"github.com/pdf/boomerangz/internal/policy"
	"github.com/pdf/boomerangz/internal/zfs"
)

// SnapshotPrefix and metadata keys define the ownership contract.
const (
	SnapshotPrefix   = "boomerangz-"
	LineageProperty  = policy.StateNamespace + "lineage"
	SnapshotProperty = policy.StateNamespace + "snapshot"
	CreatedProperty  = policy.StateNamespace + "created"
	nameTimeFormat   = "20060102T150405.000000000Z"
)

// NewID creates an RFC 9562 version 4 UUID from cryptographic randomness.
func NewID() (string, error) { return identity.New() }

// ValidID accepts canonical UUIDs used as lineage and snapshot identities.
func ValidID(value string) bool { return identity.Valid(value) }

// Metadata is explicitly stored snapshot ownership information.
type Metadata struct {
	Lineage  string    `json:"lineage"`
	Snapshot string    `json:"snapshot"`
	Created  time.Time `json:"created"`
}

// NewMetadata validates the lineage and creates a fresh snapshot identity.
func NewMetadata(lineage string, now time.Time) (Metadata, error) {
	if !ValidID(lineage) || now.IsZero() || now.Year() < 1 || now.Year() > 9999 {
		return Metadata{}, fmt.Errorf("valid lineage and creation time are required")
	}
	id, err := NewID()
	if err != nil {
		return Metadata{}, err
	}
	return Metadata{Lineage: lineage, Snapshot: id, Created: now.UTC()}, nil
}

// Name is the snapshot component, suitable for recursive snapshot creation.
func (m Metadata) Name() string {
	if !ValidID(m.Snapshot) {
		return ""
	}
	return SnapshotPrefix + m.Created.UTC().Format(nameTimeFormat) + "-" + m.Snapshot[:8]
}

// Properties returns a fresh map of metadata to attach at snapshot creation.
func (m Metadata) Properties() map[string]string {
	return map[string]string{LineageProperty: m.Lineage, SnapshotProperty: m.Snapshot, CreatedProperty: m.Created.UTC().Format(time.RFC3339Nano)}
}

// Snapshot is the operational inventory needed for safe pruning. Properties
// must be explicitly stored snapshot properties, not inherited dataset values.
type Snapshot struct {
	Name           string         `json:"name"`
	GUID           uint64         `json:"guid"`
	Properties     []zfs.Property `json:"properties"`
	Holds          []string       `json:"holds,omitempty"`
	Clones         []string       `json:"clones,omitempty"`
	ResumeRequired bool           `json:"resume_required"`
}

// Ownership verifies the complete name/metadata/lineage contract. A nonzero GUID
// is required so callers can revalidate the exact object before mutation.
func Ownership(snapshot Snapshot, lineage string) (Metadata, error) {
	if !ValidID(lineage) || snapshot.GUID == 0 {
		return Metadata{}, fmt.Errorf("valid lineage and snapshot GUID are required")
	}
	dataset, component, exists := strings.Cut(snapshot.Name, "@")
	if !exists || zfs.ValidateDataset(dataset) != nil || !strings.HasPrefix(component, SnapshotPrefix) {
		return Metadata{}, fmt.Errorf("snapshot name does not establish ownership")
	}
	values := make(map[string]string)
	for _, property := range snapshot.Properties {
		if property.Name != LineageProperty && property.Name != SnapshotProperty && property.Name != CreatedProperty {
			continue
		}
		if property.Dataset != snapshot.Name || (property.Source != zfs.SourceLocal && property.Source != zfs.SourceReceived) {
			return Metadata{}, fmt.Errorf("ownership metadata is not stored on snapshot")
		}
		if _, duplicate := values[property.Name]; duplicate {
			return Metadata{}, fmt.Errorf("ambiguous ownership metadata")
		}
		values[property.Name] = property.Value
	}
	m := Metadata{Lineage: values[LineageProperty], Snapshot: values[SnapshotProperty]}
	if m.Lineage != lineage || !ValidID(m.Snapshot) {
		return Metadata{}, fmt.Errorf("missing or mismatched ownership UUIDs")
	}
	var err error
	m.Created, err = time.Parse(time.RFC3339Nano, values[CreatedProperty])
	if err != nil || m.Created.IsZero() || m.Created.Year() < 1 || m.Created.Year() > 9999 {
		return Metadata{}, fmt.Errorf("invalid ownership creation time")
	}
	m.Created = m.Created.UTC()
	if component != m.Name() {
		return Metadata{}, fmt.Errorf("snapshot name does not match its metadata")
	}
	return m, nil
}

// Adopt identifies one lineage from fully proven owned snapshots. The caller
// must require a missing dataset lineage and revalidate before writing it.
func Adopt(snapshots []Snapshot) (string, error) {
	lineage := ""
	for _, snapshot := range snapshots {
		for _, property := range snapshot.Properties {
			if property.Name != LineageProperty {
				continue
			}
			if _, err := Ownership(snapshot, property.Value); err != nil {
				continue
			}
			if lineage != "" && lineage != property.Value {
				return "", fmt.Errorf("multiple candidate lineages; explicit resolution is required")
			}
			lineage = property.Value
		}
	}
	if lineage == "" {
		return "", fmt.Errorf("no provable snapshot lineage found")
	}
	return lineage, nil
}
