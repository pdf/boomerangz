package lifecycle

import (
	"fmt"

	"github.com/pdf/boomerangz/internal/policy"
	"github.com/pdf/boomerangz/internal/zfs"
)

// ActiveRoot requires activation to be configured locally on root itself. An
// actor root has no enabled ancestor, and policy resolution inherits only
// locally configured public values, so a root's activation can only resolve from
// its own property; activation inherited from an ancestor makes a dataset a
// member of that ancestor's root. Application defaults and received values never
// authorize mutation.
//
// This is necessary, not sufficient: a descendant may also set enabled=on
// locally beneath an enabled ancestor. Only discovery sees the tree, and callers
// must also refuse an entry it reports as covered.
func ActiveRoot(effective policy.Effective, root string) error {
	if !effective.Enabled || !effective.Valid() {
		return fmt.Errorf("source policy must be enabled and valid")
	}
	source := effective.Values[policy.Namespace+"enabled"].Dataset
	if source == "" {
		return fmt.Errorf("source activation must come from locally configured policy")
	}
	if source != root {
		return fmt.Errorf("activation is inherited from %s, so %s is a member of that root rather than a root", source, root)
	}
	return nil
}

// OwnerProperty records the installation responsible for an exact source root.
// Received and inherited values are provenance, never administrative authority.
const OwnerProperty = policy.StateNamespace + "owner"

// RootAuthority validates the exact local root markers. Policy activation and
// operation-specific snapshot/reference consistency are additional mandatory
// checks at the caller; marker checks alone do not authorize an operation.
func RootAuthority(state zfs.State, root, installation string) (string, error) {
	if !ValidID(installation) {
		return "", fmt.Errorf("valid installation identity required")
	}
	if err := zfs.ValidateDataset(root); err != nil {
		return "", err
	}
	values := make(map[string]string)
	for _, p := range state.Properties {
		if p.Dataset != root || (p.Name != OwnerProperty && p.Name != LineageProperty) {
			continue
		}
		if p.Source != zfs.SourceLocal || !ValidID(p.Value) || values[p.Name] != "" {
			return "", fmt.Errorf("source root %s requires unambiguous explicitly local owner and lineage", root)
		}
		values[p.Name] = p.Value
	}
	if values[OwnerProperty] == "" || values[LineageProperty] == "" {
		return "", fmt.Errorf("source root %s lacks local authority; explicit adoption required", root)
	}
	if values[OwnerProperty] != installation {
		return "", fmt.Errorf("dormant foreign lineage on %s: owner %s does not match installation %s", root, values[OwnerProperty], installation)
	}
	for _, key := range []string{OwnerProperty, LineageProperty} {
		if hidden := state.Received[root][key]; hidden != "" && hidden != values[key] {
			return "", fmt.Errorf("conflicting received authority metadata on %s", root)
		}
	}
	return values[LineageProperty], nil
}

// ValidateLineageEvidence rejects conflicting ownership metadata in the exact
// operation scope. Foreign snapshots without internal metadata remain foreign
// and do not become recovery evidence merely because of their names.
func ValidateLineageEvidence(state zfs.State, root, lineage string) error {
	if !ValidID(lineage) {
		return fmt.Errorf("valid lineage required")
	}
	for _, snapshot := range snapshotsIn(state, root) {
		hasInternal := false
		for _, row := range snapshot.Properties {
			if row.Name == LineageProperty || row.Name == SnapshotProperty {
				hasInternal = true
			}
		}
		if hasInternal {
			if _, err := Ownership(snapshot, lineage); err != nil {
				return fmt.Errorf("inconsistent snapshot evidence on %s: %w", snapshot.Name, err)
			}
		}
	}
	if _, err := referenceRecords(state, root, lineage); err != nil {
		return err
	}
	return nil
}
