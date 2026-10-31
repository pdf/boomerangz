package lifecycle

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/pdf/boomerangz/internal/policy"
	"github.com/pdf/boomerangz/internal/zfs"
)

// VersionProperty records the format of the state a source root keeps: its
// authority, recovery proofs and target bindings, and the ownership metadata
// on its subtree's snapshots. It is a format version, not a release: it
// changes only when what boomerangz stores does, and boomerangz migrate moves
// a root from one format to the next.
const VersionProperty = policy.StateNamespace + "version"

// CurrentVersion is the state format this build reads and writes.
const CurrentVersion = 2

// unversioned is the format of a root that has no version marker: the first
// format, which predates the marker. A root that keeps no state at all has no
// format, and takes CurrentVersion when its authority is first written.
const unversioned = 1

// StateVersion reads root's state format from its local marker. A root without
// one is in the first format; a received marker is the sending installation's
// and is not read.
func StateVersion(state zfs.State, root string) (int, error) {
	version := 0
	for _, p := range state.Properties {
		if p.Dataset != root || p.Name != VersionProperty || p.Source != zfs.SourceLocal {
			continue
		}
		parsed, err := strconv.Atoi(p.Value)
		if version != 0 || err != nil || parsed < 1 || strconv.Itoa(parsed) != p.Value {
			return 0, fmt.Errorf("state version marker on %s is invalid", root)
		}
		version = parsed
	}
	if version == 0 {
		return unversioned, nil
	}
	return version, nil
}

// CurrentState refuses a root whose state is in a format this build does not
// read.
func CurrentState(state zfs.State, root string) error {
	version, err := StateVersion(state, root)
	if err != nil {
		return err
	}
	switch {
	case version < CurrentVersion:
		return fmt.Errorf("state on %s is format v%d, and this build reads v%d; stop the daemon and run boomerangz migrate %s", root, version, CurrentVersion, root)
	case version > CurrentVersion:
		return fmt.Errorf("state on %s is format v%d, newer than the v%d this build reads; upgrade boomerangz", root, version, CurrentVersion)
	}
	return nil
}

// keepsRecords reports whether root holds recovery proofs or target bindings
// of its own: the state whose format a version marker vouches for.
func keepsRecords(state zfs.State, root string) bool {
	for _, p := range state.Properties {
		if p.Dataset == root && p.Source == zfs.SourceLocal && (strings.HasPrefix(p.Name, ReferencePrefix) || strings.HasPrefix(p.Name, TargetStatePrefix)) {
			return true
		}
	}
	return false
}

// recordsReadable refuses records of root's own that are in a format this
// build does not read. A root without records has nothing to misread.
func recordsReadable(state zfs.State, root string) error {
	if !keepsRecords(state, root) {
		return nil
	}
	return CurrentState(state, root)
}

// authorityProperties are the properties that first make dataset a source
// root: its owner, its lineage, and the format its state is written in.
func (s *Service) authorityProperties(lineage string) map[string]string {
	return map[string]string{LineageProperty: lineage, OwnerProperty: s.installation, VersionProperty: strconv.Itoa(CurrentVersion)}
}
