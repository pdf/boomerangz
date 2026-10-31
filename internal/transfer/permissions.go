package transfer

import (
	"slices"
	"strings"

	"github.com/pdf/boomerangz/internal/policy"
)

// The delegated ZFS permissions each operation's preflight requires, spelled
// once so the preflights, the daemon's destination check, and the documented
// zfs allow commands cannot drift apart. A receive also needs the permission
// for each native property a policy sets or excludes on receive; see
// propertyPermissions.
var (
	// sendPermissions are what a transfer needs on its source.
	sendPermissions = []string{"bookmark", "destroy", "hold", "mount", "release", "send", "snapshot", "userprop"}
	// receivePermissions are what a transfer needs on its destination anchor.
	receivePermissions = []string{"create", "destroy", "mount", "receive:append", "userprop"}
	// placeholderPermissions are what replacing a placeholder with its replica
	// needs on the placeholder's parent: the placeholder is renamed aside, the
	// replica received in its place, and the placeholder then destroyed.
	placeholderPermissions = []string{"create", "destroy", "mount", "rename"}
	// reseedSourcePermissions are what a reseed needs on its source, to
	// release recovery references and remove the target binding.
	reseedSourcePermissions = []string{"destroy", "release", "userprop"}
	// reseedCleanupPermissions are what a reseed needs on each destination
	// dataset it destroys.
	reseedCleanupPermissions = []string{"destroy", "mount"}
	// filesystemReceivePermissions are what a filesystem receive needs beyond
	// receivePermissions: Build sets canmount=noauto unless the policy sets
	// canmount itself. A transfer's preflight finds it among the plan's
	// receive properties; a reseed's, planned before any stream, asks for it
	// outright.
	filesystemReceivePermissions = []string{"canmount"}
	// abortPermissions are what discarding an interrupted receive needs on
	// the dataset that holds it.
	abortPermissions = []string{"destroy", "mount", "receive:append"}
)

// SourcePermissions returns every delegated permission boomerangz needs on a
// source dataset.
func SourcePermissions() []string {
	return union(sendPermissions, reseedSourcePermissions)
}

// DestinationPermissions returns every delegated permission boomerangz may
// need on a destination root for streams under the given policies: receives,
// placeholder replacement, reseed, and abort-receive, plus each native
// property the policies set or exclude on receive. Most transfers exercise
// only part of it; a missing permission surfaces only when the operation that
// needs it runs.
func DestinationPermissions(policies ...policy.Effective) []string {
	permissions := union(receivePermissions, filesystemReceivePermissions, placeholderPermissions, reseedCleanupPermissions, abortPermissions)
	for _, p := range policies {
		permissions = union(permissions, propertyPermissions(p.SetProperties, p.IgnoreProperties))
	}
	return permissions
}

// propertyPermissions are the permissions to set or exclude native properties
// on receive. User properties are covered by userprop.
func propertyPermissions(set map[string]string, exclude []string) []string {
	var permissions []string
	for property := range set {
		if !strings.Contains(property, ":") {
			permissions = append(permissions, property)
		}
	}
	for _, property := range exclude {
		if !strings.Contains(property, ":") {
			permissions = append(permissions, property)
		}
	}
	return union(permissions)
}

// union returns the sorted, deduplicated permissions of every set.
func union(sets ...[]string) []string {
	var result []string
	for _, set := range sets {
		result = append(result, set...)
	}
	slices.Sort(result)
	return slices.Compact(result)
}
