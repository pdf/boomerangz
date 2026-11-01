package zfs

import (
	"fmt"
	"strings"
	"unicode"
)

func validateDataset(name string) error {
	if name == "" || strings.HasPrefix(name, "/") || strings.HasPrefix(name, "-") || strings.ContainsAny(name, "@#") {
		return fmt.Errorf("invalid ZFS dataset name %q", name)
	}
	for _, part := range strings.Split(name, "/") {
		if part == "" || part == "." || part == ".." {
			return fmt.Errorf("invalid ZFS dataset name %q", name)
		}
	}
	for _, r := range name {
		if unicode.IsSpace(r) || unicode.IsControl(r) {
			return fmt.Errorf("invalid ZFS dataset name %q", name)
		}
	}
	return nil
}

// ValidateDataset rejects malformed names and command-option injection.
func ValidateDataset(name string) error { return validateDataset(name) }

// ValidateSnapshot accepts exactly one snapshot name: a valid dataset, one
// '@', and a component naming nothing else.
func ValidateSnapshot(name string) error { return validateSnapshot(name) }

func validateComponent(kind, name string) error {
	if name == "" || strings.ContainsAny(name, "/@#") {
		return fmt.Errorf("invalid %s %q", kind, name)
	}
	for _, r := range name {
		if unicode.IsSpace(r) || unicode.IsControl(r) {
			return fmt.Errorf("invalid %s %q", kind, name)
		}
	}
	return nil
}

func validateSnapshot(name string) error {
	dataset, snapshot, ok := strings.Cut(name, "@")
	// `zfs destroy` reads a comma as a list and a percent sign as a range, so
	// either would destroy more than the one snapshot named.
	if !ok || strings.ContainsAny(snapshot, "@,%") {
		return fmt.Errorf("invalid ZFS snapshot name %q", name)
	}
	if err := validateDataset(dataset); err != nil {
		return err
	}
	return validateComponent("snapshot", snapshot)
}

func validateBookmark(name string) error {
	dataset, bookmark, ok := strings.Cut(name, "#")
	if !ok || strings.Contains(bookmark, "#") {
		return fmt.Errorf("invalid ZFS bookmark name %q", name)
	}
	if err := validateDataset(dataset); err != nil {
		return err
	}
	return validateComponent("bookmark", bookmark)
}
