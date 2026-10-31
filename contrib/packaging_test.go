// Package contrib holds the host-safe checks on the packaging inputs. The
// inputs themselves are shell, systemd and package metadata, not Go.
package contrib

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// TestActionVersionsMatchUpgradeNotes keeps each package's upgrade reminder
// in step with docs/operations/upgrading.md: every version with a section
// there is one the Arch and Debian packages name when an upgrade crosses it,
// and neither names a version that has no notes to point at.
func TestActionVersionsMatchUpgradeNotes(t *testing.T) {
	t.Parallel()
	notes := readFile(t, filepath.Join("..", "docs", "operations", "upgrading.md"))
	var documented []string
	for _, match := range regexp.MustCompile(`(?m)^## v([0-9]+\.[0-9]+\.[0-9]+)$`).FindAllStringSubmatch(notes, -1) {
		documented = append(documented, match[1])
	}
	slices.Sort(documented)

	for _, source := range []struct {
		path    string
		pattern string
	}{
		{filepath.Join("arch", "boomerangz.install"), `(?m)^action_versions=\(([^)]*)\)$`},
		{filepath.Join("debian", "postinst"), `(?m)^action_versions="([^"]*)"$`},
	} {
		match := regexp.MustCompile(source.pattern).FindStringSubmatch(readFile(t, source.path))
		if match == nil {
			t.Errorf("%s defines no action_versions", source.path)
			continue
		}
		listed := strings.Fields(match[1])
		slices.Sort(listed)
		if !slices.Equal(listed, documented) {
			t.Errorf("%s action_versions = %v, want the versions upgrading.md has notes for: %v", source.path, listed, documented)
		}
	}
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(contents)
}
