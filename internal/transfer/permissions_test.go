package transfer

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/pdf/boomerangz/internal/policy"
	"github.com/pdf/boomerangz/internal/zfs"
)

func TestDestinationPermissionsCoverEveryDestinationPreflight(t *testing.T) {
	effective := policy.Effective{SetProperties: map[string]string{"compression": "lz4", "org.example:note": "x"}, IgnoreProperties: []string{"atime"}}
	member := policy.Effective{IgnoreProperties: []string{"recordsize"}}
	got := DestinationPermissions(effective, member)
	plan := Plan{Receive: zfs.ReceiveOptions{Set: map[string]string{"canmount": "noauto", "compression": "lz4", "org.example:note": "x"}, Exclude: []string{"atime"}}}
	request := Request{Policy: effective, Members: []Member{{Dataset: "tank/data/child", Policy: member}}}
	for name, required := range map[string][]string{
		"receive":                destinationPermissions(plan),
		"placeholder":            placeholderPermissions,
		"reseed cleanup":         union(reseedCleanupPermissions, abortPermissions),
		"reseed fresh receive":   reseedReceivePermissions(request),
		"abort receive":          abortPermissions,
		"member receive options": {"recordsize"},
	} {
		for _, permission := range required {
			if !slices.Contains(got, permission) {
				t.Errorf("%s needs %s, which DestinationPermissions = %v leaves out", name, permission, got)
			}
		}
	}
	if slices.Contains(got, "org.example:note") {
		t.Errorf("DestinationPermissions = %v asks for a user property by name", got)
	}
	if !slices.IsSorted(got) || len(slices.Compact(slices.Clone(got))) != len(got) {
		t.Errorf("DestinationPermissions = %v is not sorted and unique", got)
	}
}

// The zfs allow commands the documentation tells operators to run must grant
// exactly what the preflights check: less, and a rare operation fails; more,
// and the documented delegation is broader than boomerangz needs.
func TestDocumentedDelegationsMatchPermissionSets(t *testing.T) {
	source, destination := strings.Join(SourcePermissions(), ","), strings.Join(DestinationPermissions(), ",")
	found := 0
	err := filepath.WalkDir(filepath.Join("..", "..", "docs"), func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() && entry.Name() == "node_modules" {
			return filepath.SkipDir
		}
		if entry.IsDir() || filepath.Ext(path) != ".md" {
			return nil
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		lines := strings.Split(string(content), "\n")
		for i, line := range lines {
			// The multi-line form is the full delegation; a one-line zfs allow
			// grants a single permission, as an upgrade note does.
			if !strings.Contains(line, "zfs allow -u ") || !strings.HasSuffix(strings.TrimSpace(line), `\`) || i+1 == len(lines) {
				continue
			}
			found++
			granted := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(lines[i+1]), `\`))
			want := destination
			if slices.Contains(strings.Split(granted, ","), "send") {
				want = source
			}
			if granted != want {
				t.Errorf("%s:%d grants %s, want %s", path, i+2, granted, want)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if found == 0 {
		t.Fatal("found no documented zfs allow commands")
	}
}
