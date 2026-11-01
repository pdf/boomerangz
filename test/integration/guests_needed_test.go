package integration

import (
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestGuestSuiteNeeded pins which changes may skip CI's guest jobs. Only a
// change made entirely of documentation may: a wrong "false" silently drops
// the suite from a change that needed it, so anything else, an empty list
// included, must run it.
func TestGuestSuiteNeeded(t *testing.T) {
	t.Parallel()
	script := filepath.Join("..", "..", ".github", "scripts", "guest-suite-needed.sh")
	for _, tc := range []struct {
		name  string
		paths []string
		want  string
	}{
		{"site pages", []string{"docs/getting-started/installation.md", "docs/reference/files.md"}, "false"},
		{"site build inputs", []string{"docs/package.json", "docs/.vitepress/config.mts"}, "false"},
		{"markdown anywhere", []string{"README.md", "AGENTS.md", "design/integration-coverage.md", "test/integration/README.md"}, "false"},
		{"upgrade notes", []string{"docs/operations/upgrading.md"}, "false"},
		{"go code", []string{"internal/transfer/plan.go"}, "true"},
		{"documentation and code", []string{"docs/guide/remotes.md", "internal/daemon/runtime.go"}, "true"},
		{"code after documentation", []string{"README.md", "cmd/boomerangz/main.go"}, "true"},
		{"workflow", []string{".github/workflows/quality.yml"}, "true"},
		{"ci script", []string{".github/scripts/guest-suite-needed.sh"}, "true"},
		{"guest harness", []string{"test/integration/guest/run-common.sh"}, "true"},
		{"packaging", []string{"contrib/debian/postinst"}, "true"},
		{"markdown-looking directory", []string{"notes.md/run.sh"}, "true"},
		{"nothing listed", nil, "true"},
		{"blank lines only", []string{"", ""}, "true"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cmd := exec.CommandContext(t.Context(), "bash", script)
			cmd.Stdin = strings.NewReader(strings.Join(tc.paths, "\n") + "\n")
			out, err := cmd.Output()
			if err != nil {
				t.Fatal(err)
			}
			if got := strings.TrimSpace(string(out)); got != "guests="+tc.want {
				t.Errorf("paths %q: got %q, want guests=%s", tc.paths, got, tc.want)
			}
		})
	}
}
