// Package integration holds the host-safe checks on how CI runs the guest
// suite. The guest tests themselves live in the subpackages, behind the
// integration build tag.
package integration

import (
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// TestCIShardsCoverEveryStage keeps CI's parallel shards equal to the whole
// suite: every stage the guest runner defines is run by exactly one shard. A
// shard runs unfiltered, so this is what makes the jobs together a verifying
// run rather than several partial ones.
func TestCIShardsCoverEveryStage(t *testing.T) {
	t.Parallel()
	listed, err := exec.CommandContext(t.Context(), "bash", filepath.Join("guest", "run-common.sh"), "--list-stages").Output()
	if err != nil {
		t.Fatal(err)
	}
	stages := strings.Fields(string(listed))
	workflow, err := os.ReadFile(filepath.Join("..", "..", ".github", "workflows", "quality.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var shards []string
	in := false
	for _, line := range strings.Split(string(workflow), "\n") {
		trimmed := strings.TrimSpace(line)
		switch {
		case trimmed == "shard:":
			in = true
		case in && strings.HasPrefix(trimmed, "- "):
			shards = append(shards, strings.TrimPrefix(trimmed, "- "))
		case in && trimmed != "" && !strings.HasPrefix(trimmed, "#"):
			in = false
		}
	}
	if len(shards) == 0 {
		t.Fatal("quality.yml defines no integration shards")
	}
	covered := map[string]int{}
	for _, shard := range shards {
		for _, stage := range strings.FieldsFunc(shard, func(r rune) bool { return r == ',' || r == ' ' }) {
			if !slices.Contains(stages, stage) {
				t.Errorf("shard %q names unknown stage %q; stages are %v", shard, stage, stages)
			}
			covered[stage]++
		}
	}
	for _, stage := range stages {
		if covered[stage] != 1 {
			t.Errorf("stage %s is run by %d shards, want exactly 1 (shards %v)", stage, covered[stage], shards)
		}
	}
}
