package cli

import (
	"bytes"
	"io"
	"testing"
)

func TestDatasetCleanCommandName(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"clean", "cleanup"} {
		var output bytes.Buffer
		args := []string{"dataset", "--config", "testdata/empty.toml", "--config-dir", t.TempDir(), name, "tank/data"}
		err := runWithReader(t.Context(), args, &output, io.Discard, BuildInfo{}, &cleanExecutor{})
		if (err == nil) != (name == "clean") {
			t.Fatalf("command %s: %v", name, err)
		}
	}
}
