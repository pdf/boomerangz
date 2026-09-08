package cli

import (
	"bytes"
	"io"
	"strings"
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

func TestPairingIsTopLevelAndScopesAreEnumerated(t *testing.T) {
	t.Parallel()
	for _, args := range [][]string{{"auth", "pairing", "list"}, {"pairing", "create", "--scope", "unknown"}} {
		err := runWithReader(t.Context(), args, io.Discard, io.Discard, BuildInfo{}, nil)
		if err == nil {
			t.Fatalf("accepted invalid arguments %v", args)
		}
		if args[0] == "pairing" && (!strings.Contains(err.Error(), "status") || !strings.Contains(err.Error(), "admin")) {
			t.Fatalf("scope error does not state allowed values: %v", err)
		}
	}
}
