package cli

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDatasetCleanCommandName(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"clean", "cleanup"} {
		var output bytes.Buffer
		directory := t.TempDir()
		configPath := filepath.Join(directory, "config.toml")
		configBody := []byte("[paths]\nsocket_path = \"" + filepath.Join(directory, "control.sock") + "\"\n")
		if err := os.WriteFile(configPath, configBody, 0600); err != nil {
			t.Fatal(err)
		}
		args := []string{"dataset", "--config", configPath, "--config-dir", filepath.Join(directory, "config.d"), name, "tank/data"}
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

func TestSSHShellRequiresServerConfiguredRoots(t *testing.T) {
	t.Parallel()
	err := runWithReader(t.Context(), []string{"ssh-shell", "--config", "testdata/empty.toml", "--config-dir", t.TempDir()}, io.Discard, io.Discard, BuildInfo{}, nil)
	if err == nil || !strings.Contains(err.Error(), "ssh_shell.replication_roots") {
		t.Fatalf("ssh-shell error = %v, want missing server-side roots", err)
	}
}
