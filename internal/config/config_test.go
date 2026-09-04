package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadMergesDropInsAndTracksProvenance(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	mainPath := filepath.Join(directory, "config.toml")
	dropInDir := filepath.Join(directory, "config.d")
	if err := os.Mkdir(dropInDir, 0o700); err != nil {
		t.Fatal(err)
	}
	writeTestFile(t, mainPath, `
[daemon]
management_workers = 3

[remotes.home]
transport = "ssh"
host = "backup.example.net"
root = "tank/backups"
`)
	lastPath := filepath.Join(dropInDir, "20-workers.toml")
	writeTestFile(t, lastPath, `
[daemon]
management_workers = 6

[remotes.home]
user = "replicator"
`)

	loaded, err := Load(mainPath, dropInDir)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Config.Daemon.ManagementWorkers != 6 {
		t.Fatalf("management workers = %d, want 6", loaded.Config.Daemon.ManagementWorkers)
	}
	if loaded.Config.Daemon.TransferWorkers != DefaultTransferWorkers {
		t.Fatalf("transfer workers = %d, want default %d", loaded.Config.Daemon.TransferWorkers, DefaultTransferWorkers)
	}
	if loaded.Config.Remotes["home"].Host != "backup.example.net" || loaded.Config.Remotes["home"].User != "replicator" {
		t.Fatalf("remote was not recursively merged: %#v", loaded.Config.Remotes["home"])
	}
	if got := loaded.Provenance["daemon.management_workers"]; got != lastPath {
		t.Fatalf("provenance = %q, want %q", got, lastPath)
	}
}

func TestLoadRejectsUnknownFields(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	mainPath := filepath.Join(directory, "config.toml")
	writeTestFile(t, mainPath, "[daemon]\nunknown = true\n")
	_, err := Load(mainPath, filepath.Join(directory, "missing"))
	if err == nil || !strings.Contains(err.Error(), "strict mode") {
		t.Fatalf("Load error = %v, want unknown field error", err)
	}
}

func TestDefaultsValidate(t *testing.T) {
	t.Parallel()
	config := Defaults()
	if err := config.Validate(); err != nil {
		t.Fatal(err)
	}
	if config.Daemon.ReconcileInterval.Duration != time.Minute {
		t.Fatalf("interval = %s, want 1m", config.Daemon.ReconcileInterval)
	}
}

func writeTestFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
}
