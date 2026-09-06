package config

import (
	"os"
	"path/filepath"
	"runtime"
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
	if loaded.Config.Daemon.LocalTransferWorkers != DefaultLocalTransferWorkers {
		t.Fatalf("transfer workers = %d, want default %d", loaded.Config.Daemon.LocalTransferWorkers, DefaultLocalTransferWorkers)
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
	if config.Daemon.InactiveGracePeriod.Duration != 24*time.Hour {
		t.Fatalf("inactive grace period = %s, want 24h", config.Daemon.InactiveGracePeriod)
	}
}

func TestRemoteEndpointModes(t *testing.T) {
	t.Parallel()
	for _, endpoint := range []string{"", "auto", "direct", "ssh-shell"} {
		cfg := Defaults()
		cfg.Remotes["home"] = RemoteConfig{Transport: "ssh", Endpoint: endpoint, Host: "backup.example.net", Root: "tank/backups"}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("endpoint %q rejected: %v", endpoint, err)
		}
	}
	cfg := Defaults()
	cfg.Remotes["home"] = RemoteConfig{Transport: "ssh", Endpoint: "shell-command", Host: "backup.example.net", Root: "tank/backups"}
	if err := cfg.Validate(); err == nil {
		t.Fatal("accepted unknown remote endpoint mode")
	}
}

func TestWorkerSizing(t *testing.T) {
	t.Parallel()
	cfg := Defaults()
	if cfg.Daemon.ManagementWorkers != 0 || cfg.Daemon.EffectiveManagementWorkers() != runtime.NumCPU() {
		t.Fatal("incorrect automatic management sizing")
	}
	if cfg.Daemon.LocalTransferWorkers != 2 || cfg.Daemon.RemoteTransferWorkers != 1 {
		t.Fatal("incorrect transfer defaults")
	}
	cfg.Daemon.ManagementWorkers = 3
	cfg.Daemon.LocalTransferWorkers = 1
	cfg.Daemon.RemoteTransferWorkers = 1
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	if cfg.Daemon.EffectiveManagementWorkers() != 3 {
		t.Fatal("explicit worker limit ignored")
	}
	for _, field := range []string{"management", "local", "remote"} {
		invalid := cfg
		switch field {
		case "management":
			invalid.Daemon.ManagementWorkers = -1
		case "local":
			invalid.Daemon.LocalTransferWorkers = 0
		case "remote":
			invalid.Daemon.RemoteTransferWorkers = 0
		}
		if err := invalid.Validate(); err == nil {
			t.Fatalf("accepted invalid %s limit", field)
		}
	}
	invalid := cfg
	invalid.Daemon.InactiveGracePeriod.Duration = -time.Second
	if err := invalid.Validate(); err == nil {
		t.Fatal("accepted negative inactive grace period")
	}
}

func TestWorkerSchemaMigration(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")
	writeTestFile(t, path, "[daemon]\nmanagement_workers=0\nlocal_transfer_workers=1\nremote_transfer_workers=1\n")
	loaded, err := Load(path, filepath.Join(dir, "missing"))
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Config.Daemon.LocalTransferWorkers != 1 || loaded.Config.Daemon.RemoteTransferWorkers != 1 {
		t.Fatal("transfer limits not loaded")
	}
	writeTestFile(t, path, "[daemon]\ntransfer_workers=1\n")
	if _, err := Load(path, filepath.Join(dir, "missing")); err == nil {
		t.Fatal("accepted removed transfer_workers field")
	}
}

func TestMTLSListenerRequiresClientCA(t *testing.T) {
	t.Parallel()
	cfg := Defaults()
	cfg.Listeners["remote"] = ListenerConfig{Network: "tcp", Address: "127.0.0.1:8443", AuthMode: "mtls", TLSCert: "/cert", TLSKey: "/key"}
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "client_ca") {
		t.Fatalf("validation error=%v", err)
	}
	cfg.Listeners["remote"] = ListenerConfig{Network: "tcp", Address: "127.0.0.1:8443", AuthMode: "mtls", TLSCert: "/cert", TLSKey: "/key", ClientCA: "/ca"}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
}

func writeTestFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
}
