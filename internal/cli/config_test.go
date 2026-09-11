package cli

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// config check and config show read configuration files and nothing else, so
// they carry no behaviour a real pool could contradict. Their coverage lives
// here rather than in the integration suite.

func writeConfig(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestConfigCheckCountsEverySourceFile(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	dropInDir := filepath.Join(root, "config.d")
	if err := os.Mkdir(dropInDir, 0o700); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(root, "config.toml")
	writeConfig(t, configPath, "[daemon]\nmanagement_workers = 3\n")
	writeConfig(t, filepath.Join(dropInDir, "10-workers.toml"), "[daemon]\nmanagement_workers = 4\n")
	writeConfig(t, filepath.Join(dropInDir, "20-remote.toml"),
		"[remotes.home]\ntransport = \"ssh\"\nhost = \"backup.example.net\"\nroot = \"tank/backups\"\n")
	// A non-TOML file in the drop-in directory is not a source and must not be
	// counted, which is the only thing the reported number is good for.
	writeConfig(t, filepath.Join(dropInDir, "notes.txt"), "ignored\n")

	var output bytes.Buffer
	if err := run(t.Context(), []string{"config", "check", "--config", configPath, "--config-dir", dropInDir}, &output, io.Discard, BuildInfo{}); err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(output.String()); got != "configuration valid (3 source files)" {
		t.Fatalf("config check output=%q", got)
	}
}

func TestConfigCheckReportsDropInFailures(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name     string
		contents string
		want     string
	}{
		// An unknown field is refused but attributed to neither the drop-in
		// nor the key: the strict decode runs over the merged document, which
		// no longer carries the per-key provenance Load collected. Asserted as
		// it behaves rather than as it should read; see chunk E in
		// design/integration-coverage.md.
		{"unknown field", "[daemon]\nnot_a_setting = 1\n", "strict mode"},
		{"invalid value", "[remotes.home]\ntransport = \"carrier-pigeon\"\n", "transport"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			dropInDir := filepath.Join(root, "config.d")
			if err := os.Mkdir(dropInDir, 0o700); err != nil {
				t.Fatal(err)
			}
			configPath := filepath.Join(root, "config.toml")
			writeConfig(t, configPath, "[daemon]\nmanagement_workers = 3\n")
			dropIn := filepath.Join(dropInDir, "50-broken.toml")
			writeConfig(t, dropIn, test.contents)

			var output bytes.Buffer
			err := run(t.Context(), []string{"config", "check", "--config", configPath, "--config-dir", dropInDir}, &output, io.Discard, BuildInfo{})
			if err == nil {
				t.Fatalf("config check accepted %s: %s", test.name, output.String())
			}
			if !strings.Contains(err.Error(), test.want) {
				t.Fatalf("config check error does not name the problem: %v", err)
			}
			if output.Len() != 0 {
				t.Fatalf("config check reported validity as well as failing: %q", output.String())
			}
		})
	}
}

func TestConfigShowMergesAndRedactsListenerSecrets(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	dropInDir := filepath.Join(root, "config.d")
	if err := os.Mkdir(dropInDir, 0o700); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(root, "config.toml")
	writeConfig(t, configPath, `[listeners.remote]
network = "tcp"
address = "127.0.0.1:8443"
auth_mode = "mtls"
tls_cert = "/etc/boomerangz/tls/remote.crt"
tls_key = "/etc/boomerangz/tls/remote.key"
`)
	writeConfig(t, filepath.Join(dropInDir, "10-workers.toml"), "[daemon]\nmanagement_workers = 7\n")

	var output bytes.Buffer
	if err := run(t.Context(), []string{"config", "show", "--config", configPath, "--config-dir", dropInDir}, &output, io.Discard, BuildInfo{}); err != nil {
		t.Fatal(err)
	}
	shown := output.String()
	if strings.Contains(shown, "/etc/boomerangz/tls/remote.key") {
		t.Fatalf("config show disclosed the listener private key:\n%s", shown)
	}
	if !strings.Contains(shown, "tls_key = '<redacted>'") && !strings.Contains(shown, `tls_key = "<redacted>"`) {
		t.Fatalf("config show did not mark the private key redacted:\n%s", shown)
	}
	// Redaction must not cost the rest of the effective configuration: the
	// non-secret neighbour, the drop-in override, and an untouched default.
	for _, want := range []string{"/etc/boomerangz/tls/remote.crt", "management_workers = 7", "auth_mode = 'mtls'"} {
		if !strings.Contains(shown, want) {
			t.Fatalf("config show is missing %q:\n%s", want, shown)
		}
	}
}
