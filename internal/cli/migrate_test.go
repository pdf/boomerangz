package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pdf/boomerangz/internal/config"
	"github.com/pdf/boomerangz/internal/lifecycle"
	"github.com/pdf/boomerangz/internal/zfs"
)

func TestMigrateCLIPreviewsBesideTheDaemonAndAppliesWithoutIt(t *testing.T) {
	t.Parallel()
	cfg := config.Defaults()
	cfg.Paths.SocketPath = filepath.Join(t.TempDir(), "control.sock")
	const owner = "11111111-1111-4111-8111-111111111111"
	e := &adoptionExecutor{properties: []zfs.Property{
		{Dataset: "tank", Name: lifecycle.OwnerProperty, Value: owner, Source: zfs.SourceLocal},
		{Dataset: "tank", Name: lifecycle.LineageProperty, Value: "22222222-2222-4222-8222-222222222222", Source: zfs.SourceLocal},
	}}
	version := func() string {
		for _, p := range e.properties {
			if p.Dataset == "tank" && p.Name == lifecycle.VersionProperty {
				return p.Value
			}
		}
		return ""
	}
	if err := runMigrate(t.Context(), &bytes.Buffer{}, cfg, e, []string{"tank"}, true, false); err == nil {
		t.Fatal("accepted names with --all")
	}
	if err := os.WriteFile(cfg.Paths.SocketPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	// --all finds only the root: backup keeps no state.
	if err := runMigrate(t.Context(), &out, cfg, e, nil, true, false); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), `"status": "pending"`) || strings.Contains(out.String(), `"backup"`) || version() != "" {
		t.Fatalf("preview=%s version=%q", out.String(), version())
	}
	if err := runMigrate(t.Context(), &bytes.Buffer{}, cfg, e, []string{"tank"}, false, true); err == nil || !strings.Contains(err.Error(), "stop the daemon") || version() != "" {
		t.Fatalf("applied beside the daemon: err=%v version=%q", err, version())
	}
	if err := os.Remove(cfg.Paths.SocketPath); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	if err := runMigrate(t.Context(), &out, cfg, e, []string{"tank"}, false, true); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), `"status": "migrated"`) || version() != "2" {
		t.Fatalf("apply=%s version=%q", out.String(), version())
	}
}
