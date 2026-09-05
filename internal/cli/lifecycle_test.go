package cli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/pdf/boomerangz/internal/config"
	"github.com/pdf/boomerangz/internal/zfs"
)

type cleanupExecutor struct {
	zfs.Executor
	properties []zfs.Property
	writes     int
}

func (e *cleanupExecutor) ListDatasets(context.Context) ([]zfs.Dataset, error) {
	return []zfs.Dataset{{Name: "tank", Type: zfs.Filesystem}, {Name: "tank/data", Type: zfs.Filesystem}, {Name: "tank/data/child", Type: zfs.Filesystem}}, nil
}
func (e *cleanupExecutor) GetActivationProperties(context.Context) ([]zfs.Property, error) {
	return nil, nil
}
func (e *cleanupExecutor) InspectState(_ context.Context, name string, _ bool) (zfs.State, error) {
	return zfs.State{Objects: []zfs.Object{{Name: name, Type: "filesystem", GUID: 1}}, Properties: append([]zfs.Property(nil), e.properties...)}, nil
}
func (e *cleanupExecutor) InheritProperty(context.Context, string, string) error {
	e.properties = nil
	e.writes++
	return nil
}

func TestCleanupScopesRequireExplicitSelection(t *testing.T) {
	t.Parallel()
	e := &cleanupExecutor{}
	for _, tc := range []struct {
		names []string
		all   bool
	}{{nil, false}, {[]string{"tank"}, true}} {
		if _, _, err := cleanupScopes(t.Context(), e, tc.names, false, tc.all); err == nil {
			t.Fatal("accepted ambiguous scope")
		}
	}
	names, recursive, err := cleanupScopes(t.Context(), e, nil, false, true)
	if err != nil || !recursive || !reflect.DeepEqual(names, []string{"tank"}) {
		t.Fatalf("all scopes: %v %v", names, err)
	}
}

func TestCleanupCLIPreviewAndApply(t *testing.T) {
	t.Parallel()
	cfg := config.Defaults()
	cfg.Paths.SocketPath = filepath.Join(t.TempDir(), "control.sock")
	e := &cleanupExecutor{properties: []zfs.Property{{Dataset: "tank/data", Name: "org.boomerangz:enabled", Value: "off", Source: zfs.SourceLocal}}}
	var out bytes.Buffer
	if err := runCleanup(t.Context(), &out, cfg, e, []string{"tank/data"}, false, false, false, false); err != nil {
		t.Fatal(err)
	}
	if e.writes != 0 || !bytes.Contains(out.Bytes(), []byte(`"operation": "inherit"`)) {
		t.Fatal("preview mutated or omitted action")
	}
	if _, err := os.Stat(cfg.Paths.SocketPath + ".lifecycle.lock"); !os.IsNotExist(err) {
		t.Fatal("preview created a lock")
	}
	out.Reset()
	if err := runCleanup(t.Context(), &out, cfg, e, []string{"tank/data"}, false, false, false, true); err != nil {
		t.Fatal(err)
	}
	if e.writes != 1 {
		t.Fatal("apply did not execute preview")
	}
}

func TestStandaloneLifecycleRefusesDaemonAndConcurrentApply(t *testing.T) {
	t.Parallel()
	cfg := config.Defaults()
	cfg.Paths.SocketPath = filepath.Join(t.TempDir(), "control.sock")
	lock, err := lifecycleLock(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := lock.Close(); err != nil {
			t.Error(err)
		}
	}()
	second, err := lifecycleLock(cfg)
	if err == nil {
		if closeErr := second.Close(); closeErr != nil {
			t.Error(closeErr)
		}
		t.Fatal("allowed concurrent apply")
	}
	if err := os.WriteFile(cfg.Paths.SocketPath, []byte{}, 0600); err != nil {
		t.Fatal(err)
	}
	if err := (standaloneSafety{socket: cfg.Paths.SocketPath}).Quiescent(t.Context(), nil); err == nil {
		t.Fatal("ignored existing control socket")
	}
}
