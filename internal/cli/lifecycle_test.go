package cli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"

	"github.com/pdf/boomerangz/internal/config"
	"github.com/pdf/boomerangz/internal/lifecycle"
	"github.com/pdf/boomerangz/internal/policy"
	"github.com/pdf/boomerangz/internal/zfs"
)

type cleanExecutor struct {
	zfs.Executor
	properties []zfs.Property
	writes     int
}

type adoptionExecutor struct {
	zfs.Executor
	properties []zfs.Property
}

func (e *adoptionExecutor) ListDatasets(context.Context) ([]zfs.Dataset, error) {
	return []zfs.Dataset{{Name: "backup", Type: zfs.Filesystem, EncryptionRoot: "-"}, {Name: "tank", Type: zfs.Filesystem, EncryptionRoot: "-"}}, nil
}
func (e *adoptionExecutor) GetActivationProperties(context.Context) ([]zfs.Property, error) {
	return slices.DeleteFunc(slices.Clone(e.properties), func(row zfs.Property) bool { return row.Name != policy.Namespace+"enabled" }), nil
}
func (e *adoptionExecutor) GetStoredProperties(_ context.Context, datasets []string) ([]zfs.Property, error) {
	selected := make(map[string]bool)
	for _, dataset := range datasets {
		selected[dataset] = true
	}
	return slices.DeleteFunc(slices.Clone(e.properties), func(row zfs.Property) bool { return !selected[row.Dataset] }), nil
}
func (e *adoptionExecutor) InspectState(_ context.Context, dataset string, _ bool) (zfs.State, error) {
	return zfs.State{Objects: []zfs.Object{{Name: dataset, Type: "filesystem", GUID: 1}}, Properties: slices.Clone(e.properties)}, nil
}
func (e *adoptionExecutor) InspectDatasetIdentity(_ context.Context, dataset string) (zfs.DatasetIdentity, error) {
	return zfs.DatasetIdentity{Name: dataset, Type: zfs.Filesystem, GUID: 20, Pool: "backup", PoolGUID: 30}, nil
}
func (e *adoptionExecutor) SetProperties(_ context.Context, object string, values map[string]string) error {
	for key, value := range values {
		e.properties = slices.DeleteFunc(e.properties, func(row zfs.Property) bool {
			return row.Dataset == object && row.Name == key && row.Source == zfs.SourceLocal
		})
		e.properties = append(e.properties, zfs.Property{Dataset: object, Name: key, Value: value, Source: zfs.SourceLocal})
	}
	return nil
}

func (e *cleanExecutor) ListDatasets(context.Context) ([]zfs.Dataset, error) {
	return []zfs.Dataset{{Name: "tank", Type: zfs.Filesystem}, {Name: "tank/data", Type: zfs.Filesystem}, {Name: "tank/data/child", Type: zfs.Filesystem}}, nil
}
func (e *cleanExecutor) GetActivationProperties(context.Context) ([]zfs.Property, error) {
	return nil, nil
}
func (e *cleanExecutor) InspectState(_ context.Context, name string, _ bool) (zfs.State, error) {
	return zfs.State{Objects: []zfs.Object{{Name: name, Type: "filesystem", GUID: 1}}, Properties: append([]zfs.Property(nil), e.properties...)}, nil
}
func (e *cleanExecutor) InheritProperty(context.Context, string, string) error {
	e.properties = nil
	e.writes++
	return nil
}

func TestCleanScopesRequireExplicitSelection(t *testing.T) {
	t.Parallel()
	e := &cleanExecutor{}
	for _, tc := range []struct {
		names []string
		all   bool
	}{{nil, false}, {[]string{"tank"}, true}} {
		if _, _, err := cleanScopes(t.Context(), e, tc.names, false, tc.all); err == nil {
			t.Fatal("accepted ambiguous scope")
		}
	}
	names, recursive, err := cleanScopes(t.Context(), e, nil, false, true)
	if err != nil || !recursive || !reflect.DeepEqual(names, []string{"tank"}) {
		t.Fatalf("all scopes: %v %v", names, err)
	}
}

func TestCleanCLIPreviewAndApply(t *testing.T) {
	t.Parallel()
	cfg := config.Defaults()
	cfg.Paths.SocketPath = filepath.Join(t.TempDir(), "control.sock")
	cfg.Paths.IdentityDir = filepath.Join(t.TempDir(), "identity")
	e := &cleanExecutor{properties: []zfs.Property{{Dataset: "tank/data", Name: "org.boomerangz:enabled", Value: "off", Source: zfs.SourceLocal}}}
	var out bytes.Buffer
	if err := runClean(t.Context(), &out, cfg, e, []string{"tank/data"}, false, false, false, false); err != nil {
		t.Fatal(err)
	}
	if e.writes != 0 || !bytes.Contains(out.Bytes(), []byte(`"operation": "inherit"`)) {
		t.Fatal("preview mutated or omitted action")
	}
	if _, err := os.Stat(cfg.Paths.SocketPath + ".lifecycle.lock"); !os.IsNotExist(err) {
		t.Fatal("preview created a lock")
	}
	out.Reset()
	if err := runClean(t.Context(), &out, cfg, e, []string{"tank/data"}, false, false, false, true); err != nil {
		t.Fatal(err)
	}
	if e.writes != 1 {
		t.Fatal("apply did not execute preview")
	}
}

func TestAdoptionReviewsLocalTargetWithoutSecondFlag(t *testing.T) {
	t.Parallel()
	const lineage = "11111111-1111-4111-8111-111111111111"
	const oldOwner = "22222222-2222-4222-8222-222222222222"
	executor := &adoptionExecutor{properties: []zfs.Property{
		{Dataset: "tank", Name: policy.Namespace + "enabled", Value: "on", Source: zfs.SourceLocal},
		{Dataset: "tank", Name: policy.Namespace + "local", Value: "backup/data", Source: zfs.SourceLocal},
		{Dataset: "tank", Name: lifecycle.LineageProperty, Value: lineage, Source: zfs.SourceLocal},
		{Dataset: "tank", Name: lifecycle.OwnerProperty, Value: oldOwner, Source: zfs.SourceLocal},
	}}
	cfg := config.Defaults()
	cfg.Paths.IdentityDir = filepath.Join(t.TempDir(), "identity")
	cfg.Paths.SocketPath = filepath.Join(t.TempDir(), "control.sock")
	var preview bytes.Buffer
	if err := runAdopt(t.Context(), &preview, cfg, executor, "tank", false); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(preview.Bytes(), []byte(`"status": "unbound"`)) {
		t.Fatalf("target review missing: %s", preview.String())
	}
	var applied bytes.Buffer
	if err := runAdopt(t.Context(), &applied, cfg, executor, "tank", true); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(applied.Bytes(), []byte(`"applied": true`)) {
		t.Fatalf("adoption not applied: %s", applied.String())
	}
	applied.Reset()
	if err := runAdopt(t.Context(), &applied, cfg, executor, "tank", true); err != nil {
		t.Fatalf("owned dataset could not be revalidated through adoption: %v", err)
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
