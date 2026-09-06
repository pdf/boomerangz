package cli

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	"github.com/pdf/boomerangz/internal/config"
	installidentity "github.com/pdf/boomerangz/internal/identity"
	"github.com/pdf/boomerangz/internal/lifecycle"
	"github.com/pdf/boomerangz/internal/policy"
	"github.com/pdf/boomerangz/internal/zfs"
)

type recoveryExecutor struct {
	zfs.Executor
	properties []zfs.Property
}

func (e recoveryExecutor) ListDatasets(context.Context) ([]zfs.Dataset, error) {
	return []zfs.Dataset{{Name: "tank", Type: zfs.Filesystem, EncryptionRoot: "-"}}, nil
}

func (e recoveryExecutor) GetActivationProperties(context.Context) ([]zfs.Property, error) {
	var rows []zfs.Property
	for _, row := range e.properties {
		if row.Name == policy.Namespace+"enabled" {
			rows = append(rows, row)
		}
	}
	return rows, nil
}

func (e recoveryExecutor) GetStoredProperties(context.Context, []string) ([]zfs.Property, error) {
	return append([]zfs.Property(nil), e.properties...), nil
}

func (e recoveryExecutor) InspectState(context.Context, string, bool) (zfs.State, error) {
	return zfs.State{Objects: []zfs.Object{{Name: "tank", Type: "filesystem", GUID: 10}}, Properties: append([]zfs.Property(nil), e.properties...)}, nil
}

func TestIdentityRecoveryPreviewAndApply(t *testing.T) {
	t.Parallel()
	const owner = "11111111-1111-4111-8111-111111111111"
	const lineage = "22222222-2222-4222-8222-222222222222"
	executor := recoveryExecutor{properties: []zfs.Property{
		{Dataset: "tank", Name: policy.Namespace + "enabled", Value: "on", Source: zfs.SourceLocal},
		{Dataset: "tank", Name: lifecycle.OwnerProperty, Value: owner, Source: zfs.SourceLocal},
		{Dataset: "tank", Name: lifecycle.LineageProperty, Value: lineage, Source: zfs.SourceLocal},
	}}
	cfg := config.Defaults()
	cfg.Paths.IdentityDir = filepath.Join(t.TempDir(), "identity")
	cfg.Paths.SocketPath = filepath.Join(t.TempDir(), "control.sock")
	current, err := installidentity.LoadOrCreate(cfg.Paths.IdentityDir)
	if err != nil {
		t.Fatal(err)
	}
	var preview bytes.Buffer
	if err := runIdentityRecover(t.Context(), &preview, cfg, executor, executor, "", false); err != nil {
		t.Fatal(err)
	}
	if after, _ := installidentity.Read(cfg.Paths.IdentityDir); after != current {
		t.Fatal("preview replaced installation identity")
	}
	var applied bytes.Buffer
	if err := runIdentityRecover(t.Context(), &applied, cfg, executor, executor, "", true); err != nil {
		t.Fatal(err)
	}
	if after, _ := installidentity.Read(cfg.Paths.IdentityDir); after != owner {
		t.Fatalf("recovered identity=%s", after)
	}
}
