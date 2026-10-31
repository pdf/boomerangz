package cli

import (
	"bytes"
	"context"
	"io"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/pdf/boomerangz/internal/config"
	"github.com/pdf/boomerangz/internal/discovery"
	"github.com/pdf/boomerangz/internal/zfs"
)

// memberReader holds an actor root whose member sends to a target of its own,
// and an excluded dataset beside it.
type memberReader struct{}

func (memberReader) ListDatasets(context.Context) ([]zfs.Dataset, error) {
	var datasets []zfs.Dataset
	for _, name := range []string{"archive", "backup", "tank", "tank/home", "tank/home/cache", "tank/home/docs"} {
		datasets = append(datasets, zfs.Dataset{Name: name, Type: zfs.Filesystem, EncryptionRoot: "-"})
	}
	return datasets, nil
}

func (memberReader) GetActivationProperties(context.Context) ([]zfs.Property, error) {
	return []zfs.Property{
		{Dataset: "tank/home", Name: "org.boomerangz:enabled", Value: "on", Source: zfs.SourceLocal},
		{Dataset: "tank/home/cache", Name: "org.boomerangz:enabled", Value: "off", Source: zfs.SourceLocal},
	}, nil
}

func (r memberReader) GetStoredProperties(ctx context.Context, names []string) ([]zfs.Property, error) {
	properties, err := r.GetActivationProperties(ctx)
	properties = append(properties,
		zfs.Property{Dataset: "tank/home", Name: "org.boomerangz:local", Value: "backup", Source: zfs.SourceLocal},
		zfs.Property{Dataset: "tank/home/docs", Name: "org.boomerangz:local", Value: "backup,archive", Source: zfs.SourceLocal},
	)
	return slices.DeleteFunc(properties, func(property zfs.Property) bool { return !slices.Contains(names, property.Dataset) }), err
}

func TestReseedResolvesTargetsAcrossTheRootsMembers(t *testing.T) {
	t.Parallel()
	scanner, err := discovery.New(memberReader{}, discovery.Options{})
	if err != nil {
		t.Fatal(err)
	}
	generation, err := scanner.Scan(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	root, _ := generation.Inspect("tank/home")
	// Only docs sends to archive, but the binding and proofs for it live on
	// the root, so it is the root's to reseed.
	request, local, err := reseedRequest(generation, root, "archive")
	if err != nil || !local || len(request.Members) != 1 || request.Members[0].Dataset != "tank/home/docs" {
		t.Fatalf("archive: request=%+v local=%t err=%v", request, local, err)
	}
	request, local, err = reseedRequest(generation, root, "backup")
	if err != nil || !local || len(request.Members) != 1 {
		t.Fatalf("backup: request=%+v local=%t err=%v", request, local, err)
	}
	if _, _, err := reseedRequest(generation, root, "elsewhere"); err == nil {
		t.Fatal("reseeded a target nothing in the root sends to")
	}
}

func TestDatasetListNamesMembersAndExclusions(t *testing.T) {
	t.Parallel()
	var output bytes.Buffer
	args := []string{"dataset", "--config", filepath.Join("testdata", "empty.toml"), "--config-dir", t.TempDir(), "list"}
	if err := runWithReader(t.Context(), args, &output, io.Discard, BuildInfo{}, memberReader{}); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"active    tank/home", "member    tank/home/docs", "excluded  tank/home/cache"} {
		if !strings.Contains(output.String(), want) {
			t.Errorf("list output lacks %q:\n%s", want, output.String())
		}
	}
}

// collidingReader holds two actor roots whose replicas would both land at
// backup/home, neither established there.
type collidingReader struct{}

func (collidingReader) ListDatasets(context.Context) ([]zfs.Dataset, error) {
	var datasets []zfs.Dataset
	for _, name := range []string{"backup", "tank", "tank/a", "tank/a/home", "tank/b", "tank/b/home"} {
		datasets = append(datasets, zfs.Dataset{Name: name, Type: zfs.Filesystem, EncryptionRoot: "-"})
	}
	return datasets, nil
}

func (collidingReader) GetActivationProperties(context.Context) ([]zfs.Property, error) {
	return []zfs.Property{
		{Dataset: "tank/a/home", Name: "org.boomerangz:enabled", Value: "on", Source: zfs.SourceLocal},
		{Dataset: "tank/b/home", Name: "org.boomerangz:enabled", Value: "on", Source: zfs.SourceLocal},
	}, nil
}

func (r collidingReader) GetStoredProperties(ctx context.Context, names []string) ([]zfs.Property, error) {
	properties, err := r.GetActivationProperties(ctx)
	for _, root := range []string{"tank/a/home", "tank/b/home"} {
		properties = append(properties,
			zfs.Property{Dataset: root, Name: "org.boomerangz:local", Value: "backup", Source: zfs.SourceLocal},
			zfs.Property{Dataset: root, Name: "org.boomerangz:discard", Value: "all", Source: zfs.SourceLocal},
		)
	}
	return slices.DeleteFunc(properties, func(property zfs.Property) bool { return !slices.Contains(names, property.Dataset) }), err
}

func TestTargetOperationsRefuseABlockedTarget(t *testing.T) {
	t.Parallel()
	cfg := config.Defaults()
	cfg.Paths.IdentityDir = t.TempDir()
	cfg.Paths.CredentialsDir = t.TempDir()
	cfg.Paths.SocketPath = filepath.Join(t.TempDir(), "absent.sock")
	// The replica location overlaps tank/b/home's, so a reseed would destroy
	// or an abort would discard what may be the other root's. Nothing reaches
	// the executor: it is nil.
	for name, run := range map[string]func() error{
		"reseed": func() error {
			return runReseed(t.Context(), io.Discard, cfg, collidingReader{}, nil, "tank/a/home", "backup", false)
		},
		"abort-receive": func() error {
			return runAbortReceive(t.Context(), io.Discard, cfg, collidingReader{}, nil, "tank/a/home", "backup", false)
		},
	} {
		if err := run(); err == nil || !strings.Contains(err.Error(), "tank/b/home") {
			t.Errorf("%s on a blocked target: err=%v, want a refusal naming the other root", name, err)
		}
	}
}

func TestDatasetOutputShowsBlockedTargets(t *testing.T) {
	t.Parallel()
	run := func(args ...string) string {
		t.Helper()
		var output bytes.Buffer
		args = append(args, "--config", filepath.Join("testdata", "empty.toml"), "--config-dir", t.TempDir())
		if err := runWithReader(t.Context(), append([]string{"dataset"}, args...), &output, io.Discard, BuildInfo{}, collidingReader{}); err != nil {
			t.Fatal(err)
		}
		return output.String()
	}
	// Both roots stay active; only their shared target is blocked.
	list := run("list")
	for _, want := range []string{"active    tank/a/home  backup", "active    tank/b/home  backup", "BLOCKED TARGETS", "Inspect blocked targets"} {
		if !strings.Contains(list, want) {
			t.Errorf("list output lacks %q:\n%s", want, list)
		}
	}
	inspect := run("inspect", "tank/a/home")
	if !strings.Contains(inspect, "Blocked targets:\n  - backup: ") || !strings.Contains(inspect, "tank/b/home") || !strings.Contains(inspect, "Policy: valid") {
		t.Errorf("inspect output does not show the blocked target:\n%s", inspect)
	}
	if json := run("inspect", "tank/a/home", "--json"); !strings.Contains(json, `"blocked"`) {
		t.Errorf("inspect JSON lacks the blocked targets:\n%s", json)
	}
}
