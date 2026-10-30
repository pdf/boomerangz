package cli

import (
	"bytes"
	"context"
	"io"
	"path/filepath"
	"slices"
	"strings"
	"testing"

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
