package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
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

func TestDatasetListFiltersByStatus(t *testing.T) {
	t.Parallel()
	run := func(args ...string) string {
		t.Helper()
		var output bytes.Buffer
		args = append([]string{"dataset", "--config", filepath.Join("testdata", "empty.toml"), "--config-dir", t.TempDir()}, args...)
		if err := runWithReader(t.Context(), args, &output, io.Discard, BuildInfo{}, memberReader{}); err != nil {
			t.Fatal(err)
		}
		return output.String()
	}
	rows := func(output string) []string {
		var rows []string
		for line := range strings.SplitSeq(output, "\n") {
			if fields := strings.Fields(line); len(fields) == 2 && fields[0] != "STATUS" {
				rows = append(rows, fields[0]+" "+fields[1])
			}
		}
		return rows
	}
	// backup and archive are where tank/home and its member place their
	// replicas; tank is inactive and hidden by default.
	byDefault := run("list")
	if got, want := rows(byDefault), []string{"target archive", "target backup", "active tank/home", "excluded tank/home/cache", "member tank/home/docs"}; !slices.Equal(got, want) {
		t.Errorf("default rows = %q, want %q", got, want)
	}
	if !strings.Contains(byDefault, "1 inactive dataset hidden; show it with: boomerangz dataset list --status all") {
		t.Errorf("default list does not say inactive datasets were hidden:\n%s", byDefault)
	}
	all := run("list", "--status", "all")
	if got := rows(all); len(got) != 6 || !slices.Contains(got, "inactive tank") || strings.Contains(all, "hidden") {
		t.Errorf("--status all:\n%s", all)
	}
	if got, want := rows(run("list", "--status", "target,inactive")), []string{"target archive", "target backup", "inactive tank"}; !slices.Equal(got, want) {
		t.Errorf("--status target,inactive rows = %q, want %q", got, want)
	}
	var entries []struct {
		Dataset struct {
			Name string `json:"name"`
		} `json:"dataset"`
		Replica discovery.Replica `json:"replica"`
	}
	if err := json.Unmarshal([]byte(run("list", "--json", "--status", "target")), &entries); err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 || entries[1].Dataset.Name != "backup" || entries[1].Replica != (discovery.Replica{Source: "tank/home", Destination: "backup"}) {
		t.Errorf("--json --status target = %+v", entries)
	}
}

// inactiveReader holds only datasets nothing manages.
type inactiveReader struct{}

func (inactiveReader) ListDatasets(context.Context) ([]zfs.Dataset, error) {
	return []zfs.Dataset{{Name: "tank", Type: zfs.Filesystem, EncryptionRoot: "-"}, {Name: "tank/data", Type: zfs.Filesystem, EncryptionRoot: "-"}}, nil
}

func (inactiveReader) GetActivationProperties(context.Context) ([]zfs.Property, error) {
	return nil, nil
}

func (inactiveReader) GetStoredProperties(context.Context, []string) ([]zfs.Property, error) {
	return nil, nil
}

func TestDatasetListMarksReplicasBeneathConfiguredReplicationRoots(t *testing.T) {
	t.Parallel()
	configPath := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(configPath, []byte("[ssh_shell]\nreplication_roots = [\"tank\"]\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	args := []string{"dataset", "--config", configPath, "--config-dir", t.TempDir(), "list"}
	if err := runWithReader(t.Context(), args, &output, io.Discard, BuildInfo{}, inactiveReader{}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "target  tank/data\n") || !strings.Contains(output.String(), "1 inactive dataset hidden") {
		t.Errorf("tank/data beneath the replication root is not a target:\n%s", output.String())
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
			return runReseed(t.Context(), io.Discard, cfg, collidingReader{}, nil, "tank/a/home", "backup", false, false)
		},
		"abort-receive": func() error {
			return runAbortReceive(t.Context(), io.Discard, cfg, collidingReader{}, nil, "tank/a/home", "backup", false, false)
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
	for _, want := range []string{"active  tank/a/home  backup", "active  tank/b/home  backup", "BLOCKED TARGETS", "Inspect blocked targets"} {
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
