package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/alecthomas/kong"
	"github.com/pdf/boomerangz/internal/zfs"
)

type inspectionReader struct{}

func (inspectionReader) ListDatasets(context.Context) ([]zfs.Dataset, error) {
	return []zfs.Dataset{
		{Name: "backup", Type: zfs.Filesystem, EncryptionRoot: "-"},
		{Name: "tank", Type: zfs.Filesystem, EncryptionRoot: "-"},
		{Name: "tank/bad", Type: zfs.Filesystem, EncryptionRoot: "-"},
		{Name: "tank/data", Type: zfs.Filesystem, EncryptionRoot: "-"},
	}, nil
}
func (inspectionReader) GetActivationProperties(context.Context) ([]zfs.Property, error) {
	return []zfs.Property{
		{Dataset: "backup", Name: "org.boomerangz:enabled", Value: "on", Source: zfs.SourceReceived},
		{Dataset: "tank/bad", Name: "org.boomerangz:enabled", Value: "on", Source: zfs.SourceLocal},
		{Dataset: "tank/data", Name: "org.boomerangz:enabled", Value: "on", Source: zfs.SourceLocal},
	}, nil
}
func (r inspectionReader) GetStoredProperties(ctx context.Context, names []string) ([]zfs.Property, error) {
	properties, err := r.GetActivationProperties(ctx)
	properties = append(properties,
		zfs.Property{Dataset: "tank/bad", Name: "org.boomerangz:remote", Value: "missing", Source: zfs.SourceLocal},
		zfs.Property{Dataset: "tank/data", Name: "org.boomerangz:local", Value: "backup", Source: zfs.SourceLocal},
	)
	return slices.DeleteFunc(properties, func(property zfs.Property) bool { return !slices.Contains(names, property.Dataset) }), err
}

func TestDatasetHumanOutput(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name    string
		command []string
		want    []string
	}{
		{"list", []string{"list"}, []string{"STATUS", "active    tank/data", "invalid   tank/bad", "dataset inspect <dataset>"}},
		{"inspect", []string{"inspect", "tank/bad"}, []string{"Dataset: tank/bad", "Activation: active", "Policy: invalid", "Remote destinations: missing", "remote = missing (set on tank/bad)", "Errors:", `unknown remote "missing"`}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			args := []string{"dataset", "--config", filepath.Join("testdata", "empty.toml"), "--config-dir", t.TempDir()}
			args = append(args, test.command...)
			err := runWithReader(t.Context(), args, &output, io.Discard, BuildInfo{}, inspectionReader{})
			if err != nil {
				t.Fatal(err)
			}
			for _, want := range test.want {
				if !strings.Contains(output.String(), want) {
					t.Fatalf("output missing %q:\n%s", want, output.String())
				}
			}
		})
	}
}

func TestDatasetJSONCompatibility(t *testing.T) {
	t.Parallel()
	for _, command := range [][]string{{"list", "--json"}, {"inspect", "--json", "backup"}} {
		var output bytes.Buffer
		args := []string{"dataset", "--config", filepath.Join("testdata", "empty.toml"), "--config-dir", t.TempDir()}
		args = append(args, command...)
		if err := runWithReader(t.Context(), args, &output, io.Discard, BuildInfo{}, inspectionReader{}); err != nil {
			t.Fatal(err)
		}
		if !json.Valid(output.Bytes()) {
			t.Fatalf("invalid JSON: %s", output.String())
		}
		if command[0] == "inspect" {
			var result struct {
				Inspected bool `json:"inspected"`
				Policy    struct {
					Enabled bool `json:"enabled"`
				} `json:"policy"`
				Stored []zfs.Property `json:"stored"`
			}
			if err := json.Unmarshal(output.Bytes(), &result); err != nil {
				t.Fatal(err)
			}
			if !result.Inspected || result.Policy.Enabled || len(result.Stored) != 1 {
				t.Fatalf("unexpected inspection: %s", output.String())
			}
		}
	}
}

func TestDatasetHelpDescribesJSON(t *testing.T) {
	t.Parallel()
	for _, command := range [][]string{{"dataset", "list", "--help"}, {"dataset", "inspect", "tank/data", "--help"}} {
		var output bytes.Buffer
		parser, err := kong.New(&commandLine{}, kong.Name("boomerangz"), kong.Writers(&output, io.Discard), kong.Exit(func(int) {}))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := parser.Parse(command); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(output.String(), "--json") || !strings.Contains(output.String(), "Emit JSON") {
			t.Fatalf("missing JSON help: %s", output.String())
		}
	}
}
