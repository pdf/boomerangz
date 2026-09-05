package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"path/filepath"
	"testing"

	"github.com/pdf/boomerangz/internal/zfs"
)

type inspectionReader struct{}

func (inspectionReader) ListDatasets(context.Context) ([]zfs.Dataset, error) {
	return []zfs.Dataset{{Name: "backup", Type: zfs.Filesystem, EncryptionRoot: "-"}}, nil
}
func (inspectionReader) GetActivationProperties(context.Context) ([]zfs.Property, error) {
	return []zfs.Property{{Dataset: "backup", Name: "org.boomerangz:enabled", Value: "on", Source: zfs.SourceReceived}}, nil
}
func (r inspectionReader) GetStoredProperties(ctx context.Context, _ []string) ([]zfs.Property, error) {
	return r.GetActivationProperties(ctx)
}

func TestDatasetInspection(t *testing.T) {
	t.Parallel()
	for _, command := range [][]string{{"list"}, {"inspect", "backup"}} {
		var output bytes.Buffer
		args := []string{"dataset", "--config", filepath.Join("testdata", "empty.toml"), "--config-dir", t.TempDir()}
		args = append(args, command...)
		err := runWithReader(t.Context(), args, &output, io.Discard, BuildInfo{}, inspectionReader{})
		if err != nil {
			t.Fatal(err)
		}
		if command[0] == "inspect" {
			// Decode only the fields tested here; Grid intentionally has no mutable decoder.
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
		} else if !json.Valid(output.Bytes()) {
			t.Fatalf("invalid JSON: %s", output.String())
		}
	}
}
