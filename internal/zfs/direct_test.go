package zfs

import (
	"bytes"
	"context"
	"io"
	"reflect"
	"testing"
)

type fakeRunner struct {
	output []byte
	args   []string
}

func TestInspectDatasetIdentity(t *testing.T) {
	t.Parallel()
	dataset := &fakeRunner{output: []byte("tank/data\tfilesystem\t42\n")}
	pool := &fakeRunner{output: []byte("tank\tguid\t99\n")}
	direct := &Direct{runner: dataset, poolRunner: pool}
	identity, err := direct.InspectDatasetIdentity(t.Context(), "tank/data")
	if err != nil {
		t.Fatal(err)
	}
	want := DatasetIdentity{Name: "tank/data", Type: Filesystem, GUID: 42, Pool: "tank", PoolGUID: 99}
	if !reflect.DeepEqual(identity, want) {
		t.Fatalf("identity=%#v want=%#v", identity, want)
	}
}

func TestActivationQuery(t *testing.T) {
	t.Parallel()
	runner := &fakeRunner{output: []byte("backup\torg.boomerangz:enabled\ton\treceived\n")}
	d := &Direct{runner: runner}
	rows, err := d.GetActivationProperties(t.Context())
	if err != nil || len(rows) != 1 || rows[0].Source != SourceReceived {
		t.Fatalf("rows=%v err=%v", rows, err)
	}
	want := []string{"get", "-H", "-p", "-s", "local,received", "-t", "filesystem,volume", "-o", "name,property,value,source", "org.boomerangz:enabled"}
	if !reflect.DeepEqual(runner.args, want) {
		t.Fatalf("args=%v", runner.args)
	}
}

func TestMalformedPropertiesAndWhitespace(t *testing.T) {
	t.Parallel()
	for _, invalid := range []string{"tank\torg.boomerangz:enabled\ton\tinherited from tank\n", "tank\torg.boomerangz:enabled\n", "\n"} {
		if _, err := parseProperties([]byte(invalid)); err == nil {
			t.Errorf("accepted %q", invalid)
		}
	}
	rows, err := parseProperties([]byte("tank\torg.boomerangz:set_prop:custom:value\t  keep spaces  \tlocal\n"))
	if err != nil || rows[0].Value != "  keep spaces  " {
		t.Fatalf("whitespace changed: %v %v", rows, err)
	}
}

func FuzzDatasetRows(f *testing.F) {
	f.Add("tank\tfilesystem\t-\n")
	f.Add("tank/swap\tvolume\ttank\n")
	f.Fuzz(func(t *testing.T, input string) {
		rows, err := parseDatasets([]byte(input))
		if err != nil {
			return
		}
		for _, row := range rows {
			if err := ValidateDataset(row.Name); err != nil {
				t.Fatal(err)
			}
		}
	})
}

func TestBoundedCommandOutput(t *testing.T) {
	t.Parallel()
	b := &boundedOutput{}
	_, err := io.Copy(b, bytes.NewReader(make([]byte, 32*1024*1024+1)))
	if err == nil || b.buffer.Len() > 32*1024*1024 {
		t.Fatal("output limit bypassed")
	}
}

func FuzzPropertyRows(f *testing.F) {
	f.Add("tank\torg.boomerangz:enabled\ton\tlocal\n")
	f.Add("tank\tcompression\tzstd\tlocal\n")
	f.Fuzz(func(t *testing.T, input string) {
		rows, err := parseProperties([]byte(input))
		if err != nil {
			return
		}
		for _, row := range rows {
			if row.Source != SourceLocal && row.Source != SourceReceived {
				t.Fatal("invalid source accepted")
			}
		}
	})
}

func (f *fakeRunner) Run(_ context.Context, args ...string) ([]byte, error) {
	f.args = append([]string(nil), args...)
	return f.output, nil
}

func TestListDatasets(t *testing.T) {
	t.Parallel()
	runner := &fakeRunner{output: []byte("tank/data\tfilesystem\ttank/data\ntank/swap\tvolume\t-\n")}
	direct := &Direct{runner: runner}
	datasets, err := direct.ListDatasets(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := []Dataset{{Name: "tank/data", Type: Filesystem, EncryptionRoot: "tank/data"}, {Name: "tank/swap", Type: Volume, EncryptionRoot: "-"}}
	if !reflect.DeepEqual(datasets, want) {
		t.Fatalf("datasets = %#v, want %#v", datasets, want)
	}
}

func TestGetStoredPropertiesFiltersNamespace(t *testing.T) {
	t.Parallel()
	runner := &fakeRunner{output: []byte("tank/data\torg.boomerangz:enabled\ton\tlocal\ntank/data\tcompression\tzstd\tlocal\n")}
	direct := &Direct{runner: runner}
	properties, err := direct.GetStoredProperties(context.Background(), []string{"tank/data"})
	if err != nil {
		t.Fatal(err)
	}
	want := []Property{{Dataset: "tank/data", Name: "org.boomerangz:enabled", Value: "on", Source: SourceLocal}}
	if !reflect.DeepEqual(properties, want) {
		t.Fatalf("properties = %#v, want %#v", properties, want)
	}
}

func TestSnapshotBuildsDeterministicArguments(t *testing.T) {
	t.Parallel()
	runner := &fakeRunner{}
	direct := &Direct{runner: runner}
	err := direct.Snapshot(context.Background(), "tank/data", "boomerangz-test", true, map[string]string{
		"org.boomerangz:state:snapshot": "abc",
		"org.boomerangz:state:created":  "now",
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"snapshot", "-r", "-o", "org.boomerangz:state:created=now", "-o", "org.boomerangz:state:snapshot=abc", "tank/data@boomerangz-test"}
	if !reflect.DeepEqual(runner.args, want) {
		t.Fatalf("arguments = %#v, want %#v", runner.args, want)
	}
}

func TestRejectsArgumentInjection(t *testing.T) {
	t.Parallel()
	runner := &fakeRunner{}
	direct := &Direct{runner: runner}
	if err := direct.Hold(context.Background(), "tag", "tank/data@ok --recursive"); err == nil {
		t.Fatal("Hold accepted whitespace in snapshot name")
	}
	if runner.args != nil {
		t.Fatalf("runner invoked with %#v", runner.args)
	}
}
