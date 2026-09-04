package zfs

import (
	"context"
	"reflect"
	"testing"
)

type fakeRunner struct {
	output []byte
	args   []string
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
