package zfs

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
)

type stateRunner struct {
	calls     [][]string
	fail      bool
	disappear bool
	unstable  bool
	inventory string
}

func (r *stateRunner) Run(_ context.Context, args ...string) ([]byte, error) {
	r.calls = append(r.calls, append([]string(nil), args...))
	switch args[0] {
	case "list":
		return []byte(r.inventory), nil
	case "holds":
		if r.fail {
			return nil, fmt.Errorf("hold query failed")
		}
		var out strings.Builder
		for _, snapshot := range args[2:] {
			if snapshot == "tank/data@snap" {
				out.WriteString(snapshot + "\tforeign-hold\t123\n")
			}
		}
		return []byte(out.String()), nil
	case "get":
		// get -H -p -o <columns> <keys> <object>...
		keys, objects := args[5], args[6:]
		if (r.disappear || r.unstable) && slices.Contains(objects, "tank/data@snap") {
			r.disappear = false
			if !r.unstable {
				r.inventory = "tank/data\tfilesystem\t1\t123\t6\n"
			}
			return nil, fmt.Errorf("zfs get: exit status 1: cannot open 'tank/data@snap': dataset does not exist")
		}
		var out strings.Builder
		for _, object := range objects {
			if keys != "all" {
				out.WriteString(object + "\torg.boomerangz:state:lineage\t-\thidden\t-\n")
				continue
			}
			out.WriteString(object + "\torg.boomerangz:enabled\ton\t-\tlocal\n" + object + "\tcompression\tzstd\t-\tlocal\n")
		}
		return []byte(out.String()), nil
	}
	return nil, nil
}

func TestInspectStateBoundsRepeatedInventoryChurn(t *testing.T) {
	t.Parallel()
	r := &stateRunner{unstable: true, inventory: "tank/data\tfilesystem\t1\t123\t6\ntank/data@snap\tsnapshot\t2\t123\t6\n"}
	d := &Direct{runner: r}
	_, err := d.InspectState(t.Context(), "tank/data", false)
	var temporary interface{ Temporary() bool }
	if !errors.As(err, &temporary) || !temporary.Temporary() {
		t.Fatalf("repeated inventory churn was not retryable: %v", err)
	}
	listCalls := 0
	for _, call := range r.calls {
		if call[0] == "list" {
			listCalls++
		}
	}
	if listCalls != inspectStateAttempts {
		t.Fatalf("inventory list calls = %d, want %d", listCalls, inspectStateAttempts)
	}
}

func TestInspectStateRestartsWhenListedObjectDisappears(t *testing.T) {
	t.Parallel()
	r := &stateRunner{disappear: true, inventory: "tank/data\tfilesystem\t1\t123\t6\ntank/data@snap\tsnapshot\t2\t123\t6\n"}
	d := &Direct{runner: r}
	state, err := d.InspectState(t.Context(), "tank/data", false)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Objects) != 1 || state.Objects[0].Name != "tank/data" {
		t.Fatalf("unexpected restarted inventory: %#v", state.Objects)
	}
	listCalls := 0
	for _, call := range r.calls {
		if call[0] == "list" {
			listCalls++
		}
	}
	if listCalls != 2 {
		t.Fatalf("inventory list calls = %d, want 2", listCalls)
	}
}

func TestInspectStateScopeAndHiddenMetadata(t *testing.T) {
	t.Parallel()
	r := &stateRunner{inventory: "tank/data\tfilesystem\t1\t123\t6\ntank/data@snap\tsnapshot\t2\t123\t6\ntank/data#cursor\tbookmark\t2\t123\t6\ntank/data/child\tfilesystem\t3\t123\t6\n"}
	d := &Direct{runner: r}
	state, err := d.InspectState(t.Context(), "tank/data", false)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Objects) != 3 || len(state.Properties) != 2 || len(state.Holds["tank/data@snap"]) != 1 || state.Received["tank/data"][propertyNamespace+"state:lineage"] != "hidden" {
		t.Fatalf("unexpected state: %#v", state)
	}
	for _, args := range r.calls[1:] {
		for _, arg := range args {
			if strings.Contains(arg, "#") || arg == "tank/data/child" {
				t.Fatalf("queried excluded object: %v", args)
			}
		}
	}
	r.fail = true
	failed, err := d.InspectState(t.Context(), "tank/data", false)
	if err == nil || !reflect.DeepEqual(failed, State{}) {
		t.Fatal("returned partial failed inventory")
	}
}

func TestInspectStateRejectsInvalidInventory(t *testing.T) {
	t.Parallel()
	for _, row := range []string{"tank/data\tsnapshot\t1\t123\t6\n", "tank/data@snap\tfilesystem\t1\t123\t6\n", "other\tfilesystem\t1\t123\t6\n", "tank/data\tfilesystem\t0\t123\t6\n", "tank/data\tfilesystem\t1\t-1\t6\n", "tank/data@snap\tsnapshot\t2\t123\t6\n"} {
		d := &Direct{runner: &stateRunner{inventory: row}}
		if _, err := d.InspectState(t.Context(), "tank/data", false); err == nil {
			t.Errorf("accepted %q", row)
		}
	}
}

func TestNamespaceMutationArguments(t *testing.T) {
	t.Parallel()
	r := &fakeRunner{}
	d := &Direct{runner: r}
	if err := d.SetProperties(t.Context(), "tank/data", map[string]string{propertyNamespace + "enabled": "off", propertyNamespace + "policy": "1x1h"}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(r.args, []string{"set", propertyNamespace + "enabled=off", propertyNamespace + "policy=1x1h", "tank/data"}) {
		t.Fatal(r.args)
	}
	if err := d.InheritProperty(t.Context(), "tank/data@snap", propertyNamespace+"state:lineage"); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(r.args, []string{"inherit", propertyNamespace + "state:lineage", "tank/data@snap"}) {
		t.Fatal(r.args)
	}
	for _, name := range []string{"compression", propertyNamespace, propertyNamespace + "bad=value", propertyNamespace + "bad key"} {
		if err := d.InheritProperty(t.Context(), "tank/data", name); err == nil {
			t.Errorf("accepted %q", name)
		}
	}
}

func TestCreateReceiveParentArguments(t *testing.T) {
	t.Parallel()
	r := &fakeRunner{}
	d := &Direct{runner: r}
	if err := d.CreateReceiveParent(t.Context(), "backup/root/data"); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(r.args, []string{"create", "-o", "canmount=noauto", "-o", "org.boomerangz:state:placeholder=on", "backup/root/data"}) {
		t.Fatalf("args=%v", r.args)
	}
	if err := d.CreateReceiveParent(t.Context(), "-invalid"); err == nil {
		t.Fatal("accepted invalid dataset")
	}
}

func TestRenameDatasetArguments(t *testing.T) {
	t.Parallel()
	r := &fakeRunner{}
	d := &Direct{runner: r}
	if err := d.RenameDataset(t.Context(), "backup/root/hole", "backup/root/hole.boomerangz-placeholder"); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(r.args, []string{"rename", "backup/root/hole", "backup/root/hole.boomerangz-placeholder"}) {
		t.Fatalf("args=%v", r.args)
	}
	for _, pair := range [][2]string{
		{"backup/a", "other/a"},
		{"backup", "backup/a"},
		{"backup/a", "backup"},
		{"backup/a", "backup/a"},
		{"backup/a", "backup/a/b"},
		{"backup/a", "-x"},
	} {
		r.args = nil
		if err := d.RenameDataset(t.Context(), pair[0], pair[1]); err == nil || r.args != nil {
			t.Errorf("rename %s to %s accepted", pair[0], pair[1])
		}
	}
}

// recordedPool answers list, get and holds for a fixed set of objects, one or
// many per query, the way zfs does: one row per property of each object named.
type recordedPool struct {
	inventory string
	all       map[string]string // object -> its "get all" rows
	state     map[string]string // object -> its fixed-key rows
	holds     map[string]string // snapshot -> its hold rows
	calls     [][]string
}

func (r *recordedPool) Run(_ context.Context, args ...string) ([]byte, error) {
	r.calls = append(r.calls, slices.Clone(args))
	var out strings.Builder
	switch args[0] {
	case "list":
		return []byte(r.inventory), nil
	case "get":
		for _, object := range args[6:] {
			if args[5] == "all" {
				out.WriteString(r.all[object])
			} else {
				out.WriteString(r.state[object])
			}
		}
	case "holds":
		for _, snapshot := range args[2:] {
			out.WriteString(r.holds[snapshot])
		}
	}
	return []byte(out.String()), nil
}

func newRecordedPool() *recordedPool {
	ns := propertyNamespace
	return &recordedPool{
		inventory: "tank/data\tfilesystem\t1\t100\t5\n" +
			"tank/data@a\tsnapshot\t2\t101\t6\n" +
			"tank/data@b\tsnapshot\t3\t102\t7\n" +
			"tank/data@c\tsnapshot\t4\t103\t8\n" +
			"tank/data#a\tbookmark\t2\t101\t6\n" +
			"tank/data@d\tsnapshot\t5\t104\t9\n",
		all: map[string]string{
			"tank/data": "tank/data\t" + ns + "enabled\ton\t-\tlocal\n" +
				"tank/data\t" + ns + "state:owner\towner\t-\tlocal\n" +
				"tank/data\treceive_resume_token\t1-token\t-\t-\n" +
				"tank/data\tcompression\tzstd\t-\tlocal\n",
			"tank/data@a": "tank/data@a\t" + ns + "state:snapshot\tsnap-a\t-\tlocal\n" +
				"tank/data@a\tclones\tpool/clone\t-\t-\n",
			"tank/data@b": "tank/data@b\t" + ns + "state:snapshot\tsnap-b\tsnap-b\treceived\n",
			"tank/data@c": "tank/data@c\t" + ns + "enabled\toff\t-\tlocal\n",
			"tank/data@d": "",
		},
		state: map[string]string{
			"tank/data":   "tank/data\t" + ns + "state:lineage\tline\t-\tlocal\n",
			"tank/data@a": "tank/data@a\t" + ns + "state:lineage\t-\thidden-a\t-\n",
			"tank/data@b": "tank/data@b\t" + ns + "state:snapshot\tsnap-b\tsnap-b\treceived\n",
			"tank/data@c": "",
			"tank/data@d": "tank/data@d\t" + ns + "state:lineage\tline\t-\tlocal\n",
		},
		holds: map[string]string{
			"tank/data@a": "tank/data@a\tz-hold\t123\ntank/data@a\ta-hold\t123\n",
			"tank/data@c": "tank/data@c\tonly\t123\n",
		},
	}
}

func TestBatchedInspectionMatchesOneQueryPerObject(t *testing.T) {
	t.Parallel()
	perObject := &Direct{runner: newRecordedPool(), batch: 1}
	want, err := perObject.InspectState(t.Context(), "tank/data", false)
	if err != nil {
		t.Fatal(err)
	}
	// Two per batch splits the objects across batch boundaries; the default
	// reads them all at once.
	for _, batch := range []int{2, 0} {
		pool := newRecordedPool()
		got, err := (&Direct{runner: pool, batch: batch}).InspectState(t.Context(), "tank/data", false)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("batch %d: state differs from one query per object:\n got %#v\nwant %#v", batch, got, want)
		}
		if batch == 0 && len(pool.calls) != 4 {
			t.Fatalf("one inspection ran %d commands, want list, two gets and holds: %v", len(pool.calls), pool.calls)
		}
	}
	// The fixture exercises what the regrouping must preserve.
	if want.ResumeTokens["tank/data"] != "1-token" || len(want.Clones["tank/data@a"]) != 1 || want.Received["tank/data@a"][propertyNamespace+"state:lineage"] != "hidden-a" || !slices.Equal(want.Holds["tank/data@a"], []string{"a-hold", "z-hold"}) {
		t.Fatalf("fixture not exercised: %#v", want)
	}
}

func TestBatchedInspectionRefusesARowForAnUnnamedObject(t *testing.T) {
	t.Parallel()
	pool := newRecordedPool()
	pool.all["tank/data@b"] += "tank/data@elsewhere\tcompression\tlz4\t-\tlocal\n"
	if _, err := (&Direct{runner: pool}).InspectState(t.Context(), "tank/data", false); err == nil || !strings.Contains(err.Error(), "outside exact object query") {
		t.Fatalf("a row for an object the query did not name was accepted: %v", err)
	}
}

func TestInspectionIgnoresTheOrderZFSListsPropertiesIn(t *testing.T) {
	t.Parallel()
	ns := propertyNamespace
	ordered := newRecordedPool()
	ordered.all["tank/data@a"] = "tank/data@a\t" + ns + "enabled\ton\t-\tlocal\n" + "tank/data@a\t" + ns + "state:lineage\tline\t-\tlocal\n" + "tank/data@a\t" + ns + "state:snapshot\tsnap-a\t-\tlocal\n"
	reversed := newRecordedPool()
	reversed.all["tank/data@a"] = "tank/data@a\t" + ns + "state:snapshot\tsnap-a\t-\tlocal\n" + "tank/data@a\t" + ns + "state:lineage\tline\t-\tlocal\n" + "tank/data@a\t" + ns + "enabled\ton\t-\tlocal\n"
	want, err := (&Direct{runner: ordered}).InspectState(t.Context(), "tank/data", false)
	if err != nil {
		t.Fatal(err)
	}
	got, err := (&Direct{runner: reversed}).InspectState(t.Context(), "tank/data", false)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("the same properties in another order read as a different state:\n got %#v\nwant %#v", got.Properties, want.Properties)
	}
}
