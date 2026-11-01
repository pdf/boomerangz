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
				out.WriteString(projectRows(args[4], object+"\torg.boomerangz:state:lineage\t-\thidden\t-\n"))
				continue
			}
			out.WriteString(projectRows(args[4], object+"\torg.boomerangz:enabled\ton\t-\tlocal\n"+object+"\tcompression\tzstd\t-\tlocal\n"))
		}
		return []byte(out.String()), nil
	}
	return nil, nil
}

// projectRows answers a property query the way zfs does, with only the columns
// it asked for: rows are written as name, property, value, received, source.
func projectRows(columns, rows string) string {
	index := map[string]int{"name": 0, "property": 1, "value": 2, "received": 3, "source": 4}
	var out strings.Builder
	for row := range strings.Lines(rows) {
		fields := strings.Split(strings.TrimSuffix(row, "\n"), "\t")
		var projected []string
		for column := range strings.SplitSeq(columns, ",") {
			projected = append(projected, fields[index[column]])
		}
		out.WriteString(strings.Join(projected, "\t") + "\n")
	}
	return out.String()
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

func TestDestroySnapshotNamesExactlyOneSnapshot(t *testing.T) {
	t.Parallel()
	r := &fakeRunner{}
	d := &Direct{runner: r}
	if err := d.DestroySnapshot(t.Context(), "backup/a@s"); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(r.args, []string{"destroy", "backup/a@s"}) {
		t.Fatalf("args=%v", r.args)
	}
	// A dataset, bookmark, list, or range would destroy more than one snapshot.
	for _, name := range []string{"backup/a", "backup/a#b", "backup/a@s@t", "backup/a@s,t", "backup/a@s%t", "backup/a@", "-x@s"} {
		r.args = nil
		if err := d.DestroySnapshot(t.Context(), name); err == nil || r.args != nil {
			t.Errorf("destroy %s accepted", name)
		}
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
// many per query, the way zfs does: one row per property of each object named,
// with the columns asked for. A query for all lists what all shows; a query
// naming keys answers each of them, hidden or not, and "-" for an unset one.
type recordedPool struct {
	inventory string
	all       map[string]string // object -> the rows "get all" lists
	hidden    map[string]string // object -> rows only a query naming the key reveals
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
		columns, keys := args[4], args[5]
		for _, object := range args[6:] {
			if keys == "all" {
				out.WriteString(projectRows(columns, r.all[object]))
				continue
			}
			for key := range strings.SplitSeq(keys, ",") {
				row := object + "\t" + key + "\t-\t-\t-\n"
				for line := range strings.Lines(r.all[object] + r.hidden[object]) {
					if strings.Split(line, "\t")[1] == key {
						row = line
					}
				}
				out.WriteString(projectRows(columns, row))
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
			// enabled is set locally over a received value it masks.
			"tank/data": "tank/data\t" + ns + "enabled\ton\toff\tlocal\n" +
				"tank/data\t" + ns + "state:owner\towner\t-\tlocal\n" +
				"tank/data\t" + ns + "state:lineage\tline\t-\tlocal\n" +
				"tank/data\t" + ns + "state:target:t\tbinding\t-\tlocal\n" +
				"tank/data\treceive_resume_token\t1-token\t-\t-\n" +
				"tank/data\tcompression\tzstd\tlz4\tlocal\n",
			"tank/data@a": "tank/data@a\t" + ns + "state:snapshot\tsnap-a\t-\tlocal\n" +
				"tank/data@a\tclones\tpool/clone\t-\t-\n",
			"tank/data@b": "tank/data@b\t" + ns + "state:snapshot\tsnap-b\tsnap-b\treceived\n",
			"tank/data@c": "tank/data@c\t" + ns + "enabled\toff\t-\tlocal\n",
			"tank/data@d": "tank/data@d\t" + ns + "state:lineage\tline\t-\tlocal\n",
		},
		hidden: map[string]string{
			"tank/data@a": "tank/data@a\t" + ns + "state:lineage\t-\thidden-a\t-\n",
			// all omits this, and it is no fixed key, so it is not read.
			"tank/data@c": "tank/data@c\t" + ns + "state:target:t\t-\thidden-c\t-\n",
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

// The received column multiplies what a property query costs in ZFS, so it is
// asked only for the namespace's keys, never for all, and the state read is
// still what one query for all with the received column would have produced.
func TestInspectionAsksForReceivedValuesOnlyInTheNamespace(t *testing.T) {
	t.Parallel()
	ns := propertyNamespace
	pool := newRecordedPool()
	state, err := (&Direct{runner: pool}).InspectState(t.Context(), "tank/data", false)
	if err != nil {
		t.Fatal(err)
	}
	var received []string
	for _, call := range pool.calls {
		if call[0] != "get" || !strings.Contains(call[4], "received") {
			continue
		}
		if call[5] == "all" {
			t.Fatalf("asked for the received column of every property: %v", call)
		}
		received = append(received, call[5])
	}
	want := ns + "enabled," + ns + "state:lineage," + ns + "state:owner," + ns + "state:snapshot," + ns + "state:target:t"
	if !slices.Equal(received, []string{want}) {
		t.Fatalf("received column asked for %q, want the keys all listed and the fixed keys %q", received, want)
	}
	// A received value masked by a local one, and a hidden fixed key.
	if state.Received["tank/data"][ns+"enabled"] != "off" || state.Received["tank/data@a"][ns+"state:lineage"] != "hidden-a" {
		t.Fatalf("received values not read: %#v", state.Received)
	}
	// Named for the chunk because tank/data lists it, but all omits it on
	// tank/data@c and it is no fixed key, so it is not read there.
	if value, ok := state.Received["tank/data@c"][ns+"state:target:t"]; ok {
		t.Fatalf("read a hidden value all omits for a key that is not fixed: %q", value)
	}
	for object, values := range state.Received {
		if _, ok := values["compression"]; ok {
			t.Fatalf("read a received value outside the namespace on %s", object)
		}
	}
	if !slices.Contains(state.Properties, Property{Dataset: "tank/data@b", Name: ns + "state:snapshot", Value: "snap-b", Source: SourceReceived}) {
		t.Fatalf("received property missing: %#v", state.Properties)
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
