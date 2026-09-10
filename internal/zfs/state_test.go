package zfs

import (
	"context"
	"errors"
	"fmt"
	"reflect"
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
		return []byte("tank/data@snap\tforeign-hold\t123\n"), nil
	case "get":
		object := args[len(args)-1]
		if (r.disappear || r.unstable) && object == "tank/data@snap" {
			r.disappear = false
			if !r.unstable {
				r.inventory = "tank/data\tfilesystem\t1\t123\t6\n"
			}
			return nil, fmt.Errorf("zfs get: exit status 1: cannot open 'tank/data@snap': dataset does not exist")
		}
		if args[len(args)-2] != "all" {
			return []byte(object + "\torg.boomerangz:state:lineage\t-\thidden\t-\n"), nil
		}
		return []byte(object + "\torg.boomerangz:enabled\ton\t-\tlocal\n" + object + "\tcompression\tzstd\t-\tlocal\n"), nil
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
		if strings.Contains(args[len(args)-1], "#") || args[len(args)-1] == "tank/data/child" {
			t.Fatalf("queried excluded object: %v", args)
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
	if !reflect.DeepEqual(r.args, []string{"create", "-o", "canmount=noauto", "backup/root/data"}) {
		t.Fatalf("args=%v", r.args)
	}
	if err := d.CreateReceiveParent(t.Context(), "-invalid"); err == nil {
		t.Fatal("accepted invalid dataset")
	}
}
