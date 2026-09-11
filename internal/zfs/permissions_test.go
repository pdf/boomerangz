package zfs

import (
	"context"
	"reflect"
	"testing"
)

func TestEffectivePermissionsIncludesUserGroupsEveryoneAndSets(t *testing.T) {
	output := []byte(`---- Permissions on tank -----------------------------------------
Permission sets:
	@source send,@references
	@references bookmark,hold,release
Descendent permissions:
	group backup destroy
Local+Descendent permissions:
	everyone userprop
	user boomerangz @source,snapshot
---- Permissions on tank/data ------------------------------------
Local permissions:
	group operators compression
Descendent permissions:
	user boomerangz create
`)
	blocks, err := parsePermissionBlocks(output)
	if err != nil {
		t.Fatal(err)
	}
	got := effectivePermissions(blocks, "tank/data", effectiveIdentity{name: "boomerangz", groups: map[string]bool{"backup": true, "operators": true}})
	want := map[string]bool{"bookmark": true, "compression": true, "destroy": true, "hold": true, "release": true, "send": true, "snapshot": true, "userprop": true}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("permissions = %#v, want %#v", got, want)
	}
}

func TestEffectivePermissionsHonoursDelegationScope(t *testing.T) {
	output := []byte(`---- Permissions on tank/data ------------------------------------
Local permissions:
	user boomerangz snapshot
Descendent permissions:
	user boomerangz create
Local+Descendent permissions:
	group backup receive
`)
	blocks, err := parsePermissionBlocks(output)
	if err != nil {
		t.Fatal(err)
	}
	identity := effectiveIdentity{name: "boomerangz", groups: map[string]bool{"backup": true}}
	if got := effectivePermissions(blocks, "tank/data", identity); !reflect.DeepEqual(got, map[string]bool{"receive": true, "receive:append": true, "snapshot": true}) {
		t.Fatalf("local permissions = %#v", got)
	}
	if got := effectivePermissions(blocks, "tank/data/child", identity); !reflect.DeepEqual(got, map[string]bool{"create": true, "receive": true, "receive:append": true}) {
		t.Fatalf("descendent permissions = %#v", got)
	}
}

func TestReceivePermissionIncludesAppendOnlyReceive(t *testing.T) {
	blocks, err := parsePermissionBlocks([]byte("---- Permissions on tank/data ----\nLocal+Descendent permissions:\n\tuser boomerangz receive\n"))
	if err != nil {
		t.Fatal(err)
	}
	got := effectivePermissions(blocks, "tank/data", effectiveIdentity{name: "boomerangz", groups: map[string]bool{}})
	if !got["receive:append"] {
		t.Fatal("full receive permission did not satisfy append-only receive")
	}
}

func TestCheckPermissionsReportsSortedMissingPermissions(t *testing.T) {
	zfsRunner := &fakeRunner{output: []byte("---- Permissions on tank/data ----\nLocal+Descendent permissions:\n\tuser boomerangz send,snapshot\n")}
	identity := &sequenceRunner{outputs: [][]byte{[]byte("1000\n"), []byte("boomerangz\n"), []byte("boomerangz storage\n")}}
	direct, err := NewDirectWithRunnersAndIdentity(zfsRunner, &fakeRunner{}, identity)
	if err != nil {
		t.Fatal(err)
	}
	err = direct.CheckPermissions(t.Context(), "tank/data", []string{"userprop", "send", "hold"})
	if err == nil || err.Error() != "effective account boomerangz lacks delegated ZFS permissions on tank/data: hold,userprop" {
		t.Fatalf("error = %v", err)
	}
}

// A dataset with nothing delegated at or above it makes `zfs allow` print
// nothing. Reporting that as unreadable output hides the one diagnostic an
// operator can act on: which account is missing which permissions where.
func TestCheckPermissionsReportsEveryPermissionMissingWhenNothingIsDelegated(t *testing.T) {
	zfsRunner := &fakeRunner{output: []byte("")}
	identity := &sequenceRunner{outputs: [][]byte{[]byte("1000\n"), []byte("boomerangz\n"), []byte("boomerangz storage\n")}}
	direct, err := NewDirectWithRunnersAndIdentity(zfsRunner, &fakeRunner{}, identity)
	if err != nil {
		t.Fatal(err)
	}
	err = direct.CheckPermissions(t.Context(), "tank", []string{"create", "receive:append"})
	if err == nil || err.Error() != "effective account boomerangz lacks delegated ZFS permissions on tank: create,receive:append" {
		t.Fatalf("error = %v", err)
	}
}

func TestCheckPermissionsAllowsRootWithoutDelegationQuery(t *testing.T) {
	zfsRunner := &fakeRunner{}
	identity := &sequenceRunner{outputs: [][]byte{[]byte("0\n")}}
	direct, err := NewDirectWithRunnersAndIdentity(zfsRunner, &fakeRunner{}, identity)
	if err != nil {
		t.Fatal(err)
	}
	if err := direct.CheckPermissions(t.Context(), "tank/data", []string{"snapshot"}); err != nil {
		t.Fatal(err)
	}
	if len(zfsRunner.args) != 0 {
		t.Fatalf("root made ZFS delegation query: %#v", zfsRunner.args)
	}
}

type sequenceRunner struct {
	outputs [][]byte
	calls   int
}

func (r *sequenceRunner) Run(_ context.Context, _ ...string) ([]byte, error) {
	result := r.outputs[r.calls]
	r.calls++
	return result, nil
}
