//go:build integration

package transfer_test

import (
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"

	"github.com/pdf/boomerangz/internal/policy"
	"github.com/pdf/boomerangz/internal/testutil/zfstest"
	"github.com/pdf/boomerangz/internal/zfs"
)

// TestGuestReceivedPropertyLayers pins the received-property semantics that
// internal/transfer/binding.go depends on.
//
// A received value can survive with no effective value and no source. ZFS
// reaches that state by two routes - "receive -x", and a plain "zfs inherit"
// afterwards - and in both the value is invisible in the source column while
// the received column still carries it.
//
// What makes such a value dangerous is that "zfs get all" omits it: a user
// property with no effective value is simply not listed. Naming the property
// explicitly does return it. That asymmetry is why InspectState queries twice
// - once for "all", once for the fixed ownership keys - and it is the whole
// reason State.Received exists separately from State.Properties, since
// binding.go treats a received value it can see as ambiguous rather than
// absent.
//
// So the contract has two halves, and the test asserts both: an ownership key
// that InspectState names explicitly stays visible once hidden, and a property
// it does not name disappears from the inventory entirely.
//
// This is kernel behaviour, so a fake zfs.Executor cannot express it - it
// would only assert what the fake was told to return. It replaced
// guest/property-layers.sh, which printed these columns and asserted nothing.
func TestGuestReceivedPropertyLayers(t *testing.T) {
	runID := os.Getenv("BOOMERANGZ_TRANSFER_GUEST_RUN")
	if runID == "" {
		t.Skip("disposable guest only")
	}
	sourceDevice := os.Getenv("BOOMERANGZ_INTEGRATION_SOURCE_DEVICE")
	destinationDevice := os.Getenv("BOOMERANGZ_INTEGRATION_DESTINATION_DEVICE")
	if sourceDevice == "" || destinationDevice == "" {
		t.Fatal("source and destination test devices are required")
	}
	sourcePool, err := zfstest.VerifyGuestPool(t.Context(), runID, zfstest.SourceDisk, sourceDevice)
	if err != nil {
		t.Fatal(err)
	}
	destinationPool, err := zfstest.VerifyGuestPool(t.Context(), runID, zfstest.DestinationDisk, destinationDevice)
	if err != nil {
		t.Fatal(err)
	}
	direct, err := zfs.NewDirect("zfs")
	if err != nil {
		t.Fatal(err)
	}
	command := func(t *testing.T, args ...string) string {
		t.Helper()
		out, cmdErr := exec.CommandContext(t.Context(), "zfs", args...).CombinedOutput()
		if cmdErr != nil {
			t.Fatalf("guest zfs %s: %v: %s", args[0], cmdErr, out)
		}
		return strings.TrimSpace(string(out))
	}

	property := policy.Namespace + "clean-probe"
	// One of the fixed ownership keys InspectState names explicitly.
	metadata := policy.StateNamespace + "snapshot"
	source := zfstest.FixtureName(sourcePool, "properties")
	target := zfstest.FixtureName(destinationPool, "property-layers")
	command(t, "create", "-u", source)
	zfstest.RegisterCleanup(t, source)
	command(t, "set", property+"=on", source)
	command(t, "snapshot", "-o", metadata+"=probe", source+"@property-layers")

	// -p carries the properties; -x excludes one of them at the receiving end,
	// which is the first of the two routes to a hidden received value.
	send := exec.CommandContext(t.Context(), "zfs", "send", "-p", source+"@property-layers")
	receive := exec.CommandContext(t.Context(), "zfs", "receive", "-u", "-x", property, target)
	pipe, err := send.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	receive.Stdin = pipe
	var receiveOutput strings.Builder
	receive.Stderr = &receiveOutput
	if err := send.Start(); err != nil {
		t.Fatal(err)
	}
	if err := receive.Run(); err != nil {
		t.Fatalf("receive: %v: %s", err, receiveOutput.String())
	}
	if err := send.Wait(); err != nil {
		t.Fatalf("send: %v", err)
	}
	zfstest.RegisterCleanup(t, target)

	// layers reports the raw columns InspectState parses, for one object.
	layers := func(t *testing.T, object, name string) (value, received, source string) {
		t.Helper()
		fields := strings.Split(command(t, "get", "-H", "-p", "-o", "value,received,source", name, object), "\t")
		if len(fields) != 3 {
			t.Fatalf("unexpected column count for %s on %s: %q", name, object, fields)
		}
		return fields[0], fields[1], fields[2]
	}
	// assertInventory checks how the raw columns above reach the state
	// boomerangz actually reasons over. It takes the subtest's own *testing.T
	// so a failure is reported against the case that caused it.
	assertInventory := func(t *testing.T, object, name string, wantProperties, wantReceived bool) {
		t.Helper()
		state, stateErr := direct.InspectState(t.Context(), target, true)
		if stateErr != nil {
			t.Fatal(stateErr)
		}
		_, inReceived := state.Received[object][name]
		inProperties := slices.ContainsFunc(state.Properties, func(p zfs.Property) bool {
			return p.Dataset == object && p.Name == name
		})
		if inProperties != wantProperties {
			t.Errorf("%s on %s: State.Properties=%t, want %t", name, object, inProperties, wantProperties)
		}
		if inReceived != wantReceived {
			t.Errorf("%s on %s: State.Received=%t, want %t", name, object, inReceived, wantReceived)
		}
	}

	// The cases are sequential: each mutation acts on the state the previous
	// one left, which is the point - the transitions matter as much as the
	// resting states.
	for _, step := range []struct {
		name       string
		mutate     []string
		value      string
		received   string
		source     string
		properties bool
		receivedIn bool
	}{
		// clean-probe is not an ownership key, so once hidden it leaves the
		// inventory altogether: "all" does not list it and nothing names it.
		{name: "receive-exclusion", value: "-", received: "on", source: "-"},
		{name: "restore-received", mutate: []string{"inherit", "-S", property, target}, value: "on", received: "on", source: "received", properties: true, receivedIn: true},
		{name: "local-override", mutate: []string{"set", property + "=off", target}, value: "off", received: "on", source: "local", properties: true, receivedIn: true},
		{name: "inherit-hides-received", mutate: []string{"inherit", property, target}, value: "-", received: "on", source: "-"},
		{name: "restore-after-inherit", mutate: []string{"inherit", "-S", property, target}, value: "on", received: "on", source: "received", properties: true, receivedIn: true},
	} {
		t.Run(step.name, func(t *testing.T) {
			if len(step.mutate) > 0 {
				command(t, step.mutate...)
			}
			value, received, source := layers(t, target, property)
			if value != step.value || received != step.received || source != step.source {
				t.Fatalf("value=%q received=%q source=%q, want %q/%q/%q",
					value, received, source, step.value, step.received, step.source)
			}
			assertInventory(t, target, property, step.properties, step.receivedIn)
		})
	}

	// The ownership key half of the contract. The raw columns match the
	// clean-probe cases exactly, but because InspectState names this key it
	// stays in State.Received after being hidden - which is what lets
	// boomerangz tell "no value" apart from "a received value I must resolve".
	snapshot := target + "@property-layers"
	t.Run("snapshot-metadata", func(t *testing.T) {
		value, received, source := layers(t, snapshot, metadata)
		if value != "probe" || received != "probe" || source != "received" {
			t.Fatalf("value=%q received=%q source=%q, want probe/probe/received", value, received, source)
		}
		assertInventory(t, snapshot, metadata, true, true)
	})
	t.Run("snapshot-metadata-after-inherit", func(t *testing.T) {
		command(t, "inherit", metadata, snapshot)
		value, received, source := layers(t, snapshot, metadata)
		if value != "-" || received != "probe" || source != "-" {
			t.Fatalf("value=%q received=%q source=%q, want -/probe/-", value, received, source)
		}
		assertInventory(t, snapshot, metadata, false, true)
	})
}
