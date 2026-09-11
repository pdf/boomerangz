//go:build integration

package transfer_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/pdf/boomerangz/internal/lifecycle"
	"github.com/pdf/boomerangz/internal/policy"
	"github.com/pdf/boomerangz/internal/testutil/zfstest"
	"github.com/pdf/boomerangz/internal/transfer"
	"github.com/pdf/boomerangz/internal/zfs"
)

// TestGuestEncryptedTransfer covers the encryption paths against a real
// encryption root, a real key and real raw streams.
//
// The refusals themselves are unit-tested over hand-built zfs.Dataset values
// (see policy's resolve tests and TestBuildRefusesEncryptedPropertyStreamWithoutRaw).
// What only a real pool can show is that ZFS's own encryptionroot reaches
// those guards at all: every code path here keys off Dataset.EncryptionRoot,
// which zfs.Direct.ListDatasets reads from the pool, and a fake can only
// return what it was told. So each phase resolves policy from a dataset the
// pool described, not from a literal.
func TestGuestEncryptedTransfer(t *testing.T) {
	runID := os.Getenv("BOOMERANGZ_TRANSFER_GUEST_RUN")
	if runID == "" {
		t.Fatal("BOOMERANGZ_TRANSFER_GUEST_RUN is unset: the disposable guest harness did not provide a run ID")
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
	command := func(t *testing.T, args ...string) string {
		t.Helper()
		out, cmdErr := exec.CommandContext(t.Context(), "zfs", args...).CombinedOutput()
		if cmdErr != nil {
			t.Fatalf("guest zfs %s: %v: %s", args[0], cmdErr, out)
		}
		return strings.TrimSpace(string(out))
	}
	direct, err := zfs.NewDirect("zfs")
	if err != nil {
		t.Fatal(err)
	}
	stream, err := zfs.NewLocalStream("zfs")
	if err != nil {
		t.Fatal(err)
	}
	const installation = "abcdefab-cdef-4abc-8def-abcdefabcdef"
	engine, err := transfer.NewLocal(direct, stream, installation)
	if err != nil {
		t.Fatal(err)
	}
	snapshots, err := lifecycle.NewService(direct, installation)
	if err != nil {
		t.Fatal(err)
	}

	// The key outlives the test only as long as its directory does, which is
	// the whole point: nothing here should survive into another test's pool
	// state. keylocation is recorded on the dataset, so the path must stay
	// valid for every phase below.
	keyFile := filepath.Join(t.TempDir(), "encryption.key")
	if err := os.WriteFile(keyFile, []byte("integration-test-passphrase"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Dir(keyFile), 0755); err != nil {
		t.Fatal(err)
	}

	source := zfstest.FixtureName(sourcePool, "encrypted")
	command(t, "create", "-u",
		"-o", "encryption=aes-256-gcm",
		"-o", "keyformat=passphrase",
		"-o", "keylocation=file://"+keyFile,
		source)
	zfstest.RegisterCleanup(t, source)

	// described returns the dataset as the pool describes it, so the policy
	// under test sees ZFS's encryptionroot rather than a literal.
	described := func(t *testing.T, name string) zfs.Dataset {
		t.Helper()
		inventory, listErr := direct.ListDatasets(t.Context())
		if listErr != nil {
			t.Fatal(listErr)
		}
		for _, dataset := range inventory {
			if dataset.Name == name {
				return dataset
			}
		}
		t.Fatalf("%s is missing from the pool inventory", name)
		return zfs.Dataset{}
	}
	resolve := func(t *testing.T, name string) policy.Effective {
		t.Helper()
		rows, rowErr := direct.GetStoredProperties(t.Context(), []string{name})
		if rowErr != nil {
			t.Fatal(rowErr)
		}
		return policy.Resolve(described(t, name), nil, rows, nil)
	}

	chainOK := true
	chain := func(name string, fn func(*testing.T)) {
		if !chainOK {
			t.Run(name, func(t *testing.T) { t.Skip("depends on an earlier phase that failed") })
			return
		}
		chainOK = t.Run(name, fn)
	}
	// Registered here rather than in the phase that creates it: a cleanup
	// registered inside a subtest runs when that subtest ends, which would
	// destroy the destination the later phases still need.
	destination := destinationPool + "/data/encrypted-" + time.Now().UTC().Format("150405.000")
	zfstest.RegisterCleanup(t, destination)

	chain("raw-forced-for-encryption-root", func(t *testing.T) {
		if got := described(t, source).EncryptionRoot; got != source {
			t.Fatalf("pool reports encryptionroot=%q, want %q", got, source)
		}
		command(t, "set", policy.Namespace+"enabled=on", policy.Namespace+"local="+destination, source)
		effective := resolve(t, source)
		if !effective.Send.Raw {
			t.Fatal("raw was not defaulted on for a real encryption root")
		}
		if !effective.Valid() {
			t.Fatalf("policy invalid: %v", effective.Errors)
		}
		if _, err := snapshots.CreateSnapshot(t.Context(), source, false, time.Now().UTC(), effective); err != nil {
			t.Fatal(err)
		}
		result, err := engine.Apply(t.Context(), transfer.Request{Source: source, DestinationRoot: destination, Policy: effective}, nil)
		if err != nil || !result.Verified || result.Plan.Mode != "full" {
			t.Fatalf("raw bootstrap=%+v err=%v", result, err)
		}
		// A raw stream of an encryption root makes the receiving dataset an
		// encryption root in its own right, rather than a child of the
		// destination's key.
		received := described(t, result.Plan.Destination)
		if received.EncryptionRoot != result.Plan.Destination {
			t.Fatalf("received encryptionroot=%q, want %q", received.EncryptionRoot, result.Plan.Destination)
		}
		if got := command(t, "get", "-H", "-o", "value", "encryption", result.Plan.Destination); got != "aes-256-gcm" {
			t.Fatalf("received encryption=%s", got)
		}
	})

	chain("replicate-without-raw-refused", func(t *testing.T) {
		command(t, "set", policy.Namespace+"replicate=on", policy.Namespace+"raw=off", source)
		effective := resolve(t, source)
		if effective.Valid() {
			t.Fatal("encrypted recursive replication accepted without raw")
		}
		if !slices.ContainsFunc(effective.Errors, func(e string) bool { return strings.Contains(e, "requires raw=on") }) {
			t.Fatalf("errors=%v", effective.Errors)
		}
	})

	chain("props-without-raw-refused", func(t *testing.T) {
		// Not a policy error - the planner is what refuses this combination,
		// and it reads encryptionroot from its own inventory of the real pool.
		command(t, "set", policy.Namespace+"replicate=off", policy.Namespace+"props=on", policy.Namespace+"raw=off", source)
		effective := resolve(t, source)
		if !effective.Valid() {
			t.Fatalf("policy rejected before the planner could: %v", effective.Errors)
		}
		_, err := engine.Preview(t.Context(), transfer.Request{Source: source, DestinationRoot: destination, Policy: effective})
		if err == nil || !strings.Contains(err.Error(), "raw mode") {
			t.Fatalf("encrypted property stream without raw: err=%v", err)
		}
	})

	chain("key-unavailable", func(t *testing.T) {
		command(t, "set", policy.Namespace+"props=off", policy.Namespace+"raw=on", source)
		command(t, "unload-key", source)
		if got := command(t, "get", "-H", "-o", "value", "keystatus", source); got != "unavailable" {
			t.Fatalf("keystatus=%s after unload-key", got)
		}
		// A raw stream is ciphertext end to end, so it neither needs nor can
		// use the key. This is the property that makes raw the default for
		// encrypted sources.
		effective := resolve(t, source)
		if !effective.Send.Raw {
			t.Fatal("raw was not in effect for the key-unavailable transfer")
		}
		if _, err := snapshots.CreateSnapshot(t.Context(), source, false, time.Now().UTC(), effective); err != nil {
			t.Fatal(err)
		}
		result, err := engine.Apply(t.Context(), transfer.Request{Source: source, DestinationRoot: destination, Policy: effective}, nil)
		if err != nil || !result.Verified {
			t.Fatalf("raw transfer with key unloaded=%+v err=%v", result, err)
		}
		// The same send without raw has to read plaintext, and cannot. A fresh
		// snapshot first: the raw transfer above brought the destination up to
		// date, so without new work "it succeeded" would only mean there was
		// nothing to send, and the phase would prove nothing either way.
		command(t, "set", policy.Namespace+"raw=off", source)
		plain := resolve(t, source)
		if plain.Send.Raw {
			t.Fatal("raw=off was not honoured")
		}
		if _, err := snapshots.CreateSnapshot(t.Context(), source, false, time.Now().UTC().Add(time.Minute), plain); err != nil {
			t.Fatal(err)
		}
		plainRequest := transfer.Request{Source: source, DestinationRoot: destination, Policy: plain}
		preview, err := engine.Preview(t.Context(), plainRequest)
		if err != nil {
			t.Fatalf("non-raw preview: %v", err)
		}
		if preview.Mode == "" {
			t.Fatal("nothing left to send, so the refusal below would prove nothing")
		}
		if _, err := engine.Apply(t.Context(), plainRequest, nil); err == nil {
			t.Fatalf("non-raw %s send succeeded with the key unavailable", preview.Mode)
		} else {
			t.Logf("non-raw %s send refused with the key unavailable: %v", preview.Mode, err)
		}
		command(t, "load-key", "-L", "file://"+keyFile, source)
		if got := command(t, "get", "-H", "-o", "value", "keystatus", source); got != "available" {
			t.Fatalf("keystatus=%s after load-key", got)
		}
		command(t, "set", policy.Namespace+"raw=on", source)
	})

	// Independent of the chain: its own source and its own encrypted root.
	t.Run("destination-under-encryption-root", func(t *testing.T) {
		plainSource := zfstest.FixtureName(sourcePool, "plain-under-enc")
		command(t, "create", "-u", plainSource)
		zfstest.RegisterCleanup(t, plainSource)
		// The encryption root has to be the parent: with discard=off the
		// destination root is itself the received dataset, so receiving onto an
		// existing encryption root would be a collision, not an inheritance.
		encryptedParent := destinationPool + "/data/encparent-" + time.Now().UTC().Format("150405.000")
		command(t, "create", "-u",
			"-o", "encryption=aes-256-gcm",
			"-o", "keyformat=passphrase",
			"-o", "keylocation=file://"+keyFile,
			encryptedParent)
		zfstest.RegisterCleanup(t, encryptedParent)
		encryptedRoot := encryptedParent + "/received"
		command(t, "set", policy.Namespace+"enabled=on", policy.Namespace+"local="+encryptedRoot, plainSource)
		effective := resolve(t, plainSource)
		if effective.Send.Raw {
			t.Fatal("raw was defaulted on for an unencrypted source")
		}
		if _, err := snapshots.CreateSnapshot(t.Context(), plainSource, false, time.Now().UTC(), effective); err != nil {
			t.Fatal(err)
		}
		result, err := engine.Apply(t.Context(), transfer.Request{Source: plainSource, DestinationRoot: encryptedRoot, Policy: effective}, nil)
		if err != nil || !result.Verified {
			t.Fatalf("receive under an encryption root=%+v err=%v", result, err)
		}
		// Unencrypted data received beneath an encryption root is encrypted by
		// it, and inherits its key rather than becoming a root of its own.
		received := described(t, result.Plan.Destination)
		if received.EncryptionRoot != encryptedParent {
			t.Fatalf("received encryptionroot=%q, want %q", received.EncryptionRoot, encryptedParent)
		}
		if got := command(t, "get", "-H", "-o", "value", "encryption", result.Plan.Destination); got == "off" {
			t.Fatal("data received beneath an encryption root was left unencrypted")
		}
	})
}
