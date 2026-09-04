package zfstest

import (
	"os"
	"path/filepath"
	"testing"
)

func TestVerifyGuestGuard(t *testing.T) {
	t.Parallel()
	marker := filepath.Join(t.TempDir(), "guest-marker")
	if err := os.WriteFile(marker, []byte("run-123\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := VerifyGuestGuard(marker, "run-123", "boomerangz-test-run-123-source", []Vdev{
		{Path: "/dev/disk/by-id/virtio-test-a", Serial: DiskSerial("run-123", SourceDisk)},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestDiskSerialFitsVirtioLimit(t *testing.T) {
	t.Parallel()
	serial := DiskSerial("run-1234567890-very-long", DestinationDisk)
	if len(serial) > 20 {
		t.Fatalf("serial length = %d, want at most 20: %q", len(serial), serial)
	}
}

func TestVerifyGuestGuardRejectsWrongSerial(t *testing.T) {
	t.Parallel()
	marker := filepath.Join(t.TempDir(), "guest-marker")
	if err := os.WriteFile(marker, []byte("run-123"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := VerifyGuestGuard(marker, "run-123", "boomerangz-test-run-123-source", []Vdev{
		{Path: "/dev/vdb", Serial: "unrelated-disk"},
	})
	if err == nil {
		t.Fatal("VerifyGuestGuard accepted an unrelated disk")
	}
}
