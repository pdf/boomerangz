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
		{Path: "/dev/disk/by-id/virtio-test-a", Serial: "boomerangz-test-run-123-a"},
	})
	if err != nil {
		t.Fatal(err)
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
