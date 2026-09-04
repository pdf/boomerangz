// Package zfstest contains safety primitives for destructive guest-only tests.
package zfstest

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"strings"
)

const objectPrefix = "boomerangz-test-"

// DiskRole distinguishes the two disposable pool devices.
type DiskRole string

// Supported disposable disk roles.
const (
	SourceDisk      DiskRole = "src"
	DestinationDisk DiskRole = "dst"
)

// DiskSerial returns a deterministic virtio-safe serial of at most 20 bytes.
func DiskSerial(runID string, role DiskRole) string {
	digest := sha256.Sum256([]byte(runID))
	return fmt.Sprintf("bz-%x-%s", digest[:6], role)
}

// Vdev identifies a guest block device and the serial reported by the guest.
type Vdev struct {
	Path   string
	Serial string
}

// VerifyGuestGuard validates the marker, pool name, and virtual disk serials
// required before a harness may issue any destructive ZFS operation.
func VerifyGuestGuard(markerPath, runID, pool string, vdevs []Vdev) error {
	if runID == "" || strings.ContainsAny(runID, "/\x00\r\n") {
		return errors.New("integration run ID is empty or invalid")
	}
	marker, err := os.ReadFile(markerPath)
	if err != nil {
		return fmt.Errorf("read guest marker: %w", err)
	}
	if strings.TrimSpace(string(marker)) != runID {
		return errors.New("guest marker does not match the integration run ID")
	}
	wantPrefix := objectPrefix + runID
	if !strings.HasPrefix(pool, wantPrefix) {
		return fmt.Errorf("pool %q does not start with %q", pool, wantPrefix)
	}
	if len(vdevs) == 0 {
		return errors.New("at least one virtual test disk is required")
	}
	wantSerials := map[string]bool{
		DiskSerial(runID, SourceDisk):      true,
		DiskSerial(runID, DestinationDisk): true,
	}
	for _, vdev := range vdevs {
		if !strings.HasPrefix(vdev.Path, "/dev/") {
			return fmt.Errorf("vdev %q is not an absolute device path", vdev.Path)
		}
		if !wantSerials[vdev.Serial] {
			return fmt.Errorf("vdev %q has unexpected serial %q", vdev.Path, vdev.Serial)
		}
	}
	return nil
}
