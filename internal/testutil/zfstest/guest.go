package zfstest

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

const guestBootstrapVersion = "2"

// VerifyGuestPool validates a role-specific serial and the pool's actual vdevs.
// The marker is checked before invoking even read-only guest commands.
func VerifyGuestPool(ctx context.Context, runID string, role DiskRole, device string) (string, error) {
	if role != SourceDisk && role != DestinationDisk {
		return "", fmt.Errorf("invalid disk role")
	}
	pool := objectPrefix + runID + "-" + string(role)
	marker := "/run/boomerangz-vmtest/guest-marker"
	if err := VerifyGuestGuard(marker, runID, pool, []Vdev{{Path: device, Serial: DiskSerial(runID, role)}}); err != nil {
		return "", err
	}
	version, err := os.ReadFile("/run/boomerangz-vmtest/bootstrap-version")
	if err != nil || strings.TrimSpace(string(version)) != guestBootstrapVersion {
		return "", fmt.Errorf("guest bootstrap version mismatch; refresh the reusable base image")
	}
	query := func(name string, args ...string) (string, error) {
		out, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
		if err != nil {
			return "", fmt.Errorf("guest guard %s: %w", name, err)
		}
		return strings.TrimSpace(string(out)), nil
	}
	kind, err := query("lsblk", "-dn", "-o", "TYPE", device)
	if err != nil || kind != "disk" {
		return "", fmt.Errorf("guard requires a whole test disk")
	}
	serial, err := query("lsblk", "-dn", "-o", "SERIAL", device)
	if err != nil {
		return "", err
	}
	if serial != DiskSerial(runID, role) {
		return "", fmt.Errorf("guest disk role/serial mismatch")
	}
	status, err := query("zpool", "status", "-LP", pool)
	if err != nil {
		return "", err
	}
	count := 0
	for _, line := range strings.Split(status, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 || !strings.HasPrefix(fields[0], "/dev/") {
			continue
		}
		count++
		parent, err := query("lsblk", "-dn", "-o", "PKNAME", fields[0])
		if err != nil {
			return "", err
		}
		if (parent != "" && "/dev/"+parent != device) || (parent == "" && fields[0] != device) {
			return "", fmt.Errorf("pool contains unexpected vdev")
		}
	}
	if count != 1 {
		return "", fmt.Errorf("unexpected guest pool topology")
	}
	return pool, nil
}
