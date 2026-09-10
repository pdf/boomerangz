package zfstest

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// FixtureName returns a dataset name unique to one test under the pool's
// delegated "data" container.
//
// Integration suites are separate binaries run in a fixed order against one
// pair of pools. A fixture that two of them mutate is an ordering dependency
// that neither declares, so each test names its own.
func FixtureName(pool, role string) string {
	return pool + "/data/" + role + "-" + time.Now().UTC().Format("150405.000")
}

// PayloadVolume creates a sparse zvol seeded with random data for one test's
// exclusive use and registers its destruction with the test.
//
// The seed is what makes a send carry a measurable payload; tests that only
// need a dataset to hang properties on should create one directly rather than
// pay for the write.
func PayloadVolume(tb testing.TB, dataset string, sizeMiB, seedMiB int) string {
	tb.Helper()
	if seedMiB > sizeMiB {
		tb.Fatalf("payload seed %dM exceeds volume size %dM", seedMiB, sizeMiB)
	}
	fixtureCommand(tb, "create", "-s", "-b", "128K", "-V", fmt.Sprintf("%dM", sizeMiB), dataset)
	RegisterCleanup(tb, dataset)
	device, err := waitForDevice(tb.Context(), dataset)
	if err != nil {
		tb.Fatal(err)
	}
	// dd rather than a Go copy: the guest seeds hundreds of megabytes and the
	// block size matters more than the portability.
	seed := exec.CommandContext(tb.Context(), "dd", "if=/dev/urandom", "of="+device,
		"bs=1M", fmt.Sprintf("count=%d", seedMiB), "status=none")
	if output, seedErr := seed.CombinedOutput(); seedErr != nil {
		tb.Fatalf("seed %s: %v: %s", device, seedErr, output)
	}
	return dataset
}

// RegisterCleanup destroys a fixture tree when the test finishes, releasing
// the holds boomerangz places on its snapshots first.
//
// Failure is logged rather than fatal: the guarded pool-level teardown in
// bootstrap.sh is the backstop, and a cleanup fault should not be reported as
// the behaviour under test failing.
func RegisterCleanup(tb testing.TB, dataset string) {
	tb.Helper()
	tb.Cleanup(func() {
		// The test's own context is already cancelled by the time cleanup
		// runs, so this work gets a fresh one with its own bound.
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if err := destroyTree(ctx, dataset); err != nil {
			tb.Logf("fixture cleanup for %s: %v", dataset, err)
		}
	})
}

// destroyTree releases every hold beneath dataset and then destroys it.
// Held snapshots refuse destruction regardless of -f, and boomerangz holds
// the snapshots it considers references, so the release pass is required
// rather than defensive.
func destroyTree(ctx context.Context, dataset string) error {
	list := exec.CommandContext(ctx, "zfs", "list", "-H", "-o", "name", "-t", "snapshot", "-r", dataset)
	output, err := list.CombinedOutput()
	if err != nil {
		return fmt.Errorf("list snapshots: %w: %s", err, output)
	}
	for _, snapshot := range strings.Fields(string(output)) {
		holds, holdErr := exec.CommandContext(ctx, "zfs", "holds", "-H", snapshot).CombinedOutput()
		if holdErr != nil {
			return fmt.Errorf("list holds for %s: %w: %s", snapshot, holdErr, holds)
		}
		for _, line := range strings.Split(strings.TrimSpace(string(holds)), "\n") {
			fields := strings.Fields(line)
			if len(fields) < 2 {
				continue
			}
			if release, releaseErr := exec.CommandContext(ctx, "zfs", "release", fields[1], snapshot).CombinedOutput(); releaseErr != nil {
				return fmt.Errorf("release %s on %s: %w: %s", fields[1], snapshot, releaseErr, release)
			}
		}
	}
	if destroy, destroyErr := exec.CommandContext(ctx, "zfs", "destroy", "-R", dataset).CombinedOutput(); destroyErr != nil {
		return fmt.Errorf("destroy: %w: %s", destroyErr, destroy)
	}
	return nil
}

func fixtureCommand(tb testing.TB, args ...string) {
	tb.Helper()
	if output, err := exec.CommandContext(tb.Context(), "zfs", args...).CombinedOutput(); err != nil {
		tb.Fatalf("guest zfs %s: %v: %s", args[0], err, output)
	}
}

// waitForDevice blocks until udev has published the zvol's device node.
func waitForDevice(ctx context.Context, dataset string) (string, error) {
	device := "/dev/zvol/" + dataset
	if output, err := exec.CommandContext(ctx, "udevadm", "settle").CombinedOutput(); err != nil {
		return "", fmt.Errorf("udevadm settle: %w: %s", err, output)
	}
	for range 50 {
		if _, err := os.Stat(device); err == nil {
			return device, nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return "", fmt.Errorf("zvol device was not created: %s", device)
}
