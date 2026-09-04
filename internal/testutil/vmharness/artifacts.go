package vmharness

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/pdf/boomerangz/internal/testutil/zfstest"
)

// Artifacts contains paths and names owned by one unique integration run.
type Artifacts struct {
	RunDir            string `json:"run_dir"`
	Domain            string `json:"domain"`
	SystemOverlay     string `json:"system_overlay"`
	SourceDisk        string `json:"source_disk"`
	DestinationDisk   string `json:"destination_disk"`
	SourceSerial      string `json:"source_serial"`
	DestinationSerial string `json:"destination_serial"`
}

// Prepare creates a unique run directory, a base overlay, and two scratch
// disks. It never modifies the read-only base image.
func Prepare(ctx context.Context, config Config, tools Tools) (Artifacts, error) {
	return prepare(ctx, config, tools, execRunner{})
}

func prepare(ctx context.Context, config Config, tools Tools, commandRunner runner) (Artifacts, error) {
	config = config.Defaults()
	if err := config.Validate(); err != nil {
		return Artifacts{}, err
	}
	if tools.QEMUImg == "" {
		return Artifacts{}, fmt.Errorf("qemu-img path is required")
	}
	runDir := filepath.Join(config.WorkDir, config.RunID)
	if err := os.Mkdir(runDir, 0o700); err != nil {
		return Artifacts{}, fmt.Errorf("create unique run directory: %w", err)
	}
	prefix := "boomerangz-test-" + config.RunID
	artifacts := Artifacts{
		RunDir:            runDir,
		Domain:            prefix,
		SystemOverlay:     filepath.Join(runDir, "system.qcow2"),
		SourceDisk:        filepath.Join(runDir, "source.qcow2"),
		DestinationDisk:   filepath.Join(runDir, "destination.qcow2"),
		SourceSerial:      zfstest.DiskSerial(config.RunID, zfstest.SourceDisk),
		DestinationSerial: zfstest.DiskSerial(config.RunID, zfstest.DestinationDisk),
	}
	commands := [][]string{
		{"create", "-f", "qcow2", "-F", "qcow2", "-b", config.BaseImage, artifacts.SystemOverlay},
		{"create", "-f", "qcow2", artifacts.SourceDisk, config.DiskSize},
		{"create", "-f", "qcow2", artifacts.DestinationDisk, config.DiskSize},
	}
	for _, args := range commands {
		if _, err := commandRunner.Run(ctx, tools.QEMUImg, args...); err != nil {
			return artifacts, err
		}
	}
	return artifacts, nil
}

// VirtInstallArgs returns a transient, session-libvirt launch command using
// passt networking and two serial-labelled scratch disks.
func VirtInstallArgs(config Config, artifacts Artifacts) []string {
	config = config.Defaults()
	return []string{
		"--connect", LibvirtURI,
		"--name", artifacts.Domain,
		"--memory", fmt.Sprint(config.MemoryMiB),
		"--vcpus", fmt.Sprint(config.VCPUs),
		"--import",
		"--transient",
		"--noautoconsole",
		"--console", "pty,target.type=serial",
		"--graphics", "vnc,listen=127.0.0.1",
		"--video", "virtio",
		"--channel", "unix,target.type=virtio,target.name=org.qemu.guest_agent.0",
		"--osinfo", "detect=on,require=off",
		"--disk", "path=" + artifacts.SystemOverlay + ",format=qcow2,bus=virtio,cache=none",
		"--disk", "path=" + artifacts.SourceDisk + ",format=qcow2,bus=virtio,cache=none,serial=" + artifacts.SourceSerial,
		"--disk", "path=" + artifacts.DestinationDisk + ",format=qcow2,bus=virtio,cache=none,serial=" + artifacts.DestinationSerial,
		"--network", fmt.Sprintf("passt,portForward=127.0.0.1:%d:22", config.SSHPort),
	}
}

// Launch starts a transient domain. Its disk artifacts remain until explicit,
// validated cleanup is implemented.
func Launch(ctx context.Context, config Config, tools Tools, artifacts Artifacts) error {
	if tools.VirtInstall == "" {
		return fmt.Errorf("virt-install path is required")
	}
	_, err := execRunner{}.Run(ctx, tools.VirtInstall, VirtInstallArgs(config, artifacts)...)
	return err
}
