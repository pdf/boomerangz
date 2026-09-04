package vmharness

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/pdf/boomerangz/internal/testutil/zfstest"
)

type fakeRunner struct{ commands [][]string }

func (f *fakeRunner) Run(_ context.Context, executable string, args ...string) ([]byte, error) {
	command := append([]string{executable}, args...)
	f.commands = append(f.commands, command)
	return nil, nil
}

func TestPrepareBuildsOwnedArtifacts(t *testing.T) {
	t.Parallel()
	workDir := t.TempDir()
	baseImage := filepath.Join(t.TempDir(), "cachyos.qcow2")
	if err := os.WriteFile(baseImage, []byte("test image"), 0o400); err != nil {
		t.Fatal(err)
	}
	config := Config{BaseImage: baseImage, WorkDir: workDir, RunID: "run-123456", SSHPort: freePort(t)}.Defaults()
	commandRunner := &fakeRunner{}
	artifacts, err := prepare(context.Background(), config, Tools{QEMUImg: "/usr/bin/qemu-img"}, commandRunner)
	if err != nil {
		t.Fatal(err)
	}
	if artifacts.Domain != "boomerangz-test-run-123456" {
		t.Fatalf("domain = %q", artifacts.Domain)
	}
	if artifacts.SourceSerial != zfstest.DiskSerial(config.RunID, zfstest.SourceDisk) {
		t.Fatalf("source serial = %q", artifacts.SourceSerial)
	}
	wantCommands := [][]string{
		{"/usr/bin/qemu-img", "create", "-f", "qcow2", "-F", "qcow2", "-b", baseImage, filepath.Join(artifacts.RunDir, "system.qcow2")},
		{"/usr/bin/qemu-img", "create", "-f", "qcow2", filepath.Join(artifacts.RunDir, "source.qcow2"), "8G"},
		{"/usr/bin/qemu-img", "create", "-f", "qcow2", filepath.Join(artifacts.RunDir, "destination.qcow2"), "8G"},
	}
	if !reflect.DeepEqual(commandRunner.commands, wantCommands) {
		t.Fatalf("commands = %#v, want %#v", commandRunner.commands, wantCommands)
	}
}

func TestVirtInstallArgsAreTransientAndUsePasst(t *testing.T) {
	t.Parallel()
	config := Config{SSHPort: 22222, MemoryMiB: 4096, VCPUs: 2}
	artifacts := Artifacts{
		Domain:            "boomerangz-test-run-123456",
		SystemOverlay:     "/tmp/run/system.qcow2",
		SourceDisk:        "/tmp/run/source.qcow2",
		DestinationDisk:   "/tmp/run/destination.qcow2",
		SourceSerial:      zfstest.DiskSerial("run-123456", zfstest.SourceDisk),
		DestinationSerial: zfstest.DiskSerial("run-123456", zfstest.DestinationDisk),
	}
	args := VirtInstallArgs(config, artifacts)
	for _, required := range []string{
		"--transient",
		"pty,target.type=serial",
		"vnc,listen=127.0.0.1",
		"unix,target.type=virtio,target.name=org.qemu.guest_agent.0",
		"passt,portForward=127.0.0.1:22222:22",
	} {
		if !slices.Contains(args, required) {
			t.Fatalf("arguments do not contain %q: %#v", required, args)
		}
	}
}

func TestValidateRejectsWritableBaseImage(t *testing.T) {
	t.Parallel()
	baseImage := filepath.Join(t.TempDir(), "base.qcow2")
	if err := os.WriteFile(baseImage, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	config := Config{
		BaseImage: baseImage,
		WorkDir:   t.TempDir(),
		RunID:     "run-123456",
		SSHPort:   freePort(t),
	}.Defaults()
	if err := config.Validate(); err == nil || !strings.Contains(err.Error(), "base image must be read-only") {
		t.Fatal("Validate accepted a writable base image")
	}
}

func TestValidateRejectsBaseImageInsideWorkDir(t *testing.T) {
	t.Parallel()
	workDir := t.TempDir()
	baseImage := filepath.Join(workDir, "base.qcow2")
	if err := os.WriteFile(baseImage, nil, 0o400); err != nil {
		t.Fatal(err)
	}
	config := Config{BaseImage: baseImage, WorkDir: workDir, RunID: "run-123456", SSHPort: freePort(t)}.Defaults()
	if err := config.Validate(); err == nil || !strings.Contains(err.Error(), "base image must not be stored") {
		t.Fatal("Validate accepted a base image inside the work directory")
	}
}

func freePort(t *testing.T) int {
	t.Helper()
	listenConfig := net.ListenConfig{}
	listener, err := listenConfig.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return port
}
