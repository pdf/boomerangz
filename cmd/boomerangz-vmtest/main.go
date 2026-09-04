// Command boomerangz-vmtest manages disposable guest-only ZFS test VMs.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/alecthomas/kingpin/v2"
	"github.com/pdf/boomerangz/internal/testutil/vmharness"
)

func main() {
	if err := run(context.Background(), os.Args[1:]); err != nil {
		if _, writeErr := fmt.Fprintln(os.Stderr, "boomerangz-vmtest:", err); writeErr != nil {
			os.Exit(1)
		}
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	app := kingpin.New("boomerangz-vmtest", "Run destructive ZFS tests only in disposable virtual machines.")
	preflight := app.Command("preflight", "Check host and VM prerequisites without creating disks.")
	preflightConfig := addConfigFlags(preflight)
	prepare := app.Command("prepare", "Create a system overlay and two scratch disks.")
	prepareConfig := addConfigFlags(prepare)
	launch := app.Command("launch", "Prepare disks and start a transient guest.")
	launchConfig := addConfigFlags(launch)

	command, err := app.Parse(args)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	switch command {
	case preflight.FullCommand():
		result := vmharness.Preflight(ctx, preflightConfig.config())
		if err := encoder.Encode(result); err != nil {
			return fmt.Errorf("write preflight result: %w", err)
		}
		if !result.OK() {
			return errors.New("preflight failed")
		}
		return nil
	case prepare.FullCommand():
		config := prepareConfig.config()
		result := vmharness.Preflight(ctx, config)
		if !result.OK() {
			if err := encoder.Encode(result); err != nil {
				return fmt.Errorf("write preflight result: %w", err)
			}
			return errors.New("preflight failed")
		}
		artifacts, err := vmharness.Prepare(ctx, config, result.Tools)
		if err != nil {
			return err
		}
		return encoder.Encode(artifacts)
	case launch.FullCommand():
		config := launchConfig.config()
		result := vmharness.Preflight(ctx, config)
		if !result.OK() {
			if err := encoder.Encode(result); err != nil {
				return fmt.Errorf("write preflight result: %w", err)
			}
			return errors.New("preflight failed")
		}
		artifacts, err := vmharness.Prepare(ctx, config, result.Tools)
		if err != nil {
			return err
		}
		if err := vmharness.Launch(ctx, config, result.Tools, artifacts); err != nil {
			return err
		}
		return encoder.Encode(artifacts)
	default:
		return errors.New("no command selected")
	}
}

type configFlags struct {
	baseImage *string
	workDir   *string
	runID     *string
	sshPort   *int
	memoryMiB *int
	vcpus     *int
	diskSize  *string
}

func addConfigFlags(command *kingpin.CmdClause) configFlags {
	runID, err := vmharness.NewRunID()
	if err != nil {
		runID = "unavailable"
	}
	return configFlags{
		baseImage: command.Flag("base-image", "Absolute path to a read-only installed CachyOS qcow2 image.").Required().ExistingFile(),
		workDir:   command.Flag("work-dir", "Existing directory for per-run disposable artifacts.").Required().ExistingDir(),
		runID:     command.Flag("run-id", "Unique lowercase identifier for this run.").Default(runID).String(),
		sshPort:   command.Flag("ssh-port", "Loopback TCP port forwarded to guest SSH.").Required().Int(),
		memoryMiB: command.Flag("memory", "Guest memory in MiB.").Default("4096").Int(),
		vcpus:     command.Flag("vcpus", "Guest virtual CPU count.").Default("2").Int(),
		diskSize:  command.Flag("scratch-size", "Size of each ZFS scratch disk.").Default("8G").String(),
	}
}

func (f configFlags) config() vmharness.Config {
	return vmharness.Config{
		BaseImage: *f.baseImage,
		WorkDir:   *f.workDir,
		RunID:     *f.runID,
		SSHPort:   *f.sshPort,
		MemoryMiB: *f.memoryMiB,
		VCPUs:     *f.vcpus,
		DiskSize:  *f.diskSize,
	}
}
