// Command boomerangz-vmtest manages disposable guest-only ZFS test VMs.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/alecthomas/kong"
	"github.com/pdf/boomerangz/internal/testutil/vmharness"
)

type commandLine struct {
	Preflight preflightCommand `cmd:"" help:"Check host and VM prerequisites without creating disks."`
	Prepare   prepareCommand   `cmd:"" help:"Create a system overlay and two scratch disks."`
	Launch    launchCommand    `cmd:"" help:"Prepare disks and start a transient guest."`
}

type configFlags struct {
	BaseImage string `name:"base-image" required:"" type:"existingfile" help:"Absolute path to a read-only installed CachyOS qcow2 image."`
	WorkDir   string `name:"work-dir" required:"" type:"existingdir" help:"Existing directory for per-run disposable artifacts."`
	RunID     string `name:"run-id" help:"Unique lowercase identifier for this run."`
	SSHPort   int    `name:"ssh-port" required:"" help:"Loopback TCP port forwarded to guest SSH."`
	MemoryMiB int    `name:"memory" default:"4096" help:"Guest memory in MiB."`
	VCPUs     int    `name:"vcpus" default:"2" help:"Guest virtual CPU count."`
	DiskSize  string `name:"scratch-size" default:"8G" help:"Size of each ZFS scratch disk."`
}

func (f *configFlags) BeforeApply() error {
	if f.RunID != "" {
		return nil
	}
	runID, err := vmharness.NewRunID()
	if err != nil {
		return fmt.Errorf("generate run ID: %w", err)
	}
	f.RunID = runID
	return nil
}

func (f configFlags) config() vmharness.Config {
	return vmharness.Config{BaseImage: f.BaseImage, WorkDir: f.WorkDir, RunID: f.RunID, SSHPort: f.SSHPort, MemoryMiB: f.MemoryMiB, VCPUs: f.VCPUs, DiskSize: f.DiskSize}
}

type environment struct {
	Context context.Context
	Output  io.Writer
}

type preflightCommand struct {
	configFlags `embed:""`
}

func (c *preflightCommand) Run(env *environment) error {
	result := vmharness.Preflight(env.Context, c.config())
	if err := encode(env.Output, result); err != nil {
		return fmt.Errorf("write preflight result: %w", err)
	}
	if !result.OK() {
		return errors.New("preflight failed")
	}
	return nil
}

type prepareCommand struct {
	configFlags `embed:""`
}

func (c *prepareCommand) Run(env *environment) error {
	config := c.config()
	result := vmharness.Preflight(env.Context, config)
	if !result.OK() {
		if err := encode(env.Output, result); err != nil {
			return fmt.Errorf("write preflight result: %w", err)
		}
		return errors.New("preflight failed")
	}
	artifacts, err := vmharness.Prepare(env.Context, config, result.Tools)
	if err != nil {
		return err
	}
	return encode(env.Output, artifacts)
}

type launchCommand struct {
	configFlags `embed:""`
}

func (c *launchCommand) Run(env *environment) error {
	config := c.config()
	result := vmharness.Preflight(env.Context, config)
	if !result.OK() {
		if err := encode(env.Output, result); err != nil {
			return fmt.Errorf("write preflight result: %w", err)
		}
		return errors.New("preflight failed")
	}
	artifacts, err := vmharness.Prepare(env.Context, config, result.Tools)
	if err != nil {
		return err
	}
	if err := vmharness.Launch(env.Context, config, result.Tools, artifacts); err != nil {
		return err
	}
	return encode(env.Output, artifacts)
}

func encode(output io.Writer, value any) error {
	encoder := json.NewEncoder(output)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

func main() {
	if err := run(context.Background(), os.Args[1:], os.Stdout, os.Stderr); err != nil {
		if _, writeErr := fmt.Fprintln(os.Stderr, "boomerangz-vmtest:", err); writeErr != nil {
			os.Exit(1)
		}
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	root := &commandLine{}
	parser, err := kong.New(root, kong.Name("boomerangz-vmtest"), kong.Description("Run destructive ZFS tests only in disposable virtual machines."), kong.Writers(stdout, stderr), kong.UsageOnError())
	if err != nil {
		return err
	}
	parsed, err := parser.Parse(args)
	if err != nil {
		return err
	}
	return parsed.Run(&environment{Context: ctx, Output: stdout})
}
