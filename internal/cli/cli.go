// Package cli defines boomerangz's command-line interface.
package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/alecthomas/kingpin/v2"
	"github.com/pdf/boomerangz/internal/config"
	"github.com/pdf/boomerangz/internal/discovery"
	remoterpc "github.com/pdf/boomerangz/internal/replication/rpc"
	"github.com/pdf/boomerangz/internal/zfs"
)

const (
	defaultConfig  = "/etc/boomerangz/config.toml"
	defaultDropIns = "/etc/boomerangz/config.d"
)

// BuildInfo describes the binary version injected by the build system.
type BuildInfo struct {
	Version string `json:"version"`
	Commit  string `json:"commit"`
	Date    string `json:"date"`
}

// Run executes the CLI and returns a process exit code.
func Run(ctx context.Context, args []string, stdout, stderr io.Writer, build BuildInfo) int {
	if err := run(ctx, args, stdout, stderr, build); err != nil {
		if _, writeErr := fmt.Fprintln(stderr, "boomerangz:", err); writeErr != nil {
			return 1
		}
		return 1
	}
	return 0
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer, build BuildInfo) error {
	return runWithReader(ctx, args, stdout, stderr, build, nil)
}

func runWithReader(ctx context.Context, args []string, stdout, _ io.Writer, build BuildInfo, reader discovery.Reader) error {
	app := kingpin.New("boomerangz", "Property-driven ZFS snapshot and replication manager.")
	app.HelpFlag.Short('h')
	app.UsageWriter(stdout)

	configCmd := app.Command("config", "Inspect and validate daemon configuration.")
	checkCmd := configCmd.Command("check", "Validate configuration and all drop-ins.")
	checkPath := checkCmd.Flag("config", "Primary configuration file.").Default(defaultConfig).String()
	checkDropIns := checkCmd.Flag("config-dir", "Configuration drop-in directory.").Default(defaultDropIns).String()

	showCmd := configCmd.Command("show", "Show merged, effective configuration.")
	showPath := showCmd.Flag("config", "Primary configuration file.").Default(defaultConfig).String()
	showDropIns := showCmd.Flag("config-dir", "Configuration drop-in directory.").Default(defaultDropIns).String()

	versionCmd := app.Command("version", "Show version information.")
	versionJSON := versionCmd.Flag("json", "Emit JSON.").Bool()
	sshShellCmd := app.Command("ssh-shell", "Serve the restricted replication protocol over an SSH command channel.").Hidden()
	sshShellRoot := sshShellCmd.Flag("root", "Allowed destination ZFS root.").Required().String()

	datasetCmd := app.Command("dataset", "Dataset inspection and explicit lifecycle administration.")
	datasetPath := datasetCmd.Flag("config", "Primary configuration file.").Default(defaultConfig).String()
	datasetDropIns := datasetCmd.Flag("config-dir", "Configuration drop-in directory.").Default(defaultDropIns).String()
	listCmd := datasetCmd.Command("list", "List sparse inventory and active policies as JSON.")
	inspectCmd := datasetCmd.Command("inspect", "Inspect effective policy and stored properties as JSON.")
	inspectName := inspectCmd.Arg("dataset", "Exact ZFS dataset name.").Required().String()
	adoptCmd := datasetCmd.Command("adopt", "Preview transfer of an existing lineage to this installation.")
	adoptName := adoptCmd.Arg("dataset", "Exact ZFS dataset name.").Required().String()
	adoptApply := adoptCmd.Flag("apply", "Apply the adoption after revalidation.").Bool()
	cleanCmd := datasetCmd.Command("clean", "Preview explicit local decommissioning; preserves snapshots by default.")
	cleanNames := cleanCmd.Arg("datasets", "Exact ZFS dataset scopes.").Strings()
	cleanRecursive := cleanCmd.Flag("recursive", "Include descendants of the selected scopes.").Bool()
	cleanAll := cleanCmd.Flag("all", "Explicitly select all local datasets recursively.").Bool()
	cleanDestroy := cleanCmd.Flag("destroy-owned-snapshots", "Also delete fully proven owned snapshots without dependencies.").Bool()
	cleanApply := cleanCmd.Flag("apply", "Apply clean after revalidation.").Bool()

	identityCmd := app.Command("identity", "Inspect and recover installation identity.")
	identityPath := identityCmd.Flag("config", "Primary configuration file.").Default(defaultConfig).String()
	identityDropIns := identityCmd.Flag("config-dir", "Configuration drop-in directory.").Default(defaultDropIns).String()
	recoverCmd := identityCmd.Command("recover", "Preview recovery from local source-root owner markers.")
	recoverOwner := recoverCmd.Flag("owner", "Explicit owner UUID when more than one candidate exists.").String()
	recoverApply := recoverCmd.Flag("apply", "Apply the identity replacement after revalidation.").Bool()

	command, err := app.Parse(args)
	if err != nil {
		return err
	}

	switch command {
	case sshShellCmd.FullCommand():
		executor, err := zfs.NewDirect("zfs")
		if err != nil {
			return err
		}
		service, err := remoterpc.NewServer(executor, *sshShellRoot, "zfs")
		if err != nil {
			return err
		}
		return remoterpc.ServeStdio(ctx, service, os.Stdin, stdout)
	case adoptCmd.FullCommand(), cleanCmd.FullCommand(), recoverCmd.FullCommand():
		configPath, configDir := *datasetPath, *datasetDropIns
		if command == recoverCmd.FullCommand() {
			configPath, configDir = *identityPath, *identityDropIns
		}
		loaded, err := config.Load(configPath, configDir)
		if err != nil {
			return err
		}
		if reader == nil {
			reader, err = zfs.NewDirect("zfs")
			if err != nil {
				return err
			}
		}
		executor, ok := reader.(zfs.Executor)
		if !ok {
			return fmt.Errorf("lifecycle executor unavailable")
		}
		if command == recoverCmd.FullCommand() {
			return runIdentityRecover(ctx, stdout, loaded.Config, reader, executor, *recoverOwner, *recoverApply)
		}
		if command == adoptCmd.FullCommand() {
			return runAdopt(ctx, stdout, loaded.Config, executor, *adoptName, *adoptApply)
		}
		return runClean(ctx, stdout, loaded.Config, executor, *cleanNames, *cleanRecursive, *cleanAll, *cleanDestroy, *cleanApply)
	case listCmd.FullCommand(), inspectCmd.FullCommand():
		loaded, err := config.Load(*datasetPath, *datasetDropIns)
		if err != nil {
			return err
		}
		if reader == nil {
			reader, err = zfs.NewDirect("zfs")
			if err != nil {
				return err
			}
		}
		var remotes []string
		for name := range loaded.Config.Remotes {
			remotes = append(remotes, name)
		}
		scanner, err := discovery.New(reader, discovery.Options{Remotes: remotes})
		if err != nil {
			return err
		}
		var inspect []string
		if command == inspectCmd.FullCommand() {
			inspect = []string{*inspectName}
		}
		generation, err := scanner.Scan(ctx, inspect)
		if err != nil {
			return err
		}
		encoder := json.NewEncoder(stdout)
		encoder.SetIndent("", "  ")
		if command == inspectCmd.FullCommand() {
			entry, exists := generation.Inspect(*inspectName)
			if !exists {
				return fmt.Errorf("dataset %q was not found", *inspectName)
			}
			return encoder.Encode(entry)
		}
		return encoder.Encode(generation.Entries())
	case checkCmd.FullCommand():
		loaded, err := config.Load(*checkPath, *checkDropIns)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintf(stdout, "configuration valid (%d source files)\n", len(loaded.Sources))
		return err
	case showCmd.FullCommand():
		loaded, err := config.Load(*showPath, *showDropIns)
		if err != nil {
			return err
		}
		encoded, err := config.MarshalRedacted(loaded.Config)
		if err != nil {
			return fmt.Errorf("encode configuration: %w", err)
		}
		_, err = stdout.Write(encoded)
		return err
	case versionCmd.FullCommand():
		if *versionJSON {
			return json.NewEncoder(stdout).Encode(build)
		}
		_, err = fmt.Fprintf(stdout, "boomerangz %s (commit %s, built %s)\n", build.Version, build.Commit, build.Date)
		return err
	default:
		return errors.New("no command selected")
	}
}
