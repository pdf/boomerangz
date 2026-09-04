// Package cli defines boomerangz's command-line interface.
package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/alecthomas/kingpin/v2"
	"github.com/pdf/boomerangz/internal/config"
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

func run(_ context.Context, args []string, stdout, _ io.Writer, build BuildInfo) error {
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

	command, err := app.Parse(args)
	if err != nil {
		return err
	}

	switch command {
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
