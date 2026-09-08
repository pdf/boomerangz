// Package cli defines boomerangz's command-line interface.
package cli

import (
	"context"
	"fmt"
	"io"

	"github.com/alecthomas/kong"
	"github.com/pdf/boomerangz/internal/discovery"
)

// BuildInfo describes the module and VCS metadata embedded in the binary.
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

func runWithReader(ctx context.Context, args []string, stdout, stderr io.Writer, build BuildInfo, reader discovery.Reader) error {
	root := &commandLine{}
	parser, err := kong.New(
		root,
		kong.Name("boomerangz"),
		kong.Description("Property-driven ZFS snapshot and replication manager."),
		kong.Writers(stdout, stderr),
		kong.UsageOnError(),
	)
	if err != nil {
		return err
	}
	parsed, err := parser.Parse(args)
	if err != nil {
		return err
	}
	return parsed.Run(&commandEnvironment{Context: ctx, Stdout: stdout, Stderr: stderr, Build: build, Reader: reader, Root: root})
}
