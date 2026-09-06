// Command boomerangz provides the daemon and its administrative CLI.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/pdf/boomerangz/internal/cli"
)

var (
	version = "devel"
	commit  = "unknown"
	date    = "unknown"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(cli.Run(ctx, os.Args[1:], os.Stdout, os.Stderr, cli.BuildInfo{
		Version: version,
		Commit:  commit,
		Date:    date,
	}))
}
