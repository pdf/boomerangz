// Command boomerangz provides the daemon and its administrative CLI.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/pdf/boomerangz/internal/cli"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(cli.Run(ctx, os.Args[1:], os.Stdout, os.Stderr, currentBuildInfo()))
}
