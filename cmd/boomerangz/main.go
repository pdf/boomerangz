// Command boomerangz provides the daemon and its administrative CLI.
package main

import (
	"context"
	"os"

	"github.com/pdf/boomerangz/internal/cli"
)

var (
	version = "devel"
	commit  = "unknown"
	date    = "unknown"
)

func main() {
	os.Exit(cli.Run(context.Background(), os.Args[1:], os.Stdout, os.Stderr, cli.BuildInfo{
		Version: version,
		Commit:  commit,
		Date:    date,
	}))
}
