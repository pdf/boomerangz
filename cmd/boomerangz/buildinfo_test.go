package main

import (
	"runtime/debug"
	"testing"

	"github.com/pdf/boomerangz/internal/cli"
)

func TestResolveBuildInfoUsesGoVCSMetadata(t *testing.T) {
	t.Parallel()
	info := &debug.BuildInfo{
		Main: debug.Module{Version: "v0.1.0"},
		Settings: []debug.BuildSetting{
			{Key: "vcs.revision", Value: "0123456789abcdef"},
			{Key: "vcs.time", Value: "2026-09-08T12:34:56Z"},
			{Key: "vcs.modified", Value: "false"},
		},
	}
	want := cli.BuildInfo{Version: "v0.1.0", Commit: "0123456789abcdef", Date: "2026-09-08T12:34:56Z"}
	if got := resolveBuildInfo(info, true, cli.BuildInfo{}); got != want {
		t.Fatalf("resolveBuildInfo() = %#v, want %#v", got, want)
	}
}

func TestResolveBuildInfoUsesSourceReleaseMetadataWithoutVCS(t *testing.T) {
	t.Parallel()
	want := cli.BuildInfo{Version: "v0.1.0", Commit: "fedcba9876543210", Date: "2026-09-08T12:34:56Z"}
	if got := resolveBuildInfo(&debug.BuildInfo{Main: debug.Module{Version: "(devel)"}}, true, want); got != want {
		t.Fatalf("resolveBuildInfo() = %#v, want %#v", got, want)
	}
}

func TestResolveBuildInfoDefaults(t *testing.T) {
	t.Parallel()
	want := cli.BuildInfo{Version: "devel", Commit: "unknown", Date: "unknown"}
	if got := resolveBuildInfo(nil, false, cli.BuildInfo{}); got != want {
		t.Fatalf("resolveBuildInfo() = %#v, want %#v", got, want)
	}
}
