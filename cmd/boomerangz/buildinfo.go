package main

import (
	"runtime/debug"

	"github.com/pdf/boomerangz/internal/cli"
)

// Source archives have no VCS database for the Go toolchain to inspect. The
// source package supplies these values from its authenticated release metadata.
var (
	sourceVersion string
	sourceCommit  string
	sourceDate    string
)

func currentBuildInfo() cli.BuildInfo {
	info, ok := debug.ReadBuildInfo()
	return resolveBuildInfo(info, ok, cli.BuildInfo{
		Version: sourceVersion,
		Commit:  sourceCommit,
		Date:    sourceDate,
	})
}

func resolveBuildInfo(info *debug.BuildInfo, ok bool, source cli.BuildInfo) cli.BuildInfo {
	result := cli.BuildInfo{Version: "devel", Commit: "unknown", Date: "unknown"}
	if ok {
		if info.Main.Version != "" && info.Main.Version != "(devel)" {
			result.Version = info.Main.Version
		}
		for _, setting := range info.Settings {
			switch setting.Key {
			case "vcs.revision":
				result.Commit = setting.Value
			case "vcs.time":
				result.Date = setting.Value
			}
		}
	}
	if source.Version != "" {
		result.Version = source.Version
	}
	if source.Commit != "" {
		result.Commit = source.Commit
	}
	if source.Date != "" {
		result.Date = source.Date
	}
	return result
}
