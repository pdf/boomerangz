package vmharness

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
)

type runner interface {
	Run(context.Context, string, ...string) ([]byte, error)
}

type execRunner struct{}

func (execRunner) Run(ctx context.Context, executable string, args ...string) ([]byte, error) {
	output, err := exec.CommandContext(ctx, executable, args...).CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("run %s: %w: %s", filepathBase(executable), err, strings.TrimSpace(string(output)))
	}
	return output, nil
}

func filepathBase(path string) string {
	if index := strings.LastIndexByte(path, '/'); index >= 0 {
		return path[index+1:]
	}
	return path
}
