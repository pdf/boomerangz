package cli

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

func TestDaemonCommandIsVisible(t *testing.T) {
	t.Parallel()
	var output bytes.Buffer
	err := runWithReader(t.Context(), []string{"daemon", "--config", t.TempDir() + "/absent.toml", "--config-dir", t.TempDir()}, &output, io.Discard, BuildInfo{}, nil)
	if err == nil || !strings.Contains(err.Error(), "read config") {
		t.Fatalf("daemon command was not parsed before config load: %v", err)
	}
}
