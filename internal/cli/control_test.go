package cli

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/pdf/boomerangz/internal/config"
	"github.com/pdf/boomerangz/internal/control"
	"github.com/pdf/boomerangz/internal/daemon"
	"github.com/pdf/boomerangz/internal/daemonstate"
	"github.com/pdf/boomerangz/internal/lifecycle"
)

type cliControlRuntime struct {
	mu        sync.Mutex
	triggered []string
}

func (*cliControlRuntime) ControlStatus() daemon.ControlSnapshot {
	return daemon.ControlSnapshot{Revision: 3, Observed: time.Unix(20, 0), Generation: 9, Datasets: []daemon.DatasetStatus{{Name: "tank/data", Active: true}}, Queues: map[string]daemon.QueueSnapshot{}}
}
func (*cliControlRuntime) WaitStatus(ctx context.Context, _ uint64) error {
	<-ctx.Done()
	return ctx.Err()
}
func (r *cliControlRuntime) Trigger(names []string) ([]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.triggered = append([]string(nil), names...)
	return names, nil
}
func (*cliControlRuntime) Reconcile() {}
func (*cliControlRuntime) Clean(_ context.Context, names []string, recursive, _, destroy, apply bool) ([]lifecycle.CleanPlan, error) {
	return []lifecycle.CleanPlan{{Dataset: names[0], Options: lifecycle.CleanOptions{Recursive: recursive, DestroyOwnedSnapshots: destroy}, Applied: map[bool]int{true: 1}[apply]}}, nil
}

func TestControlCommandsUseRunningDaemon(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfg := config.Defaults()
	cfg.Paths.SocketPath = filepath.Join(dir, "control.sock")
	cfg.Paths.IdentityDir = filepath.Join(dir, "identity")
	cfg.Paths.CredentialsDir = filepath.Join(dir, "credentials")
	runtime := &cliControlRuntime{}
	server, err := control.StartServer(cfg, runtime, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = server.Close() }()
	server.SetReloadHandler(func(context.Context) (daemonstate.ReloadResult, error) {
		return daemonstate.ReloadResult{Generation: 4, Applied: []string{"daemon.reconcile_interval"}}, nil
	})
	configPath := filepath.Join(dir, "config.toml")
	document := fmt.Sprintf("[paths]\nsocket_path=%q\nidentity_dir=%q\ncredentials_dir=%q\n", cfg.Paths.SocketPath, cfg.Paths.IdentityDir, cfg.Paths.CredentialsDir)
	if err := os.WriteFile(configPath, []byte(document), 0o600); err != nil {
		t.Fatal(err)
	}
	common := []string{"--config", configPath, "--config-dir", filepath.Join(dir, "missing")}
	var statusOutput bytes.Buffer
	if err := runWithReader(t.Context(), append([]string{"status"}, common...), &statusOutput, io.Discard, BuildInfo{}, nil); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(statusOutput.Bytes(), []byte(`"generation":9`)) {
		t.Fatalf("status=%s", statusOutput.String())
	}
	var triggerOutput bytes.Buffer
	triggerArgs := append([]string{"trigger"}, common...)
	triggerArgs = append(triggerArgs, "tank/data")
	if err := runWithReader(t.Context(), triggerArgs, &triggerOutput, io.Discard, BuildInfo{}, nil); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(runtime.triggered, []string{"tank/data"}) {
		t.Fatalf("triggered=%v", runtime.triggered)
	}
	var cleanOutput bytes.Buffer
	cleanArgs := []string{"dataset", "--config", configPath, "--config-dir", filepath.Join(dir, "missing"), "clean", "--apply", "tank/data"}
	if err := runWithReader(t.Context(), cleanArgs, &cleanOutput, io.Discard, BuildInfo{}, nil); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(cleanOutput.Bytes(), []byte(`"applied": 1`)) {
		t.Fatalf("clean=%s", cleanOutput.String())
	}
	var reloadOutput bytes.Buffer
	reloadArgs := []string{"config", "reload", "--socket", cfg.Paths.SocketPath}
	if err := runWithReader(t.Context(), reloadArgs, &reloadOutput, io.Discard, BuildInfo{}, nil); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(reloadOutput.Bytes(), []byte(`"generation": 4`)) || !bytes.Contains(reloadOutput.Bytes(), []byte(`"daemon.reconcile_interval"`)) {
		t.Fatalf("reload=%s", reloadOutput.String())
	}
}
