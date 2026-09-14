package cli

import (
	"bytes"
	"context"
	"encoding/json"
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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type cliControlRuntime struct {
	mu        sync.Mutex
	triggered []string
}

func (*cliControlRuntime) ControlStatus() daemon.ControlSnapshot {
	return daemon.ControlSnapshot{Revision: 3, Observed: time.Unix(20, 0), Generation: 9, Datasets: []daemon.DatasetStatus{{Name: "tank/data", Active: true}}, Queues: map[string]daemon.QueueSnapshot{}}
}
func (*cliControlRuntime) SubscribeStatus(context.Context) (daemonstate.Subscription, error) {
	return idleSubscription{}, nil
}

type idleSubscription struct{}

func (idleSubscription) Updates() <-chan daemonstate.Update { return nil }
func (idleSubscription) Err() error                         { return nil }
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
	var statusJSONOutput bytes.Buffer
	statusJSONArgs := append([]string{"status", "--json"}, common...)
	if err := runWithReader(t.Context(), statusJSONArgs, &statusJSONOutput, io.Discard, BuildInfo{}, nil); err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		Generation uint64 `json:"generation"`
	}
	if err := json.Unmarshal(statusJSONOutput.Bytes(), &decoded); err != nil {
		t.Fatalf("status --json=%s: %v", statusJSONOutput.String(), err)
	}
	if decoded.Generation != 9 {
		t.Fatalf("status --json generation=%d", decoded.Generation)
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

// TestStatusRendererSelection pins the rule that decides between the terminal
// and JSON renderings of `status`.
//
// The terminal rendering itself is covered in internal/statusui, and the
// integration suite cannot reach it at all: a test harness gives the command
// a pipe, not a terminal. What is left to pin here is the selector, and the
// property that matters operationally is its negative half - a redirected or
// piped `status` stays machine-readable.
func TestStatusRendererSelection(t *testing.T) {
	t.Parallel()
	if interactive, width := terminalWidth(&bytes.Buffer{}); interactive || width != 0 {
		t.Fatalf("a writer that is not a file reported interactive=%v width=%d", interactive, width)
	}
	file, err := os.CreateTemp(t.TempDir(), "status")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()
	if interactive, width := terminalWidth(file); interactive || width != 0 {
		t.Fatalf("a redirected file reported interactive=%v width=%d", interactive, width)
	}
}

// scriptedWatchRuntime delivers a fixed sequence of updates to a watch and
// then ends the subscription, as the daemon does when a watcher falls behind.
type scriptedWatchRuntime struct {
	cliControlRuntime
	updates []daemonstate.Update
}

type scriptedSubscription struct{ updates chan daemonstate.Update }

func (s scriptedSubscription) Updates() <-chan daemonstate.Update { return s.updates }
func (scriptedSubscription) Err() error                           { return daemon.ErrSubscriberOverflow }

func (r *scriptedWatchRuntime) SubscribeStatus(context.Context) (daemonstate.Subscription, error) {
	updates := make(chan daemonstate.Update, len(r.updates))
	for _, update := range r.updates {
		updates <- update
	}
	close(updates)
	return scriptedSubscription{updates: updates}, nil
}

// TestStatusWatchJSONEmitsEveryTransition runs `status --watch --json` against
// a daemon whose job went waiting-retry, probing, waiting-retry between two
// snapshots, each of which holds one row for it. Every transition appears in
// the output in order, and a watch that falls behind exits with an error.
func TestStatusWatchJSONEmitsEveryTransition(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfg := config.Defaults()
	cfg.Paths.SocketPath = filepath.Join(dir, "control.sock")
	cfg.Paths.IdentityDir = filepath.Join(dir, "identity")
	cfg.Paths.CredentialsDir = filepath.Join(dir, "credentials")
	const job = "remote:tank/data:offsite"
	at := time.Unix(40, 0).UTC()
	event := func(state, reason string, offset time.Duration) daemonstate.Event {
		return daemonstate.Event{Kind: daemonstate.EventTransition, Pool: "transfer", Job: job, Scope: "tank/data", Target: "offsite", State: state, Reason: reason, At: at.Add(offset)}
	}
	first, second, third, fourth := event("waiting-retry", "connection refused", 0), event("probing", "", time.Second), event("waiting-retry", "connection reset", 2*time.Second), event("sending", "", 3*time.Second)
	runtime := &scriptedWatchRuntime{updates: []daemonstate.Update{
		{State: daemon.ControlSnapshot{Revision: 1}},
		{Transitions: []daemonstate.Event{first, second, third}, State: daemon.ControlSnapshot{Revision: 4, Jobs: []daemonstate.Event{third}}},
		{Transitions: []daemonstate.Event{fourth}, State: daemon.ControlSnapshot{Revision: 5, Jobs: []daemonstate.Event{fourth}}},
	}}
	server, err := control.StartServer(cfg, runtime, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = server.Close() }()
	configPath := filepath.Join(dir, "config.toml")
	document := fmt.Sprintf("[paths]\nsocket_path=%q\nidentity_dir=%q\ncredentials_dir=%q\n", cfg.Paths.SocketPath, cfg.Paths.IdentityDir, cfg.Paths.CredentialsDir)
	if err := os.WriteFile(configPath, []byte(document), 0o600); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	watchErr := runWithReader(t.Context(), []string{"status", "--watch", "--json", "--config", configPath, "--config-dir", filepath.Join(dir, "missing")}, &output, io.Discard, BuildInfo{}, nil)
	if status.Code(watchErr) != codes.Aborted {
		t.Fatalf("watch that fell behind returned %v, want Aborted; output=%s", watchErr, output.String())
	}
	type transition struct {
		Job     string `json:"job"`
		State   string `json:"state"`
		Reason  string `json:"reason"`
		Changed int64  `json:"changed_unix_nano"`
	}
	var got []transition
	var revisions []uint64
	decoder := json.NewDecoder(&output)
	for decoder.More() {
		var message struct {
			Revision    uint64       `json:"revision"`
			Jobs        []transition `json:"jobs"`
			Transitions []transition `json:"transitions"`
		}
		if err := decoder.Decode(&message); err != nil {
			t.Fatal(err)
		}
		if message.Transitions == nil {
			t.Fatalf("revision %d has no transitions array", message.Revision)
		}
		if len(message.Jobs) > 1 {
			t.Fatalf("revision %d holds %d rows for one job", message.Revision, len(message.Jobs))
		}
		revisions = append(revisions, message.Revision)
		got = append(got, message.Transitions...)
	}
	var want []transition
	for _, event := range []daemonstate.Event{first, second, third, fourth} {
		want = append(want, transition{Job: job, State: event.State, Reason: event.Reason, Changed: event.At.UnixNano()})
	}
	if !reflect.DeepEqual(revisions, []uint64{1, 4, 5}) || !reflect.DeepEqual(got, want) {
		t.Fatalf("revisions=%v transitions=%+v, want %+v", revisions, got, want)
	}
}
