package ssh

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"

	remoterpc "github.com/pdf/boomerangz/internal/replication/rpc"
	"github.com/pdf/boomerangz/internal/zfs"
)

type shellTestBackend struct{ zfs.Executor }

func (shellTestBackend) ListDatasets(context.Context) ([]zfs.Dataset, error) {
	return []zfs.Dataset{{Name: "tank", Type: zfs.Filesystem, EncryptionRoot: "-"}, {Name: "tank/backups", Type: zfs.Filesystem, EncryptionRoot: "-"}, {Name: "other/private", Type: zfs.Filesystem, EncryptionRoot: "-"}}, nil
}
func (shellTestBackend) InspectDatasetIdentity(_ context.Context, dataset string) (zfs.DatasetIdentity, error) {
	return zfs.DatasetIdentity{Name: dataset, Type: zfs.Filesystem, GUID: 10, Pool: "tank", PoolGUID: 11}, nil
}
func (shellTestBackend) InspectState(_ context.Context, dataset string, _ bool) (zfs.State, error) {
	return zfs.State{Objects: []zfs.Object{{Name: dataset, Type: "filesystem", GUID: 10}}, ResumeTokens: map[string]string{}}, nil
}
func (shellTestBackend) SetProperties(context.Context, string, map[string]string) error { return nil }
func (shellTestBackend) InheritProperty(context.Context, string, string) error          { return nil }

func TestSSHHelperProcess(_ *testing.T) {
	switch os.Getenv("BOOMERANGZ_SSH_HELPER") {
	case "inventory":
		fmt.Print("tank\tfilesystem\t-\ntank/backups\tfilesystem\t-\n")
	case "send":
		_, _ = io.Copy(os.Stdout, bytes.NewBuffer(bytes.Repeat([]byte{1}, 256*1024)))
	case "receive":
		_, _ = io.Copy(io.Discard, os.Stdin)
	case "shell":
		service, err := remoterpc.NewServerWithReceiver(shellTestBackend{}, "tank/backups", func(ctx context.Context, _ []string) *exec.Cmd {
			return helperCommand(ctx, "receive")
		})
		if err != nil || remoterpc.ServeStdio(context.Background(), service, os.Stdin, os.Stdout) != nil {
			os.Exit(7)
		}
	case "unavailable":
		os.Exit(255)
	default:
		return
	}
	os.Exit(0)
}

func helperCommand(ctx context.Context, mode string) *exec.Cmd {
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestSSHHelperProcess$")
	command.Env = append(os.Environ(), "BOOMERANGZ_SSH_HELPER="+mode)
	return command
}

func TestClientArgumentsAndProbe(t *testing.T) {
	t.Parallel()
	var got []string
	client, err := newClient(Config{Host: "Backup.EXAMPLE.net", Port: 2222, User: "replicator", Root: "tank/backups", IdentityFile: "/keys/backup", ConnectTimeout: 1500 * time.Millisecond}, func(ctx context.Context, args []string) *exec.Cmd {
		got = append([]string(nil), args...)
		return helperCommand(ctx, "inventory")
	})
	if err != nil {
		t.Fatal(err)
	}
	inventory, err := client.Probe(t.Context())
	if err != nil || len(inventory) != 2 {
		t.Fatalf("inventory=%v err=%v", inventory, err)
	}
	want := []string{"-T", "-o", "BatchMode=yes", "-o", "ClearAllForwardings=yes", "-o", "StrictHostKeyChecking=yes", "-o", "ConnectTimeout=2", "-p", "2222", "-o", "IdentitiesOnly=yes", "-i", "/keys/backup", "--", "replicator@Backup.EXAMPLE.net", "'zfs' 'list' '-H' '-p' '-t' 'filesystem,volume' '-o' 'name,type,encryptionroot'"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("SSH args:\n got %#v\nwant %#v", got, want)
	}
	if got := client.CanonicalTarget(); got != "ssh://replicator@backup.example.net:2222/tank/backups" {
		t.Fatalf("canonical target = %q", got)
	}
}

func TestRemoteCommandQuoting(t *testing.T) {
	t.Parallel()
	got := remoteCommand("zfs", []string{"set", "comment=it's safe; $(false)", "tank/data"})
	want := "'zfs' 'set' 'comment=it'\"'\"'s safe; $(false)' 'tank/data'"
	if got != want {
		t.Fatalf("remote command = %q, want %q", got, want)
	}
}

func TestConfigValidation(t *testing.T) {
	t.Parallel()
	valid := Config{Host: "backup.example.net", Root: "tank/backups"}
	if _, err := newClient(valid, func(ctx context.Context, _ []string) *exec.Cmd { return helperCommand(ctx, "inventory") }); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*Config){
		func(c *Config) { c.Host = "-option" },
		func(c *Config) { c.Host = "bad host" },
		func(c *Config) { c.User = "root@other" },
		func(c *Config) { c.Port = 65536 },
		func(c *Config) { c.IdentityFile = "relative/key" },
		func(c *Config) { c.Root = "/tank/backups" },
	} {
		candidate := valid
		mutate(&candidate)
		if _, err := newClient(candidate, func(ctx context.Context, _ []string) *exec.Cmd { return helperCommand(ctx, "inventory") }); err == nil {
			t.Fatalf("accepted invalid config %#v", candidate)
		}
	}
}

func TestUnavailableClassification(t *testing.T) {
	t.Parallel()
	client, err := newClient(Config{Host: "backup.example.net", Root: "tank/backups"}, func(ctx context.Context, _ []string) *exec.Cmd {
		return helperCommand(ctx, "unavailable")
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Probe(t.Context())
	if !IsUnavailable(err) {
		t.Fatalf("error was not classified unavailable: %v", err)
	}
}

func TestUnavailableDoesNotRetryCancellation(t *testing.T) {
	t.Parallel()
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if unavailable(cancelled, context.Canceled) {
		t.Fatal("caller cancellation was classified as a retryable endpoint failure")
	}
	deadline, deadlineCancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer deadlineCancel()
	if !unavailable(deadline, context.DeadlineExceeded) {
		t.Fatal("connection deadline was not classified as a retryable endpoint failure")
	}
}

func TestSSHStream(t *testing.T) {
	t.Parallel()
	var remoteArgs []string
	client, err := newClient(Config{Host: "backup.example.net", Root: "tank/backups"}, func(ctx context.Context, args []string) *exec.Cmd {
		remoteArgs = append([]string(nil), args...)
		return helperCommand(ctx, "receive")
	})
	if err != nil {
		t.Fatal(err)
	}
	stream := &Stream{client: client, sender: func(ctx context.Context, _ []string) *exec.Cmd { return helperCommand(ctx, "send") }}
	result, err := stream.Run(t.Context(), zfs.SendOptions{Source: "tank/data", Snapshot: "tank/data@end"}, zfs.ReceiveOptions{Root: "tank/backups", Discard: zfs.ReceiveExact}, zfs.Estimate{}, nil)
	if err != nil || !result.Completed || result.Bytes != 256*1024 {
		t.Fatalf("result=%v err=%v", result, err)
	}
	if len(remoteArgs) == 0 || !strings.HasSuffix(remoteArgs[len(remoteArgs)-1], "'zfs' 'receive' '-u' '-s' 'tank/backups'") {
		t.Fatalf("remote receive args = %#v", remoteArgs)
	}
}

func TestSSHShellGRPCSessionAndStream(t *testing.T) {
	t.Parallel()
	client, err := newClient(Config{Host: "backup.example.net", Root: "tank/backups"}, func(ctx context.Context, _ []string) *exec.Cmd {
		return helperCommand(ctx, "shell")
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	shell, err := NewShell(ctx, client)
	if err != nil {
		t.Fatal(err)
	}
	inventory, err := shell.Executor().ListDatasets(ctx)
	if err != nil || len(inventory) != 2 || inventory[0].Name != "tank" || inventory[1].Name != "tank/backups" {
		t.Fatalf("inventory=%v err=%v", inventory, err)
	}
	identity, err := shell.Executor().InspectDatasetIdentity(ctx, "tank/backups")
	if err != nil || identity.GUID != 10 || identity.PoolGUID != 11 {
		t.Fatalf("identity=%+v err=%v", identity, err)
	}
	state, err := shell.Executor().InspectState(ctx, "tank/backups", true)
	if err != nil || len(state.Objects) != 1 || state.Objects[0].GUID != 10 {
		t.Fatalf("state=%+v err=%v", state, err)
	}
	if _, err := shell.Executor().InspectDatasetIdentity(ctx, "other/private"); err == nil {
		t.Fatal("SSH shell exposed an identity outside its configured root")
	}
	stream := &ShellStream{shell: shell, sender: func(ctx context.Context, _ []string) *exec.Cmd { return helperCommand(ctx, "send") }}
	progress, err := stream.Run(ctx, zfs.SendOptions{Source: "tank/data", Snapshot: "tank/data@end"}, zfs.ReceiveOptions{Root: "tank/backups", Discard: zfs.ReceiveExact}, zfs.Estimate{}, nil)
	if err != nil || !progress.Completed || progress.Bytes != 256*1024 {
		t.Fatalf("progress=%+v err=%v", progress, err)
	}
	if err := shell.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestEndpointSelectionAndExplicitFallback(t *testing.T) {
	t.Parallel()
	var commands []string
	client, err := newClient(Config{Host: "backup.example.net", Root: "tank/backups"}, func(ctx context.Context, args []string) *exec.Cmd {
		remote := args[len(args)-1]
		commands = append(commands, remote)
		if strings.Contains(remote, "'boomerangz' 'ssh-shell'") {
			return exec.CommandContext(ctx, "false")
		}
		return helperCommand(ctx, "inventory")
	})
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := OpenEndpoint(t.Context(), client, "/usr/bin/zfs", "auto")
	if err != nil || endpoint.Mode != "direct" || len(commands) != 1 {
		t.Fatalf("endpoint=%+v err=%v commands=%v", endpoint, err, commands)
	}
	if _, err := endpoint.Executor.ListDatasets(t.Context()); err != nil || len(commands) != 2 {
		t.Fatalf("direct fallback probe err=%v commands=%v", err, commands)
	}
	if _, err := OpenEndpoint(t.Context(), client, "/usr/bin/zfs", "ssh-shell"); !IsShellUnavailable(err) {
		t.Fatalf("required SSH shell did not fail closed: %v", err)
	}
}
