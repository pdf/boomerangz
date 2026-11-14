//go:build integration

package daemon_test

import (
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/pdf/boomerangz/internal/config"
	"github.com/pdf/boomerangz/internal/control"
	controlrpc "github.com/pdf/boomerangz/internal/control/rpc"
	"github.com/pdf/boomerangz/internal/daemon"
	"github.com/pdf/boomerangz/internal/replication/native"
	"github.com/pdf/boomerangz/internal/testutil/zfstest"
	"github.com/pdf/boomerangz/internal/zfs"
)

// receiveStatusPause is how long the native sender holds its stream partway:
// longer than the daemon's resume-token window, so that the token the paused
// receive leaves behind has had time to be taken for an interrupted receive.
const receiveStatusPause = 25 * time.Second

// receiveWatch follows the receiving daemon's status over a WatchStatus
// subscription on its control socket, which delivers every change.
type receiveWatch struct {
	updates <-chan []*controlrpc.ReceiveStatus
	failed  <-chan error
}

func watchReceives(t *testing.T, client *control.Client) *receiveWatch {
	t.Helper()
	stream, err := client.Status.WatchStatus(t.Context(), &controlrpc.WatchStatusRequest{})
	if err != nil {
		t.Fatal(err)
	}
	updates := make(chan []*controlrpc.ReceiveStatus)
	failed := make(chan error, 1)
	go func() {
		for {
			response, recvErr := stream.Recv()
			if recvErr != nil {
				failed <- recvErr
				return
			}
			select {
			case updates <- response.GetStatus().GetReceives():
			case <-t.Context().Done():
				return
			}
		}
	}()
	return &receiveWatch{updates: updates, failed: failed}
}

// next returns the receives of the next update, failing the test when none
// arrives by deadline or the watch ends.
func (w *receiveWatch) next(t *testing.T, deadline <-chan time.Time, what string, last []*controlrpc.ReceiveStatus) []*controlrpc.ReceiveStatus {
	t.Helper()
	select {
	case receives := <-w.updates:
		return receives
	case err := <-w.failed:
		t.Fatalf("status watch ended waiting for %s: %v", what, err)
	case <-deadline:
		t.Fatalf("status never showed %s; last receives: %v", what, last)
	}
	return nil
}

// await waits for an update whose receives match accepts.
func (w *receiveWatch) await(t *testing.T, limit time.Duration, what string, match func([]*controlrpc.ReceiveStatus) bool) []*controlrpc.ReceiveStatus {
	t.Helper()
	deadline := time.After(limit)
	var last []*controlrpc.ReceiveStatus
	for {
		last = w.next(t, deadline, what, last)
		if match(last) {
			return last
		}
	}
}

func receivesInto(receives []*controlrpc.ReceiveStatus, dataset string) []*controlrpc.ReceiveStatus {
	var result []*controlrpc.ReceiveStatus
	for _, receive := range receives {
		if receive.GetDataset() == dataset {
			result = append(result, receive)
		}
	}
	return result
}

// TestGuestReceiveStatus serves a receiving daemon the way `boomerangz
// daemon` does, with a native replication listener, and checks what its
// status shows of receives into it: a native receive with the sender's host
// and estimate and its bytes so far, shown once even while the resume token
// it leaves is unchanged long enough to look interrupted; and a bare
// `zfs receive -s`, as a plain ssh transfer runs, shown without figures as
// receiving while its token moves and as interrupted once it stops.
func TestGuestReceiveStatus(t *testing.T) {
	sourcePool, destinationPool := guestPools(t)
	direct, err := zfs.NewDirect("zfs")
	if err != nil {
		t.Fatal(err)
	}
	source := zfstest.PayloadVolume(t, zfstest.FixtureName(sourcePool, "receive-status"), 128, 48)
	zfsCommand(t, "snapshot", source+"@a")
	destinationRoot := zfstest.FixtureName(destinationPool, "receive-status")
	zfsCommand(t, "create", "-o", "mountpoint=none", destinationRoot)
	zfstest.RegisterCleanup(t, destinationRoot)

	dir := t.TempDir()
	identityDir := filepath.Join(dir, "identity")
	listener := config.ListenerConfig{Network: "tcp", Address: "127.0.0.1:" + freePort(t), AuthMode: "token", ReplicationRoots: []string{destinationRoot}}
	listener.AdvertisedAddress = strings.Replace(listener.Address, "127.0.0.1:", "localhost:", 1)
	if err := os.MkdirAll(identityDir, 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := control.NewTokenStore(identityDir)
	if err != nil {
		t.Fatal(err)
	}
	bundle, err := control.CreateListenerPairing(store, identityDir, "replication", listener, "", "", []string{"replicate"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	cfg := config.Defaults()
	cfg.Daemon.ReconcileInterval.Duration = 200 * time.Millisecond
	cfg.Paths.IdentityDir = identityDir
	cfg.Paths.CredentialsDir = filepath.Join(dir, "credentials")
	cfg.Paths.SocketPath = filepath.Join(dir, "daemon.sock")
	cfg.Listeners["replication"] = listener
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	runtime, err := daemon.New(cfg, &scopedDaemonBackend{Direct: direct, root: destinationRoot}, guestInstallation, logger)
	if err != nil {
		t.Fatal(err)
	}
	server, err := control.StartServerWithReplication(cfg, runtime, direct, "zfs", logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })
	serveDaemon(t, runtime)
	client, err := control.DialLocal(t.Context(), cfg.Paths.SocketPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Connection.Close() })
	watch := watchReceives(t, client)

	t.Run("native", func(t *testing.T) {
		// The sender's zfs is a wrapper that passes half a megabyte of the
		// stream and then holds the rest for receiveStatusPause. dd with
		// fullblock reads exactly what it passes on, so the stream stays
		// whole.
		wrapper := filepath.Join(t.TempDir(), "zfs")
		script := fmt.Sprintf("#!/bin/sh\nzfs \"$@\" | { dd bs=64K count=8 iflag=fullblock status=none; sleep %d; cat; }\n", int(receiveStatusPause/time.Second))
		if err := os.WriteFile(wrapper, []byte(script), 0o755); err != nil {
			t.Fatal(err)
		}
		endpoint, err := native.Open(t.Context(), bundle, destinationRoot, wrapper)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = endpoint.Close() })
		send := zfs.SendOptions{Source: source, Snapshot: source + "@a"}
		estimate, err := direct.EstimateSend(t.Context(), send)
		if err != nil || !estimate.Known {
			t.Fatalf("estimate=%+v err=%v: the receive's total needs a known estimate", estimate, err)
		}
		target := destinationRoot + "/native"
		done := make(chan error, 1)
		go func() {
			_, runErr := endpoint.Stream().Run(t.Context(), send, zfs.ReceiveOptions{Root: target, Discard: zfs.ReceiveExact}, estimate, nil)
			done <- runErr
		}()
		host, _ := os.Hostname()
		receives := watch.await(t, 30*time.Second, "the native receive with its first bytes", func(receives []*controlrpc.ReceiveStatus) bool {
			matched := receivesInto(receives, target)
			return len(matched) == 1 && matched[0].GetBytes() >= 8*64*1024
		})
		receive := receivesInto(receives, target)[0]
		if receive.GetState() != "receiving" || receive.GetTransport() != "native" || receive.GetSenderHost() != host || !receive.GetTotalKnown() || receive.GetTotalBytes() != estimate.Bytes || receive.GetBytes() >= estimate.Bytes || receive.GetStartedUnixNano() == 0 {
			t.Fatalf("native receive shown as %v, want receiving over native from %q with total %d", receive, host, estimate.Bytes)
		}
		// Through the pause the receive's resume token stands still for
		// longer than the window, which alone would read as an interrupted
		// receive. Every update until the transfer ends shows this one
		// receive, and nothing else.
		deadline := time.After(2 * time.Minute)
		for finished := false; !finished; {
			select {
			case runErr := <-done:
				if runErr != nil {
					t.Fatal(runErr)
				}
				finished = true
			case receives = <-watch.updates:
				mine := slices.DeleteFunc(slices.Clone(receives), func(receive *controlrpc.ReceiveStatus) bool {
					return !strings.HasPrefix(receive.GetDataset(), destinationRoot+"/")
				})
				if matched := receivesInto(receives, target); len(mine) > 1 || (len(matched) == 1 && matched[0].GetTransport() != "native") {
					t.Fatalf("the native receive shows as %v", receives)
				}
			case err := <-watch.failed:
				t.Fatalf("status watch ended during the native receive: %v", err)
			case <-deadline:
				t.Fatal("native transfer did not finish")
			}
		}
		watch.await(t, 10*time.Second, "no receive once the transfer finished", func(receives []*controlrpc.ReceiveStatus) bool {
			return len(receivesInto(receives, target)) == 0
		})
	})

	t.Run("traced", func(t *testing.T) {
		stream, err := exec.CommandContext(t.Context(), "zfs", "send", source+"@a").Output()
		if err != nil {
			t.Fatal(err)
		}
		target := destinationRoot + "/plain"
		receiver := exec.CommandContext(t.Context(), "zfs", "receive", "-u", "-s", target)
		input, err := receiver.StdinPipe()
		if err != nil {
			t.Fatal(err)
		}
		if err := receiver.Start(); err != nil {
			t.Fatal(err)
		}
		// Feed the receive slowly enough that it runs across several
		// transaction groups, as a transfer over a network would.
		feeding := make(chan struct{})
		fed := make(chan struct{})
		go func() {
			defer close(fed)
			chunk := len(stream) / 64
			for offset := 0; offset+chunk < len(stream)/2; offset += chunk {
				if _, err := input.Write(stream[offset : offset+chunk]); err != nil {
					return
				}
				select {
				case <-feeding:
					return
				case <-time.After(500 * time.Millisecond):
				}
			}
		}()
		receives := watch.await(t, 40*time.Second, "the traced receive as receiving", func(receives []*controlrpc.ReceiveStatus) bool {
			matched := receivesInto(receives, target)
			return len(matched) == 1 && matched[0].GetState() == "receiving"
		})
		if receive := receivesInto(receives, target)[0]; receive.GetTransport() != "" || receive.GetBytes() != 0 || receive.GetTotalKnown() || receive.GetSenderHost() != "" {
			t.Fatalf("a traced receive carries figures it cannot know: %v", receive)
		}
		close(feeding)
		<-fed
		_ = input.Close()
		if err := receiver.Wait(); err == nil {
			t.Fatal("a truncated receive succeeded")
		}
		watch.await(t, 40*time.Second, "the traced receive as interrupted", func(receives []*controlrpc.ReceiveStatus) bool {
			matched := receivesInto(receives, target)
			return len(matched) == 1 && matched[0].GetState() == "interrupted"
		})
		zfsCommand(t, "receive", "-A", target)
		watch.await(t, 10*time.Second, "no receive once the partial state was discarded", func(receives []*controlrpc.ReceiveStatus) bool {
			return len(receivesInto(receives, target)) == 0
		})
	})
}
