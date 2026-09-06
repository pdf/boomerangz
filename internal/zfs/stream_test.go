package zfs

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
)

func TestStreamArguments(t *testing.T) {
	t.Parallel()
	send := SendOptions{Snapshot: "tank/data@end", Base: "tank/data@base", Intermediates: true, Properties: true, LargeBlocks: true}
	args, err := sendArgs(send, false)
	if err != nil || !reflect.DeepEqual(args, []string{"send", "-L", "-p", "-I", "tank/data@base", "tank/data@end"}) {
		t.Fatalf("args=%v err=%v", args, err)
	}
	receive := ReceiveOptions{Root: "backup/data", Discard: ReceiveDropFirst, Set: map[string]string{"readonly": "on"}, Exclude: []string{"readonly", "org.boomerangz:enabled", "compression", "compression"}}
	args, err = receiveArgs(receive)
	if err != nil || !reflect.DeepEqual(args, []string{"receive", "-u", "-s", "-d", "-o", "readonly=on", "-x", "compression", "-x", "org.boomerangz:enabled", "backup/data"}) {
		t.Fatalf("args=%v err=%v", args, err)
	}
	send.Base = "tank/data#cursor"
	if _, err := sendArgs(send, false); err == nil {
		t.Fatal("accepted bookmark for -I")
	}
	send.Intermediates = false
	if _, err := sendArgs(send, false); err != nil {
		t.Fatal(err)
	}
	receive.Set = map[string]string{"org.boomerangz:enabled": "on"}
	if _, err := receiveArgs(receive); err == nil {
		t.Fatal("accepted reserved receive override")
	}
}

func TestReceiveMapping(t *testing.T) {
	t.Parallel()
	for mode, want := range map[ReceiveDiscard]string{ReceiveExact: "backup/root", ReceiveDropFirst: "backup/root/projects/app", ReceiveDropAll: "backup/root/app"} {
		got, err := MapReceiveDataset("tank/projects/app", "backup/root", mode)
		if err != nil || got != want {
			t.Fatalf("%s: %s %v", mode, got, err)
		}
	}
}

func TestSendEstimate(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		output  string
		known   bool
		size    uint64
		wantErr bool
	}{{"full\ttank/data@end\t123\nsize\t123\n", true, 123, false}, {"unsupported\n", false, 0, false}, {"size\tbogus\n", false, 0, true}, {"size\t1\nsize\t2\n", false, 0, true}} {
		d := &Direct{runner: &fakeRunner{output: []byte(tc.output)}}
		estimate, err := d.EstimateSend(t.Context(), SendOptions{Snapshot: "tank/data@end"})
		if (err != nil) != tc.wantErr || estimate.Known != tc.known || estimate.Bytes != tc.size {
			t.Fatalf("estimate=%v err=%v", estimate, err)
		}
	}
}

// Helper subprocesses avoid invoking real ZFS in host tests.
func TestStreamHelperProcess(_ *testing.T) {
	mode := os.Getenv("BOOMERANGZ_STREAM_HELPER")
	if mode == "" {
		return
	}
	switch mode {
	case "send":
		_, err := io.Copy(os.Stdout, bytes.NewReader(bytes.Repeat([]byte{7}, 1024*1024)))
		if err != nil {
			os.Exit(2)
		}
	case "receive":
		_, err := io.Copy(io.Discard, os.Stdin)
		if err != nil {
			os.Exit(3)
		}
	case "fail":
		fmt.Fprintln(os.Stderr, "injected failure")
		os.Exit(4)
	case "large-stderr":
		fmt.Fprint(os.Stderr, strings.Repeat("e", 128*1024))
		os.Exit(5)
	case "blocked":
		time.Sleep(time.Minute)
	default:
		os.Exit(6)
	}
	os.Exit(0)
}

func TestLocalStreamProcesses(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		send, receive string
		success       bool
	}{{"send", "receive", true}, {"fail", "receive", false}, {"send", "fail", false}, {"send", "large-stderr", false}, {"blocked", "fail", false}} {
		t.Run(tc.send+"-"+tc.receive, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			stream := &LocalStream{command: func(ctx context.Context, args ...string) *exec.Cmd {
				mode := tc.receive
				if args[0] == "send" {
					mode = tc.send
				}
				cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestStreamHelperProcess$")
				cmd.Env = append(os.Environ(), "BOOMERANGZ_STREAM_HELPER="+mode)
				return cmd
			}}
			var events []Progress
			result, err := stream.Run(ctx, SendOptions{Snapshot: "tank/data@end"}, ReceiveOptions{Root: "backup/data", Discard: ReceiveExact}, Estimate{}, func(p Progress) { events = append(events, p) })
			if (err == nil) != tc.success || result.Completed != tc.success {
				t.Fatalf("result=%v err=%v", result, err)
			}
			if ctx.Err() != nil {
				t.Fatal("pipeline failed to reap promptly")
			}
			if tc.success && (result.Bytes != 1024*1024 || len(events) < 2 || events[len(events)-1].ETA != nil) {
				t.Fatalf("progress=%v", events)
			}
			if err != nil && len(err.Error()) > 70*1024 {
				t.Fatal("unbounded diagnostics")
			}
		})
	}
}

func TestLocalStreamCancellation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	stream := &LocalStream{command: func(ctx context.Context, _ ...string) *exec.Cmd {
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestStreamHelperProcess$")
		cmd.Env = append(os.Environ(), "BOOMERANGZ_STREAM_HELPER=blocked")
		return cmd
	}}
	result, err := stream.Run(ctx, SendOptions{Snapshot: "tank/data@end"}, ReceiveOptions{Root: "backup/data", Discard: ReceiveExact}, Estimate{}, func(Progress) { cancel() })
	if err == nil || result.Completed {
		t.Fatal("cancelled stream succeeded")
	}
}
