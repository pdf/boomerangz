package ssh

import (
	"context"
	"fmt"

	"github.com/pdf/boomerangz/internal/zfs"
)

// Endpoint is one negotiated destination implementation for a replication
// attempt. Mode is suitable for status output.
type Endpoint struct {
	Mode     string
	Executor zfs.Executor
	Stream   interface {
		Run(context.Context, zfs.SendOptions, zfs.ReceiveOptions, zfs.Estimate, func(zfs.Progress)) (zfs.Progress, error)
	}
	shell *Shell
}

// OpenEndpoint selects direct or SSH-shell operation. Empty mode means auto.
// Connectivity failures never trigger fallback because direct mode would use
// the same unavailable SSH host.
func OpenEndpoint(ctx context.Context, client *Client, zfsPath, mode string) (*Endpoint, error) {
	if client == nil {
		return nil, fmt.Errorf("SSH client is required")
	}
	if mode == "" {
		mode = "auto"
	}
	if mode == "auto" || mode == "ssh-shell" {
		shell, err := NewShell(ctx, client)
		if err == nil {
			stream, streamErr := shell.NewStream(zfsPath)
			if streamErr != nil {
				_ = shell.Close()
				return nil, streamErr
			}
			return &Endpoint{Mode: "ssh-shell", Executor: shell.Executor(), Stream: stream, shell: shell}, nil
		}
		if mode == "ssh-shell" || !IsShellUnavailable(err) {
			return nil, err
		}
	}
	if mode != "auto" && mode != "direct" {
		return nil, fmt.Errorf("unsupported SSH endpoint mode %q", mode)
	}
	stream, err := NewStream(client, zfsPath)
	if err != nil {
		return nil, err
	}
	return &Endpoint{Mode: "direct", Executor: client.Executor(), Stream: stream}, nil
}

// Close releases an optional persistent SSH-shell session.
func (e *Endpoint) Close() error {
	if e == nil || e.shell == nil {
		return nil
	}
	return e.shell.Close()
}
