package ssh

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"

	"github.com/pdf/boomerangz/internal/zfs"
)

// Stream sends local ZFS output into a direct remote ZFS receive over SSH.
type Stream struct {
	client *Client
	sender zfs.CommandFactory
}

// NewStream constructs a direct SSH/ZFS stream without requiring boomerangz on
// the destination.
func NewStream(client *Client, zfsPath string) (*Stream, error) {
	if client == nil || zfsPath == "" {
		return nil, fmt.Errorf("SSH client and local ZFS executable are required")
	}
	return &Stream{client: client, sender: func(ctx context.Context, args []string) *exec.Cmd {
		return exec.CommandContext(ctx, zfsPath, args...)
	}}, nil
}

// Run executes one validated local-send/remote-receive pipeline.
func (s *Stream) Run(ctx context.Context, send zfs.SendOptions, receive zfs.ReceiveOptions, estimate zfs.Estimate, report func(zfs.Progress)) (zfs.Progress, error) {
	if receive.Root != s.client.config.Root && !strings.HasPrefix(receive.Root, s.client.config.Root+"/") {
		return zfs.Progress{}, fmt.Errorf("receive root differs from configured SSH destination scope")
	}
	result, err := zfs.RunPipeline(ctx, send, receive, estimate, report, s.sender, func(ctx context.Context, args []string) *exec.Cmd {
		return s.client.command(ctx, s.client.sshArguments(remoteCommand("zfs", args)))
	})
	if err != nil && unavailable(ctx, err) {
		return result, &UnavailableError{Err: err}
	}
	return result, err
}

// IsUnavailable reports whether an error represents connectivity suitable for
// roadwarrior retry rather than a replication safety failure.
func IsUnavailable(err error) bool {
	var unavailableError *UnavailableError
	return errors.As(err, &unavailableError)
}
