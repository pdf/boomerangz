// Package ssh provides bounded, non-interactive SSH replication transports.
package ssh

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/pdf/boomerangz/internal/zfs"
)

const remoteOutputLimit = 32 * 1024 * 1024

// DefaultConnectTimeout bounds an otherwise unspecified SSH handshake.
const DefaultConnectTimeout = 10 * time.Second

// Config contains only supported SSH connection settings, never arbitrary
// options or remote shell fragments.
type Config struct {
	Host           string
	Port           int
	User           string
	Root           string
	IdentityFile   string
	ShellPath      string
	ConnectTimeout time.Duration
}

// Client owns one authenticated SSH endpoint and its typed remote executor.
type Client struct {
	config      Config
	command     zfs.CommandFactory
	exec        zfs.Executor
	controlPath string
	controlDir  string
}

// UnavailableError identifies a connection-level failure suitable for bounded
// retry. A remote ZFS safety or validation failure is not temporary.
type UnavailableError struct{ Err error }

func (e *UnavailableError) Error() string { return "SSH endpoint unavailable: " + e.Err.Error() }
func (e *UnavailableError) Unwrap() error { return e.Err }

// Temporary permits roadwarrior retry classification.
func (e *UnavailableError) Temporary() bool { return true }

// New constructs a strict OpenSSH client using an explicit executable path.
func New(path string, config Config) (*Client, error) {
	if path == "" {
		return nil, fmt.Errorf("SSH executable path is required")
	}
	return newClient(config, func(ctx context.Context, args []string) *exec.Cmd {
		return exec.CommandContext(ctx, path, args...)
	})
}

func newClient(config Config, command zfs.CommandFactory) (*Client, error) {
	if command == nil {
		return nil, fmt.Errorf("SSH command factory is required")
	}
	if err := validateConfig(config); err != nil {
		return nil, err
	}
	client := &Client{config: config, command: command}
	executor, err := zfs.NewDirectWithRunners(remoteRunner{client: client, binary: "zfs"}, remoteRunner{client: client, binary: "zpool"})
	if err != nil {
		return nil, err
	}
	client.exec = &scopedExecutor{backend: executor, root: config.Root}
	return client, nil
}

func validateConfig(config Config) error {
	if config.Host == "" || strings.HasPrefix(config.Host, "-") || strings.ContainsAny(config.Host, "@\x00\r\n\t ") {
		return fmt.Errorf("valid SSH host is required")
	}
	if config.Port < 0 || config.Port > 65535 {
		return fmt.Errorf("SSH port must be between 0 and 65535")
	}
	if config.ConnectTimeout < 0 {
		return fmt.Errorf("SSH connect timeout cannot be negative")
	}
	if config.User != "" && (strings.HasPrefix(config.User, "-") || strings.ContainsAny(config.User, "@/:\x00\r\n\t ")) {
		return fmt.Errorf("invalid SSH user")
	}
	if config.IdentityFile != "" && (!filepath.IsAbs(config.IdentityFile) || strings.ContainsAny(config.IdentityFile, "\x00\r\n")) {
		return fmt.Errorf("SSH identity file must be an absolute path")
	}
	if config.ShellPath != "" && config.ShellPath != "boomerangz" && (!filepath.IsAbs(config.ShellPath) || strings.ContainsAny(config.ShellPath, "\x00\r\n\t ")) {
		return fmt.Errorf("SSH shell path must be boomerangz or an absolute executable path")
	}
	if err := zfs.ValidateDataset(config.Root); err != nil {
		return fmt.Errorf("SSH destination root: %w", err)
	}
	return nil
}

func (c *Client) shellPath() string {
	if c.config.ShellPath == "" {
		return "boomerangz"
	}
	return c.config.ShellPath
}

func (c *Client) port() int {
	if c.config.Port == 0 {
		return 22
	}
	return c.config.Port
}

func (c *Client) connectTimeout() time.Duration {
	if c.config.ConnectTimeout == 0 {
		return DefaultConnectTimeout
	}
	return c.config.ConnectTimeout
}

// CanonicalTarget returns the stable endpoint identity stored in source-side
// target bindings. It is independent of direct or SSH-shell endpoint mode.
func (c *Client) CanonicalTarget() string {
	host := net.JoinHostPort(strings.ToLower(c.config.Host), strconv.Itoa(c.port()))
	u := url.URL{Scheme: "ssh", Host: host, Path: "/" + c.config.Root}
	if c.config.User != "" {
		u.User = url.User(c.config.User)
	}
	return u.String()
}

// Executor exposes typed, bounded ZFS operations over this endpoint.
func (c *Client) Executor() zfs.Executor { return c.exec }

// Probe performs one just-in-time sparse inventory query.
func (c *Client) Probe(ctx context.Context) ([]zfs.Dataset, error) {
	return c.exec.ListDatasets(ctx)
}

func (c *Client) sshArguments(remoteCommand string) []string {
	args := []string{"-T", "-o", "BatchMode=yes", "-o", "ClearAllForwardings=yes", "-o", "ForwardAgent=no", "-o", "StrictHostKeyChecking=yes"}
	if c.controlPath != "" {
		args = append(args, "-o", "ControlMaster=auto", "-o", "ControlPersist=60", "-o", "ControlPath="+c.controlPath)
	}
	seconds := int((c.connectTimeout() + time.Second - 1) / time.Second)
	args = append(args, "-o", "ConnectTimeout="+strconv.Itoa(seconds))
	if c.config.Port != 0 {
		args = append(args, "-p", strconv.Itoa(c.config.Port))
	}
	if c.config.IdentityFile != "" {
		args = append(args, "-o", "IdentitiesOnly=yes", "-i", c.config.IdentityFile)
	}
	destination := c.config.Host
	if c.config.User != "" {
		destination = c.config.User + "@" + destination
	}
	args = append(args, "--", destination)
	if remoteCommand != "" {
		args = append(args, remoteCommand)
	}
	return args
}

// multiplexed returns an endpoint-scoped client whose OpenSSH processes share
// one authenticated transport through a private control socket.
func (c *Client) multiplexed() (*Client, error) {
	directory, err := os.MkdirTemp("", "boomerangz-ssh-")
	if err != nil {
		return nil, fmt.Errorf("create SSH control directory: %w", err)
	}
	clone, err := newClient(c.config, c.command)
	if err != nil {
		_ = os.RemoveAll(directory)
		return nil, err
	}
	clone.controlDir = directory
	clone.controlPath = filepath.Join(directory, "control")
	return clone, nil
}

func (c *Client) closeMultiplexed(ctx context.Context) error {
	if c == nil || c.controlPath == "" {
		return nil
	}
	args := c.sshArguments("")
	separator := -1
	for index, arg := range args {
		if arg == "--" {
			separator = index
			break
		}
	}
	if separator < 0 {
		return fmt.Errorf("construct SSH control command")
	}
	controlArgs := []string{"-O", "exit"}
	controlArgs = append(controlArgs, args[separator:]...)
	args = append(args[:separator], controlArgs...)
	command := c.command(ctx, args)
	err := command.Run()
	removeErr := os.RemoveAll(c.controlDir)
	var exit *exec.ExitError
	if errors.As(err, &exit) && exit.ExitCode() == 255 {
		// No master is a normal outcome when opening the endpoint failed before
		// the first remote command established the control connection.
		err = nil
	}
	return errors.Join(err, removeErr)
}

func quoteArgument(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}

func remoteCommand(binary string, args []string) string {
	parts := make([]string, 0, len(args)+1)
	parts = append(parts, quoteArgument(binary))
	for _, arg := range args {
		parts = append(parts, quoteArgument(arg))
	}
	return strings.Join(parts, " ")
}

type boundedOutput struct{ buffer bytes.Buffer }

func (b *boundedOutput) Write(data []byte) (int, error) {
	if len(data) > remoteOutputLimit-b.buffer.Len() {
		return 0, fmt.Errorf("remote output exceeds 32 MiB limit")
	}
	return b.buffer.Write(data)
}

type remoteRunner struct {
	client *Client
	binary string
}

func (r remoteRunner) Run(ctx context.Context, args ...string) ([]byte, error) {
	command := r.client.command(ctx, r.client.sshArguments(remoteCommand(r.binary, args)))
	output := &boundedOutput{}
	command.Stdout, command.Stderr = output, output
	err := command.Run()
	if err != nil {
		wrapped := fmt.Errorf("remote %s %s: %w: %s", r.binary, firstArgument(args), err, strings.TrimSpace(output.buffer.String()))
		if unavailable(ctx, err) {
			return nil, &UnavailableError{Err: wrapped}
		}
		return nil, wrapped
	}
	return output.buffer.Bytes(), nil
}

func firstArgument(args []string) string {
	if len(args) == 0 {
		return "command"
	}
	return args[0]
}

func unavailable(ctx context.Context, err error) bool {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return true
	}
	if errors.Is(ctx.Err(), context.Canceled) {
		return false
	}
	var exit *exec.ExitError
	return errors.As(err, &exit) && exit.ExitCode() == 255
}
