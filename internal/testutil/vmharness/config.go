// Package vmharness builds and launches disposable guest-only ZFS test VMs.
package vmharness

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// Harness defaults and its deliberately unprivileged libvirt connection.
const (
	LibvirtURI       = "qemu:///session"
	DefaultMemoryMiB = 4096
	DefaultVCPUs     = 2
	DefaultDiskSize  = "8G"
)

var (
	runIDPattern    = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{5,47}$`)
	diskSizePattern = regexp.MustCompile(`^[1-9][0-9]*[GM]$`)
)

// NewRunID returns a collision-resistant identifier safe for paths and names.
func NewRunID() (string, error) {
	random := make([]byte, 8)
	if _, err := rand.Read(random); err != nil {
		return "", fmt.Errorf("generate integration run ID: %w", err)
	}
	return "run-" + hex.EncodeToString(random), nil
}

// Config controls one disposable integration guest.
type Config struct {
	BaseImage string
	WorkDir   string
	RunID     string
	SSHPort   int
	MemoryMiB int
	VCPUs     int
	DiskSize  string
}

// Defaults applies non-zero resource defaults.
func (c Config) Defaults() Config {
	if c.MemoryMiB == 0 {
		c.MemoryMiB = DefaultMemoryMiB
	}
	if c.VCPUs == 0 {
		c.VCPUs = DefaultVCPUs
	}
	if c.DiskSize == "" {
		c.DiskSize = DefaultDiskSize
	}
	return c
}

// Validate checks paths, identifiers, resources, and the forwarded SSH port.
func (c Config) Validate() error {
	var problems []error
	if !filepath.IsAbs(c.BaseImage) {
		problems = append(problems, errors.New("base image path must be absolute"))
	} else if info, err := os.Stat(c.BaseImage); err != nil {
		problems = append(problems, fmt.Errorf("inspect base image: %w", err))
	} else if !info.Mode().IsRegular() {
		problems = append(problems, errors.New("base image must be a regular file"))
	} else if info.Mode().Perm()&0o222 != 0 {
		problems = append(problems, errors.New("base image must be read-only"))
	}
	if !filepath.IsAbs(c.WorkDir) {
		problems = append(problems, errors.New("work directory must be absolute"))
	} else if info, err := os.Stat(c.WorkDir); err != nil {
		problems = append(problems, fmt.Errorf("inspect work directory: %w", err))
	} else if !info.IsDir() {
		problems = append(problems, errors.New("work directory must be a directory"))
	}
	if !runIDPattern.MatchString(c.RunID) {
		problems = append(problems, errors.New("run ID must be 6-48 lowercase letters, digits, or hyphens"))
	}
	if c.SSHPort < 1024 || c.SSHPort > 65535 {
		problems = append(problems, errors.New("SSH port must be between 1024 and 65535"))
	} else if available, err := localPortAvailable(c.SSHPort); err != nil {
		problems = append(problems, fmt.Errorf("check SSH port: %w", err))
	} else if !available {
		problems = append(problems, fmt.Errorf("SSH port %d is already in use", c.SSHPort))
	}
	if c.MemoryMiB < 2048 {
		problems = append(problems, errors.New("guest memory must be at least 2048 MiB"))
	}
	if c.VCPUs < 1 {
		problems = append(problems, errors.New("guest must have at least one vCPU"))
	}
	if !diskSizePattern.MatchString(c.DiskSize) {
		problems = append(problems, errors.New("disk size must be a positive integer followed by G or M"))
	}
	if c.BaseImage != "" && c.WorkDir != "" {
		base, baseErr := filepath.EvalSymlinks(c.BaseImage)
		work, workErr := filepath.EvalSymlinks(c.WorkDir)
		if baseErr == nil && workErr == nil && pathWithin(work, base) {
			problems = append(problems, errors.New("base image must not be stored inside the per-run work directory"))
		}
	}
	return errors.Join(problems...)
}

func localPortAvailable(port int) (bool, error) {
	listenConfig := net.ListenConfig{}
	listener, err := listenConfig.Listen(context.Background(), "tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		if strings.Contains(err.Error(), "address already in use") {
			return false, nil
		}
		return false, err
	}
	return true, listener.Close()
}

func pathWithin(parent, child string) bool {
	relative, err := filepath.Rel(parent, child)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}
