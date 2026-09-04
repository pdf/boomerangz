package vmharness

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

// Tools contains resolved host executables used by the harness.
type Tools struct {
	QEMUImg     string `json:"qemu_img"`
	Virsh       string `json:"virsh"`
	VirtInstall string `json:"virt_install"`
	Passt       string `json:"passt"`
}

// Check is one preflight result.
type Check struct {
	Name   string `json:"name"`
	OK     bool   `json:"ok"`
	Detail string `json:"detail"`
}

// PreflightResult reports every prerequisite instead of stopping at the first.
type PreflightResult struct {
	Checks []Check `json:"checks"`
	Tools  Tools   `json:"tools"`
}

// OK reports whether every prerequisite passed.
func (r PreflightResult) OK() bool {
	for _, check := range r.Checks {
		if !check.OK {
			return false
		}
	}
	return true
}

// Preflight validates host tools, KVM access, image format, and session libvirt.
// It never invokes zfs or zpool.
func Preflight(ctx context.Context, config Config) PreflightResult {
	result := PreflightResult{}
	tools := []struct {
		name        string
		destination *string
	}{
		{name: "qemu-img", destination: &result.Tools.QEMUImg},
		{name: "virsh", destination: &result.Tools.Virsh},
		{name: "virt-install", destination: &result.Tools.VirtInstall},
		{name: "passt", destination: &result.Tools.Passt},
	}
	for _, tool := range tools {
		path, err := exec.LookPath(tool.name)
		if err != nil {
			result.Checks = append(result.Checks, Check{Name: tool.name, Detail: "not found in PATH"})
			continue
		}
		*tool.destination = path
		result.Checks = append(result.Checks, Check{Name: tool.name, OK: true, Detail: path})
	}

	kvm, err := os.OpenFile("/dev/kvm", os.O_RDWR, 0)
	if err != nil {
		result.Checks = append(result.Checks, Check{Name: "kvm-access", Detail: err.Error()})
	} else {
		result.Checks = append(result.Checks, Check{Name: "kvm-access", OK: true, Detail: "/dev/kvm is readable and writable"})
		_ = kvm.Close()
	}

	if err := config.Defaults().Validate(); err != nil {
		result.Checks = append(result.Checks, Check{Name: "configuration", Detail: err.Error()})
	} else {
		result.Checks = append(result.Checks, Check{Name: "configuration", OK: true, Detail: "paths and resources are valid"})
	}

	commandRunner := execRunner{}
	if result.Tools.QEMUImg != "" && config.BaseImage != "" {
		if _, err := commandRunner.Run(ctx, result.Tools.QEMUImg, "info", "--output=json", config.BaseImage); err != nil {
			result.Checks = append(result.Checks, Check{Name: "base-image", Detail: err.Error()})
		} else {
			result.Checks = append(result.Checks, Check{Name: "base-image", OK: true, Detail: config.BaseImage})
		}
	}
	if result.Tools.Virsh != "" {
		output, err := commandRunner.Run(ctx, result.Tools.Virsh, "-c", LibvirtURI, "uri")
		if err != nil {
			result.Checks = append(result.Checks, Check{Name: "session-libvirt", Detail: err.Error()})
		} else {
			result.Checks = append(result.Checks, Check{Name: "session-libvirt", OK: true, Detail: fmt.Sprintf("connected to %s", strings.TrimSpace(string(output)))})
		}
	}
	return result
}
