package config

import (
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
)

var namePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]*$`)

// Validate checks the complete merged configuration.
func (c Config) Validate() error {
	var problems []error
	if c.Daemon.ReconcileInterval.Duration <= 0 {
		problems = append(problems, errors.New("daemon.reconcile_interval must be positive"))
	}
	if c.Daemon.ManagementWorkers < 1 {
		problems = append(problems, errors.New("daemon.management_workers must be at least 1"))
	}
	if c.Daemon.TransferWorkers < 2 {
		problems = append(problems, errors.New("daemon.transfer_workers must be at least 2"))
	}
	for field, value := range map[string]string{
		"paths.credentials_dir": c.Paths.CredentialsDir,
		"paths.identity_dir":    c.Paths.IdentityDir,
		"paths.socket_path":     c.Paths.SocketPath,
	} {
		if !filepath.IsAbs(value) {
			problems = append(problems, fmt.Errorf("%s must be an absolute path", field))
		}
	}
	for name, remote := range c.Remotes {
		if !namePattern.MatchString(name) {
			problems = append(problems, fmt.Errorf("remote %q has an invalid name", name))
		}
		if remote.Transport != "ssh" {
			problems = append(problems, fmt.Errorf("remote %q: transport must be ssh", name))
		}
		if remote.Host == "" || remote.Root == "" {
			problems = append(problems, fmt.Errorf("remote %q: host and root are required", name))
		}
		if remote.Port < 0 || remote.Port > 65535 {
			problems = append(problems, fmt.Errorf("remote %q: port must be between 0 and 65535", name))
		}
		if remote.ConnectTimeout.Duration < 0 {
			problems = append(problems, fmt.Errorf("remote %q: connect_timeout cannot be negative", name))
		}
		if strings.ContainsAny(remote.Root, "@#\t\r\n ") || strings.HasPrefix(remote.Root, "/") {
			problems = append(problems, fmt.Errorf("remote %q: root must be a ZFS dataset name", name))
		}
	}
	for name, listener := range c.Listeners {
		if !namePattern.MatchString(name) {
			problems = append(problems, fmt.Errorf("listener %q has an invalid name", name))
		}
		switch listener.Network {
		case "unix":
			if !filepath.IsAbs(listener.Address) {
				problems = append(problems, fmt.Errorf("listener %q: unix address must be absolute", name))
			}
		case "tcp":
			if listener.Address == "" {
				problems = append(problems, fmt.Errorf("listener %q: address is required", name))
			}
			if listener.AuthMode != "token" && listener.AuthMode != "mtls" && listener.AuthMode != "mtls+token" {
				problems = append(problems, fmt.Errorf("listener %q: TCP auth_mode must be token, mtls, or mtls+token", name))
			}
			if listener.TLSCert == "" || listener.TLSKey == "" {
				problems = append(problems, fmt.Errorf("listener %q: TCP tls_cert and tls_key are required", name))
			}
		default:
			problems = append(problems, fmt.Errorf("listener %q: network must be unix or tcp", name))
		}
	}
	return errors.Join(problems...)
}
