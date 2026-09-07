package config

import (
	"errors"
	"fmt"
	"net"
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
	if c.Daemon.InactiveGracePeriod.Duration < 0 {
		problems = append(problems, errors.New("daemon.inactive_grace_period must be nonnegative (0 disables automatic retirement)"))
	}
	if c.Daemon.ManagementWorkers < 0 {
		problems = append(problems, errors.New("daemon.management_workers must be nonnegative (0 selects automatic sizing)"))
	}
	if c.Daemon.LocalTransferWorkers < 1 {
		problems = append(problems, errors.New("daemon.local_transfer_workers must be at least 1"))
	}
	if c.Daemon.RemoteTransferWorkers < 1 {
		problems = append(problems, errors.New("daemon.remote_transfer_workers must be at least 1"))
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
		switch remote.Transport {
		case "ssh":
			if remote.Endpoint != "" && remote.Endpoint != "auto" && remote.Endpoint != "direct" && remote.Endpoint != "ssh-shell" {
				problems = append(problems, fmt.Errorf("remote %q: endpoint must be auto, direct, or ssh-shell", name))
			}
			if remote.Host == "" {
				problems = append(problems, fmt.Errorf("remote %q: host is required for SSH", name))
			}
			if remote.Credential != "" {
				problems = append(problems, fmt.Errorf("remote %q: credential is only valid for native transport", name))
			}
		case "native":
			if remote.Credential == "" || (!namePattern.MatchString(remote.Credential) && !filepath.IsAbs(remote.Credential)) {
				problems = append(problems, fmt.Errorf("remote %q: credential must be an imported name or absolute path", name))
			}
			if remote.Endpoint != "" || remote.Host != "" || remote.Port != 0 || remote.User != "" || remote.IdentityFile != "" || remote.SSHShellPath != "" {
				problems = append(problems, fmt.Errorf("remote %q: SSH connection fields are not valid for native transport", name))
			}
		default:
			problems = append(problems, fmt.Errorf("remote %q: transport must be ssh or native", name))
		}
		if remote.Root == "" {
			problems = append(problems, fmt.Errorf("remote %q: root is required", name))
		}
		if remote.Port < 0 || remote.Port > 65535 {
			problems = append(problems, fmt.Errorf("remote %q: port must be between 0 and 65535", name))
		}
		if remote.ConnectTimeout.Duration < 0 {
			problems = append(problems, fmt.Errorf("remote %q: connect_timeout cannot be negative", name))
		}
		if remote.SSHShellPath != "" && remote.SSHShellPath != "boomerangz" && (!filepath.IsAbs(remote.SSHShellPath) || strings.ContainsAny(remote.SSHShellPath, "\x00\r\n\t ")) {
			problems = append(problems, fmt.Errorf("remote %q: ssh_shell_path must be boomerangz or an absolute executable path", name))
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
			if len(listener.ReplicationRoots) != 0 {
				problems = append(problems, fmt.Errorf("listener %q: replication_roots require TCP", name))
			}
		case "tcp":
			if listener.Address == "" {
				problems = append(problems, fmt.Errorf("listener %q: address is required", name))
			}
			if listener.AuthMode != "token" && listener.AuthMode != "mtls" && listener.AuthMode != "mtls+token" {
				problems = append(problems, fmt.Errorf("listener %q: TCP auth_mode must be token, mtls, or mtls+token", name))
			}
			if (listener.TLSCert == "") != (listener.TLSKey == "") {
				problems = append(problems, fmt.Errorf("listener %q: tls_cert and tls_key must be supplied together", name))
			}
			if listener.TLSCert == "" {
				host, _, err := net.SplitHostPort(listener.AdvertisedAddress)
				if err != nil || host == "" || host == "0.0.0.0" || host == "::" {
					problems = append(problems, fmt.Errorf("listener %q: managed TLS requires a client-visible advertised_address", name))
				}
			}
			for _, field := range []struct{ name, value string }{{"tls_cert", listener.TLSCert}, {"tls_key", listener.TLSKey}, {"client_ca", listener.ClientCA}, {"pairing_ca", listener.PairingCA}} {
				if field.value != "" && !filepath.IsAbs(field.value) {
					problems = append(problems, fmt.Errorf("listener %q: %s must be absolute", name, field.name))
				}
			}
			for _, root := range listener.ReplicationRoots {
				if root == "" || strings.ContainsAny(root, "@#\t\r\n ") || strings.HasPrefix(root, "/") {
					problems = append(problems, fmt.Errorf("listener %q: replication root must be a ZFS dataset name", name))
				}
			}
		default:
			problems = append(problems, fmt.Errorf("listener %q: network must be unix or tcp", name))
		}
	}
	return errors.Join(problems...)
}
