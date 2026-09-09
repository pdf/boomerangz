// Package config loads, merges, validates, and safely renders global config.
package config

import (
	"runtime"
	"time"
)

// Default global daemon settings and filesystem paths.
const (
	DefaultReconcileInterval     = time.Minute
	DefaultInactiveGracePeriod   = 24 * time.Hour
	DefaultManagementWorkers     = 0
	DefaultLocalTransferWorkers  = 2
	DefaultRemoteTransferWorkers = 1
	DefaultCredentialsDir        = "/etc/boomerangz/credentials.d"
	DefaultIdentityDir           = "/var/lib/boomerangz/identity"
	DefaultSocketPath            = "/run/boomerangz/boomerangz.sock"
)

// Config is the complete global configuration.
type Config struct {
	Daemon    DaemonConfig              `toml:"daemon" json:"daemon"`
	Paths     PathsConfig               `toml:"paths" json:"paths"`
	SSHShell  SSHShellConfig            `toml:"ssh_shell" json:"ssh_shell"`
	Remotes   map[string]RemoteConfig   `toml:"remotes" json:"remotes"`
	Listeners map[string]ListenerConfig `toml:"listeners" json:"listeners"`
}

// DaemonConfig controls reconciliation and worker concurrency.
type DaemonConfig struct {
	ReconcileInterval     Duration `toml:"reconcile_interval" json:"reconcile_interval"`
	InactiveGracePeriod   Duration `toml:"inactive_grace_period" json:"inactive_grace_period"`
	ManagementWorkers     int      `toml:"management_workers" json:"management_workers"`
	LocalTransferWorkers  int      `toml:"local_transfer_workers" json:"local_transfer_workers"`
	RemoteTransferWorkers int      `toml:"remote_transfer_workers" json:"remote_transfer_workers"`
}

// EffectiveManagementWorkers resolves auto mode to the logical CPUs available
// to this process. The configured value remains zero in config show.
func (d DaemonConfig) EffectiveManagementWorkers() int {
	if d.ManagementWorkers == 0 {
		return runtime.NumCPU()
	}
	return d.ManagementWorkers
}

// PathsConfig contains persistent and runtime filesystem locations.
type PathsConfig struct {
	CredentialsDir string `toml:"credentials_dir" json:"credentials_dir"`
	IdentityDir    string `toml:"identity_dir" json:"identity_dir"`
	SocketPath     string `toml:"socket_path" json:"socket_path"`
}

// SSHShellConfig bounds the replication service exposed by the restricted
// SSH login shell.
type SSHShellConfig struct {
	ReplicationRoots []string `toml:"replication_roots" json:"replication_roots,omitempty"`
}

// RemoteConfig defines an SSH or native replication destination.
type RemoteConfig struct {
	Transport      string   `toml:"transport" json:"transport"`
	Credential     string   `toml:"credential" json:"credential,omitempty"`
	Endpoint       string   `toml:"endpoint" json:"endpoint,omitempty"`
	Host           string   `toml:"host" json:"host"`
	Port           int      `toml:"port" json:"port"`
	User           string   `toml:"user" json:"user"`
	Root           string   `toml:"root" json:"root"`
	IdentityFile   string   `toml:"identity_file" json:"identity_file,omitempty"`
	SSHShellPath   string   `toml:"ssh_shell_path" json:"ssh_shell_path,omitempty"`
	ConnectTimeout Duration `toml:"connect_timeout" json:"connect_timeout"`
}

// ListenerConfig defines a local Unix or authenticated TCP control listener.
type ListenerConfig struct {
	Network               string   `toml:"network" json:"network"`
	Address               string   `toml:"address" json:"address"`
	AdvertisedAddress     string   `toml:"advertised_address" json:"advertised_address,omitempty"`
	AuthMode              string   `toml:"auth_mode" json:"auth_mode,omitempty"`
	TLSCert               string   `toml:"tls_cert" json:"tls_cert,omitempty"`
	TLSKey                string   `toml:"tls_key" json:"tls_key,omitempty" secret:"true"`
	ClientCA              string   `toml:"client_ca" json:"client_ca,omitempty"`
	PairingCA             string   `toml:"pairing_ca" json:"pairing_ca,omitempty"`
	PairingPinCertificate bool     `toml:"pairing_pin_certificate" json:"pairing_pin_certificate,omitempty"`
	ReplicationRoots      []string `toml:"replication_roots" json:"replication_roots,omitempty"`
}

// Defaults returns a valid configuration with no remotes or listeners.
func Defaults() Config {
	return Config{
		Daemon: DaemonConfig{
			ReconcileInterval:     Duration{DefaultReconcileInterval},
			InactiveGracePeriod:   Duration{DefaultInactiveGracePeriod},
			ManagementWorkers:     DefaultManagementWorkers,
			LocalTransferWorkers:  DefaultLocalTransferWorkers,
			RemoteTransferWorkers: DefaultRemoteTransferWorkers,
		},
		Paths: PathsConfig{
			CredentialsDir: DefaultCredentialsDir,
			IdentityDir:    DefaultIdentityDir,
			SocketPath:     DefaultSocketPath,
		},
		Remotes:   make(map[string]RemoteConfig),
		Listeners: make(map[string]ListenerConfig),
	}
}

// Duration provides TOML text encoding for time.Duration.
type Duration struct{ time.Duration }

// UnmarshalText parses a Go duration.
func (d *Duration) UnmarshalText(text []byte) error {
	parsed, err := time.ParseDuration(string(text))
	if err != nil {
		return err
	}
	d.Duration = parsed
	return nil
}

// MarshalText formats a Go duration.
func (d Duration) MarshalText() ([]byte, error) { return []byte(d.String()), nil }
