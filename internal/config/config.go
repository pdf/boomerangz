// Package config loads, merges, validates, and safely renders global config.
package config

import "time"

// Default global daemon settings and filesystem paths.
const (
	DefaultReconcileInterval = time.Minute
	DefaultManagementWorkers = 4
	DefaultTransferWorkers   = 2
	DefaultCredentialsDir    = "/etc/boomerangz/credentials.d"
	DefaultIdentityDir       = "/var/lib/boomerangz/identity"
	DefaultSocketPath        = "/run/boomerangz/boomerangz.sock"
)

// Config is the complete global configuration.
type Config struct {
	Daemon    DaemonConfig              `toml:"daemon" json:"daemon"`
	Paths     PathsConfig               `toml:"paths" json:"paths"`
	Remotes   map[string]RemoteConfig   `toml:"remotes" json:"remotes"`
	Listeners map[string]ListenerConfig `toml:"listeners" json:"listeners"`
}

// DaemonConfig controls reconciliation and worker concurrency.
type DaemonConfig struct {
	ReconcileInterval Duration `toml:"reconcile_interval" json:"reconcile_interval"`
	ManagementWorkers int      `toml:"management_workers" json:"management_workers"`
	TransferWorkers   int      `toml:"transfer_workers" json:"transfer_workers"`
}

// PathsConfig contains persistent and runtime filesystem locations.
type PathsConfig struct {
	CredentialsDir string `toml:"credentials_dir" json:"credentials_dir"`
	IdentityDir    string `toml:"identity_dir" json:"identity_dir"`
	SocketPath     string `toml:"socket_path" json:"socket_path"`
}

// RemoteConfig defines an SSH replication destination.
type RemoteConfig struct {
	Transport      string   `toml:"transport" json:"transport"`
	Host           string   `toml:"host" json:"host"`
	Port           int      `toml:"port" json:"port"`
	User           string   `toml:"user" json:"user"`
	Root           string   `toml:"root" json:"root"`
	IdentityFile   string   `toml:"identity_file" json:"identity_file,omitempty"`
	ConnectTimeout Duration `toml:"connect_timeout" json:"connect_timeout"`
}

// ListenerConfig defines a local Unix or authenticated TCP control listener.
type ListenerConfig struct {
	Network  string `toml:"network" json:"network"`
	Address  string `toml:"address" json:"address"`
	AuthMode string `toml:"auth_mode" json:"auth_mode,omitempty"`
	TLSCert  string `toml:"tls_cert" json:"tls_cert,omitempty"`
	TLSKey   string `toml:"tls_key" json:"tls_key,omitempty" secret:"true"`
}

// Defaults returns a valid configuration with no remotes or listeners.
func Defaults() Config {
	return Config{
		Daemon: DaemonConfig{
			ReconcileInterval: Duration{DefaultReconcileInterval},
			ManagementWorkers: DefaultManagementWorkers,
			TransferWorkers:   DefaultTransferWorkers,
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
