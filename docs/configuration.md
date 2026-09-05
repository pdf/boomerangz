# Configuration schema

This describes every currently accepted global configuration field. Dataset
policy belongs in [ZFS user properties](dataset-policy.md), not this file.
Configuration loading and validation are implemented; worker scheduling, SSH
transfers, and listener operation are planned for their respective later phases.
Accepting a field does not mean its runtime feature is available yet.

The primary file is `/etc/boomerangz/config.toml`. Files ending in `.toml` from
`/etc/boomerangz/config.d` are applied afterward in bytewise filename order.
Tables merge recursively, scalar values override earlier values, and arrays
replace earlier arrays. Unknown fields are errors.

```toml
[daemon]
reconcile_interval = "1m"
management_workers = 4
transfer_workers = 2

[paths]
credentials_dir = "/etc/boomerangz/credentials.d"
identity_dir = "/var/lib/boomerangz/identity"
socket_path = "/run/boomerangz/boomerangz.sock"

```

These are the built-in values. No remotes or named listeners are supplied by
default. The user must choose every destination; installation cannot infer a
useful host, account, or destination dataset.

## Daemon

| Field | Default | Purpose and constraints |
| --- | --- | --- |
| `reconcile_interval` | `"1m"` | Interval between global dataset discovery/reconciliation passes; not the snapshot cadence, which comes from each dataset's grid. Positive Go duration. |
| `management_workers` | `4` | Maximum concurrent short management tasks, such as property and snapshot operations. At least one. |
| `transfer_workers` | `2` | Maximum concurrent long-running transfers, separate from management work so streams do not monopolize it. At least two. |

## Paths

All three paths must be absolute. Private keys are generated at runtime or
provided by the administrator, never supplied by a package.

| Field | Default | Purpose |
| --- | --- | --- |
| `credentials_dir` | `/etc/boomerangz/credentials.d` | Location for imported client credential bundles and dedicated SSH credentials. |
| `identity_dir` | `/var/lib/boomerangz/identity` | Persistent server identity and token-verifier storage; preserve across restarts. |
| `socket_path` | `/run/boomerangz/boomerangz.sock` | Default local control socket used to communicate with the daemon. |

## Remotes

Each optional `[remotes.NAME]` table defines a user-selected destination referenced
by `org.boomerangz:remote`. Names start with a letter or digit and otherwise
contain letters, digits, dots, underscores, and hyphens. SSH is the only currently
accepted transport type; there is no default remote.

| Field | Default | Purpose and constraints |
| --- | --- | --- |
| `transport` | Required | Connection protocol; currently must be `"ssh"`. |
| `host` | Required | User-selected SSH server hostname or address. |
| `port` | `0` | SSH server port, from 0 to 65535; zero leaves the SSH default in effect. |
| `user` | Empty | SSH login account; empty leaves account selection to SSH. |
| `root` | Required | Destination ZFS dataset used as the receive path base, not a filesystem path. Final mapping also depends on `org.boomerangz:discard`. |
| `identity_file` | Empty | Dedicated SSH private-key file; empty leaves identity selection to SSH. This field contains a path, not key contents. |
| `connect_timeout` | `"0s"` | Limits connection establishment, not total transfer duration. Nonnegative Go duration; zero selects the application default, whose runtime value will be defined in the SSH phase. |

Illustrative only—replace these details with your own before use:

```toml
[remotes.my_backup]
transport = "ssh"
host = "backup.example.net"
port = 22
user = "replicator"
root = "tank/backups"
identity_file = "/etc/boomerangz/credentials.d/my_backup_ed25519"
connect_timeout = "10s"
```

Arbitrary SSH options, passwords, and shell fragments are intentionally absent
from the schema.

## Listeners

Each optional `[listeners.NAME]` table describes a control endpoint. Names follow
the same syntax as remote names. TCP is opt-in and never unauthenticated.

| Field | Default | Purpose and constraints |
| --- | --- | --- |
| `network` | Required | `"unix"` for local socket access, or `"tcp"` for network access. |
| `address` | Required | Unix socket absolute path, or TCP bind address (`host:port`). Bind only to intended interfaces. |
| `auth_mode` | Empty | Required for TCP: `"token"` authorizes scoped tokens, `"mtls"` authenticates client certificates, or `"mtls+token"` requires both. Unused for Unix sockets, which rely on filesystem permissions and peer credentials. |
| `tls_cert` | Empty | Server certificate-chain file, required for TCP. May be externally managed, including ACME-issued certificates. |
| `tls_key` | Empty | Matching server private-key file, required for TCP; protect access to this file. |

Planned TCP server verification supports normal CA-chain and hostname validation
or explicit public-key pinning. Pinning is optional, not required for rotating
CA-issued certificates. Trust configuration and certificate reload details will
be finalized in the TCP phase; these are not additional accepted TOML fields yet.

The `config show` command redacts private-key fields. Secret token material will
be stored in credential bundles rather than this configuration.
