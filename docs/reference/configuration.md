# Configuration reference

All durations use Go duration syntax such as `10s`, `5m`, or `24h`.

Validated changes can be applied with `systemctl reload boomerangz.service` for
the packaged service, or directly with `boomerangz config reload`. Every field
below is reloadable without interrupting active work except
`paths.identity_dir`, which is reported as restart-required and retains its
previous value until the daemon restarts.

## `[daemon]`

| Field | Default | Valid values |
| --- | --- | --- |
| `reconcile_interval` | `"1m"` | Positive duration. |
| `inactive_grace_period` | `"24h"` | Non-negative duration; `"0s"` disables automatic retirement. |
| `management_workers` | `0` | Non-negative integer; zero selects automatic sizing. |
| `local_transfer_workers` | `2` | Integer of at least one; concurrency applies to non-overlapping mapped destination datasets. |
| `remote_transfer_workers` | `1` | Integer of at least one. |

## `[paths]`

| Field | Default | Valid values |
| --- | --- | --- |
| `credentials_dir` | `"/etc/boomerangz/credentials.d"` | Absolute path. |
| `identity_dir` | `"/var/lib/boomerangz/identity"` | Absolute path. |
| `socket_path` | `"/run/boomerangz/boomerangz.sock"` | Absolute path. |

## `[ssh_shell]`

| Field | Default | Valid values |
| --- | --- | --- |
| `replication_roots` | Empty | Array of ZFS destination roots exposed by the restricted SSH login shell. An empty array disables the service. |

Each request is confined to one of these roots or its descendants. This list is
enforced on the receiving host independently of the root requested by the
sending host.

## `[remotes.NAME]`

Names begin with a letter or digit and may contain letters, digits, dots,
underscores, and hyphens.

| Field | Default | Valid values |
| --- | --- | --- |
| `transport` | Required | `ssh` or `native`. |
| `credential` | Empty | Required for `native`: imported name or absolute bundle path. Invalid for SSH. |
| `endpoint` | `auto` | For SSH: `auto`, `direct`, or `ssh-shell`. |
| `host` | Required for SSH | Hostname or address. |
| `port` | `0` | `0` through `65535`; zero uses SSH's default. |
| `user` | Empty | SSH login name; empty leaves selection to SSH. |
| `root` | Required | ZFS dataset name, not a filesystem path. |
| `identity_file` | Empty | SSH private-key path. |
| `ssh_shell_path` | `boomerangz` | Remote command name or absolute executable path without whitespace. For an explicit packaged `ssh-shell` endpoint, prefer `/usr/lib/boomerangz/boomerangz-shell`; the bare-name default supports discovery through the remote account's command path. |
| `connect_timeout` | `10s` | Non-negative duration. An explicit `0s` also selects the effective ten-second default. |

Native remotes accept `transport`, `credential`, and `root` only. Their address
and trust information comes from the imported pairing.

## `[listeners.NAME]`

| Field | Default | Valid values |
| --- | --- | --- |
| `network` | Required | `unix` or `tcp`. |
| `address` | Required | Absolute path for Unix; `host:port` for TCP. |
| `advertised_address` | Empty | Client-visible DNS name or IP address plus port; required for managed TLS. Wildcard listening addresses are invalid. |
| `auth_mode` | Empty | Required for TCP: `token`, `mtls`, or `mtls+token`. |
| `tls_cert` | Empty | Absolute certificate-chain path; supply with `tls_key`. |
| `tls_key` | Empty | Absolute private-key path; supply with `tls_cert`. |
| `client_ca` | Empty | Absolute external CA path for client certificates. |
| `pairing_ca` | Empty | Absolute CA path included in generated pairings for server verification. |
| `pairing_pin_certificate` | `false` | Boolean; additionally pin the server public key. |
| `replication_roots` | Empty | Array of ZFS roots exposed for native replication; TCP only. |

When `tls_cert` and `tls_key` are omitted, Boomerangz manages the server CA and
certificate. When `client_ca` is omitted for mTLS, it manages a client CA.

## Complete examples

SSH destination:

```toml
[remotes.home-backup]
transport = "ssh"
endpoint = "ssh-shell"
host = "backup.example.net"
port = 22
user = "boomerangz"
root = "tank/backups"
identity_file = "/etc/boomerangz/credentials.d/home-backup-ed25519"
ssh_shell_path = "/usr/lib/boomerangz/boomerangz-shell"
connect_timeout = "10s"
```

Receiving-host restriction for that destination:

```toml
[ssh_shell]
replication_roots = ["tank/backups"]
```

Secured listener and native client:

```toml
[listeners.replication]
network = "tcp"
address = "0.0.0.0:7443"
advertised_address = "backup.example.net:7443"
auth_mode = "mtls+token"
replication_roots = ["tank/backups"]

[remotes.home-backup]
transport = "native"
credential = "home-backup"
root = "tank/backups"
```
