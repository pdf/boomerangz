# Configuration schema

This describes every currently accepted global configuration field. Dataset
policy belongs in [ZFS user properties](dataset-policy.md), not this file.
Configuration loading and validation are available. Automatic scheduling,
transfers, and control listeners are not yet available.

The primary file is `/etc/boomerangz/config.toml`. Files ending in `.toml` from
`/etc/boomerangz/config.d` are applied afterward in bytewise filename order.
Tables merge recursively, scalar values override earlier values, and arrays
replace earlier arrays. Unknown fields are errors.

```toml
[daemon]
reconcile_interval = "1m"
management_workers = 0
local_transfer_workers = 2
remote_transfer_workers = 1

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
| `management_workers` | `0` | Concurrent short ZFS management tasks. `0` automatically uses the logical CPU count available to the process; positive integers select an explicit limit. Negative values are invalid. |
| `local_transfer_workers` | `2` | Maximum concurrent same-host transfers, independently limiting local storage load. Integer of at least one. |
| `remote_transfer_workers` | `1` | Maximum concurrent network transfers across all remotes, independently limiting network load. Integer of at least one. |

Snapshot timing is independent of reconciliation. The scheduling design uses
known active policies to determine snapshot deadlines without rescanning ZFS on
each snapshot tick. New or changed policies take effect after reconciliation.

The reconciliation loop runs one pass at a time. If a pass (including its result
callback) exceeds `reconcile_interval`, a pending tick can start another pass
immediately afterward. Missed ticks do not accumulate into an unbounded backlog;
there are no overlapping passes or automatic timeout at the interval. Sustained
slow passes can therefore keep the reconciler continuously busy. Explicit refresh
requests coalesce separately into one pending request.

`local_transfer_workers` and `remote_transfer_workers` replace `transfer_workers`;
the old field is no longer accepted. Setting both to `1` allows one local and one
remote transfer concurrently, not a single global transfer. Concurrency is not a
bandwidth-rate limit.

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
| `connect_timeout` | `"0s"` | Limits connection establishment, not total transfer duration. Nonnegative Go duration; zero selects the application default, whose runtime value is not yet defined because SSH transfers are unavailable. |

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

TCP trust and certificate-renewal design details are maintained in the
[development plan](../PLAN.md#91-listener-security). No additional trust-related
TOML fields are accepted yet.

The `config show` command redacts private-key fields. Secret token material will
be stored in credential bundles rather than this configuration.
