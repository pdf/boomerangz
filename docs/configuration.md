# Configuration schema

This is the phase-one global configuration contract. Dataset policy does not
belong in this file; it is stored in `org.boomerangz:*` ZFS user properties.

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

[remotes.home]
transport = "ssh"
host = "backup.example.net"
port = 22
user = "replicator"
root = "tank/backups"
identity_file = "/etc/boomerangz/credentials.d/home_ed25519"
connect_timeout = "10s"

[listeners.local]
network = "unix"
address = "/run/boomerangz/boomerangz.sock"
```

## Daemon

- `reconcile_interval` must be a positive Go duration and defaults to `1m`.
- `management_workers` must be at least one and defaults to four.
- `transfer_workers` must be at least two and defaults to two.

## Paths

All paths are absolute. Private key material is generated at runtime and is
never supplied by a package.

## Remotes

Remote names contain letters, digits, dots, underscores, and hyphens. The
initial transport is `ssh`; `host` and the destination ZFS dataset `root` are
required. Port zero means the SSH default. `identity_file` may name a dedicated
private key, while `connect_timeout` may be zero to use the application default.

Arbitrary SSH options, passwords, and shell fragments are intentionally absent
from the schema.

## Listeners

A Unix listener uses `network = "unix"` and an absolute `address`. TCP listeners
are optional and require an address, TLS certificate and key, and one of the
authentication modes `token`, `mtls`, or `mtls+token`. TCP never has an
unauthenticated mode.

The `config show` command redacts private-key fields. Secret token material will
be stored in credential bundles rather than this configuration.
