# Configure the daemon

Boomerangz keeps host-wide settings in TOML and dataset-specific policy in ZFS
properties. This separation lets a dataset carry its own snapshot and
replication policy while connection details and credentials remain on the host.

## Configuration files

The primary file is `/etc/boomerangz/config.toml`. Files ending in `.toml` in
`/etc/boomerangz/config.d` are applied afterward in filename order.

Later scalar values override earlier values, tables merge, and arrays replace
earlier arrays. Unknown fields are errors.

Start with only the settings you need:

```toml
[daemon]
reconcile_interval = "1m"
inactive_grace_period = "24h"

[paths]
credentials_dir = "/etc/boomerangz/credentials.d"
identity_dir = "/var/lib/boomerangz/identity"
socket_path = "/run/boomerangz/boomerangz.sock"
```

## Validate changes

```sh
boomerangz config check
boomerangz config show
```

`config check` validates the primary file, all drop-ins, and the merged result.
`config show` displays the effective configuration with private-key fields
redacted.

Apply the files to the running daemon without interrupting active work:

::: code-group

```sh [Linux / systemd]
sudo systemctl reload boomerangz.service
```

```sh [Direct command]
boomerangz config reload
```

:::

Reload reads and validates the same primary file and drop-ins that started the
daemon. If validation or preparation fails, the running configuration remains
unchanged and `systemctl reload` fails. The direct command prints a JSON result
containing the new configuration `generation`, the fields `applied` live, and
any `restart_required` fields. The service action records that result in the
system journal; inspect it with `journalctl -u boomerangz.service`. Check
`restart_required` before considering the change complete.

Most settings, including worker counts, remotes, listeners, credential and
socket paths, and restricted-shell roots, are applied live. Running jobs finish
with the configuration they captured when they started. Changing
`paths.identity_dir` requires a restart and is retained at its previous value
until then because it changes the installation's identity and authorization
boundary.

The packaged systemd service runs the reload command as the `boomerangz` user.
By default, the direct `config reload` command connects to
`/run/boomerangz/boomerangz.sock`. Use `--socket PATH` when the running daemon's
current control socket is elsewhere. If the reload changes `socket_path`, use
the new path for later control commands and update the service's `ExecReload`
override so future `systemctl reload` operations select it.

## Scheduling and concurrency

| Setting | Default | What it controls |
| --- | --- | --- |
| `reconcile_interval` | `1m` | How often the daemon discovers new or changed dataset policy. This is not the snapshot interval. |
| `inactive_grace_period` | `24h` | How long an inactive owned dataset remains recoverable before automatic retirement. `0s` disables automatic retirement. |
| `management_workers` | `0` | Concurrent short management tasks. Zero uses the logical CPU count available to the process. |
| `local_transfer_workers` | `2` | Concurrent transfers between datasets on this host. |
| `remote_transfer_workers` | `1` | Concurrent network transfers across all remote destinations. |

Worker counts limit concurrency, not bandwidth. Increase them only after
observing storage and network load.

## Paths

All configured paths must be absolute. Keep the defaults unless your host has a
clear reason to relocate them.

| Setting | Default | Purpose |
| --- | --- | --- |
| `credentials_dir` | `/etc/boomerangz/credentials.d` | Imported pairings and dedicated SSH credentials. |
| `identity_dir` | `/var/lib/boomerangz/identity` | Installation identity and managed certificate material. Preserve it for the life of this installation. |
| `socket_path` | `/run/boomerangz/boomerangz.sock` | Local status and control socket. |

For the complete schema, see the [configuration reference](/reference/configuration).
