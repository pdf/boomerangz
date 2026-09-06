# Running the daemon

`boomerangz daemon` runs snapshot scheduling, local transfers, SSH transfers,
recovery, pruning, and inactive-dataset retirement in the foreground. It reads
`/etc/boomerangz/config.toml` and `/etc/boomerangz/config.d/*.toml` by default:

```sh
boomerangz daemon
```

Alternative configuration paths can be supplied explicitly:

```sh
boomerangz daemon --config /path/to/config.toml --config-dir /path/to/config.d
```

The process writes structured JSON logs to standard error. `SIGTERM` and
`SIGINT` stop new scheduling, discard work that has not started, cancel active
transfers, and allow an already-running short management operation to finish.
ZFS resume tokens, holds, bookmarks, and snapshot evidence remain available for
recovery after restart.

The daemon also serves the local status and control API at
`paths.socket_path`. See [Status and remote control](control-api.md) for status
watching, manual triggers, daemon-coordinated clean, and optional authenticated
TCP listeners.

## systemd

The source tree supplies:

- `contrib/systemd/boomerangz.service`
- `contrib/sysusers.d/boomerangz.conf`
- `contrib/tmpfiles.d/boomerangz.conf`

Install these in the corresponding systemd directories, reload systemd, and
create the declared user and directories before starting the service. A package
can perform those steps automatically.

Before enabling the service, delegate only the required ZFS permissions on the
selected source and local destination roots to the `boomerangz` account. Also
make any configured SSH private key readable by that account without making it
group- or world-writable. The service runs without Linux capabilities and limits
device access to `/dev/zfs`; it does not make root-owned ZFS datasets writable by
itself.

After installing the configuration, validate it and start the service:

```sh
boomerangz config check
systemctl enable --now boomerangz.service
systemctl status boomerangz.service
journalctl -u boomerangz.service
```

Only one daemon or standalone lifecycle apply operation can use an installation
at a time. If the lifecycle lock is already held, startup fails instead of
allowing concurrent mutation.
