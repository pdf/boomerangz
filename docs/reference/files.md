# Paths and files

::: code-group

```text [Arch Linux / derivatives]
/usr/bin/boomerangz
    Executable.

/usr/lib/boomerangz/boomerangz-shell
    Restricted login-shell wrapper for the packaged boomerangz account.

/etc/boomerangz/config.toml
    Protected primary configuration.

/etc/boomerangz/config.d/*.toml
    Configuration drop-ins, applied in filename order.

/etc/boomerangz/credentials.d/
    Imported pairing bundles and dedicated SSH credentials.

/var/lib/boomerangz/identity/
    Persistent installation identity and managed certificate material.

/run/boomerangz/boomerangz.sock
    Local status and control socket; boomerangz:boomerangz, mode 0660.

/usr/lib/systemd/system/boomerangz.service
    systemd service unit, including start and live-reload actions.
```

```text [Ubuntu]
/usr/bin/boomerangz
    Executable.

/usr/lib/boomerangz/boomerangz-shell
    Restricted login-shell wrapper for the packaged boomerangz account.

/etc/boomerangz/config.toml
    Protected primary configuration.

/etc/boomerangz/config.d/*.toml
    Configuration drop-ins, applied in filename order.

/etc/boomerangz/credentials.d/
    Imported pairing bundles and dedicated SSH credentials.

/var/lib/boomerangz/identity/
    Persistent installation identity and managed certificate material.

/run/boomerangz/boomerangz.sock
    Local status and control socket; boomerangz:boomerangz, mode 0660.

/usr/lib/systemd/system/boomerangz.service
    systemd service unit, including start and live-reload actions.
```

:::

The configuration file is owned by `root` and readable by the `boomerangz`
group. Credential and identity material must not be world-accessible.
Commands run as root keep the service able to read what they write: anything
created under the identity directory takes the directory's owner, and an
imported pairing bundle takes the credentials directory's group and is
readable by it, `root:boomerangz` mode `0640` on a packaged host.
Membership in the `boomerangz` group also grants full access to the local
control API.

The package registers `/usr/lib/boomerangz/boomerangz-shell` in `/etc/shells`
while installed. Do not assign the packaged account a general-purpose shell.

Preserve `/var/lib/boomerangz/identity` when restoring the same installation.
Do not copy it to another simultaneously active host.
