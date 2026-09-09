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
    systemd service unit.
```

:::

The configuration file is owned by `root` and readable by the `boomerangz`
group. Credential and identity material must not be world-accessible.
Membership in the `boomerangz` group also grants full access to the local
control API.

The package registers `/usr/lib/boomerangz/boomerangz-shell` in `/etc/shells`
while installed. Do not assign the packaged account a general-purpose shell.

Preserve `/var/lib/boomerangz/identity` when restoring the same installation.
Do not copy it to another simultaneously active host.
