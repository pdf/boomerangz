# Troubleshooting

Start with the service and recent logs:

::: code-group

```sh [Linux / systemd]
systemctl status boomerangz.service
journalctl -u boomerangz.service --since today
```

:::

Then check the merged configuration and effective dataset policy:

```sh
boomerangz config check
boomerangz config show
boomerangz dataset inspect tank/data
```

## The service does not start

**Look for:** an invalid configuration, an unreadable key, a missing runtime
directory, or another process holding the installation lifecycle lock.

**Check:**

```sh
boomerangz config check
sudo -u boomerangz test -r /etc/boomerangz/config.toml
```

Correct the reported configuration or permissions. Run only one daemon for an
installation.

## A dataset is not managed

**Look for:** `enabled=off`, inherited values from an unexpected parent, an
unknown remote name, or another policy validation error.

**Check on the source host:**

```sh
boomerangz dataset inspect tank/data
zfs get -r all tank/data | grep org.boomerangz
```

Read the `Errors` section and the source shown beside each effective or stored
property. Locally configured properties govern policy; received copies do not
activate a backup dataset. Add `--json` only when you need the structured
`policy.errors` and property values for automated diagnostics.

## Configuration reload fails

**Look for:** invalid merged configuration, unreadable credentials, an unusable
listener address, or another resource needed to prepare the candidate.

**Check:**

```sh
boomerangz config check
boomerangz config reload
```

A failed reload leaves the active configuration and its generation unchanged.
If a successful result lists `restart_required`, the named fields retain their
previous values until the service is restarted. When `socket_path` was changed
successfully, use the new control-socket path for later commands.

## Snapshot creation is denied

**Look for:** missing delegated permissions on the selected source root.

**Check on the source host:**

```sh
sudo zfs allow tank/data
```

Boomerangz also performs this alignment check before a transfer starts. An
error beginning with `source permission preflight` or `destination permission
preflight` lists the permissions missing from the effective endpoint account.
Correct the delegation on the reported dataset or its appropriate ancestor;
do not grant the account unrestricted `sudo` access.

Compare the result with the supported
[source delegation set](/guide/security#zfs-delegation). Do not work around the
error with unrestricted sudo.

## SSH authentication fails

**Look for:** the wrong key, unreadable key permissions, a missing host key, an
incorrect receiving account, a changed login shell, or a destination root that
is absent from `ssh_shell.replication_roots`.

**Check from the source host as the service account:**

```sh
sudo -u boomerangz ssh \
  -o IdentitiesOnly=yes \
  -o ForwardAgent=no \
  -i /etc/boomerangz/credentials.d/home-backup-ed25519 \
  boomerangz@backup.example.net
```

The packaged `boomerangz` account is not an interactive login. A manual SSH
connection may close after starting the restricted protocol; that does not by
itself indicate a failure. Confirm that its login shell is
`/usr/lib/boomerangz/boomerangz-shell` and verify the host-key fingerprint
separately before accepting it.

For `endpoint = "direct"`, run the check as the configured
`boomerangz-replication` account instead. It must have destination-only ZFS
delegation and no `sudo` access.

## A remote stays unavailable

**Look for:** DNS or routing failure, an unimported destination pool, listener
or SSH service failure, certificate expiry, or a destination identity mismatch.

Check connectivity from the source host, then inspect the destination pool and
service on the receiving host. Do not delete retained source snapshots while a
transfer is pending.

If the destination was replaced or intentionally recreated, do not rely on its
name alone. Review the adoption procedure indicated by status before
allowing replication to resume.

## TLS or pairing fails

**Look for:** a client-visible address that differs from the certificate,
incorrect system time, an expired pairing, the wrong CA, a revoked credential,
or a missing required authentication component.

Confirm that `advertised_address` is the address clients use. For
`mtls+token`, both the client certificate and token must be present. Recreate a
pairing only after verifying the listener identity through a trusted channel.

## Clean or adoption reports blockers

A blocker means Boomerangz cannot prove that the proposed change is safe.
Restore unavailable destinations, resolve incomplete receives, or correct the
selected dataset before previewing again. Do not edit
`org.boomerangz:state:*` properties to bypass the check.

If you need help, retain the preview, `dataset inspect` output, relevant journal
messages, Boomerangz version, OpenZFS version, and platform version. Remove
credential and token values before sharing diagnostics.
