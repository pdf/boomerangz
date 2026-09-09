# Configure destinations

Replication creates an independent copy only when the destination is on
different storage. Boomerangz supports destinations on the same host, over SSH,
and through a secured native listener.

## Local destination

Create a destination root on different storage and delegate the receive
permissions to the service account:

```sh
sudo zfs create backup/boomerangz
sudo zfs allow -u boomerangz \
  canmount,compression,create,destroy,mount,mountpoint,readonly,receive,receive:append,userprop \
  backup/boomerangz
```

The destination root may remain mounted. With `discard=first` or
`discard=all`, Boomerangz uses it only as a container and leaves its mount state
and properties unchanged. With `discard=off`, the root is the receive target;
it remains mounted if it is already mounted, while its `canmount` property
defaults to `noauto`. Received filesystems and intermediate filesystems created
below the root also use `canmount=noauto`. This prevents automatic mounting
while allowing an administrator to mount them explicitly for recovery.

Select it on the source:

```sh
sudo zfs set org.boomerangz:local=backup/boomerangz tank/data
```

## SSH destination

SSH works without installing Boomerangz on the destination. When it is
installed, the restricted `ssh-shell` endpoint provides a narrower command
surface.

| Endpoint | Destination requirement | When to choose it |
| --- | --- | --- |
| `direct` | OpenSSH and OpenZFS tools | Boomerangz cannot be installed remotely. |
| `ssh-shell` | Boomerangz installed remotely | Preferred when the packaged restricted account is available. |
| `auto` | OpenSSH and OpenZFS tools | Try `ssh-shell`, then use `direct` if that endpoint is unavailable. |

Remote discovery happens when transfer work is due or explicitly retried, not
on every ordinary local policy scan.

### Prepare the `ssh-shell` account

When Boomerangz is installed from a package on the receiving host, use its
existing `boomerangz` system account. The package sets
`/usr/lib/boomerangz/boomerangz-shell` as this account's login shell and
registers the wrapper in `/etc/shells`. The wrapper ignores SSH command
arguments and starts only the restricted replication service; it does not
provide an interactive shell.

Create the account's SSH directory:

::: code-group

```sh [Arch Linux / derivatives]
sudo install -d -m 0700 -o boomerangz -g boomerangz /var/lib/boomerangz/.ssh
```

:::

Create the destination dataset and delegate its receive permissions:

```sh
sudo zfs create tank/backups
sudo zfs allow -u boomerangz \
  canmount,compression,create,destroy,mount,mountpoint,readonly,receive,receive:append,userprop \
  tank/backups
```

Allow the restricted service to access this destination root in the receiving
host's Boomerangz configuration:

```toml
[ssh_shell]
replication_roots = ["tank/backups"]
```

Every request is checked against this server-side list. Restart the receiving
host's daemon after changing shared configuration. Each new SSH session reads
the current configuration itself.

### Prepare a direct-SSH account

For a direct-SSH destination without Boomerangz, create a dedicated
`boomerangz-replication` system user. This account is separate from the
packaged daemon account and has a state-directory home rather than a regular
user home:

::: code-group

```sh [Arch Linux / derivatives]
sudo useradd --system --user-group --create-home \
  --home-dir /var/lib/boomerangz-replication \
  --shell /bin/sh boomerangz-replication
sudo install -d -m 0700 \
  -o boomerangz-replication -g boomerangz-replication \
  /var/lib/boomerangz-replication/.ssh
```

:::

Delegate only the destination permissions to this account:

```sh
sudo zfs create tank/backups
sudo zfs allow -u boomerangz-replication \
  canmount,compression,create,destroy,mount,mountpoint,readonly,receive,receive:append,userprop \
  tank/backups
```

Do not add `boomerangz-replication` to administrative groups, give it source
dataset permissions, or grant it `sudo` access. Direct mode must execute a
small set of `zfs` and `zpool` commands through its login shell, so the narrow
account and ZFS delegation form its main authorization boundary.

Delegate every additional receive property selected through
`set_prop:<name>` or `ignore_prop:<name>`.

### Install a dedicated key

Generate a key dedicated to this destination. Do not reuse a personal key:

```sh
sudo ssh-keygen -t ed25519 \
  -f /etc/boomerangz/credentials.d/home-backup-ed25519 \
  -C boomerangz-home-backup
sudo chown root:boomerangz /etc/boomerangz/credentials.d/home-backup-ed25519
sudo chmod 0640 /etc/boomerangz/credentials.d/home-backup-ed25519
```

Install only the public key for the receiving account. For `ssh-shell`, add it
to `/var/lib/boomerangz/.ssh/authorized_keys` with OpenSSH's restrictions:

```text
restrict ssh-ed25519 AAAA... boomerangz-home-backup
```

The account's packaged login shell is the command restriction. The client
sends the requested destination root through the replication API, and
`ssh_shell.replication_roots` determines whether the receiving host accepts it.
Do not change this account back to a general-purpose shell.

For `direct` mode, add the key to
`/var/lib/boomerangz-replication/.ssh/authorized_keys` and retain `restrict`:

```text
restrict ssh-ed25519 AAAA... boomerangz-home-backup
```

Direct mode can run the remote OpenZFS commands required for replication.
Contain it with a dedicated account, filesystem permissions, and narrow ZFS
delegation.

Verify the host-key fingerprint through a separate trusted channel and add it
to the service account's `known_hosts` file. Strict host-key checking is always
enabled.

### Configure the sender

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

For the direct fallback account, change the endpoint and user:

```toml
endpoint = "direct"
user = "boomerangz-replication"
```

For a packaged `ssh-shell` destination, explicitly naming
`/usr/lib/boomerangz/boomerangz-shell` documents and verifies the intended
remote endpoint. The account's restricted login shell still ignores the
SSH-supplied command and arguments. The application's current fallback when
`ssh_shell_path` is omitted is to resolve `boomerangz` through the remote
account's command path.

Use `auto` only when the same configured SSH account is deliberately able to
support the fallback. The packaged `boomerangz` account is intentionally
restricted to `ssh-shell` and cannot run direct commands.

## Native secured destination

A native destination uses a configured TCP listener and an imported pairing.

### Configure the receiving host

Create the destination dataset and delegate its receive permissions on the host
that will store the replica:

```sh
sudo zfs create tank/backups
sudo zfs allow -u boomerangz \
  canmount,compression,create,destroy,mount,mountpoint,readonly,receive,receive:append,userprop \
  tank/backups
```

Configure the listener on that receiving host:

```toml
[listeners.replication]
network = "tcp"
address = "0.0.0.0:7443"
advertised_address = "backup.example.net:7443"
auth_mode = "mtls+token"
replication_roots = ["tank/backups"]
```

When `tls_cert` and `tls_key` are omitted, Boomerangz creates and renews a
private server identity. `advertised_address` must contain the DNS name or IP
address that clients use, followed by the port. A stable DNS name is generally
preferable; wildcard listening addresses such as `0.0.0.0` and `::` are not
client identities.

Create a narrowly scoped pairing on the receiving host and transfer the output
through a trusted channel:

```sh
umask 077
sudo boomerangz pairing create \
  --listener replication \
  --scope replicate >home-backup-pairing.json
```

### Configure the source host

Transfer `home-backup-pairing.json` to the source host through a trusted
channel, then import it there:

```sh
sudo boomerangz pairing import home-backup home-backup-pairing.json
```

Add the native destination to the source host's configuration:

```toml
[remotes.home-backup]
transport = "native"
credential = "home-backup"
root = "tank/backups"
```

Treat pairing bundles as secrets. See [Secure your deployment](./security) for
authentication choices and certificate trust.
