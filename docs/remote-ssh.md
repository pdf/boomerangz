# SSH destinations

An SSH destination needs a dedicated account, key authentication, a verified
host key, and delegated ZFS permissions on the configured destination root.
Root login and password authentication are not required.

Two endpoint modes are available:

- `direct` runs the required ZFS commands remotely. The destination does not
  need boomerangz installed.
- `ssh-shell` runs a constrained boomerangz service through one SSH command
  channel. This is the recommended mode when boomerangz can be installed on the
  destination.

`auto` tries `ssh-shell` and uses `direct` only when the remote command is not
available. It does not fall back when the host itself is unreachable. Remote
inspection occurs when replication work is due, during a retry, or when an
operator explicitly requests reconciliation; ordinary local policy scans do not
connect to every destination.

Direct mode multiplexes its remote commands and receive stream over one
authenticated SSH connection for each opened replication endpoint. Its private
control socket is removed when the endpoint closes.

The configured leaf dataset may be absent before its first replication, provided
an existing ancestor has the required delegation. If the destination pool is
temporarily unavailable or unimported, the daemon retains the protected source
snapshot and retries with bounded backoff.

## Destination account

Create a dedicated account that has no administrative group memberships and no
access to unrelated application data. Its login shell must permit remote command
execution when using `direct` or an unforced `ssh-shell` configuration.

Create the destination dataset before delegating permissions. This baseline was
exercised on CachyOS with OpenZFS 2.4.3:

```sh
zfs allow -u boomerangz-receive \
  compression,create,destroy,mount,mountpoint,readonly,receive,receive:append,userprop \
  tank/backups
```

Delegate every additional native property named by a configured `set_prop` or
`ignore_prop` rule. Permission behavior can differ between OpenZFS releases, so
verify the effective delegation on each supported destination platform. Do not
grant unrestricted sudo or run the receiving account as root.

Install the sender's public key in the destination account's
`.ssh/authorized_keys`. OpenSSH's `restrict` option disables forwarding, PTY,
user-rc, and agent/X11 features for that key.

For direct mode:

```text
restrict ssh-ed25519 AAAA... dedicated-boomerangz-key
```

This limits SSH session features but does not restrict the account to a ZFS-only
command vocabulary. Direct mode therefore relies on a dedicated account, normal
filesystem permissions, and narrowly delegated ZFS permissions. Restricted
shells and ad-hoc `SSH_ORIGINAL_COMMAND` parsing are not recommended as security
boundaries.

When boomerangz is installed remotely, use a forced `ssh-shell` command:

```text
restrict,command="/usr/bin/boomerangz ssh-shell --root tank/backups" ssh-ed25519 AAAA... dedicated-boomerangz-key
```

The forced root must exactly match the configured remote `root`. Select
`endpoint = "ssh-shell"` for this key; a forced SSH-shell key cannot also execute
the direct fallback commands.

## Sender configuration

Verify the destination host-key fingerprint through a separate trusted channel,
then place the key in either the boomerangz account's `known_hosts` file or the
system-wide SSH known-hosts file. Strict host-key checking is always enabled.

An SSH-shell destination can be configured as follows:

```toml
[remotes.home]
transport = "ssh"
endpoint = "ssh-shell"
host = "backup.example.net"
port = 22
user = "boomerangz-receive"
root = "tank/backups"
identity_file = "/etc/boomerangz/credentials.d/home_ed25519"
connect_timeout = "10s"
```

Use `endpoint = "direct"` when the destination has only OpenSSH and ZFS. If the
remote executable is installed outside the SSH account's command path, set
`ssh_shell_path` to its absolute path. Passwords, arbitrary SSH options, and
remote shell fragments are not accepted in configuration.
