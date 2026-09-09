# Secure your deployment

The recommended deployment runs Boomerangz as its dedicated unprivileged
account and grants access only to the ZFS roots it manages.

## ZFS delegation

Use the narrowest source and destination roots that meet your needs.

### Source datasets

```sh
sudo zfs allow -u boomerangz \
  bookmark,destroy,hold,mount,release,send,snapshot,userprop \
  tank/data
```

### Destination datasets

```sh
sudo zfs allow -u boomerangz \
  canmount,compression,create,destroy,mount,mountpoint,readonly,receive,receive:append,userprop \
  backup/boomerangz
```

Add receive-property permissions only for properties you explicitly configure.
Do not compensate for a delegation error with unrestricted `sudo` or by running
the network-facing daemon as root.

## SSH

- use the packaged `boomerangz` account and its
  `/usr/lib/boomerangz/boomerangz-shell` login shell for `ssh-shell`;
- use a separate unprivileged `boomerangz-replication` account for direct SSH,
  with destination-only ZFS delegation and no `sudo` access;
- use a separate Ed25519 key for each trust relationship;
- verify host-key fingerprints out of band;
- configure `identity_file` so Boomerangz selects only the intended key;
- use `restrict` in `authorized_keys`, which disables forwarding, PTY, user-rc,
  agent forwarding, and X11 for that key;
- do not replace the packaged restricted login shell with a general-purpose
  shell; the wrapper admits only `ssh-shell`, while `ssh_shell.replication_roots`
  limits the destination trees exposed through its API;
- protect private keys from world read access and from group or world write
  access.

## TCP listeners and pairing

TCP listeners are opt-in and always require TLS plus `token`, `mtls`, or
`mtls+token` authentication.

- `token` authorizes a scoped secret.
- `mtls` authenticates a client certificate.
- `mtls+token` requires both.

Prefer `mtls+token` for replication over an untrusted network. Grant only the
scopes a client needs: `status`, `trigger`, `replicate`, `prune`, or `admin`.
The `admin` scope includes every operation.

Bind listeners only to intended interfaces and apply host firewall rules.
An empty `replication_roots` list leaves a listener available for authenticated
control but does not expose a destination for replication.

Boomerangz can manage its own private CA and renewable leaf certificates. For
an externally managed server certificate, configure both `tls_cert` and
`tls_key`. Clients use system trust roots unless `pairing_ca` supplies a
specific CA. `pairing_pin_certificate = true` additionally pins the server
public key while retaining normal CA, hostname, validity, and usage checks.

## Protect persistent identity

`/var/lib/boomerangz/identity` establishes which installation may manage an
existing dataset lineage. Back it up as part of the host's protected system
state, but never deploy the same identity directory to two active hosts.

Credentials under `/etc/boomerangz/credentials.d` may contain private keys or
tokens. Limit access to administrators and the `boomerangz` group.

## Local control access

The packaged service creates `/run/boomerangz/boomerangz.sock` as
`boomerangz:boomerangz` with mode `0660`; its `/run/boomerangz` directory has
mode `0750`. Add an administrative user to the group when they need to interact
with the running instance without `sudo`:

```sh
sudo usermod --append --groups boomerangz USERNAME
```

The user must sign out and back in before the new membership is available.
Group membership grants access to all operations on the local control API,
including state-changing operations; it is not read-only status access. Add
only trusted daemon administrators, and do not make the socket or its containing
directory world-accessible.
