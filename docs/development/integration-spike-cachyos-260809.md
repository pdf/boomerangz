# CachyOS delegated-operation integration spike

## Environment

The initial spike ran on 2026-09-04 in a transient `qemu:///session` guest built
from the official CachyOS desktop 260809 ISO. The downloaded ISO matched the
published SHA-256 digest:

```text
959f6577f45e25ee9fd8c220fd221b08e4ea79412c7315c0f922dd6d86d5e33c
```

Guest versions recorded by the matrix were:

```text
kernel=6.18.42-1-cachyos-lts
linux-cachyos-lts 6.18.42-1
linux-cachyos-lts-zfs 6.18.42-1
zfs-utils 2.4.3-2
zfs_module=2.4.3-1
```

The current reusable read-only base is stored in the default libvirt pool as
`boomerangz-cachyos-260809-base-v2.qcow2`. Per-run overlays and scratch images use
the `boomerangz-runs` directory in that pool, avoiding tmpfs-backed storage.

## Delegation result

The clean-state matrix passed as the unprivileged `boomerangz` account with
these local-and-descendent permissions:

```text
source: bookmark,destroy,hold,mount,release,send,snapshot,userprop
destination: compression,create,destroy,mount,mountpoint,readonly,receive,receive:append
```

`mount` was required transitively for snapshot destruction even though the
matrix never mounted a ZFS dataset. `userprop` was required for snapshot
ownership metadata. A replication stream carrying `mountpoint` completed
without that property permission but emitted permission warnings, so native
receive properties must be delegated explicitly. The `compression` and
`readonly` permissions represent the two configured properties exercised by
this spike; deployments must delegate every configured native receive property
they use.

The following cases passed:

- snapshot creation with internal user properties and snapshot pruning;
- hold and release;
- bookmark creation and destruction;
- full send and unmounted receive;
- direct `-i` and intermediary-preserving `-I` incrementals;
- recursive `-R` send and unmounted receive with a child filesystem and zvol;
- receive-side `readonly=on` override and `compression` exclusion;
- interrupted recursive receive with `-s`, descendant token discovery, and
  recovery using `zfs send -t`.

For a recursive stream, the receive resume token was stored on the interrupted
descendant zvol. Recovery succeeded when the resumed stream was received into
that token-owning descendant; attempting to resume into the replication root
required `-F` and was rejected. Token discovery must therefore retain both the
token and its owning dataset.

## Decision

Direct delegated execution is sufficient for the tested CachyOS/OpenZFS 2.4.3
combination. The spike provides no justification for a privileged helper or
`CAP_SYS_ADMIN`, so neither is introduced. This decision applies only to the
tested version matrix and must be repeated for every supported OpenZFS release.

The guest pools were destroyed through the same serial- and vdev-guarded
bootstrap after the matrix. No development-host `zfs` or `zpool` command was
run.

The v2 base was derived through a clean maintenance overlay on 2026-09-07. Its
installed guarded bootstrap matches the repository version that grants
destination `userprop` plus the fixture-only source `create` and destination
`snapshot` permissions. Guest tests also require bootstrap version 2 before
running ZFS commands, so an older base fails before reaching the permission
matrix.

Sources for the image and bootstrap interface:

- [CachyOS download and published checksum](https://wiki.cachyos.org/cachyos_basic/download/)
- [CachyOS headless installer configuration](https://github.com/CachyOS/New-Cli-Installer/blob/master/docs/config.md)
