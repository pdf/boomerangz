# ZFS integration testing

No development-host test may execute `zfs` or `zpool`. Unit tests inject a fake
runner beneath the direct executor. Real operations run only inside disposable
libvirt/QEMU guests.

The first integration spike must run in a CachyOS guest with two virtual scratch
disks and must record the guest's kernel, `zfs-utils`, and ZFS module versions.
It will test an unprivileged account delegated only the candidate permissions
against source and destination roots. The matrix is defined in section 2.1 of
`PLAN.md` and includes snapshots, bookmarks, holds, full/incremental/recursive
sends, resumable receives with `-u`, and receive property changes.

Before any destructive guest command, the in-guest harness must verify all of:

1. `/run/boomerangz-vmtest/guest-marker` contains the current run UUID.
2. The pool name starts with `boomerangz-test-` followed by that UUID.
3. Every vdev resolves to a virtio disk whose serial starts with
   `boomerangz-test-` followed by that UUID.

A missing or mismatched check aborts without cleanup. The host harness uses
`qemu:///session`, transient domains, read-only base images, copy-on-write
overlays, QEMU user networking, and per-run sockets and logs. It must not call
host `zfs` or `zpool`, modify host services, or create persistent libvirt state.

The executable harness is intentionally the next phase-one increment: this
document fixes its safety boundary before destructive code is introduced.
