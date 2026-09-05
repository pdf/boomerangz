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
3. Every vdev resolves to a virtio disk whose deterministic serial matches the
   source or destination serial derived from that UUID. Serials use a short
   hash because virtio exposes at most 20 bytes.

A missing or mismatched check aborts without cleanup. The host harness uses
`qemu:///session`, transient domains, read-only base images, copy-on-write
overlays, QEMU user networking, and per-run sockets and logs. It must not call
host `zfs` or `zpool`, modify host services, or create persistent libvirt state.

## Host prerequisites

The current Arch-family package set is:

```text
qemu-desktop qemu-img libvirt virt-install passt edk2-ovmf openssh
```

The user must have read/write access to `/dev/kvm`, and `qemu:///session` must be
available. `swtpm` is not required because this matrix does not use a virtual TPM.

Run the non-destructive prerequisite check with an installed qcow2 base image,
an existing artifact directory, and an unused loopback port:

```sh
go run ./cmd/boomerangz-vmtest preflight \
  --base-image /storage/vm/base/cachyos.qcow2 \
  --work-dir /storage/vm/boomerangz-runs \
  --ssh-port 22022
```

`prepare` performs the same checks, creates a uniquely named qcow2 system
overlay, and creates separate source and destination scratch disks. The base
image cannot reside under the artifact directory, preventing later run cleanup
from making it a possible target.

```sh
go run ./cmd/boomerangz-vmtest prepare \
  --base-image /storage/vm/base/cachyos.qcow2 \
  --work-dir /storage/vm/boomerangz-runs \
  --ssh-port 22022
```

`launch` additionally starts a transient domain with `passt` forwarding the
chosen loopback port to guest SSH. Its VNC listener is restricted to loopback,
and a QEMU guest-agent channel is available for base-image maintenance. It does
not yet enter the guest or run ZFS commands automatically.

The root-owned `guest-bootstrap.sh` asset implements the only passwordless
guest elevation. It independently checks the run marker, exact pool prefix,
whole-disk identity, virtio serials, and pool vdev parents before creating or
destroying pools. `guest-matrix.sh` repeats the marker, name, vdev, and serial
checks before exercising delegated ZFS operations. The sudo policy permits the
`boomerangz` account to invoke only the guarded bootstrap asset.

## Base-image contract

The reusable base image remains read-only and must contain:

- an installed CachyOS system using a kernel with matching ZFS modules;
- `zfs-utils`, OpenSSH, and a non-root test account reachable by key;
- passwordless elevation limited to the guest bootstrap operations needed to
  create the disposable test pools and delegated account;
- no existing pools backed by the two harness scratch-disk serial prefixes;
- a stable boot configuration compatible with virtio disks, a serial console,
  and the loopback-only diagnostic display.

The current harness deliberately stops before automating base-image installation
and SSH orchestration. The initial CachyOS base and delegated-operation spike
have been built and verified manually; the exact result is recorded in
`integration-spike-cachyos-260809.md`. No host ZFS command is introduced by the
bootstrap or matrix paths.

## Lifecycle integration

`internal/lifecycle/guest_test.go` is opt-in and skips ordinary host test runs.
Build it on the host with `go test -c ./internal/lifecycle`, copy the binary into
the disposable guest, and run there with `BOOMERANGZ_LIFECYCLE_GUEST_RUN` matching
the guarded run marker. It verifies source disk serial and actual pool vdevs
before creating uniquely named fixtures beneath the source pool's `data` subtree.
The guest account additionally needs delegated `create` for these fixture datasets.
Fixtures remain until the guarded bootstrap tears down the test pools.

To include command-line adoption and cleanup, also copy the built boomerangz CLI
and a test TOML file into the guest, then set `BOOMERANGZ_LIFECYCLE_GUEST_CLI` and
`BOOMERANGZ_LIFECYCLE_GUEST_CONFIG` to those guest paths. Use a writable guest-only
socket path in that TOML for the standalone lifecycle lock. Do not set these
variables for host test runs.

The 2026-09-05 run used Linux `6.18.42-1-cachyos-lts`, `zfs-2.4.3-1` and matching
`zfs-kmod-2.4.3-1`, in the transient `run-lifecycle-260905` guest. The Go service
and CLI passed creation of recursive and non-recursive snapshots, held-snapshot
protection, exact-dataset pruning, lineage adoption, target-specific hold and
bookmark creation/release, recursive cleanup preserving snapshot data, and CLI
owned-snapshot destruction preserving a foreign snapshot. Checkpoint tests used
a synthetic target and explicitly supplied source GUID; they do not claim real
destination verification, which belongs to the transfer phase.

`guest-property-layers.sh` separately demonstrated that receive exclusions and
plain inheritance retain hidden received values, including snapshot user-property
metadata; `inherit -S` can restore them. Queries for all properties can omit hidden
names. No native libzfs removal implementation is pursued. See the lifecycle
documentation for the practical cleanup limitation.
