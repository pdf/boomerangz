# ZFS integration testing

No development-host test may execute `zfs` or `zpool`. Unit tests inject a fake
runner beneath the direct executor. Real operations run only inside disposable
libvirt/QEMU guests.

Real-ZFS tests remain next to the packages they exercise, but every such file
uses the `integration` build tag. Ordinary `go test ./...` runs therefore do not
compile or execute guest-only test code. Build a package's guest test binary
explicitly with `go test -tags=integration -c ./internal/<package>` and run that
binary only inside a guarded disposable guest with the documented environment.

The first integration spike must run in a CachyOS guest with two virtual scratch
disks and must record the guest's kernel, `zfs-utils`, and ZFS module versions.
It will test an unprivileged account delegated only the candidate permissions
against source and destination roots. The matrix is defined in section 2.1 of
`PLAN.md` and includes snapshots, bookmarks, holds, full/incremental/recursive
sends, resumable receives with `-u`, and receive property changes.

Before any destructive guest command, the in-guest harness must verify all of:

1. `/run/boomerangz-vmtest/guest-marker` contains the current run UUID.
2. `/run/boomerangz-vmtest/bootstrap-version` matches the checked-in harness.
3. The pool name starts with `boomerangz-test-` followed by that UUID.
4. Every vdev resolves to a virtio disk whose deterministic serial matches the
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

`internal/lifecycle/guest_test.go` is integration-only. Build it on the host with
`go test -tags=integration -c ./internal/lifecycle`, copy the binary into the
disposable guest, and run there with `BOOMERANGZ_LIFECYCLE_GUEST_RUN` matching the
guarded run marker. It verifies source disk serial and actual pool vdevs before
creating uniquely named fixtures beneath the source pool's `data` subtree. The
guest account additionally needs delegated `create` for these fixture datasets.
Fixtures remain until the guarded bootstrap tears down the test pools.

To include command-line adoption and clean, also copy the built boomerangz CLI
and a test TOML file into the guest, then set `BOOMERANGZ_LIFECYCLE_GUEST_CLI` and
`BOOMERANGZ_LIFECYCLE_GUEST_CONFIG` to those guest paths. Use a writable guest-only
socket path in that TOML for the standalone lifecycle lock. Do not set these
variables for host test runs.

The 2026-09-05 run used Linux `6.18.42-1-cachyos-lts`, `zfs-2.4.3-1` and matching
`zfs-kmod-2.4.3-1`, in the transient `run-lifecycle-260905` guest. The Go service
and CLI passed creation of recursive and non-recursive snapshots, held-snapshot
protection, exact-dataset pruning, lineage adoption, target-specific hold and
bookmark creation/release, recursive clean preserving snapshot data, and CLI
owned-snapshot destruction preserving a foreign snapshot. Checkpoint tests used
a synthetic target and explicitly supplied source GUID; they do not claim real
destination verification, which belongs to the transfer phase.

The final 2026-09-06 phase-4 authority run used the same kernel and ZFS versions
in the transient `run-transfer-final-260906` guest. The 80.83-second local transfer
matrix passed full bootstraps, direct and intermediary-preserving incrementals,
foreign intermediate retention, received public-property isolation, native
receive overrides, bookmark-based incrementals, persistent pool/dataset GUID
bindings, recursive full and incremental `first`/`all` mappings on separate
authoritative roots, and refusal of foreign latest destination history.
The guarded cleanup verified the marker, serials, exact pool names and vdev parent
disks before destroying both scratch pools; the transient domain then exited.

The 2026-09-06 Phase 5 remote run used the transient
`run-remote-260906a` guest. Direct SSH and gRPC over the persistent
`ssh-shell` command channel each transferred and verified a 33,637,624-byte ZFS
stream. The run also confirmed that destination delegation needs `userprop` for
lineage and ownership reconciliation; the guarded bootstrap now grants it, in
line with the documented SSH destination baseline.

The completed 2026-09-07 Phase 8 matrix used Linux
`6.18.42-1-cachyos-lts`, `zfs-utils 2.4.3-2`, and ZFS module `2.4.3-1`.
OpenZFS 2.4.3 is therefore the minimum verified version for the initial CachyOS
support baseline; other operating-system and OpenZFS combinations remain
unverified until their own disposable-guest matrix passes. The production
delegation baseline remains:

```text
source: bookmark,destroy,hold,mount,release,send,snapshot,userprop
destination: compression,create,destroy,mount,mountpoint,readonly,receive,receive:append,userprop
```

The guest bootstrap additionally grants source `create` and destination
`snapshot` only so tests can construct and isolate their own fixtures. Those
permissions are not implied production requirements. No Phase 8 result required
a privileged helper or Linux capability.

A two-guest fault run exported the destination pool for longer than one natural
one-minute source cadence. The daemon retained a target-specific ZFS hold,
coalesced to newer scheduled snapshots without allowing source pruning to remove
the selected generation, treated the missing remote anchor as retryable, and
completed through gRPC over `ssh-shell` after pool import. The configured leaf
did not exist before reconnection; bootstrap created it and the received snapshot
GUID matched the newest source generation. The completed recovery hold was then
released. Direct SSH remains covered by the single-guest transfer matrix; the
nested two-guest `passt` port-forward topology is unsuitable for its sequence of
short independent SSH connections and is not used as evidence about production
direct-SSH behavior.

`guest-property-layers.sh` separately demonstrated that receive exclusions and
plain inheritance retain hidden received values, including snapshot user-property
metadata; `inherit -S` can restore them. Queries for all properties can omit hidden
names. No native libzfs removal implementation is pursued. See the lifecycle
documentation for the practical cleanup limitation.
