# ZFS integration testing

No development-host test may execute `zfs` or `zpool`. Unit tests inject a fake
runner beneath the direct executor. Real operations run only inside disposable
QEMU guests.

Real-ZFS and installed-system tests live under `test/integration`, separate from
the application packages. Every Go guest suite uses the `integration` build
tag, so ordinary `go test ./...` does not compile or execute guest-only code.
The host harness builds static test binaries and runs them only inside its
guarded disposable guest.

The initial target runs in a CachyOS guest with two virtual scratch disks and
records the guest's kernel, `zfs-utils`, and ZFS module versions. It tests an
unprivileged account delegated only the candidate permissions
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

A missing or mismatched check aborts without destructive cleanup. The generic
host harness uses direct KVM-backed QEMU, read-only checksum-pinned base images,
copy-on-write overlays, QEMU user networking, and per-run keys and logs. It must
not call host `zfs` or `zpool`, modify host services, or create persistent
virtualization state.

## Running the canonical harness

There are two deliberately different integration-test operations:

```sh
make integration-test-compile
make integration-test
```

`integration-test-compile` compiles the integration-only Go packages and runs
no tests. The `integration` build tag only makes the guest-side tests available
to the Go tool; it does not authorize them to operate on the development host.
Running `go test -tags=integration ./test/integration/...` directly therefore
reports those guarded tests as skipped when their guest environment variables
are absent. This protects the host, but it is not a successful real-ZFS test
run.

`integration-test` is the real suite. It creates the guarded disposable guest
and invokes the test binaries there. The default target is `cachyos`; select a
different supported matrix adapter with, for example,
`make integration-test INTEGRATION_TARGET=<target>`.

GitHub Actions installs these Ubuntu runner packages:

```text
cloud-image-utils openssh-client qemu-system-x86 qemu-utils
```

The host must have read/write access to `/dev/kvm`. `libvirt`, `passt`, and a
virtual TPM are not required.

The workflow names a target adapter. The same command runs locally on a Linux
host with the required tools and KVM access:

```sh
make integration-test
```

The Make target and GitHub Actions both invoke the underlying harness at
`test/integration/host/run.sh`.

The generic host layer validates the target name and required adapter fields,
verifies the base image, creates per-run overlays and scratch disks, boots QEMU,
and handles SSH and diagnostics. Files under `test/integration/targets/<name>`
own target-specific image preparation, seed data, provisioning, device names,
and installed-system assertions. Adding another operating system does not add
its package manager or service manager to the generic layer.

Local runs use `/tmp` by default and are intentionally disposable. Set
`RUNNER_TEMP` to a dedicated durable scratch directory if diagnostics must
survive a reboot.

The `bootstrap.sh` asset independently checks the run marker, exact pool prefix,
whole-disk identity, virtual-disk serials, and pool vdev parents before creating
or destroying pools. `delegated-matrix.sh` repeats the marker, name, vdev, and
serial checks before exercising delegated ZFS operations. Target provisioning
may require guest root access, but all Boomerangz and delegated-ZFS assertions
run as the unprivileged test account. Destructive pool setup and teardown go
only through the guarded bootstrap.

## Target adapter contract

Each target adapter supplies a versioned, checksum-pinned base image and the
logic needed to prepare it. Before the target suite runs, its guest must have:

- its target operating system, OpenZFS implementation, and matching kernel
  components;
- the target's ZFS tools, SSH service, and a non-root test account reachable
  only through the generated test key;
- no existing pools backed by the two harness scratch-disk serial prefixes;
- a boot and device configuration matching the adapter's declared QEMU and
  guest-device settings.

The CachyOS adapter starts from a pinned official Arch cloud image, provisions
the CachyOS repositories and matching LTS kernel/OpenZFS module entirely inside
the guest, and reboots before testing. Other targets may use different image,
boot, package, device, and service conventions without changing the generic
host lifecycle. No host ZFS command is introduced by any adapter.

## Lifecycle integration

`test/integration/lifecycle/guest_test.go` is integration-only. The harness
builds it on the host, copies the binary into the disposable guest, and runs it
with `BOOMERANGZ_LIFECYCLE_GUEST_RUN` matching the
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

The corrected 2026-09-08 Phase 9 loopback run used the guarded transient
`phase9mux-260908a` guest and the same 33,637,624-byte ZFS payload for three
fresh end-to-end samples on each remote path. Direct mode multiplexed all ZFS
commands and the receive stream over one authenticated SSH connection per
sample. Median totals were 1.842 seconds for direct SSH (17.42 MiB/s), 1.825
seconds for gRPC over SSH shell (17.58 MiB/s), and 1.669 seconds for native TLS
gRPC (19.22 MiB/s). The corresponding ranges were 1.745-1.926, 1.796-1.919,
and 1.630-1.718 seconds. Every sample completed destination GUID verification.
These short guest-loopback measurements check for material regressions; they do
not predict throughput on real networks. Host access and guest-loopback SSH used
separate dedicated test keys with agent forwarding disabled.

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
released. Direct SSH remains covered by the single-guest transfer matrix. The
nested two-guest `passt` port-forward topology is retained for outage behavior
rather than transport performance comparisons.

`guest-property-layers.sh` separately demonstrated that receive exclusions and
plain inheritance retain hidden received values, including snapshot user-property
metadata; `inherit -S` can restore them. Queries for all properties can omit hidden
names. No native libzfs removal implementation is pursued. See the lifecycle
documentation for the practical cleanup limitation.
