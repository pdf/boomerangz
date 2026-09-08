# Phase 6 hand-off

Phase 6 is complete. Phase 7 has not started.

## Delivered

- `boomerangz daemon` runs foreground reconciliation and snapshot scheduling,
  loads the normal layered configuration, holds the installation lifecycle lock
  for its lifetime, and exits cleanly on `SIGINT` or `SIGTERM`.
- Snapshot deadlines are independent from discovery. Each active root keeps its
  own cadence, policy updates preserve unchanged deadlines, missed intervals
  coalesce, and restart reconstruction derives the next deadline from durable
  owned snapshots rather than creating a duplicate or backfilling missed work.
- Management, same-host transfer, and remote transfer work use independent
  bounded pools. Queues deduplicate jobs, schedule fairly across source roots,
  expose stable pending positions, and share per-target locks so management and
  transfer mutations cannot overlap on one destination.
- Immutable discovery generations drive activation transitions. Deactivation
  rejects new work, removes queued jobs, cancels active streams, lets a running
  short management operation finish, and records the durable inactive marker.
- The daemon reconstructs inactive owned roots from sparse lifecycle properties
  after restart. Once `inactive_grace_period` expires, retirement retries with a
  bound when blocked and applies only after live quiescence, authority, target
  identity, resume-state, metadata, and GUID checks succeed.
- Target retirement probes local and SSH destinations just in time, destroys
  only proven boomerangz-owned replicas, preserves foreign or dependent
  snapshots, and clears destination lineage only after its owned replicas are
  gone. Destination retention pruning runs after successful transfers under the
  same target serialization boundary.
- Per-target roadwarrior coordinators retain retry and recovery state while the
  daemon is running. Pending source snapshots coalesce, dirty state triggers one
  follow-up transfer, and one unavailable target does not block another.
- Structured events record queueing, active operation, retry, blocked, failed,
  and completion states. The in-memory latest-event view is ready for the Phase
  7 status and watch services.
- systemd service, sysusers, and tmpfiles definitions run the daemon as a
  dedicated unprivileged account with a constrained service sandbox.
- User documentation covers daemon operation, shutdown, service installation,
  ZFS delegation expectations, worker limits, and inactive retirement.

## Safety boundary

The daemon never treats a dataset name as target authority. Existing source
ownership must match the installation; a previously unowned active root is
claimed when its first snapshot is created. Destination work revalidates the
stored pool and anchor GUID binding just in time. Automatic retirement retains
public policy and recovery evidence whenever a target is unavailable, a resume
token exists, ownership is ambiguous, or a snapshot has holds, clones, or other
dependencies.

Standalone lifecycle apply commands and the daemon share the lifecycle lock.
Until Phase 7 supplies live control-plane coordination, standalone clean still
fails closed when a daemon socket exists rather than attempting to quiesce it.

All real ZFS validation remained inside a disposable libvirt/QEMU guest. The
CachyOS/OpenZFS 2.4.3 run verified scheduled snapshot creation, deactivation,
durable inactive marking, grace expiry, ownership-safe source retirement, and
graceful shutdown. A local target-retirement integration test verifies exact
metadata and GUID proof, owned-replica deletion, foreign-snapshot preservation,
and destination-lineage clearing against a stateful backend.

The current guest base predates destination user-property delegation required
by the present bootstrap, so this Phase 6 run did not repeat target-side local
transfer and retirement in that guest. Phase 4 already verified local transfer
against real ZFS; the broader permission and fault matrix remains Phase 8 work.
The guarded guest pools were destroyed before the transient domain was stopped.
Its ordinary run overlays remain under
`/storage/libvirt/qcow2/boomerangz-runs/daemon-260906a` because validated artifact
deletion is not yet implemented by the host harness.

## Verification

The phase boundary was verified with:

```sh
go test ./...
go test -race ./...
go vet ./...
go tool golangci-lint run
go tool buf format --diff --exit-code
go tool buf lint
go tool buf generate
git diff --check
systemd-analyze verify contrib/systemd/boomerangz.service
systemd-analyze security --offline=yes contrib/systemd/boomerangz.service
```

The source-checkout `systemd-analyze verify` parsed the unit and reported only
the expected absence of the not-yet-installed `/usr/bin/boomerangz` executable.
The offline security analysis rated the unit 1.6 (`OK`).

The opt-in guest test was compiled locally and run in the guarded guest as
`TestGuestDaemonSchedulingAndRetirement` with
`BOOMERANGZ_DAEMON_GUEST_RUN=daemon-260906a`.

## Phase 7 entry point

Phase 7 should expose the daemon's existing status store and lifecycle gate over
the versioned local gRPC control API. It should add status snapshots and watches,
trigger and reconcile operations, daemon-coordinated explicit lifecycle actions,
terminal progress, Unix-socket authorization, authenticated pairing, and optional
authenticated TLS listeners. It must not weaken the just-in-time ZFS authority
and target-binding checks already enforced by the runtime.
