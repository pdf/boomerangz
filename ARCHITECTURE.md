# boomerangz - architecture and repository map

`boomerangz` is a property-driven OpenZFS snapshot and send/receive manager,
targeting intermittently connected hosts (laptops/"roadwarriors") as well as
always-on backup targets. Policy lives in `org.boomerangz:*` ZFS user
properties on the managed datasets; TOML config holds only remote definitions
and daemon-wide settings. The full design rationale, property contract, and
delivery-phase history are in [PLAN.md](PLAN.md) - that document is the
original spec and does **not** always match the current implementation (see
"Where the code has diverged from PLAN.md" below).

Working conventions that apply to every change - how to verify, what a
user-visible change has to update, commit format - are in
[AGENTS.md](AGENTS.md). This document is the reference map: read the section
covering the package you are about to touch.

Go 1.26 (toolchain 1.26.8), module `github.com/pdf/boomerangz`. CLI built with
[kong](https://github.com/alecthomas/kong) (command structs + `Run` methods,
no central dispatch switch). Control plane is gRPC (`google.golang.org/grpc`)
over Unix sockets and optional TCP.

## Entrypoints

- [cmd/boomerangz/main.go](cmd/boomerangz/main.go) - process entrypoint;
  wires `os.Args`/signals into `cli.Run` and exits with its return code.
- [internal/cli/cli.go](internal/cli/cli.go) - `cli.Run` builds the kong
  parser and dispatches to the matched command's `Run(env *commandEnvironment)`.
- [internal/cli/commands.go](internal/cli/commands.go) - the full command
  tree (`daemon`, `status`, `trigger`, `pairing {create,import,list,revoke}`,
  `ssh-shell` (hidden), `dataset {list,inspect,adopt,reseed,clean}`,
  `identity recover`, `config {check,show,reload}`, `version`). This is the
  fastest place to see the whole CLI surface at a glance.
- `boomerangz daemon` (`daemonCommand.Run`) is the long-running service:
  loads config, acquires the lifecycle lock, loads/creates the installation
  identity, builds a `daemon.Runtime`, and starts the gRPC control server.

## Package layout (`internal/`)

Roughly bottom-up, in dependency order:

- **zfs** - shell-free typed access to OpenZFS. `Executor`/`ReseedExecutor`
  interfaces (executor.go); `Direct` is the real `os/exec`-backed
  implementation (direct.go, state.go); `stream.go` builds send/receive
  argument vectors and runs the sender->copy->receiver pipeline
  (`RunPipeline`/`LocalStream`), reused by the ssh/rpc/native transports via a
  `CommandFactory`. Everything else in the tree consumes `zfs.Executor` rather
  than shelling out directly.
- **policy** - parses `org.boomerangz:*` properties into an effective,
  inherited policy (`resolve.go`: `Resolve`/`Effective`) and the retention
  grid syntax (`grid.go`: `ParseGrid`, e.g. `12x5m,24x1h,14x1d`).
- **config** - TOML config struct, `Defaults()`, `Load` (primary file +
  `config.d/*.toml` drop-ins merged by kind), `Validate()`, and
  `MarshalRedacted` for `config show`.
- **identity** - the non-secret installation UUID persisted under
  `paths.identity_dir` (`LoadOrCreate`, `Recover`). Independent of TLS/token
  material.
- **discovery** - periodic ZFS inventory. `Scanner` walks dataset state
  through a `Reader`, resolves policy, and atomically publishes immutable
  `Generation` snapshots (`Entries`, `Inspect`, `Changed`).
- **lifecycle** - the largest package; snapshot ownership, ZFS-native
  authority, and lifecycle transitions, all gated by the installation
  identity:
  - `authority.go` / `ownership.go` - who owns a dataset root/snapshot and
    lineage validation.
  - `gate.go` - per-dataset admission control (`Gate`/`Ticket`) so
    conflicting work classes don't run concurrently on the same dataset.
  - `reference.go` - hold/bookmark bookkeeping protecting in-flight
    replication targets from pruning.
  - `prune.go` - grid-driven retention decisions.
  - `inactive.go` - deactivation and delayed automatic retirement.
  - `clean.go` - explicit, preview-first `dataset clean` planning/execution.
  - `service.go` - the facade (`Service`) other packages call.
- **transfer** - plans and executes replication jobs on top of `lifecycle`
  and `zfs`: `plan.go` (`Build` computes the send/receive plan),
  `local.go` (`Local` executor: `Preview`/`Apply`), `binding.go` (verifies a
  source is bound to the correct destination, not just a same-named one),
  `resume.go` (interrupted-receive resume), `reseed.go` (destructive
  full-resync path), `recovery.go` (`PendingSet` coalescing and the
  `Roadwarrior` reconnect/retry loop for flaky remotes).
- **replication** - transport implementations sharing one gRPC service
  contract (`proto/boomerangz/replication/v1/remote.proto`):
  - `ssh/` - default v1 transport: batch-mode SSH, either driving remote ZFS
    directly or speaking RPC-over-stdio to a remote `boomerangz ssh-shell`.
  - `native/` - the same RPCs over authenticated TLS via a control-plane
    pairing bundle (no remote `boomerangz` install needed for `ssh/` direct
    mode; `native/` requires a remote daemon).
  - `rpc/` - generated stubs plus the shared `Server`/`Client` and the
    `stdio.go` net.Conn shim that lets the gRPC service run over an SSH pipe.
  - `scope/` - pure helpers enforcing that remote operations stay under an
    allowed destination root.
- **daemon** - the running service's scheduling core: `runtime.go`
  (`Runtime`, the top-level object created by `daemon.New`), `scheduler.go`
  (next-due work from policy grids), `pool.go`/`queue.go` (bounded, fair
  worker pools - management/local-transfer/remote-transfer), `safety.go`
  (pre-flight quiescence/target checks), `retirement.go` (wires
  `lifecycle`'s inactive/retirement logic into the runtime), `status.go`
  (`StatusStore`, a revisioned in-memory event log backing `WatchStatus`).
- **daemonstate** - plain shared data types (`Event`, `ControlSnapshot`,
  `ReloadResult`, ...) with no behavior, used so `daemon` and `control` don't
  depend on each other's internals.
- **control** - the admin/control-plane gRPC server and CLI-facing client:
  `server.go` (`Server`, listener/TLS setup, live `Reload`), `service.go`
  (`StatusService`/`ControlService` implementation), `token.go`/`pairing.go`/
  `pki.go` (token and mTLS-pairing issuance/storage), `client.go`
  (`PairingBundle`, `Client.DialLocal`/`DialBundle` used by the CLI).
  `control/rpc/` holds the generated stubs for
  `proto/boomerangz/control/v1/control.proto`.
- **statusui** - renders a `StatusSnapshot` for `boomerangz status`, both as
  a terminal table (`Terminal`) and as JSON (`JSON`).
- **cli** - see Entrypoints above; also formats human-readable dataset
  output (`dataset_output.go`) and provides a CLI-only "standalone safety"
  path (`lifecycle.go`) for commands run without a live daemon.
- **testutil/zfstest** - guards used by `test/integration` to ensure
  destructive ZFS commands only ever touch disposable QEMU-guest disks/pools
  (`VerifyGuestGuard`, `VerifyGuestPool`), never a real host pool.

## Property to send/receive mapping (`internal/policy`, `internal/zfs/stream.go`, `internal/transfer/plan.go`)

`org.boomerangz:*` properties are resolved into `policy.Effective`
([internal/policy/resolve.go](internal/policy/resolve.go)), translated to ZFS
argv in [internal/zfs/stream.go](internal/zfs/stream.go), and wired together
per transfer in [internal/transfer/plan.go](internal/transfer/plan.go).

| Property | Resolves to | ZFS effect |
| --- | --- | --- |
| `enabled` | `Effective.Enabled` | Gates participation (`lifecycle.ActiveRoot` check in `plan.Build`), not a send flag. |
| `policy` (grid) | `Effective.Grid` | Drives which snapshot gets sent (scheduler/pruning), not send flags. |
| `large_blocks` | `Send.LargeBlocks` | `-L` |
| `compressed` | `Send.Compressed` | `-c` |
| `raw` | `Send.Raw` | `-w`; defaults to `on` for encrypted datasets (resolve.go:89-91) |
| `props` | `Send.Props` | `-p`; forced `on` when `replicate=on` (resolve.go:128-130) |
| `replicate` | `Send.Replicate` | `-R` |
| `incremental` | `all`/`latest` | Chooses `-I` vs `-i` in `plan.Build` (plan.go:431-435) |
| `discard` | `off`/`first`/`all` | Receive-side `-d`/`-e` via `MapReceiveDataset`/`receiveArgs` (stream.go:158-171) |
| `set_prop:<name>` | `SetProperties[name]` | Receive-side `-o name=value` |
| `ignore_prop:<name>` | `IgnoreProperties` | Receive-side `-x name` |

`zfs receive` is always invoked as `receive -u -s <root>` (stream.go:162) -
unmounted and resumable, never `-F` - regardless of any property; that is a
hard-coded invariant, not a policy toggle.

Implications for correct receive behavior:

- **Raw/encryption coupling is load-bearing.** `replicate=on` on an
  encrypted dataset without `raw=on` is a hard error (resolve.go:137-139).
  For a replication root with `raw` left unspecified, an encrypted
  descendant auto-selects raw for the whole root via `ForReplicationScope`,
  but only when `raw` was never explicitly set anywhere in the chain; an
  explicit `raw=off` blocks the auto-selection and fails validation instead
  of silently sending plaintext.
- **Raw receive locks key-derivation properties.** When `raw=on` on an
  encrypted source, `validateRawReceive` forbids `set_prop`/`ignore_prop`
  from touching `encryption`, `keyformat`, or `pbkdf2iters`
  (resolve.go:221-231); `keylocation` remains overridable since it is a
  legitimate per-host choice.
- **`org.boomerangz:*` namespace isolation is enforced three times**, and
  each layer covers a gap the others don't:
  - `policy.Resolve` rejects `set_prop`/`ignore_prop` targeting the reserved
    namespace at resolution time (resolve.go:199-201).
  - `zfs.receiveArgs` independently refuses any `-o` key with the
    `org.boomerangz:` prefix (stream.go:178) as defense in depth.
  - `transfer.completeReceiveExclusions` walks every `org.boomerangz:*`
    public key actually present in the send scope and adds an explicit `-x`
    for each (plan.go:114-127), since OpenZFS has no property-prefix `-x`;
    it also excludes `state:reference:*` and `state:target:*` keys so
    per-target recovery metadata never reaches an unrelated destination.
    `state:lineage`/`state:owner` are deliberately *not* excluded - PLAN.md's
    ownership model expects those to be reconstructed/verified receive-side.
- **`set_prop` beats `ignore_prop` on conflict**: a key in both loses its
  `IgnoreProperties` entry with a warning, and only `set_prop`'s `-o`
  survives (resolve.go:209-215).
- **`discard` changes the destination path, not just a flag**, via
  `MapReceiveDataset` (`first` strips the source's leading component, `all`
  keeps only the last). `plan.Build` re-derives this mapping from current
  inventory on every plan rather than trusting a cached value, since a wrong
  mapping can misroute an entire dataset tree.
- **`canmount` gets an implicit safety default**: unless
  `set_prop:canmount` is explicit, filesystem receives get
  `canmount=noauto` injected (plan.go:360-364), layered on top of `-u`.
- **`incremental=all` (`-I`) can leak foreign snapshots**: `plan.Build`
  flags any intermediate snapshot lacking valid ownership metadata as a
  warning ("stream includes foreign snapshot: ...") instead of silently
  including it (plan.go:463-467).
- **`replicate=on` always warns about destination impact**
  ("recursive replication uses native package semantics and may affect
  foreign destination snapshots", plan.go:438-440), independent of whether a
  foreign destination snapshot is known to exist, because OpenZFS's `-R`
  receive semantics can prune destination snapshots absent from the sender.

Net effect: the property-to-flag mapping is simple, but most properties have
a receive-side safety interaction (namespace leakage, encryption
consistency, mount behavior, or path remapping) enforced redundantly across
`policy`, `zfs`, and `transfer` rather than in one place. Extending any of
these properties should carry the corresponding check into all three layers,
not just the one that happens to be edited first.

## `org.boomerangz:state:*` properties (`internal/lifecycle`, `internal/transfer/binding.go`)

`org.boomerangz:state:*` is a separate namespace from the public policy
properties above (`policy.StateNamespace`, [internal/policy/resolve.go:16](internal/policy/resolve.go#L16)).
`policy.IsPublic` explicitly excludes it, so none of these keys ever
participate in effective policy, are ever received-side authoritative, or
are ever exposed to `set_prop`/`ignore_prop`. They exist purely as
ZFS-native recovery/ownership state - there is no separate database.

| Property | Set by | Purpose |
| --- | --- | --- |
| `state:owner` | `lifecycle.RootAuthority` callers (adoption, first management) | Installation UUID that owns a source root ([internal/lifecycle/authority.go](internal/lifecycle/authority.go), `OwnerProperty`). |
| `state:lineage` | Same as above | Per-root lineage UUID, shared by the root's owned snapshots ([internal/lifecycle/ownership.go](internal/lifecycle/ownership.go), `LineageProperty`). |
| `state:snapshot` | `lifecycle.Metadata.Properties()` at snapshot creation | Per-snapshot UUID embedded on the snapshot itself, not the dataset. |
| `state:created` | Same as above | RFC3339Nano creation timestamp, also embedded per snapshot. |
| `state:reference:<target-id>:<snapshot-uuid>` | `Service.ProtectSet` | JSON `Reference` proof binding a hold/bookmark pair to a proven source snapshot for one replication target ([internal/lifecycle/reference.go](internal/lifecycle/reference.go)). |
| `state:target:<target-id>` | `transfer` binding logic | JSON `TargetBinding` recording a source's persistent identity for one configured destination ([internal/transfer/binding.go](internal/transfer/binding.go)). |
| `state:target:<target-id>:suspended` | `transfer.SetTargetSuspended` | Marks a target unverified/suspended pending adoption revalidation. |
| `state:inactive` | `Service.ReconcileInactive` | JSON `InactiveMarker` recording when an owned root first went inactive, for delayed retirement ([internal/lifecycle/inactive.go](internal/lifecycle/inactive.go)). |

Cross-cutting rules that hold across all of these:

- **Ownership (`state:owner`, `state:lineage`) is the authority anchor.**
  `RootAuthority` (authority.go:32-61) requires both keys to be present,
  unambiguous, and `Source == local` on the *exact* root; a received or
  inherited copy is provenance only, never authority. A locally set owner
  that doesn't match the running installation's UUID is reported as a
  "dormant foreign lineage" and the daemon performs no snapshot creation,
  pruning, transfer, or recovery-state mutation for that root - this is how
  a moved pool (same properties, different host) fails closed instead of
  auto-adopting.
- **Per-snapshot metadata (`state:lineage`, `state:snapshot`, `state:created`)
  proves ownership of one exact snapshot**, independent of the root
  markers. `Ownership` (ownership.go:74-109) requires all three to be
  explicitly stored *on the snapshot object itself* (`local` or `received`
  source - a received snapshot's own metadata is trusted once its parent
  root's authority checks out), matching lineage, and requires the snapshot
  name to be the exact deterministic encoding of that metadata
  (`Metadata.Name()`). A `boomerangz-`-prefixed name alone proves nothing;
  names and metadata must agree, or the snapshot is treated as foreign.
- **Reference proofs (`state:reference:*`) gate hold/bookmark lifecycle,
  not the transfer itself.** `Protect`/`ProtectSet` write the JSON proof
  *before* placing the corresponding hold (reference.go:125-243), so an
  interruption between the two leaves a reconstructable, idempotent retry
  rather than an orphaned hold. `Checkpoint`/`CheckpointSet` only advance a
  bookmark after the caller supplies a verified destination GUID
  (reference.go:249-324) - the property never claims verification the
  daemon hasn't itself confirmed via a completed receive.
  `ReleaseReference` (reference.go:328-402) is the only path that removes a
  reference proof, and only after re-confirming the hold/bookmark GUIDs
  still match; it inherits (clears) the property last, after the ZFS-side
  hold and bookmark are already gone.
- **Target bindings (`state:target:*`) pin a source to a specific physical
  destination, not just a name.** `TargetBinding` records pool GUID, anchor
  dataset GUID, and relative path (binding.go:23-34); every planned transfer
  re-resolves and compares against the stored value
  (`transfer.plan.Build`, plan.go:269-297), and a same-named destination
  with a different GUID is blocked rather than silently treated as the same
  target - the operator must explicitly reseed or adopt instead.
- **All of these reject a "hidden received" value that disagrees with the
  local one** (`state.Received[dataset][key]` checks in authority.go:55-59,
  reference.go, inactive.go:94-97, binding.go:191-194, 253-255) - a
  conflicting received copy of internal state forces an error demanding
  explicit resolution rather than picking one value silently.
- **Discovery reads only a narrow, fixed slice of this namespace globally.**
  `GetLifecycleProperties` ([internal/zfs/direct.go:186-197](internal/zfs/direct.go#L186-L197))
  fetches only `state:owner`, `state:lineage`, and `state:inactive` across
  every dataset at startup, to reconstruct responsibility without
  enumerating arbitrary user properties; reference and target-binding keys
  are dynamic (per target/snapshot) and are only read per-dataset via
  `GetStoredProperties`'s `all`-property query when a specific dataset is
  already in scope for other reasons.
- **Clean and retirement inherit (never destroy) these properties as the
  final step**, after the underlying holds/bookmarks/snapshots they
  describe have already been resolved (`backend.InheritProperty` in
  clean.go:336, inactive.go:207) - the property is a proof *about* ZFS
  state, so it is only cleared once nothing depends on being able to
  reconstruct that proof again.

## Scheduler behavior (`internal/daemon/scheduler.go`)

`Scheduler` tracks one `deadline{policy, cadence, next, pending}` per
schedulable root, independent of `discovery`'s periodic scans. It is a plain
mutex-guarded map plus a broadcast `wake` channel (closed and replaced on
every mutation - `signal()` - to release whichever goroutine is blocked in
`Next`).

- **Schedulability** (`isSchedulable`): a discovery entry qualifies only if
  it is not covered by an ancestor root (`CoveredBy == ""`), was actually
  inspected, has policy `Enabled` and `Valid()` (no parse errors), has no
  active lifecycle transition (`lifecycle.ActiveRoot(...) == nil`), and its
  retention grid has a positive `Cadence()`. Anything else is silently
  excluded from scheduling, not errored.
- **`Update(entries, now)`** is called after every completed discovery
  generation (`Runtime.applyGeneration`, runtime.go:452). It atomically
  replaces the whole entry set: a root whose cadence is unchanged from the
  previous generation *keeps* its existing `next`/`pending` state (so a
  policy edit that doesn't touch the grid never resets or double-fires a
  deadline); a changed cadence (or a brand-new root) gets `next = now`, i.e.
  immediately due. Returns sorted `active`/`removed` name lists purely for
  the caller's own bookkeeping (`Runtime` uses them to diff `known`/`active`
  state and to fire `deactivate`/`enqueueInactive` for roots that dropped
  out).
- **`Next(ctx)`** is the daemon's snapshot-timer loop (run in its own
  goroutine from `Runtime.Run`, runtime.go:1131-1140). It repeatedly scans
  all entries for the earliest non-`pending` deadline, blocks on a
  `time.Timer` for that duration (or on `ctx.Done()`/the `wake` channel,
  whichever fires first), and re-evaluates from scratch on every wake -
  there is no per-entry timer, just one shared timer for the global
  minimum. When a deadline is actually due (`wait <= 0`) it marks that
  entry `pending = true` and returns a detached `Schedule` snapshot
  (dataset, cloned policy, deadline). `pending` is a leaky-bucket guard:
  while true, `Next` will never re-select that dataset, so exactly one
  snapshot job can be in flight per root at a time. Only `Complete` or
  `Retry` clear it.
- **`Complete(dataset, completed)`**: called after a snapshot actually
  lands (runtime.go:655). Sets `pending = false` and `next = completed +
  cadence` - deliberately anchored to actual completion time, not the
  original deadline, so a run that starts late doesn't cause the next one
  to fire early, and so missed periods (daemon was down, dataset was
  blocked) are coalesced into a single catch-up snapshot rather than
  replayed as a backlog burst.
- **`Retry(dataset, notBefore)`**: called on every non-success path out of
  `enqueueSnapshot` (gate admission failure, job-queue drop, stale-deadline
  re-check, `CreateSnapshot` error) to clear `pending` and push `next` out
  to a backoff time, usually `now + ReconcileInterval` or `now + 1s` for
  queue drops. This is the mechanism that turns a single missed attempt
  back into a live, re-selectable entry instead of a stuck root.
- **`Lookup(dataset)`** (used by `Runtime.Trigger`, runtime.go:1191) returns
  a `Schedule` without touching `pending`, so manual triggers and the
  timer-driven path can both produce a `Schedule` for the same dataset
  concurrently; `Trigger` sets `Schedule.Force = true` on the result.
  `Force` bypasses the deadline re-check in `enqueueSnapshot` (send the
  snapshot immediately even if the stored/observed next-due time is in the
  future) and suppresses the automatic `Retry` call on gate/queue failure,
  since a manual trigger's caller is expected to retry deliberately rather
  than have the scheduler silently reschedule it.
- **Double-checked deadline at execution time**: `enqueueSnapshot`
  (runtime.go:616) re-derives the true next-due time from ZFS state itself
  via `nextOwnedSnapshot` (latest owned snapshot's creation time + cadence,
  or the zero time if the root has no authoritative snapshot yet) and skips
  creating a snapshot (rescheduling instead) if that still isn't due. This
  makes the in-memory `Scheduler` an optimization/wakeup mechanism rather
  than the source of truth - ZFS-recorded snapshot ownership is what
  actually gates snapshot creation, so drift between the scheduler's `next`
  and reality (e.g. after a daemon restart, before `Update` has run) fails
  closed into a reschedule rather than a duplicate/early snapshot.
- **Concurrency model**: all scheduler state is behind a single
  `sync.Mutex`; there's no per-dataset locking. `Next`'s O(n) full-map scan
  on every wake is intentional simplicity over an ordered heap - the entry
  count is bounded by managed dataset roots, not by scan/event volume.

## Protobuf / gRPC surface

Two service definitions under `proto/boomerangz/`, built with
[Buf](https://buf.build) (`buf.yaml`, `buf.gen.yaml`); generated `.pb.go`
files are committed and regenerated via `go tool buf generate`.

- `control/v1/control.proto` - `StatusService` (`GetStatus`, `WatchStatus`
  streaming, `ListDatasets`) and `ControlService` (`Trigger`, `Reconcile`,
  `Reload`, `Clean`). This is the local admin API used by the CLI.
- `replication/v1/remote.proto` - `RemoteService`: dataset
  inspection/mutation RPCs (`InspectState`, `CreateReceiveParent`,
  `SetProperties`, `InheritProperty`, ...) plus a client-streaming `Receive`.
  Shared by both the `ssh-shell` and `native` transports.

## Tests

- Unit tests sit next to the code they cover (`*_test.go`), run with plain
  `go test ./...` - no ZFS or root privileges required.
- `test/integration/` requires the `integration` build tag and a real ZFS
  pool; it is **not** compiled by ordinary `go test ./...`. It runs inside
  disposable QEMU/KVM guests (`test/integration/host/run.sh`,
  `test/integration/guest/*.sh` for in-guest provisioning,
  `test/integration/targets/cachyos/` for the one currently supported guest
  image). Subpackages: `control/`, `daemon/`, `lifecycle/`, `transfer/`.
- Test targets are driven through the [Makefile](Makefile): `make test` is the
  host-safe gate (see [AGENTS.md](AGENTS.md) for what it covers and when to
  run it), `make integration-test-compile` compiles the integration tests
  without running them, and `make integration-test` /
  `make integration-package-test` / `make integration-benchmark` are the
  guarded QEMU-guest runs.

## Documentation and packaging

- `docs/` is a standalone VitePress site (published to boomerangz.org),
  versioned independently of the Go module.
- `contrib/` holds the Arch packaging inputs (systemd unit, sysusers/tmpfiles,
  shell completions, man pages, the restricted `boomerangz-shell` login
  shell for SSH-only replication accounts).
- `.goreleaser.yaml` / `.github/workflows/release.yml` build release
  archives; packaging/release is gated on the full CI/integration matrix.

## Where the code has diverged from PLAN.md

`PLAN.md` Section 12 sketches a layout that doesn't match what's on disk today;
treat the plan as historical intent, not a current map:

- The plan's `api/boomerangz/v1/` is `proto/boomerangz/{control,replication}/v1/`
  in the actual tree, split into two proto packages instead of one.
- `internal/model`, `internal/properties`, `internal/scheduler`,
  `internal/snapshot`, and `internal/logging` (all named in the plan) do not
  exist as separate packages. Their responsibilities live elsewhere:
  scheduling logic is `internal/daemon/scheduler.go`; property/policy parsing
  is `internal/policy`; snapshot ownership/naming is `internal/lifecycle`
  (`ownership.go`); logging is wired directly with `log/slog` in
  `internal/cli`/`internal/daemon` rather than through a dedicated package.
- `internal/daemonstate` and `internal/statusui` exist but are not named in
  the plan's package list.
- `internal/testutil/commandtest` (named in the plan) does not exist; only
  `internal/testutil/zfstest` is present.
- The CLI has grown a hidden `ssh-shell` command and `config reload` /
  human-readable `dataset list`/`dataset inspect` output beyond the plan's
  Section 11 initial CLI shape - both are described in the plan itself as
  "post-documentation follow-ups" (Section 7.3, Section 11.1), so the plan and code agree
  here; it's only Section 12's package tree that's stale.

When in doubt about current behavior, prefer reading the code (starting from
`internal/cli/commands.go` for CLI behavior, or the package list above for
everything else) over PLAN.md's structural claims. PLAN.md remains reliable
for the *design intent* behind properties, ownership, and lifecycle
semantics - just not for the file/package layout.
