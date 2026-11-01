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
  `ssh-shell` (hidden), `dataset {list,inspect,adopt,reseed,abort-receive,clean}`,
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
  - `clean.go` - explicit, preview-first `dataset clean` planning/execution;
    `forget.go` narrows it to one target (`--forget-target`).
  - `version.go` - the state format marker (`state:version`) and the gate
    that refuses a root in any other format.
  - `service.go` - the facade (`Service`) other packages call.
- **migrate** - `boomerangz migrate`: moves a source root's state to the
  current format one step at a time (`steps` in `migrate.go`, one file per
  step). See "State format versions" below.
- **transfer** - plans and executes replication jobs on top of `lifecycle`
  and `zfs`: `plan.go` (`Build` computes the send/receive plan),
  `local.go` (`Local` executor: `Preview`/`Apply`), `binding.go` (verifies a
  source is bound to the correct destination, not just a same-named one),
  `resume.go` (interrupted-receive resume), `reseed.go` (destructive
  full-resync path, and aborting interrupted receives), `hostlocal.go`
  (refusing a remote that receives into this host's own pools until it is
  validated as local), `recovery.go` (`PendingSet` coalescing and the
  `Roadwarrior` reconnect/retry loop for flaky remotes), `permissions.go`
  (the one spelling of every delegated permission set the preflights check,
  and `DestinationPermissions`, their union for a destination).
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
  (`Status`, the one goroutine that owns job states, progress, and views, and
  delivers every transition to its subscribers: `WatchStatus` and the daemon
  log), `permissions.go` (checks each destination's delegation against
  `transfer.DestinationPermissions` when the daemon first reaches it, and
  reports what is missing as a warning in status and the log).
- **daemonstate** - plain shared data types (`Event`, `ControlSnapshot`,
  `ReloadResult`, ...) with no behavior, used so `daemon` and `control` don't
  depend on each other's internals.
- **control** - the admin/control-plane gRPC server and CLI-facing client:
  `server.go` (`Server`, listener/TLS setup, live `Reload`), `service.go`
  (`StatusService`/`ControlService` implementation), `token.go`/`pairing.go`/
  `pki.go` (token and mTLS-pairing issuance/storage), `client.go`
  (`PairingBundle`, `Client.DialLocal`/`DialBundle` used by the CLI),
  `keepalive.go` (the transport keepalive shared by TCP listeners and every
  pairing-bundle client, native replication included).
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

**Actor roots own their subtrees** ([design/root-actor-scope.md](design/root-actor-scope.md)).
A dataset is an actor root iff it is enabled and no ancestor is; discovery's
`markRoots` decides this and sets `CoveredBy` on every other dataset in the
subtree, `Member` on those the root manages, and `Members` (root first,
parents before children) on the root. Lineage, owner, target bindings,
reference proofs and jobs exist only for roots; members carry no
`state:*` property. Each member keeps its own grid, send options, incremental
mode, receive overrides and targets; `discard` and `replicate` are the root's.
Under `replicate=on` the whole subtree is one `-R` stream on the root's policy.

| Property | Resolves to | ZFS effect |
| --- | --- | --- |
| `enabled` | `Effective.Enabled` | Decides actor roots and member sets in discovery; `lifecycle.ActiveRoot` then requires activation set locally on the root itself. Not a send flag. |
| `policy` (grid) | `Effective.Grid` | Drives which snapshot gets sent (scheduler/pruning), not send flags. |
| `large_blocks` | `Send.LargeBlocks` | `-L` |
| `compressed` | `Send.Compressed` | `-c` |
| `raw` | `Send.Raw` | `-w`; defaults to `on` for encrypted datasets (resolve.go:89-91) |
| `props` | `Send.Props` | `-p`; forced `on` when `replicate=on` (resolve.go:128-130) |
| `replicate` | `Send.Replicate` | `-R` on the root's one stream; otherwise one stream per member. Root only. |
| `incremental` | `all`/`latest` | Chooses `-I` vs `-i` in `plan.Build` (plan.go:431-435) |
| `discard` | `off`/`first`/`all` | Maps the root's destination (`Request.mapped`); every stream then receives exactly into its root-relative path beneath it. Root only. |
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
- **`discard` changes the destination path, not just a flag**. It maps the
  root (`first` strips the source's leading component, `all` keeps only the
  last); each member lands at its path relative to the root beneath that.
  The planner computes every destination and receives into it exactly
  (`ReceiveExact`), never passing `-d`/`-e`: receive deriving a member's
  destination from its own name would put it beside the root's replica
  under `discard=all`. `plan.Build` re-derives the mapping on every plan
  rather than trusting a cached value, since a wrong mapping can misroute an
  entire dataset tree.
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
| `state:version` | `Service.CreateSnapshot` and `Service.AdoptDataset`, with the owner and lineage; `boomerangz migrate` | The format the root's state is written in, as a decimal integer ([internal/lifecycle/version.go](internal/lifecycle/version.go)). A format version, not a release. A root with state and no marker is format v1, which predates it. |
| `state:owner` | `lifecycle.RootAuthority` callers (adoption, first management) | Installation UUID that owns a source root ([internal/lifecycle/authority.go](internal/lifecycle/authority.go), `OwnerProperty`). |
| `state:lineage` | Same as above | Per-root lineage UUID, shared by the root's owned snapshots ([internal/lifecycle/ownership.go](internal/lifecycle/ownership.go), `LineageProperty`). |
| `state:snapshot` | `lifecycle.Metadata.Properties()` at snapshot creation | Per-snapshot UUID embedded on the snapshot itself, not the dataset. The creation time is not stored: the snapshot name encodes it to the nanosecond, and `Ownership` reads it from there. |
| `state:reference:<target-id>:<stream-id>:<snapshot-uuid>` | `Service.ProtectSet` | Recovery proof for one stream's endpoint on one target: the stream's own snapshot, plus its descendants' for a `-R` stream, each held and later bookmarked ([internal/lifecycle/reference.go](internal/lifecycle/reference.go)). One per member stream, so members at different instants advance independently. The value is fixed-size JSON `{stream, created, recursive}`: the stream relative to the root (omitted for the root's own), the snapshot's creation time (bookmarks carry only the UUID, so the snapshot name cannot be rebuilt without it), and whether the stream carries its subtree. Everything else is derived: the lineage is the root's, the target ID and snapshot UUID are in the key, and the endpoint's snapshots and GUIDs are read from ZFS by `Reference.Sources`. |
| `state:target:<target-id>` | `transfer` binding logic | A source's persistent identity for one configured destination ([internal/transfer/binding.go](internal/transfer/binding.go)). The value is JSON `{pool_guid, mapped, anchor_guid, anchor}`, in the root's format with no version of its own: `mapped` is the mapped dataset the binding was made for, and `anchor` is stored only while bootstrapping, when it names the mapped dataset's nearest existing ancestor; after the first receive the binding moves to the replica, which `mapped` already names. The rest of the in-memory `TargetBinding` is what configuration says about the target and is rebuilt from it: the key hashes the canonical target, which also encodes the transport and destination root, and the pool and relative path follow from the anchor. |
| `state:target:<target-id>:suspended` | `transfer.SetTargetSuspended` | Marks a target unverified/suspended pending adoption revalidation. It stays separate from the binding because adoption suspends a remote it cannot reach, which may have no binding yet. |
| `state:inactive` | `Service.ReconcileInactive` | JSON `InactiveMarker` recording when an owned root first went inactive, for delayed retirement ([internal/lifecycle/inactive.go](internal/lifecycle/inactive.go)). |
| `state:placeholder` | `zfs.Direct.CreateReceiveParent` (`on`); every `zfs receive` (`off`) | Destination-only. `on` marks a receive parent boomerangz created for a hole and has not yet replaced ([internal/zfs/state.go](internal/zfs/state.go)); `receiveArgs` ([internal/zfs/stream.go](internal/zfs/stream.go)) passes `-o ...=off` on every receive, so a replica and a recursive stream's descendants never inherit a placeholder ancestor's marker. Only a local `on` on a dataset with no snapshot, bookmark or resume token counts (`isPlaceholder`, [internal/transfer/binding.go](internal/transfer/binding.go)), so a replica received before the `off` was introduced is still read correctly. |

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
- **Per-snapshot metadata (`state:lineage`, `state:snapshot`, and the
  creation time in the snapshot name) proves ownership of one exact
  snapshot**, independent of the root markers. `Ownership` (ownership.go)
  requires both properties to be explicitly stored *on the snapshot object
  itself* (`local` or `received`
  source - a received snapshot's own metadata is trusted once its parent
  root's authority checks out), matching lineage, and requires the snapshot
  name to be the exact deterministic encoding of that metadata
  (`Metadata.Name()`). A `boomerangz-`-prefixed name alone proves nothing;
  names and metadata must agree, or the snapshot is treated as foreign.
  Because the creation time is read from the name and the name's UUID
  suffix is only its first eight characters, renaming an owned snapshot to
  another timestamp with the same suffix is not detected: the snapshot
  keeps its ownership and is aged, for retention, from the new timestamp.
  This was accepted in exchange for not storing the time twice; do not
  rename boomerangz snapshots.
- **Reference proofs (`state:reference:*`) gate hold/bookmark lifecycle,
  not the transfer itself.** A proof records the hold intent, not the holds:
  `Reference.Sources` derives the endpoint's snapshots from ZFS as the stream
  (and, for a recursive proof, each descendant) whose `<ds>@<name>` carries
  hold `boomerangz-<target-id>`, or, once that hold is released, whose
  bookmark `<ds>#boomerangz-<target-id>-<uuid>` remains. A source's GUID is
  its snapshot's, or its bookmark's once the snapshot is pruned (a bookmark
  keeps its snapshot's GUID, as does the destination's copy); where both
  exist they must agree. So the proof is the same size however many datasets
  a `-R` stream carries, and `Direct.SetProperties` and `ProtectSet` still
  refuse any value over `zfs.MaxUserPropertyValue` as a backstop. The cost,
  accepted, is that a hold or bookmark forged with the exact tag or name
  under a proved stream is taken as part of its endpoint. `Protect`/`ProtectSet`
  write the proof *before* placing the corresponding hold, so an
  interruption between the two leaves a reconstructable, idempotent retry
  rather than an orphaned hold, and a target hold no proof stands behind is
  refused. `Checkpoint`/`CheckpointSet` only advance a bookmark after the
  caller supplies a verified destination GUID for every member not yet
  bookmarked - the property never claims verification the daemon hasn't
  itself confirmed via a completed receive. `ReleaseReference` is the only
  path that removes a reference proof, and only after re-confirming the
  hold/bookmark GUIDs still match; it inherits (clears) the property last,
  after the ZFS-side hold and bookmark are already gone. Retirement verifies
  each destination replica against the source GUIDs `Sources` derives, and
  clean and retirement map a proof's target ID back to a configured target by
  hashing each configured canonical target; a proof whose target is no
  longer configured cannot be checked, and blocks clean until it is, or
  until `dataset clean --forget-target` lets go of that one target
  (`lifecycle.forgetPlan`): it removes the target's proofs, the holds and
  bookmarks they record, its binding and its suspension marker, unchecked,
  and nothing else.
- **Target bindings (`state:target:*`) pin a source to a specific physical
  destination, not just a name.** A binding stores the pool GUID and the
  anchor dataset's GUID; every planned transfer rebuilds the binding from
  configuration and the live identity of its anchor and compares the two by
  struct equality (`transfer.plan.Build`), and a same-named destination
  with a different GUID is blocked rather than silently treated as the same
  target - the operator must explicitly reseed or adopt instead. The
  binding also stores the mapped dataset it was made for, because
  configuration alone cannot say where the replica is once the mapping
  changes: a new `discard` or destination root recomputes the mapped
  dataset, and a binding made for another one fails with
  `transfer.MappingChangedError` in transfer, resume, reseed, inspection and
  live target checks alike, during bootstrap as well as afterwards. The
  operator restores the old mapping, or forgets the target with
  `dataset clean --forget-target` so the next transfer seeds the new one.
  Discovery's overlap detection finds a root's
  binding for a configured target by hashing that target's canonical
  identity (`discovery.RemoteDestination.Canonical` for remotes).
- **On a source root, a received copy of internal state is provenance, not
  authority.** A replica received with `-R` or `props` keeps the sending
  installation's owner, inactive marker, target bindings, suspension markers
  and reference proofs, as received values or, once `receive -x` or a local
  value masks them, as hidden received values that ZFS keeps whatever the
  receive asks. Promoting such a replica with `dataset adopt` (PLAN.md 5.3)
  sets a local owner and lineage over them, and every reader takes only the
  local value and ignores the received one (`RootAuthority`,
  `storedTargetBinding`, `TargetSuspended`, `decodeInactiveMarker`,
  `referenceRecords`, `ProtectSet`). The exception is the lineage: a
  promoted replica continues its source's history, so a hidden received
  lineage that disagrees with the local one is refused (`RootAuthority`,
  `storedLineage`).
- **Discovery reads only a narrow, fixed slice of this namespace globally.**
  `GetLifecycleProperties` ([internal/zfs/direct.go:186-197](internal/zfs/direct.go#L186-L197))
  fetches only `state:owner`, `state:lineage`, and `state:inactive` across
  every dataset at startup, to reconstruct responsibility without
  enumerating arbitrary user properties; reference and target-binding keys
  are dynamic (per target/snapshot) and are only read per-dataset via
  `GetStoredProperties`'s `all`-property query when a specific dataset is
  already in scope for other reasons.
- **State is read only in the format its root says it is in.**
  `RootAuthority` refuses a root whose `state:version` is not
  `lifecycle.CurrentVersion`, after the owner and lineage checks, so every
  snapshot, prune, transfer and retirement stops on an unmigrated root with
  an error naming `boomerangz migrate`; clean and `--forget-target` block on
  one that keeps records, and adoption refuses to stamp the current format
  over records in an older one. See "State format versions" below.
- **Clean and retirement inherit (never destroy) these properties as the
  final step**, after the underlying holds/bookmarks/snapshots they
  describe have already been resolved (`backend.InheritProperty` in
  clean.go:336, inactive.go:207) - the property is a proof *about* ZFS
  state, so it is only cleared once nothing depends on being able to
  reconstruct that proof again.

### State format versions (`internal/migrate`)

The `state:*` records are versioned per source root by `state:version`, not
per record. A format version changes only when what boomerangz stores does,
and is unrelated to release numbers. Format v1 is everything written before
the marker existed; v2 has fixed-size proofs and slim bindings.

`boomerangz migrate [DATASET...|--all]` moves roots to the current format.
It previews by default, and `--apply` needs the daemon stopped, takes the
lifecycle lock, re-plans and compares, and writes each property only while it
still holds the value the plan read. Each step (`step` in `migrate.go`)
decodes one format and encodes the next with its own frozen copies of both,
never the live encoders, which would write a later format once there is one
and stamp it as the step's. The runner alone checks the end of the chain
against the live decoders. A step leaves records already in its target
format alone and the marker is written last, so an interrupted run leaves
the root in its old format and a repeated run finishes it. Steps are kept
indefinitely. `--all` selects every dataset with a local owner, proof or
binding; a member or an unmanaged dataset has no format and is never given
one.

The v1 to v2 step rewrites each proof from its listed sources to
`{stream, created, recursive}`, checking every listed GUID against the
snapshot and bookmark that still exist, and each binding to the slim form.
Keys, holds and bookmark names are the same in both formats, so nothing
else changes. v1 snapshots also carry `state:created`, which v2 never reads;
it is left to age out with the snapshots. `internal/migrate/testdata` holds
v1 state as main's own code wrote it, and `TestGuestStateMigration` migrates
a real root and replicates on from it incrementally.

## Scheduler behavior (`internal/daemon/scheduler.go`)

`Scheduler` tracks one `deadline{policy, members, order, replicate, pending}`
per schedulable root, with a `memberDeadline{grid, cadence, next}` for each
member, independent of `discovery`'s periodic scans. A root's deadline is its
earliest member's; a fired root is due for exactly the members whose own
deadlines have passed. A replication root has one timed member, itself, and
is due for its whole subtree whenever it fires. It is a plain
mutex-guarded map plus a broadcast `wake` channel (closed and replaced on
every mutation - `signal()` - to release whichever goroutine is blocked in
`Next`).

- **Schedulability** (`isSchedulable`): a discovery entry qualifies only if
  it is an actor root (`CoveredBy == ""`), was actually inspected, has policy
  `Enabled` and `Valid()` (no parse errors), and passes
  `lifecycle.ActiveRoot`; `Update` then drops a root none of whose members
  has a positive `Cadence()`. Anything else is silently excluded from
  scheduling, not errored.
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
- **`Complete(dataset, due, completed, deferred)`**: called after a
  snapshot actually lands. Sets `pending = false`, advances each due member
  to `completed + cadence`, and moves each deferred member to the deadline
  its newest snapshot already sets - deliberately anchored to actual
  completion time, not the
  original deadline, so a run that starts late doesn't cause the next one
  to fire early, and so missed periods (daemon was down, dataset was
  blocked) are coalesced into a single catch-up snapshot rather than
  replayed as a backlog burst.
- **`Retry(dataset, notBefore)`**: called on every non-success path out of
  `enqueueSnapshot` (gate admission failure, job-queue drop, `CreateSnapshot`
  error) to clear `pending` and push members already due out to a backoff
  time, leaving members due later alone, usually `now + ReconcileInterval` or `now + 1s` for
  queue drops. This is the mechanism that turns a single missed attempt
  back into a live, re-selectable entry instead of a stuck root.
- **`Lookup(dataset)`** (used by `Runtime.Trigger`) returns a `Schedule`
  with every member due, without touching `pending`, so manual triggers and the
  timer-driven path can both produce a `Schedule` for the same dataset
  concurrently; `Trigger` sets `Schedule.Force = true` on the result.
  `Force` bypasses the deadline re-check in `enqueueSnapshot` (send the
  snapshot immediately even if the stored/observed next-due time is in the
  future) and suppresses the automatic `Retry` call on gate/queue failure,
  since a manual trigger's caller is expected to retry deliberately rather
  than have the scheduler silently reschedule it.
- **Double-checked deadline at execution time**: `enqueueSnapshot`
  re-derives each due member's true next-due time from ZFS state itself via
  `satisfiedMembers` (that member's latest owned snapshot's creation time +
  its own cadence) and leaves out, deferring, every member that still isn't
  due; if none is, it creates nothing and reports `scheduled`. This
  makes the in-memory `Scheduler` an optimization/wakeup mechanism rather
  than the source of truth - ZFS-recorded snapshot ownership is what
  actually gates snapshot creation, so drift between the scheduler's `next`
  and reality (e.g. after a daemon restart, before `Update` has run) fails
  closed into a reschedule rather than a duplicate/early snapshot.
- **Concurrency model**: all scheduler state is behind a single
  `sync.Mutex`; there's no per-dataset locking. `Next`'s O(n) full-map scan
  on every wake is intentional simplicity over an ordered heap - the entry
  count is bounded by managed dataset roots, not by scan/event volume.
- **Snapshot and prune jobs run alongside transfers.** They take the root's
  lock, and a transfer takes its target's, so retention and snapshotting carry
  on through a long send. A snapshot or prune landing while a transfer
  prepares moves that stream's history, and `transfer.Local` plans the stream
  again inside the same attempt (`applySettled`, at most
  `preparationAttempts` times) rather than failing it with
  `PreparationChangedError`; a snapshot landing after preparation is the next
  run's, through the job's dirty flag. Only a stream's endpoints and base are
  held, so a prune after preparation may still destroy a `send -I`
  intermediate before it is sent; verification then expects only the
  intermediates the source still has (`settleIntermediates`).

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
- Section 5's per-snapshot metadata lists `state:created`. It is no longer
  stored: the snapshot name already encodes the creation time, and ownership
  requires the name to match the metadata anyway.
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
