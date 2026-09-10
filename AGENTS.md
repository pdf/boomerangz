# boomerangz - repository map

`boomerangz` is a property-driven OpenZFS snapshot and send/receive manager,
targeting intermittently connected hosts (laptops/"roadwarriors") as well as
always-on backup targets. Policy lives in `org.boomerangz:*` ZFS user
properties on the managed datasets; TOML config holds only remote definitions
and daemon-wide settings. The full design rationale, property contract, and
delivery-phase history are in [PLAN.md](PLAN.md) - that document is the
original spec and does **not** always match the current implementation (see
"Where the code has diverged from PLAN.md" below).

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
- Test targets are driven through the [Makefile](Makefile):
  `make test` (host-safe: `go test`, race build, vet, golangci-lint, buf
  format/lint/generate diff-check, actionlint, shellcheck),
  `make integration-test-compile` (compiles integration tests without
  running them), `make integration-test` /
  `make integration-package-test` / `make integration-benchmark` (guarded
  QEMU-guest runs).

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
