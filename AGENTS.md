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
