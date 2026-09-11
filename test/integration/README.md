# Integration suite

Tests that need a real ZFS kernel module: stream formats, resume tokens, holds
and bookmarks, delegation, mount semantics, and process lifecycle. Everything
here runs inside a disposable QEMU guest against pools the harness creates and
destroys.

Run with `make integration-test`. See [AGENTS.md](../../AGENTS.md) for when it
is expected to run and `make integration-test-compile` for the compile-only
check.

## What belongs here

Apply the sizing rule before adding anything: if a fake `zfs.Executor` can
express the behaviour, it belongs in a unit test, not here. A guest boot per
run is the cost, and a test that only asserts what a fake was told to return
proves nothing. `localBackend` in `internal/transfer/local_test.go` is the
established idiom for the unit-test side.

A refusal has to be attributable. Asserting only that an operation returned an
error is satisfied by a system that is broken, misconfigured, or not yet
ready, so pair it with something that discriminates: preview the same request
successfully before planting the condition under test, assert the error text
where the message *is* the behaviour, and check the consequence - that the
dataset really was not created. `TestGuestEncryptedTransfer/key-unavailable`
and `TestGuestAccessControlRefusals` are the models.

## Coverage ledger

One line per behaviour axis, naming what owns it. The axes follow
[ARCHITECTURE.md](../../ARCHITECTURE.md)'s headings, because those are the
axes changes get reviewed against.

**A change that adds, moves, or removes an integration behaviour updates this
ledger in the same change.** It is the answer to "is this tested?", and it is
only worth reading if it is true.

### Snapshot lifecycle and `org.boomerangz:state:*`

| Behaviour | Owned by |
| --- | --- |
| Recursive vs non-recursive snapshot scope | `TestGuestLifecycle/snapshot-scope` |
| Reference protect, checkpoint, release; wrong destination GUID refused | `TestGuestLifecycle/reference-checkpoint` |
| Pruning respects foreign holds and the newest snapshot | `TestGuestLifecycle/prune-respects-holds` |
| Adoption after lineage loss, without disturbing descendants | `TestGuestLifecycle/adopt` |
| Clean clears metadata and preserves snapshots | `TestGuestLifecycle/clean` |
| `dataset adopt` and `dataset clean` over the CLI | `TestGuestLifecycle/cli-adopt-and-clean` (gated on `BOOMERANGZ_LIFECYCLE_GUEST_CLI`) |
| Received-property layering, and which hidden values reach `State.Received` | `TestGuestReceivedPropertyLayers` |
| A root claimed but not yet snapshotted: lineage resumed, foreign claim refused, inherited markers not authority | `TestGuestInterruptedLineageInitialization` |

### Encryption and raw sends

| Behaviour | Owned by |
| --- | --- |
| `raw` defaulted on for a real encryption root; received dataset becomes an encryption root of its own | `TestGuestEncryptedTransfer/raw-forced-for-encryption-root` |
| `replicate=on` refused on an encrypted source without `raw` | `TestGuestEncryptedTransfer/replicate-without-raw-refused` |
| `props=on` without `raw` refused by the planner | `TestGuestEncryptedTransfer/props-without-raw-refused` |
| Raw send succeeds with the key unloaded; the same send without `raw` is refused | `TestGuestEncryptedTransfer/key-unavailable` |
| Data received beneath an encryption root inherits its key | `TestGuestEncryptedTransfer/destination-under-encryption-root` |

The refusals are unit-tested over hand-built `zfs.Dataset` values; what these
add is that ZFS's own `encryptionroot` reaches the guards, so each phase
resolves policy from `ListDatasets` output rather than a literal.

### Property to send/receive mapping

| Behaviour | Owned by |
| --- | --- |
| Full bootstrap to multiple local targets | `TestGuestLocalTransfer/full-bootstrap` |
| `incremental=all` base retention and hold rotation across pruning | `TestGuestLocalTransfer/incremental-all-base-retention` |
| `-i` vs `-I`, foreign intermediates, namespace isolation, receive overrides | `TestGuestLocalTransfer/incremental-modes` |
| Bookmark-based incremental after the source snapshot is pruned | `TestGuestLocalTransfer/bookmark-incremental` |
| Unrelated destination history refused before sending | `TestGuestLocalTransfer/unrelated-destination-refused` |
| Reseed recovery of an otherwise-blocked target | `TestGuestLocalTransfer/reseed-recovery` |
| Recursive `replicate` with `discard=first` and `discard=all` mapping | `TestGuestLocalTransfer/recursive-mapping` |
| Linux `canmount=noauto` ancestor preparation under an ordinary receive root | `TestGuestLocalTransfer/linux-ancestor-preparation` |
| Resume after an interrupted receive, for both incremental modes | `TestGuestInterruptedTransferRecovery` |
| A destination exhausted mid-receive, the source recovery state it retains, and the retry once room returns | `TestGuestDestinationExhaustion` |
| Raw delegated ZFS capability floor: send/recv, `-R`, `-p`, resume tokens, holds, bookmarks | `guest/delegated-matrix.sh` |

### Remote transports

| Behaviour | Owned by |
| --- | --- |
| Full bootstrap over `ssh` direct, `ssh-shell` and `native`, with transport recorded in the target binding | `TestGuestSSHTransfer` |
| Full bootstrap, `-i` vs `-I` with a foreign intermediate, receive overrides and namespace isolation, bookmark base after the source snapshot is pruned, resume after an interrupted receive, unrelated-destination refusal, and reseed recovery through the transport's `zfs.ReseedExecutor` - each run over `local`, `ssh-direct`, `ssh-shell` and `native` | `TestGuestTransportParity/<transport>` |
| Recursive `replicate` with `discard=first` and `discard=all` over each transport: the received subtree, the intermediate receive ancestor prepared remotely, the sibling bases a recursive incremental gathers, and a foreign snapshot on the recursive destination refused | `TestGuestTransportParity/<transport>/recursive-mapping` |

`TestGuestTransportParity` carries `local` as a control arm rather than as
coverage: the local engine already owns those behaviours in the table above, so
running the identical table over the local transport is what makes a remote
failure attributable to the transport instead of to the test.

### Scheduler and daemon

| Behaviour | Owned by |
| --- | --- |
| Scheduling and retirement | `TestGuestDaemonSchedulingAndRetirement` |
| Concurrent siblings, ancestor exclusion, conservative destination setup | `TestGuestLocalTransferConcurrency` |
| Remote outage: `waiting-retry` with reason retained, pending-snapshot coalescing, reconnection, canonical target identity | `TestGuestRemoteOutageReconnection` |
| Daemon-driven native replication through an imported pairing credential | `TestGuestDaemonNativeReplication` |
| `prune:<dataset>` jobs reaching `succeeded`, and destroying what falls outside the grid | `TestGuestDaemonPruneJob` |
| Retry and backoff state sequence, rather than its endpoints | `TestGuestDaemonRemoteBackoff` |

### Control plane and packaging

| Behaviour | Owned by |
| --- | --- |
| Daemon control socket lifecycle | `TestGuestDaemonControl` |
| Recovery after an abrupt restart | `TestGuestDaemonAbruptRestart` |
| Packaged systemd unit start/reload/stop, not enabled by default | `targets/cachyos/run.sh` |
| `pairing create` / `import` / `list` / `revoke` against a real listener | `TestGuestPairingLifecycle` |
| A revoked credential refused by the listener, and refused finally rather than as an outage | `TestGuestPairingLifecycle/revoke` |
| `ssh-shell` operations outside `ssh_shell.replication_roots` refused | `TestGuestAccessControlRefusals/operation-outside-replication-roots` |
| A receive root outside `ssh_shell.replication_roots` refused before anything is created | `TestGuestAccessControlRefusals/receive-root-outside-replication-roots` |
| A missing `zfs allow` grant reported with the account, dataset and permissions | `TestGuestAccessControlRefusals/missing-delegation` |
| `dataset list` status classification from real pool state | `TestGuestDatasetCLI/list` |
| `dataset inspect` provenance: local authority on the source, and receive isolation plus received state on the replica | `TestGuestDatasetCLI/inspect-source`, `TestGuestDatasetCLI/inspect-received` |
| `dataset reseed` resolving a configured target, destroying the replica, and leaving the source sendable | `TestGuestDatasetCLI/reseed` |
| `identity recover` against owner and lineage markers a real snapshot wrote | `TestGuestIdentityRecoverCLI` |
| A daemon refused when another process already holds the control socket | `TestGuestDaemonSocketContention/live-socket-refused` |
| A second daemon under one configuration refused by the lifecycle lock, without displacing the first | `TestGuestDaemonSocketContention/lock-refuses-second-daemon` |
| A killed daemon's stale socket reclaimed by its replacement | `TestGuestDaemonSocketContention/stale-socket-reclaimed` |
| SIGKILL once a snapshot has committed: metadata intact, lineage adopted on restart | `TestGuestDaemonPowerLoss/snapshot-commit` |
| SIGKILL once a bootstrap receive has committed: target blocks for a reseed, and the reseed recovers it | `TestGuestDaemonPowerLoss/receive-commit` |
| A second installation declining a dataset another owns as a scheduling root | `TestGuestDatasetContention` |
| `config check`, `config show` redaction, and the `status` renderer selection | Unit tests - see "Deliberately not here" |

Chunk letters refer to [design/integration-coverage.md](../../design/integration-coverage.md),
which carries the reasoning behind each gap and the plan for closing it.

### Deliberately not here

Three CLI surfaces were triaged into unit tests rather than covered here,
because no pool can contradict them.

| Behaviour | Owned by |
| --- | --- |
| `config check` counts source files and refuses invalid drop-ins | `TestConfigCheckCountsEverySourceFile`, `TestConfigCheckReportsDropInFailures` |
| `config show` merges drop-ins and redacts listener private keys | `TestConfigShowMergesAndRedactsListenerSecrets` |
| `status` renders JSON whenever its output is not a terminal | `TestStatusRendererSelection`, with the rendering itself in `internal/statusui` |

`config check` and `config show` read configuration files and nothing else.
`status`'s terminal rendering cannot be reached from here at all: the selector
turns on `term.IsTerminal`, and every stage gives the command a pipe.

### Not behaviour tests

Two things in these packages own no axis, and their absence from the ledger
above is deliberate rather than a gap. The transfer package's `TestMain`
doubles as a subprocess boundary: with `BOOMERANGZ_INTERRUPTED_RECEIVE_ARGS`
set the binary is the receiver the interrupted-receive test re-executes to
sever a stream mid-flight, and it never reaches the tests.
`BenchmarkGuestRemoteTransfer` measures remote transfer throughput and runs
only under `BOOMERANGZ_INTEGRATION_MODE=benchmark` via
`make integration-benchmark`, which is a separate mode that skips the test
stages entirely.

## How a run is structured

`host/run.sh` provisions the guest and hands off to `targets/<target>/run.sh`,
which installs the packaged assets and calls `guest/run-common.sh`. That runs
the raw capability probe and then each test binary package-wide, with no
name filters: a test added to any package here runs without editing a runner
script.

The `integration` build tag is the gate, and a run ID is an input rather than
an opt-in. A test compiled with the tag fails when the harness has not given
it one, so a stage whose environment wiring broke goes red instead of passing
with everything skipped, and exit status carries the whole signal. Host
protection is unaffected: `zfstest.VerifyGuestPool` checks the guest marker
file, the disk serial and the pool topology before any mutation, and those
layers fail closed. Nothing in the runner needs editing when a test is added,
renamed, moved or deleted.

`BOOMERANGZ_INTEGRATION_STAGES` narrows a run to named stages and
`BOOMERANGZ_INTEGRATION_FILTER` carries a `-test.run` regex into the stages
that remain. Both exist for iteration only: fewer tests ran, and a green
result no longer says which, so a narrowed run says
`PARTIAL RUN - NOT VERIFICATION` at both ends and in each stage summary. See
[AGENTS.md](../../AGENTS.md); an unfiltered run is what "verified" means.

Tests that split into phases pass the running `*testing.T` to every helper
rather than capturing one. A closure that closes over an outer `t` reports a
phase's failure against the whole test, and a `t.Cleanup` registered inside a
phase destroys its fixture when that phase ends rather than when the test
does - both silent, and both invisible in a diff, because moving a line into a
`t.Run` rebinds `t` without changing the token. Register cleanup at the scope
that declares the state. The `chain` helpers are the deliberate exception:
they call `t.Run` and so must use the parent's `t`.

Tests own their fixtures. `zfstest.FixtureName` names a dataset for one test,
`zfstest.PayloadVolume` creates and seeds a zvol on demand, and
`zfstest.RegisterCleanup` destroys the tree afterwards, releasing holds first.
`bootstrap.sh` creates pools and delegates permissions; it builds no fixtures,
so stage order carries no meaning and can be shuffled.

A fixture left behind is not just untidy. The pools live for the whole run, so
an abandoned source root that still carries `enabled=on` and a cadence is work
the next stage's daemon adopts as its own: it discovers every dataset on the
host, and with bounded management workers, one stale root whose transfer is
still running holds a worker for the length of that send. That is how the
transfer stage once starved the first daemon test of a worker for 30 seconds.
Register cleanup for every dataset a test creates, on both pools.
