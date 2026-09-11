# Design: integration test coverage review

Status: in progress. Chunks 0-3 and A-F have landed; G is outstanding.

This document is a work plan for auditing `test/integration/` and closing the
gaps it finds. It is written to be executed in independent chunks: chunk 0-3
are harness repairs that must land first because they change what "the suite
passed" means, and chunks A-F are behaviour coverage that can be picked up in
any order, one per sitting.

## 1. What the suite is today

One QEMU/KVM guest boot per run. `test/integration/host/run.sh` downloads and
provisions a CachyOS image, attaches two 2G qcow2 disks with run-derived
serials, cross-compiles four test binaries plus `boomerangz`, copies them in,
and runs `targets/cachyos/run.sh`, which installs the service assets and hands
off to `guest/run-common.sh`. Everything runs as the `boomerangz` service
account under `zfs allow` delegation. Teardown destroys both pools.

`guest/run-common.sh` runs, in fixed order:

| Stage | What it is |
| --- | --- |
| `delegated-matrix.sh` | Raw `zfs` capability probe, asserts its own results |
| `property-layers.sh` | Received-property inheritance probe, asserts nothing |
| `lifecycle.test` | `TestGuestLifecycle` |
| `transfer.test` | `TestGuestLocalTransfer`, `TestGuestInterruptedTransferRecovery` |
| `transfer.test` | `TestGuestSSHTransfer` |
| `daemon.test` | `TestGuestDaemonSchedulingAndRetirement`, `TestGuestLocalTransferConcurrency` |
| `control.test` | `TestGuestDaemonControl`, `TestGuestDaemonAbruptRestart` |

`targets/cachyos/run.sh` then exercises the packaged systemd unit
(start/reload/stop, not-enabled-by-default).

## 2. Structural defects found in the audit

These are findings, not speculation; each was confirmed by reading the tree.

**2.1 A test exists that nothing ever runs.**
`TestGuestRemoteOutageReconnection` ([daemon/guest_test.go:367](../test/integration/daemon/guest_test.go))
is 140 lines covering remote transport failure, `waiting-retry` state
retention, pending-snapshot hold coalescing across an outage, and canonical
target identity after reconnection. No runner names it. It also prints
`BOOMERANGZ_REMOTE_OUTAGE_OBSERVED` to stdout as a handshake for a harness step
that would sever and restore the link; that step does not exist, and no
`BOOMERANGZ_REMOTE_GUEST_*` variables are exported for `daemon.test`. As
written it would `t.Fatal` on the missing environment even if invoked.

**2.2 The runner uses exact-name allowlists.**
Every stage is `-test.run '^TestGuest(A|B)$'`. A new test added to a package is
silently not run, and its absence is indistinguishable from a pass. This is the
mechanism that produced 2.1 and will produce the next one.

**2.3 `property-layers.sh` asserts nothing.**
It runs a sequence of `zfs get` calls through `printf`/`show` and ends with
`property_layer_probe=complete`. The output is teed into
`diagnostics/test-output.txt` on the host and never read by anything. It cannot
fail on a behaviour change; it can only fail if a `zfs` command errors.

**2.4 Fixtures are shared and mutated across suites.**
`$src/data/payload` (a 256M zvol seeded with 32M of random data) is the source
for `TestGuestLocalTransfer`, `TestGuestInterruptedTransferRecovery` and
`TestGuestSSHTransfer`. Each of them `zfs set`s the same `org.boomerangz:*`
properties on it, and `TestGuestLocalTransfer` additionally destroys snapshots
and leaves target bindings behind. The suites are separate binaries run in a
fixed sequence, so this is order-dependent coupling that no single test
declares. There is no per-test cleanup; the only cleanup is the whole-pool
destroy at the end of the run.

**2.5 The large tests are sequential monoliths.**
`TestGuestLocalTransfer` is ~290 lines covering, in one function body: full
bootstrap to two targets, incremental-all base retention across pruning, hold
rotation, `-i` vs `-I` semantics, foreign intermediate snapshots, namespace
isolation, receive overrides, bookmark-based incrementals, unrelated
destination refusal, reseed recovery, recursive `discard=first|all` mapping,
and Linux `canmount=noauto` ancestor preparation. A failure in the retention
phase means the eleven behaviours after it are never evaluated, and the
guest-run cost means you learn about them one boot at a time. Only
`TestGuestInterruptedTransferRecovery` uses `t.Run`.

**2.6 There is no coverage ledger.**
Nothing in the repo states which behaviours the integration suite is supposed
to own. `ARCHITECTURE.md` section "Tests" lists the mechanism, not the
contract. So "is this tested?" is currently answered by reading 1700 lines.

## 3. Behaviour gaps

Read against the `ARCHITECTURE.md` section headings, which are the right axes
because they are the same axes later changes are reviewed against.

**Encryption and raw sends.** `policy.Resolve` has substantial
encryption-specific logic - `raw` forced on for encrypted sources
([resolve.go:242](../internal/policy/resolve.go)), `replicate` refused on an
encrypted source without `raw` ([resolve.go:137](../internal/policy/resolve.go)),
`encryption`/`keyformat`/`pbkdf2iters` handling at receive
([resolve.go:216](../internal/policy/resolve.go)), and a `props`-without-`raw`
guard in the planner ([plan.go:251](../internal/transfer/plan.go)). The guest
bootstrap creates no encrypted dataset. None of this has ever met a real
encryption root, a real key, or a real raw stream.

**Remote transport parity.** `TestGuestSSHTransfer` performs exactly one full
bootstrap per mode (`direct`, `ssh-shell`, `native`) to a fresh root, and
asserts `Verified` plus the transport in the target binding. Everything the
local engine is tested for - incremental `-i`/`-I`, bookmark bases, resume
after an interrupted receive, reseed, recursive mapping, receive overrides,
foreign-snapshot refusal - is untested over any remote transport, even though
those paths go through a different `Executor` and `Stream` implementation.

Chunk B closed all of that except recursive `replicate`, which is a large
enough shape of its own to be chunk G rather than a phase appended to work
already landed.

**Pairing and native credentials end to end.** `runNative` builds a pairing
bundle in-process via `control.CreateListenerPairing` and calls
`replicationnative.Open` directly. The `pairing create` / `pairing import` /
`pairing list` / `pairing revoke` CLI path, and the daemon resolving a
`transport = "native"` remote through an imported credential
([validate.go:61](../internal/config/validate.go)), are never exercised
against a real listener. Revocation in particular has no end-to-end proof.

**Daemon-driven remote replication.** With 2.1 unfixed, no daemon test drives
any remote at all. Retry/backoff, `waiting-retry` and `blocked` state
transitions, and reason retention are asserted only by unit tests.

**Pruning as a daemon job.** `runtime.go:751` emits a `prune:<dataset>` job.
No integration test observes one. Pruning is only tested by calling
`lifecycle.Service.Prune` directly.

**Negative and adversarial paths.** The suite is almost entirely happy-path.
Not covered anywhere against real ZFS: a destination that runs out of space
mid-receive; `ssh-shell` rejecting a command outside its allowed set; a
replication root outside `ssh_shell.replication_roots`; a delegation permission
that is missing rather than granted; two daemons contending for the same
socket or the same dataset.

**CLI surface.** `dataset adopt` and `dataset clean` are covered (in the
optional tail of `TestGuestLifecycle`, gated on
`BOOMERANGZ_LIFECYCLE_GUEST_CLI`). `dataset list`, `dataset inspect`,
`dataset reseed`, `identity recover`, `config check`, `config show` and
`status`'s non-JSON rendering have no integration coverage. Some of these are
adequately served by unit tests over the `zfs.Executor` seam and should stay
there; the ones that read real pool state are not.

## 4. Sizing rule

Before adding anything to `test/integration/`, apply the rule already in
`CLAUDE.md`: if a fake `zfs.Executor` can express the behaviour, it belongs in
a unit test. Integration exists for behaviour that only a real kernel module
produces - stream formats, resume tokens, holds and bookmarks, delegation,
encryption roots, mount semantics, and process lifecycle. Several items in
section 3 will, on inspection, turn out to be unit-test work; recording that
decision is a valid outcome for a chunk.

## 5. Work chunks

Each chunk is one sitting and lands independently. Chunks 0-3 first.

### Chunk 0 - make the runner honest (done)

- Replace the `-test.run '^TestGuest(A|B)$'` allowlists in
  `guest/run-common.sh` with package-wide runs, keeping the existing
  `os.Getenv(...) == "" -> t.Skip` guards as the only opt-in mechanism.
- Export the `BOOMERANGZ_REMOTE_GUEST_*` variables to the `daemon.test` stage
  so `TestGuestRemoteOutageReconnection` can run, and implement the outage
  injection its handshake expects (sever the loopback SSH path, wait for
  `BOOMERANGZ_REMOTE_OUTAGE_OBSERVED` on stdout, restore it). If that proves
  impractical inside the guest, delete the test and say so here rather than
  leaving it dormant.
- Add a post-run assertion that each stage's `-test.v` output contains at
  least the expected number of `--- PASS` lines, so a silently skipped test
  fails the run.

Done when: a test added to any integration package runs without editing a
runner script, and deliberately breaking one causes a red run.

**How it landed.** Every stage is now `run_stage <name> <min-passes> ENV=...
<binary>`, with no `-test.run`; the environment guards are the only opt-in.
The benchmark stage keeps its `-test.run '^$'` because it is deliberately
excluding tests rather than selecting them. `run_stage` fails when a stage
reports fewer top-level `--- PASS:` lines than its floor, and prints the
stage's `--- SKIP:` lines when it does, so a test that goes dormant is named
rather than merely missed. The floors are lower bounds, so adding a test is
still a one-line change; only removing or silently skipping one is loud.

The outage is injected as a second `sshd` on port 2222 that does not exist
until the handshake fires. The daemon's remote is configured against that
port, so its first attempts get a connection refusal - a real transport
failure, not a stubbed one - and `restore_outage_link` starts the listener
when `stage_filter` sees `BOOMERANGZ_REMOTE_OUTAGE_OBSERVED` on the stage's
stdout. The link therefore starts down and comes up, rather than being severed
and restored; the test asserts the same thing either way, and this needs no
firewall manipulation or extra package in the guest. Stage output is streamed
through `stage_filter` rather than buffered, because the test is still blocked
waiting for the link when the handshake line is read.

Two consequences worth knowing before the next chunk. The `known_hosts` entry
for `[127.0.0.1]:2222` is seeded from a keyscan of port 22 (same host key)
*after* the remote transfer stage, because `TestGuestSSHTransfer` overwrites
`known_hosts` with the port 22 entry alone - that is an instance of the
fixture coupling chunk 1 exists to remove, and the seeding should move into
the test once it does. And the outage remote's root is spelled
`boomerangz-test-<run-id>-dst/data/outage`, recomputed in the runner from the
run ID rather than read back from `bootstrap.sh`.

**Verified by a real run.** `make integration-test` is green: nine top-level
passes across the five stages, no failures, packaged unit checks green.
`TestGuestRemoteOutageReconnection` passed in 72s - a real connection refusal,
`waiting-retry` with its reason retained, coalescing across the outage, the
handshake, and canonical target identity after reconnection. The env guards
carried the opt-in exactly as intended: `TestGuestSSHTransfer` skipped itself
in the local stage and the local tests skipped in the remote stage, with both
stages still meeting their floors.

The first run failed, and usefully. `run_stage` executes as the invoking user
while `$artifact_dir` is chowned to the service account, so `tee` could not
create the stage log; under `pipefail` every stage exited 1 while its tests
reported `PASS`. Both `make test` and `make integration-test-compile` were
green against a runner on which every stage would have failed - the pass-floor
mechanism added here to catch dormant tests was itself dead on arrival, and
only a guest boot could show it. The stage log now goes to an invoking-user
`mktemp` file.

### Chunk 1 - fixture isolation (done)

- Give each test its own source tree under `$src/data/<test>-<suffix>` and its
  own destination roots. Move payload creation out of `bootstrap.sh` into a
  `zfstest` helper so a test that needs bulk data asks for it.
- Add `t.Cleanup` that destroys the per-test tree, keeping the guarded
  pool-level teardown as the backstop.

Done when: the three suites that share `$src/data/payload` no longer do, and
the stage order in `run-common.sh` can be shuffled without failures.

Chunk 0's run gave this a concrete symptom to fix. `TestGuestSSHTransfer`
sets `org.boomerangz:remote=home` on `$src/data/payload` and never clears it,
so the daemon stage - which now drives a remote, and did not before - picked
up a second job, `remote:<src>/data/payload:home`, and pointed it at the
outage remote. It did not break the assertions, because the outage test
watches its own job, but it contends for the transfer workers against that
test's three-minute success deadline. The `known_hosts` sequencing noted under
chunk 0 is the same defect seen from the other side. Both should go away when
each test owns its source tree.

**How it landed.** `bootstrap.sh` no longer builds a payload. Tests ask for
one through `zfstest.PayloadVolume`, which creates a sparse zvol under a name
from `zfstest.FixtureName`, seeds it, and registers a `t.Cleanup` that
releases boomerangz's snapshot holds before `zfs destroy -R` - held snapshots
refuse destruction regardless of `-f`, so the release pass is required rather
than defensive. Cleanup failure logs instead of failing the test, leaving the
guarded pool teardown as the backstop.

Two consequences the change forced. Creating a sparse zvol under delegation
needs `refreservation`, `volblocksize` and `volsize`, which bootstrap.sh had
never granted because it did that work as root; and seeding writes to a
`/dev/zvol` node that udev creates `root:disk`, so the service account needs
that group. Both are fixture-only and commented as such.

`delegated-matrix.sh` turned out to be a fourth consumer of the shared
payload, and the one grep does not find: it never names the dataset, it
recursively sends `$src/data` and asserts a volume child arrives, then
truncates that same stream at 4M to force a resumable receive. It needed the
payload's bulk, not just its existence. It now creates, seeds and destroys its
own - which also makes it the first thing in the run to exercise the delegated
volume permissions, so that question fails fast rather than three stages in.

`known_hosts` was a shared mutable fixture too, and the reason chunk 0 had to
sequence its seeding after the remote transfer stage: `TestGuestSSHTransfer`
truncated the file. It now appends, and the runner seeds both host key entries
before any stage runs.

Verified two ways. A normal-order run is green with the payload bleed gone -
zero `data/payload` jobs in the daemon stage, only the outage test's own, and
no cleanup warnings. Then a run with the five stages fully reversed - control,
daemon, transfer-remote, transfer-local, lifecycle - is also green, which is
the done-when above and would have failed on host key verification before the
`known_hosts` fix.

Chunk 3 finished the isolation: `$src/data/child` moved into
`delegated-matrix.sh` as `matrix-child` when `property-layers.sh` was folded
into a Go test, so `bootstrap.sh` now creates pools and delegates permissions
and builds no fixtures at all.

### Chunk 2 - split the monoliths (done)

Convert `TestGuestLocalTransfer` and `TestGuestLifecycle` into `t.Run`
subtests along the phase boundaries their existing comments already mark.
Where a phase genuinely depends on the previous one's state, keep them in one
subtest and say why in a comment. No behaviour changes.

Done when: one failing phase reports as one failing subtest and the rest still
report their own results.

**How it landed.** `TestGuestLocalTransfer` became eight phases,
`TestGuestLifecycle` six, plus two nested cases under `recursive-mapping`.

The dependent phases took a different shape than the plan suggested. Rather
than merging them into one subtest, a small `chain` helper runs a dependent
phase only while its predecessors have passed and marks the remainder
*skipped*. That keeps each phase individually named and timed, and a failure
then reports one red phase plus an explicit list of what was not evaluated,
instead of either one coarse subtest or a cascade of reds that all trace to
one cause. Phases that build their own fixtures use `t.Run` directly and
report whatever the chain did:
`incremental-all-base-retention`, `recursive-mapping` and
`linux-ancestor-preparation` in the transfer test. The lifecycle test walks a
single dataset through its whole lifecycle, so all six of its phases are
chained.

The split forced one fix without which it would have been theatre: `command`
and `request` closed over the parent's `*testing.T`, so a failure inside
either would have been reported against the whole test rather than the phase
that caused it - the same defect chunk 3's first attempt hit. Both now take
the running `*testing.T`.

No behaviour change, checked rather than asserted: the counts of `t.Fatal`
(53 and 36) and of every `engine`, `snapshots`, `direct` and `reseed` call are
identical before and after.

**Verified against the done-when, not just a green run.** A guest run is green
with all fourteen phases reporting individually. Then a deliberate `t.Fatal`
in `incremental-modes` - a chained phase in the middle - produced exactly the
intended report: `full-bootstrap` passed, `incremental-modes` failed alone,
its three dependents skipped naming the reason, the three independent phases
still passed on their own merits, and the stage failed its pass floor. The
skip path had never executed before that run, which given how chunk 0's floors
and chunk 3's assertions each turned out to be dead on arrival was worth an
extra boot to establish.

### Chunk 3 - the property-layers probe (done)

Decide what `property-layers.sh` is for. Either give it assertions on the
`received`/`source` columns it prints - which is the behaviour
`org.boomerangz:props` handling depends on - or delete it and fold the
question into a Go test. A probe nobody reads is worse than no probe.

**How it landed.** Folded into `TestGuestReceivedPropertyLayers` in the
transfer package; the script is gone. It stays integration work under the
section 4 rule - these are kernel semantics, and a fake `zfs.Executor` would
only assert what it was told to return - but as a script it sat outside
everything chunk 0 built: no pass floor counted it, and its output went to a
diagnostics file nothing read. As a Go test it is picked up by the
package-wide run, counted by the transfer-local floor (2 to 3), owns its
fixtures, and reports each of its seven cases as its own subtest.

The assertions came from reading the columns four recorded runs actually
produced, then tracing why boomerangz cares, and the trace changed the test.
The contract has two halves. `zfs get all` omits a user property with no
effective value, but naming that property explicitly still returns it - which
is why `InspectState` queries twice, once for `all` and once for the three
fixed ownership keys, and why `State.Received` exists apart from
`State.Properties`. So a hidden received value behaves differently depending
on whether boomerangz names the key: `org.boomerangz:state:snapshot` survives
into `State.Received` once hidden, while a property nothing enumerates leaves
the inventory entirely, despite identical `value`/`received`/`source` columns.
The test asserts both, which is what lets boomerangz tell "no value" apart
from "a received value I must resolve".

The first attempt failed for two reasons worth recording. The assertion helper
closed over the parent `t`, so failures reported against the parent while every
subtest printed PASS - the exact miscount chunk 2 exists to prevent, reached
from the other direction. And the expectation itself was wrong: it used
property names nothing enumerates and asserted they would reach
`State.Received`.

**Open question, not a finding.** `targetBindingPrefix` is
`org.boomerangz:state:target:<canonical>`, a dynamic key, so it is not among
the three names `InspectState` queries explicitly. If `all` cannot list a
hidden value, then a hidden received target binding may never reach
`State.Received`, which would make the guards at
[binding.go:191](../internal/transfer/binding.go) and
[binding.go:253](../internal/transfer/binding.go) unreachable for the case
their message describes. This has not been verified and may be wrong; the
visible-received path those functions reject earlier may be the only one that
matters. Worth settling in chunk D or F with a test that plants such a binding
and looks.

### Chunk A - encryption and raw sends (done)

Add an encrypted source dataset to the fixtures and cover: raw send of an
encryption root; `replicate=on` refused on an encrypted source without `raw`;
`props=on` with an encrypted source; receiving under a destination that is
itself an encryption root; and key-unavailable behaviour. This is the largest
single gap.

**How it landed.** Split by the section 4 rule rather than written wholesale
as integration work. The policy refusals were already unit-tested, but
`plan.go`'s `props`-without-`raw` guard had no test at all - every dataset in
`plan_test.go` is `EncryptionRoot: "-"` - and it is pure logic over a struct,
so it went to `TestBuildRefusesEncryptedPropertyStreamWithoutRaw` in the unit
suite, which also asserts the same source *with* raw plans cleanly so the
refusal is attributable to the combination rather than to encryption.

The integration half is `TestGuestEncryptedTransfer`, five phases against a
real encryption root, a real passphrase key and real raw streams. Its value is
not re-testing the refusals: it is that every one of those guards keys off
`Dataset.EncryptionRoot`, which the unit tests hand-construct. Each phase
therefore resolves policy from `ListDatasets` output, so a run proves the
pool's own `encryptionroot` reaches the logic. Creating an encryption root
under delegation needed `encryption`, `keyformat`, `keylocation`,
`pbkdf2iters`, `load-key` and `change-key`, which bootstrap.sh had never
granted.

Three failures across three runs, all mine, and two worth recording. The
shared destination's cleanup was registered inside the first phase, so it
fired when that phase ended and later phases found the fixture gone - the
`t`-capture defect again, in its second direction. And
`destination-under-encryption-root` was built wrong: with `discard=off` the
destination root *is* the received dataset, so pre-creating it as an
encryption root tested a collision rather than inheritance. The encryption
root has to be the parent. The third was a lax assertion: "the non-raw send
succeeded" could equally have meant the raw transfer earlier in the phase left
nothing to send, so the phase now takes a fresh snapshot and previews to
confirm real work before asserting the refusal.

**Observation for chunk E or F, not acted on here.** A non-raw send with the
key unavailable is refused as `transfer failed; source recovery references
retained: stream pipeline: exit status 1`. Nothing in that names the key, so
an operator has no indication that `zfs load-key` is the fix. That is a
diagnostics gap rather than a correctness one, and it belongs with the other
"usable error" work rather than in this chunk.

### Chunk B - remote transport parity (done)

Parameterise the local engine's behaviour table over `local`, `ssh-direct`,
`ssh-shell` and `native`, and run the incremental, bookmark, resume and
foreign-refusal cases against each. Expect this to be the chunk that finds the
most bugs, because these paths differ in implementation and have only ever
seen a full bootstrap.

**How it landed.** `TestGuestTransportParity` in the transfer package: six
phases - `full-bootstrap`, `incremental-modes`, `bookmark-incremental`,
`unrelated-destination-refused`, `reseed-recovery`,
`resume-after-interruption` - run against each of the four transports, 24
subtests in one top-level test. It joins the transfer-remote stage, whose
floor goes 1 to 2.

`local` is in the table as a control arm rather than as coverage: the local
engine already owns every one of those behaviours in
`TestGuestLocalTransfer`. Running the identical table over it is what makes a
remote failure attributable to the transport rather than to the test, which
matters here because the test had to be rebuilt around the remote shape -
one source per transport, policy resolved from the pool on every request,
and phases chained the way chunk 2 established.

`reseed-recovery` was not in the plan above and is the phase most worth
having. It is the only one that touches `zfs.ReseedExecutor`: `AbortReceive`
and `DestroyDataset` are not part of `zfs.Executor`, and
[reseed.go:82](../internal/cli/reseed.go) refuses the entire operation when an
endpoint fails that type assertion. Nothing had ever run it against a real
remote endpoint, so a transport whose executor did not satisfy the interface
would have failed for the first time in front of an operator trying to
recover a blocked target.

Three structural decisions the transports forced. A target is per destination
root, not per transport, because the root is part of the canonical target
identity - two roots reached over one connection would collide in the
source-side binding. One native listener serves every root instead, since the
RPC server scopes with `scope.Inside` against its allowed roots while each
connection still carries its own root. And destination state is asserted
through the *local* executor throughout: the guest is both source and
destination host, so that reads the pool the remote side actually wrote,
where using the transport's own executor would let a broken remote view agree
with itself.

**The interruption is byte-exact, and finding where to put it took two tries.**
The first attempt cut the stream from the progress callback, which is the only
hook `transfer.Stream` exposes. That was wrong, and the first guest run showed
why: both `RunPipeline` and the RPC stream emit one report as the copy opens
and then sample no more often than every 250ms, and a local send moved 179MB
inside that first window. A byte threshold there cuts wherever the sampling
happens to land - 179MB, 47MB, 35MB and 75MB across the four transports on
that run - and on a faster host it would not fire at all, failing the test
without a defect behind it.

The seam that does reach the transfer path is `zfsPath`. Every transport
spawns its sender as `exec.Command(zfsPath, "send", ...)` and only the
receiving half differs, and in all three remote constructors that path is the
*local* sender only - the remote side's `zfs` is hardcoded or comes from
server config. So the test writes a wrapper that pipes a send through
`head -c` and execs the real `zfs` for anything else, and passes it as
`zfsPath`. The cut is then an exact offset, chosen by the test, on the actual
stream, for all four transports - the same fault `delegated-matrix.sh` and
`TestGuestInterruptedTransferRecovery` already inject, applied from the
sending side so it does not require owning the receiver. It also let the
payload drop from 256MiB to the 32MiB the sibling tests use, since it only
has to outrun the 4MiB cut, and it made the phase stricter: the truncating
and intact engines are separate targets on one root, so the resume is picked
up from the durable token alone.

**It found no product bugs, across three guest runs.** That is the result, not
a gap in the assertions. `endpoint.Mode` is checked against the requested mode
so `ssh-shell` cannot silently fall back to `direct`; the binding transport is
asserted per mode; the refusal and resume phases both go through the remote
destination inspection path. Every one of the 24 phases passed. The prediction
at the top of this section was wrong, and the useful reading is that the
`Executor`/`Stream` abstraction is doing its job - the engine really is
transport-agnostic, and the remote implementations really are interchangeable
with the local one across every behaviour tested here. What the chunk buys is
that this is now asserted rather than assumed, and a regression in any of the
three remote implementations fails a named phase.

**Verified by three real runs, each answering a different question.** The
first established the table and exposed the sampling problem above. The
second added `reseed-recovery` and confirmed all four endpoint executors
satisfy `zfs.ReseedExecutor` against a live remote. The third ran the
truncating sender: 24 phases green, every stage at or above its floor
(lifecycle 1, transfer-local 4, transfer-remote 2, daemon 3, control 2), and
the packaged systemd checks still passing. The parity test costs ~185s of a
~10 minute run, which is the price of four transports times six phases;
almost all of it is snapshot, hold and property work rather than bytes, so
the payload size is not what to trim if that ever needs to come down.

### Chunk C - daemon and remote (done)

Once chunk 0 has revived the outage test: add daemon-driven native replication
through an imported credential, assert a `prune:` job reaches `succeeded`, and
assert the retry/backoff state sequence rather than only its endpoints.

**How it landed.** Three self-contained tests in `test/integration/daemon`
rather than phases of one, because each needs a differently arranged daemon and
none consumes the state another leaves. All three scope discovery with the
existing `scopedDaemonBackend`: the stage's other fixtures stay in the pools,
and an unscoped daemon would try to replicate them through remotes this
configuration does not define.

`TestGuestDaemonNativeReplication` is the credential path end to end. The
pairing is issued, written through `control.ImportPairingBundle`, and then
named only by `credential = "backup"` in the remote configuration, so
`buildRemoteClients` resolving it off disk is what makes the transfer possible
at all. Destination state is read with the local `zfs.Direct`, not the
transport's executor, for the reason chunk B established.

`TestGuestDaemonPruneJob` asserts the job does work, not just that it reports
success. The grid's smallest unit is a minute, so a daemon cannot be driven
through several retention cycles inside a test; instead three snapshots are
created backdated well outside a `1x1m` horizon before the daemon starts, and
the daemon's own snapshot becomes the single retained one. A prune that ran but
retained everything now fails.

`TestGuestDaemonRemoteBackoff` measures the sequence rather than the endpoints,
and the seam that makes that possible is the reason field: a real attempt
records its transport failure as the reason, while the reconciler's early
return during a live backoff records none, so counting reasoned `waiting-retry`
events counts attempts. The endpoint is named in the pairing bundle before
anything binds the port - the managed server identity is keyed by advertised
host, not port - so the outage is simply the listener not existing yet, and
recovery is starting it under that same name. Observed delays were 4.59s then
10.99s against the 5s-doubling policy with 20% jitter.

### Chunk D - pairing and access control (done)

`pairing create` / `import` / `list` / `revoke` against a real listener, then
the negative cases: revoked credential refused, `ssh-shell` command outside
its allowed set refused, replication root outside `ssh_shell.replication_roots`
refused, missing `zfs allow` permission surfacing a usable error.

**How it landed.** Split in two by what each half needs from the environment.
`TestGuestPairingLifecycle` runs in the control stage, which has the CLI, and
drives all four subcommands against a listener started in process from the same
configuration file the CLI reads - a full daemon would have added nothing the
pairing path exercises. Each step is proved by what the listener then does
rather than by what the command printed: the imported credential replicates,
and after `pairing revoke` the same credential is refused. The revocation
assertion also checks the refusal is *final*: `native.IsUnavailable` must be
false, because classifying a revoked credential as a transport outage would put
the target into unbounded retry instead of surfacing it.

`TestGuestAccessControlRefusals` runs in the transfer-remote stage, which has
the loopback key and the installed CLI that `ssh-shell` re-executes. There is
no command allowlist to test: `ssh_shell.replication_roots` is enforced by the
RPC server scoping every operation and every receive root, so the test asserts
at that seam - an operation on an allowed root's ancestor, an operation on an
unrelated dataset, and a receive whose root is outside the set, with the
destination confirmed absent afterwards.

**One product bug, in the case the chunk was named for.** A destination anchor
with nothing delegated at or above it produced `inspect delegated ZFS
permissions on <pool>: delegation output contains no permission setpoint`
instead of the intended `effective account <user> lacks delegated ZFS
permissions on <pool>: create,destroy,...`. `zfs allow` prints nothing at all
for such a dataset, and `parsePermissionBlocks` treated empty output as
malformed, conflating "nothing is delegated" with "I could not read the
delegation table" - and losing the only diagnostic an operator can act on.
Empty output is unambiguous, and any non-empty line outside a setpoint block is
already rejected earlier in the parser, so the check removed nothing. Fixed in
`internal/zfs/permissions.go` with a unit test that fails before it; root is
authorised by the kernel before the query runs, so this only ever bit a
delegated deployment. Reaching it needed the local target configured on the
source, because the planner refuses a destination the source properties do not
name, which stops the permission preflight from running at all.

### Chunk E - CLI surface (done)

Triage the untested commands from section 3 against the section 4 rule. Add
integration coverage only for those that read real pool state
(`dataset list`, `dataset inspect`, `dataset reseed`, `identity recover`);
send the rest to unit tests over `zfs.Executor` and record that here.

**How it landed.** Seven commands triaged, four covered here and three sent to
unit tests. The split was decided per command by asking what a pool could
contradict, not by which package the code lives in.

`config check` and `config show` read configuration files and never touch ZFS
at all, so they went to `internal/cli/config_test.go`. That was not a null
result: `config show` redacts `tls_key` through `config.MarshalRedacted`, and
nothing in the tree tested it, so a regression that disclosed a listener's
private key would have been silent. The new test asserts the key is gone, the
marker is present, and the rest of the effective configuration - the
non-secret neighbour, a drop-in override, an untouched default - survives the
redaction. It also pinned an existing diagnostic gap rather than papering over
it: an unknown field in a drop-in is refused as `decode merged configuration:
strict mode: fields in the document are missing in the target struct`, naming
neither the file nor the key, because the strict decode runs over the merged
document after `Load` has dropped the per-key provenance it collected. The
test asserts what the command does and says why; fixing the message is
product work outside this chunk.

`status`'s non-JSON rendering cannot be reached from the integration suite at
all, which is the more useful finding than "it is untested". The choice is
made by `terminalWidth`, which requires stdout to be an `*os.File` that
`term.IsTerminal` accepts; every stage gives the command a pipe, so a guest
test would exercise the JSON path however it was written. The rendering itself
is already covered in `internal/statusui` over the proto snapshot. What was
left unowned was the selector, so `TestStatusRendererSelection` pins its
negative half - a redirected or piped `status` stays machine-readable - which
is the property scripts depend on.

The four that earned a pool are one test each in the control stage, which is
the stage that has the CLI binary. Both write their own configuration into
`t.TempDir` and pass `--config` plus `--config-dir`: the shared stage
configuration is not available for this, because `TestGuestDaemonControl`
appends to it and asserts a reload generation of 2, and its identity directory
already belongs to another installation.

`TestGuestDatasetCLI` covers `list`, `inspect` and `reseed` against one tree.
The rendering of `list` and `inspect` is unit-tested over a fake reader
already, so the phases assert the thing a fake cannot supply: that ZFS's own
value and source columns drive the classification. A locally activated root
reads as active, its replicated descendant as covered, a root naming an
unconfigured remote as invalid, and a bare dataset as inactive - four statuses
from one `zfs get` sweep. The phase worth the boot is `inspect-received`, and it
was written on a wrong premise first. The source is sent with `props=on` and
is activated with a non-default grid naming a destination, so the expectation
was that the replica would carry `org.boomerangz:enabled=on` as a received
property for the CLI to report as provenance. It does not, and the guest run
is what said so: `completeReceiveExclusions` excludes every public
`org.boomerangz` key from the receive, so even a send that explicitly asks for
properties leaves the whole public namespace behind. What crosses is
`state:lineage` and `state:owner`, which arrive as `received`.

The phase now asserts that, which is the stronger claim: one `dataset inspect`
of the replica shows receive isolation whole - activation inactive, `enabled`
and `policy` back at their defaults, no local destination - alongside the
received state boomerangz does keep, attributed to `received` and to the
replica. Asking for properties and still not getting the public ones is the
part a fake could not have told us.

`reseed` is covered over the CLI rather than only through the service, which
`TestGuestLocalTransfer/reseed-recovery` already owns. What the command adds is
target resolution: it must resolve the name against the source's own effective
`local`/`remote` lists, and the negative - a destination root the source
policy does not name - is refused with
`must identify exactly one effective local destination or remote`. The apply
path is proved by what follows it rather than by its own output: the replica
is gone, and a fresh transfer to the same root plans as `full`, so the reset
left the source sendable rather than merely destroying the destination.

`TestGuestIdentityRecoverCLI` is the inverse of the unit test beside it. That
one hands `runIdentityRecover` a fabricated `zfs.State`; this one makes the
markers with a real snapshot under one installation, points a scratch
configuration with an empty identity directory at them, and recovers. The
preview must not write, the apply must, and - the assertion that makes it
worth a pool - the recovered installation must then be able to extend the
lineage it adopted, which exercises `RootAuthority` against markers ZFS
actually stored rather than against a literal. The owner is named with
`--owner` rather than inferred, because `recoveryInventory` scans every
dataset on the host and any other test's activated root would make the
candidate set ambiguous; that is a property of the command, not of the test.

**One piece of housekeeping the chunk forced.** `TestGuestDaemonControl` and
`TestGuestDaemonAbruptRestart` created activated source roots and never
destroyed them, which is the only leak left in the suite after chunk 1. It
does not matter to them, but `identity recover` fails outright on an activated
root whose owner and lineage it cannot resolve, so a leaked root is a loaded
gun for anything that scans the pools. Both now register
`zfstest.RegisterCleanup`.

**Verified by two guest runs.** The first was red on exactly one phase -
`inspect-received`, above - and green on everything else, including all three
of `TestGuestIdentityRecoverCLI`'s phases and both of the reseed paths. The
second run is green end to end: lifecycle 2, transfer-local 5,
transfer-remote 2, daemon 6, control 6 top-level passes, no failures, and the
packaged system checks green. The chunk cost one boot to learn that receive
isolation is stricter than the test assumed, which is the kind of thing the
suite exists to say.

### Chunk F - adversarial paths (done)

Destination out of space mid-receive; a second daemon contending for the
socket; abrupt power loss between snapshot creation and property write. Lowest
priority, highest information per test.

**How it landed.** Three tests, in three stages, chosen by environment as
chunk D established. The third item turned out to name a window that does not
exist, and finding that out was the most useful part of the chunk.

`TestGuestDestinationExhaustion` (transfer-local) receives a 96MiB payload
into a container carrying a 32MiB quota. The quota sits on the container
rather than on the receive root, because with `discard=off` the receive root
*is* the received dataset and would inherit nothing that constrains it. The
assertion that makes this an exhausted receive rather than a refused one is
that the progress callback reported bytes before the failure - the stream was
in flight when the destination ran out. The two phases after it are the point:
the source still holds the snapshot it sent, so recovery state survived the
failure, and lifting the quota lets the same request succeed. A transfer that
fails on space must be a retry, not a reseed. Creating the quota under
delegation needed a `quota` grant the destination pool had never been given;
it is fixture-only and commented as such, since nothing in boomerangz sets
quotas.

`TestGuestDaemonSocketContention` (control) is process lifecycle, which the
suite's charter already claims. A second `boomerangz daemon` against the same
configuration is refused - `another lifecycle operation is running`, from the
flock taken in `lifecycleLock` before the control server is ever started - and
the running daemon keeps serving across the refusal, which is the half that
would actually hurt if it regressed. Then the first daemon is killed with
SIGKILL so it runs no shutdown, its socket file outlives it, and a replacement
has to tell a stale socket from a live one: `listenUnix` dials it, takes
`ECONNREFUSED` as proof, and rebinds. Both branches of that function are now
covered, and neither is reachable without real processes, because the refusal
depends on a lock the kernel releases on exit.

**The power-loss window in the plan does not exist.** `Service.CreateSnapshot`
passes the ownership metadata to `zfs snapshot -o`, so a snapshot and its
properties are one transaction; there is no point at which a snapshot exists
without its metadata, and no code that writes snapshot metadata afterwards.
The window that does exist is the one before it: `CreateSnapshot` writes the
root's owner and lineage markers with `SetProperties` and only then takes the
first snapshot, so a crash in between leaves a root claimed but never
snapshotted. `TestGuestInterruptedLineageInitialization` (lifecycle) covers
the three states that leaves, all of which turn on which property source ZFS
reports. A root claimed by this installation resumes its lineage rather than
forking a new one. A root claimed by another installation is refused as a
`dormant foreign lineage` and no snapshot is created. And a child under a
claimed parent sees the parent's markers through inheritance, which ZFS
attributes to the parent - so `InspectState` drops them, and the child claims
local markers of its own instead of silently joining its parent's lineage.
That last one is the case worth a pool: it is decided entirely by a source
column, and the unit tests hand-build the state rather than reading one.

**Verified by the same two guest runs.** All three tests passed on the first
run and on the second, with the exhaustion test the most informative: 92MiB
crossed before the receiver reported
`cannot receive new filesystem stream: destination ... space quota exceeded`,
the source kept its snapshot and its hold, and the retry after `quota=none`
verified. Worth knowing for the diagnostics work: the sender's half of that
error is `stream pipeline: write |1: broken pipe` plus `signal: killed`, so the
only actionable sentence is the receiver's, and it arrives at the end of a
three-line message.

**Left open, and worth naming.** Chunk B's section proposed one more
adversarial shape for this chunk: a receive that fails for a reason other than
a truncated stream - a destination property conflict, or a stream the receiver
rejects outright - asked over each transport rather than only locally. This
chunk did not answer it. `TestGuestDestinationExhaustion` is a non-truncation
failure, so the shape is no longer entirely untested, but it runs over the
local engine alone. Whether the other shapes leave the same recoverable state
over `ssh-direct`, `ssh-shell` and `native`, or whether some of them strand a
target with neither a resume token nor a clean refusal, is still unasked.

### Chunk G - recursive replication over a remote

Starting point for a fresh session: chunk B built
`TestGuestTransportParity`, which runs six behaviours against `local`,
`ssh-direct`, `ssh-shell` and `native`. This chunk adds the one send shape it
left out. Read chunk B's "How it landed" first - the target-per-root rule, the
native listener arrangement and the truncating-sender seam all apply here
unchanged, and the test is the obvious place to put this work.

`TestGuestLocalTransfer/recursive-mapping` covers `replicate=on` with
`discard=first` and `discard=all`, an incremental recursive follow-up, and
refusal of a foreign snapshot on the recursive destination - all locally.
None of it has met a remote transport, and there are three specific reasons to
expect this one to behave differently rather than merely be untested:

- **Scope boundaries meet descendants.** `scopedExecutor.ListDatasets` filters
  with `scope.Related` and `InspectState` demands `scope.Inside`
  ([scope.go](../internal/replication/ssh/scope.go)); the RPC server does the
  same against its allowed roots. A recursive receive creates a subtree under
  the mapped destination, so this is the first time those filters are asked
  about children rather than the root itself.
- **Ancestor preparation meets the root guard.** `CreateReceiveParent`
  refuses `dataset == e.root`. `discard=first` and `discard=all` both map the
  received dataset below the destination root, so `prepareReceiveParents` has
  to create intermediate containers remotely - the exact boundary that guard
  sits on.
- **Recursive planning gathers sibling bases.** `Apply` has a
  `plan.Send.Recursive` branch that collects each child's matching base
  snapshot into a second `ProtectSet` ([local.go](../internal/transfer/local.go)),
  and `sendArgs` refuses a bookmark base for a recursive send. Both sit on the
  source side, but the destination inventory that drives them comes from the
  remote executor.

Do it as a self-contained `recursive-mapping` phase per transport inside
`TestGuestTransportParity`, building its own source tree the way the local
test's equivalent phase does. That keeps it off the chained phases, and it
does not move any stage floor: `run_stage` counts top-level `--- PASS:` lines,
so subtests are free.

Done when: recursive `replicate` with both discard modes runs over each
transport, with the received subtree, the prepared ancestors and the
foreign-destination refusal asserted on the destination - and the ledger row
under "Remote transports" names it.

## 6. Keeping this from re-rotting

Chunk 0 removes the mechanism that let a test go dormant. To keep section 3
from going stale, add a short coverage ledger to
`test/integration/README.md` - one line per behaviour axis, naming the test
that owns it - and treat it the way `CLAUDE.md` already treats
`ARCHITECTURE.md`: a change that adds or moves an integration behaviour
updates the ledger in the same change.
