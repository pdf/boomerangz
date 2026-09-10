# Design: integration test coverage review

Status: in progress. Chunks 0, 1 and 3 have landed; chunk 2 and A-F are outstanding.

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

### Chunk 2 - split the monoliths

Convert `TestGuestLocalTransfer` and `TestGuestLifecycle` into `t.Run`
subtests along the phase boundaries their existing comments already mark.
Where a phase genuinely depends on the previous one's state, keep them in one
subtest and say why in a comment. No behaviour changes.

Done when: one failing phase reports as one failing subtest and the rest still
report their own results.

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

### Chunk A - encryption and raw sends

Add an encrypted source dataset to the fixtures and cover: raw send of an
encryption root; `replicate=on` refused on an encrypted source without `raw`;
`props=on` with an encrypted source; receiving under a destination that is
itself an encryption root; and key-unavailable behaviour. This is the largest
single gap.

### Chunk B - remote transport parity

Parameterise the local engine's behaviour table over `local`, `ssh-direct`,
`ssh-shell` and `native`, and run the incremental, bookmark, resume and
foreign-refusal cases against each. Expect this to be the chunk that finds the
most bugs, because these paths differ in implementation and have only ever
seen a full bootstrap.

### Chunk C - daemon and remote

Once chunk 0 has revived the outage test: add daemon-driven native replication
through an imported credential, assert a `prune:` job reaches `succeeded`, and
assert the retry/backoff state sequence rather than only its endpoints.

### Chunk D - pairing and access control

`pairing create` / `import` / `list` / `revoke` against a real listener, then
the negative cases: revoked credential refused, `ssh-shell` command outside
its allowed set refused, replication root outside `ssh_shell.replication_roots`
refused, missing `zfs allow` permission surfacing a usable error.

### Chunk E - CLI surface

Triage the untested commands from section 3 against the section 4 rule. Add
integration coverage only for those that read real pool state
(`dataset list`, `dataset inspect`, `dataset reseed`, `identity recover`);
send the rest to unit tests over `zfs.Executor` and record that here.

### Chunk F - adversarial paths

Destination out of space mid-receive; a second daemon contending for the
socket; abrupt power loss between snapshot creation and property write. Lowest
priority, highest information per test.

## 6. Keeping this from re-rotting

Chunk 0 removes the mechanism that let a test go dormant. To keep section 3
from going stale, add a short coverage ledger to
`test/integration/README.md` - one line per behaviour axis, naming the test
that owns it - and treat it the way `CLAUDE.md` already treats
`ARCHITECTURE.md`: a change that adds or moves an integration behaviour
updates the ledger in the same change.
