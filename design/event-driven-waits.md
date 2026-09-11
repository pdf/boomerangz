# Design: event-driven waits

Status: proposed. Nothing here has landed.

The daemon's status contract is a map of the latest event per job. Everything
that wants to know what the daemon did - a test, an operator, a log pipeline -
reads that map repeatedly and hopes it did not move in between. It does move,
and what happened in between is gone.

This document proposes making the transition stream itself the contract, and
then retiring the polling that only exists because the contract was a snapshot.
The reach is deliberate: the same defect produced two integration failures and
silently degrades `boomerangz status --watch`, and fixing it in the tests alone
would leave the operator-facing half in place.

## 1. The idioms already in the tree

Nothing below invents a mechanism. Three are already load-bearing:

**Broadcast-and-recheck.** `lifecycle.Gate` keeps a `changed chan struct{}` per
scope, closes and replaces it under the lock on every transition, and waiters
re-evaluate the predicate after each wake
([internal/lifecycle/gate.go:174](../internal/lifecycle/gate.go),
[:208](../internal/lifecycle/gate.go)). `keyLocks` in the worker pool does the
same for lock handoff ([internal/daemon/pool.go:61](../internal/daemon/pool.go)).
This is the pattern for a condition this process owns.

**Revision waiting.** `StatusStore` carries a monotonic revision and a
`Wait(ctx, after)` that blocks until it moves
([internal/daemon/status.go:52](../internal/daemon/status.go)), exposed as
`Runtime.WaitStatus` ([internal/daemon/runtime.go:1245](../internal/daemon/runtime.go)).
`WatchStatus` uses it in production: it sends a snapshot, then blocks on the
revision, with the client's interval acting only as a heartbeat cap
([internal/control/service.go:69](../internal/control/service.go)).

**Request channel plus ticker.** `discovery.Scanner.Run` waits on cancellation,
the interval ticker, an explicit reconcile request, and an interval change
([internal/discovery/discovery.go:171](../internal/discovery/discovery.go)).

The harness does it too: `stage_filter` acts on the
`BOOMERANGZ_REMOTE_OUTAGE_OBSERVED` handshake line the outage test prints rather
than sleeping until the test is probably ready
([test/integration/guest/run-common.sh:111](../test/integration/guest/run-common.sh)).

The waking is therefore already event-driven in the places that matter. What is
missing is what the wake gives you when it arrives.

## 2. The defect

`StatusStore.Record` overwrites one map entry per job
([internal/daemon/status.go:25](../internal/daemon/status.go)) and `Snapshot`
returns that map. A reader learns the current state of every job and nothing
about the sequence. Two events for the same job between two reads leave no
trace of the first.

**2.1 It reaches operators.** `WatchStatus` sends a full snapshot on every
revision change ([internal/control/service.go:69](../internal/control/service.go)),
and `boomerangz status --watch` renders each one - repainting the terminal, or
emitting newline-delimited JSON when redirected
([internal/cli/control.go:67](../internal/cli/control.go),
[docs/reference/cli.md:29](../docs/reference/cli.md)). A job that goes
`waiting-retry` -> `probing` -> `waiting-retry` between two sends shows one of
those three. A transfer that failed and recovered between two sends shows
neither. Piping that JSON at a log or an alert is piping something that drops
what it feels like dropping, and the drops are invisible in the output.

**2.2 It broke two tests, and their fixes only moved it.**
`TestGuestRemoteOutageReconnection` read a `waiting-retry` event whose reason had
been overwritten; `TestGuestDaemonRemoteBackoff` timed the gaps between events it
was using as the record of real attempts. Both were fixed by changing what the
daemon records - suppressing a transition that had not happened, and not running
a remote job inside its own backoff window. Those were correct changes and they
stand. But they work by keeping the map quiet enough to sample, which is a
property every future caller has to preserve without knowing it is doing so.

**2.3 Polling is what the snapshot contract forces.** Given only a map, a caller
who wants a sequence has no option but to read it often. That is why four test
helpers loop over `runtime.Status()` on a 25-100ms sleep
([test/integration/daemon/guest_test.go:101](../test/integration/daemon/guest_test.go),
[:124](../test/integration/daemon/guest_test.go),
[:441](../test/integration/daemon/guest_test.go),
[test/integration/daemon/native_test.go:84](../test/integration/daemon/native_test.go)),
and why the control suite re-runs `boomerangz status` in a loop
([test/integration/control/guest_test.go:82](../test/integration/control/guest_test.go),
[:237](../test/integration/control/guest_test.go),
[test/integration/control/adversarial_test.go:91](../test/integration/control/adversarial_test.go)).
Those loops are a symptom. Faster polling shrinks the window and never closes
it.

The cost is not only correctness. The guest is 2 vCPUs and 4 GiB
([test/integration/targets/cachyos/config.sh:16](../test/integration/targets/cachyos/config.sh)),
every `InspectState` forks `zfs`, and a 100ms poll is up to ten processes a
second competing with the daemon whose timing the same test is asserting on.
The CI failure that started this work was a starved management worker.

## 3. The contract change

**3.1 A transition window on `StatusStore`.** Alongside the latest-per-job map,
a bounded ring of transitions, each carrying the revision at which it was
recorded, and:

```go
// Since returns transitions recorded after revision and the revision they are
// current to. It fails with ErrCursorTooOld when the caller's revision is
// older than the oldest transition retained, which is the only case in which
// this stream is not complete.
func (s *StatusStore) Since(revision uint64) (uint64, []Event, error)
```

Nothing is dropped while a consumer is reading. The bound exists for the
consumer that stops: the report path runs on the pool's worker goroutines and
the transfer progress callbacks
([internal/daemon/pool.go:280](../internal/daemon/pool.go),
[internal/daemon/runtime.go:1064](../internal/daemon/runtime.go)), so blocking
the producer would let a wedged `status --watch` stall replication, and
retaining without limit is a leak paced by whoever is not reading. Bounded, with
a defined edge, is the only remaining option.

The edge is terminal rather than advisory. A dropped count that a caller may
ignore is this defect again with a counter attached; `ErrCursorTooOld` says the
sequence is broken and the caller must resync from the snapshot the store keeps
anyway. A test waiter fails on it rather than asserting across the hole.

Capacity 4096 transitions, roughly 1.5MB at the string sizes these events carry.
The busiest minute of a full integration run produced 103 transitions, from a
daemon configured with a 100ms reconcile interval and one-minute cadences; a
production daemon is far quieter, and a healthy consumer reads within
milliseconds of a wake. The number is chosen to put the edge out of reach rather
than to ration memory, and the store exposes its high-water mark so it can be
revisited with evidence.

This is a delivery buffer, not a record. Losing nothing across a daemon restart,
or across a consumer that was absent for an hour, is a durable status journal
with retention and rotation of its own - a different feature with a different
lifecycle. Nothing has asked for one. The ring should not be grown in its
direction.

**3.2 Progress updates do not enter the window.** `recordProgress` writes an
event per progress callback with `State: "sending"`
([internal/daemon/runtime.go:1064](../internal/daemon/runtime.go)); a large send
would otherwise evict the window on its own. Split the store's entry points -
`Record` for transitions, `RecordProgress` for progress - rather than filtering
by content.

Filtering by content is the tempting version and it is wrong: deduplicating
consecutive identical events would erase the second of two identical failed
attempts, which is exactly the sequence `TestGuestDaemonRemoteBackoff` counts.
The two call paths are already distinct, so the distinction costs nothing.

Capacity: 512 transitions, which at the observed rates is minutes of a busy
daemon and well past any watcher's reconnect gap.

**3.3 `WatchStatus` carries transitions.** Two additive fields on
`WatchStatusResponse` ([proto/boomerangz/control/v1/control.proto:30](../proto/boomerangz/control/v1/control.proto)):
the transitions since the revision of the previous message, and a flag marking
the message as a resync - the server's cursor aged out, the snapshot is current,
and the sequence between them was not retained. The snapshot stays exactly as it
is, so existing clients are unaffected and the message remains self-describing
for a client that joins mid-stream.

The server already holds the revision it last sent
([internal/control/service.go:69](../internal/control/service.go)); it becomes
the cursor into `Since`.

**3.4 The CLI shows them.** Interactive `--watch` gains a short transition tail
under the table - the thing an operator is actually watching for is a change,
and today the only way to see one is to be looking at the right moment.
Redirected `--json` includes the transitions in each object, which is additive
for anything reading `.datasets` or `.jobs` today. `docs/reference/cli.md` and
the operations guide say what the new field is, and that a resync marks the one
case where the stream is not continuous. No flag changes, so the completions in `contrib/` are
untouched; the man page gains a sentence with the docs.

This is the part I would have cut for being bigger than the tests needed. It is
the half of the defect a user can hit.

## 4. What the tests then stop doing

**4.1 In-process waiter.** A helper in `internal/testutil` taking a runtime, a
predicate over `Event`, and a bound; looping `WaitStatus` and `Since` rather
than sleeping; returning the first matching transition; and on expiry failing
with the whole window it observed. Replaces the four helpers in 2.3. The bound
stays a hard failure: this removes sampling, not deadlines.

**4.2 Out-of-process waiter.** A client in the control test package that dials
the socket those tests already hold credentials for and consumes `WatchStatus`,
replacing the `boomerangz status` poll loops. Socket appearance stays a poll - a
missing file has no notifier short of inotify, and the bound is short.

**4.3 A generation anchor for negative assertions.** The restart test sleeps
500ms and then asserts that no further snapshot appeared
([test/integration/control/guest_test.go:285](../test/integration/control/guest_test.go)).
The sleep is a guess at how long "nothing else happened" has to be to mean
something. Publishing a generation should move the status revision, so a waiter
can block until the generation ID exceeds one it captured
([internal/discovery/discovery.go:59](../internal/discovery/discovery.go)) and
the assertion hangs off a positive event: two further scans have completed and
the snapshot count is unchanged.

**4.4 Wait for the transition, then assert once.** The scheduling test polls
`InspectState` until a snapshot exists, until a property is local, until the
snapshots are gone ([test/integration/daemon/guest_test.go:199](../test/integration/daemon/guest_test.go)).
The daemon reports each of those as a job transition, and reports it after the
work is durable, because the pool emits the outcome after `Run` returns
([internal/daemon/pool.go:274](../internal/daemon/pool.go)). Wait for the
transition, then read the pool once. A failure then reports a wrong result
rather than the absence of one.

Waiting on the daemon's own claim and then checking the pool is still two
independent facts. Dropping the second and trusting the event would not be, and
is not proposed.

## 5. Deferred, on merit rather than size

**Replacing periodic discovery with `zpool events`/zed.** The scanner rescans on
a ticker because ZFS changes are not pushed to it. A zed hook could wake it
instead, and that is the honest end state of "stop polling". It is deferred
because it changes what the daemon is guaranteed to notice - a missed or
coalesced event becomes a dataset the daemon does not know about, where today a
missed wake costs latency only - and because zed's availability and delivery
guarantees vary by distribution in ways this project has not surveyed. It
becomes worth doing when the periodic scan is a measured cost rather than an
assumed one, and when a zed path can be specified with the periodic scan still
underneath it as the correctness floor. It wants its own document; this one
should not pretend to have decided it.

**Everything else stays as it is because it is already right.** `waitForDevice`
([internal/testutil/zfstest/fixture.go:116](../internal/testutil/zfstest/fixture.go))
waits on udev, which is not ours to subscribe to. The scheduler's deadlines and
the remote backoff are time-driven by intent. Unit-test sleeps that keep a job
running are fixtures, not waits
([internal/daemon/pool_test.go:66](../internal/daemon/pool_test.go),
[internal/control/control_test.go:159](../internal/control/control_test.go)).

One unrelated sleep is worth fixing while nearby: a control reload hands
replaced listeners to a goroutine that sleeps 100ms before closing them
([internal/control/server.go:623](../internal/control/server.go)), which can cut
an RPC still running at 100ms and holds the listener open when none is. It
should close on a drain of in-flight calls, bounded, keeping the existing error
log if the bound expires.

## 6. Chunks

**Chunk A - the transition window.** 3.1 and 3.2. Unit tests for ordering,
eviction, the cursor-too-old boundary, and that a progress-heavy send cannot
evict a transition. Done when `Since` is complete for any cursor it accepts and
refuses the ones it cannot serve.

**Chunk B - the control plane carries transitions.** 3.3 and 3.4, with the docs
in the same change. Done when `status --watch --json` emits every transition a
job made while the client was connected, and a test asserts that a sequence
which collapses in the snapshot survives in the stream.

**Chunk C - the listener drain.** Section 5's last paragraph, independent of the
rest. Done when a call in flight across a reload completes, with a test.

**Chunk D - the in-process waiter.** 4.1, converting the four helpers. Done when
nothing in `test/integration/daemon/` sleeps while watching status, and a forced
failure prints the transitions observed.

**Chunk E - the out-of-process waiter.** 4.2, on chunk B's stream. Done when the
control suite watches the daemon over the socket it is testing rather than by
re-running the CLI.

**Chunk F - the generation anchor.** 4.3. Done when no bare sleep remains in
`test/integration/control/`.

**Chunk G - ZFS waits behind transitions.** 4.4, last because it depends on D
and changes what those tests assert rather than how they wait.

A through C are the defect. D through G are the cleanup the fix makes possible,
and each is independently droppable without leaving the contract half-changed.

## 7. Risks

- **A silently lossy window is the original bug.** A broken cursor has to be
  refused at every layer that carries the stream - `Since`, the RPC, the CLI -
  and a test waiter must fail on it rather than continue against a gap. The
  failure mode to design against is a consumer that keeps going.
- **Ring capacity is bounded by evidence, not derived from it.** 4096 is sized
  against a peak of 103 transitions a minute in a deliberately aggressive test
  daemon. Chunk A exposes the high-water mark; if a real deployment ever
  approaches it, that is the signal to look at what is producing transitions at
  that rate before enlarging anything.
- **Additive proto fields still change output.** `--json` consumers gain a
  field; that is compatible for anything selecting known keys and not for
  anything asserting an exact object. The docs change lands with the code.
- **A lossless stream invites over-specified tests.** Being able to assert on
  every intermediate state does not mean a test should. The waiter's predicate
  should name the transition the behaviour is about, not transcribe the
  sequence, or the suite gets brittle in a new way.
- **More test code coupled to the status vocabulary.** Job IDs and state names
  become load-bearing in more places. They are already public - the CLI prints
  them - but chunk G should not invent states to make a wait convenient.
