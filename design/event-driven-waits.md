# Design: event-driven waits

Status: proposed. Nothing here has landed.

Most of this tree waits on a channel. A minority samples in a loop, and the
minority is where the recent failures came from. This document separates the
polling that should become a wait from the polling that is polling because
nothing can signal it, and specifies what has to exist before the first group
can move.

It is mostly test-facing work. One production site is in scope
([internal/control/server.go:623](../internal/control/server.go)); the rest of
the daemon already waits correctly.

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
The control plane already uses it in production: `WatchStatus` sends a snapshot,
then blocks on the revision, with the client's interval acting only as a
heartbeat cap ([internal/control/service.go:69](../internal/control/service.go)).

**Request channel plus ticker.** `discovery.Scanner.Run` waits on cancellation,
the interval ticker, an explicit reconcile request, and an interval change
([internal/discovery/discovery.go:171](../internal/discovery/discovery.go)).

The harness does it too: `stage_filter` acts on the
`BOOMERANGZ_REMOTE_OUTAGE_OBSERVED` handshake line the outage test prints rather
than sleeping until the test is probably ready
([test/integration/guest/run-common.sh:111](../test/integration/guest/run-common.sh)).

So the question is not whether to adopt channels. It is why some waits never
reached them.

## 2. What still polls

Findings, each read from the tree.

**2.1 In-process daemon status, sampled.** Four helpers loop over
`runtime.Status()` with a deadline and a 25-100ms sleep:
`waitForSendIntervals` and `waitForTransferSuccesses`
([test/integration/daemon/guest_test.go:101](../test/integration/daemon/guest_test.go),
[:124](../test/integration/daemon/guest_test.go)), the `waitForEvent` closure in
the outage test ([:441](../test/integration/daemon/guest_test.go)), and
`waitForJobState` ([test/integration/daemon/native_test.go:84](../test/integration/daemon/native_test.go)).
Each of these has `WaitStatus` available on the same object it is sampling.

**2.2 Out-of-process daemon status, sampled.** The control suite runs a real
`boomerangz daemon` and watches it by stat-ing the socket and shelling out to
`boomerangz status` in a poll loop
([test/integration/control/guest_test.go:82](../test/integration/control/guest_test.go),
[:237](../test/integration/control/guest_test.go),
[test/integration/control/adversarial_test.go:91](../test/integration/control/adversarial_test.go)).
`WatchStatus` is a streaming RPC on the socket those tests already hold
credentials for.

**2.3 ZFS state polled after work the daemon announces.** The scheduling test
polls `InspectState` until a snapshot exists, until a property is local, until
the snapshots are gone ([test/integration/daemon/guest_test.go:199](../test/integration/daemon/guest_test.go)
and its five `waitFor` calls). The daemon reports each of those as a job
transition before the test can observe the pool, because the pool emits the
outcome after `Run` returns and `Run` does the ZFS work first
([internal/daemon/pool.go:274](../internal/daemon/pool.go)). The test is
sampling the world when the daemon is telling it what happened.

**2.4 ZFS state nothing announces.** `waitForDevice` waits up to 5s in 100ms
steps for udev to create a zvol node
([internal/testutil/zfstest/fixture.go:116](../internal/testutil/zfstest/fixture.go)).
Nothing in this process knows when that happens.

**2.5 Bare sleeps standing in for an anchor.** The restart test sleeps 500ms and
then asserts that no further snapshot appeared
([test/integration/control/guest_test.go:285](../test/integration/control/guest_test.go)).
The sleep is not waiting for anything; it is a guess at how long "nothing else
happens" needs to be to mean something.

**2.6 One production sleep.** A control reload that replaces listeners hands the
retired endpoints to a goroutine that sleeps 100ms before closing them
([internal/control/server.go:623](../internal/control/server.go)), which is a
guess at how long in-flight RPCs need.

## 3. Why this is worth doing

**Sampling a latest-per-job map loses transitions.** `StatusStore` keeps one
event per job ([internal/daemon/status.go:24](../internal/daemon/status.go)), so
a sampler sees whichever event is current, not the sequence. Both daemon
failures in the integration-coverage work were this:
`TestGuestRemoteOutageReconnection` read a `waiting-retry` event whose reason had
already been overwritten, and `TestGuestDaemonRemoteBackoff` timed the gaps
between events it was using as a record of attempts. Neither is fixed by polling
faster - the window shrinks and never closes. Both were fixed by changing what
the daemon records, which was the right fix for those, but the underlying
hazard stands for every other test that samples a map it hopes has not moved.

**Polls are not free where they run.** The guest is 2 vCPUs and 4 GiB
([test/integration/targets/cachyos/config.sh:16](../test/integration/targets/cachyos/config.sh)),
and every `InspectState` forks `zfs`. A 100ms poll is up to ten processes a
second competing with the daemon whose timing the same test is asserting on.
The scheduling failure in CI was a starved management worker; a test that adds
load while measuring latency is not a neutral observer.

**A timeout is the least informative sentence available.** `timed out waiting
for scheduled source and descendant snapshots` says a thing did not happen and
nothing about what did. We already patched that one call site to print
`runtime.Status()`. A waiter that owns the event stream can print the
transitions it saw, every time, without each test remembering to.

## 4. What to build

**4.1 A lossless recent-event window.** `StatusStore` gains a bounded ring of
transitions alongside the latest-per-job map, and `Since(revision) (uint64,
[]Event, int)` returning the revision it is current to, the transitions after
the caller's revision, and how many were dropped because the caller fell behind.

A transition is an event that differs from that job's previous event in `State`
or `Reason`. That rule exists because progress updates go through the same store
with `State: "sending"` ([internal/daemon/runtime.go:1064](../internal/daemon/runtime.go))
and would otherwise evict the ring during any large send.

Proposed capacity 512 transitions. The dropped count is the part that matters:
a consumer that fell behind must be able to say so rather than silently assert
on a gap.

**4.2 A waiter for in-process tests.** A helper in `internal/testutil` that
takes a runtime, a predicate over `Event`, and a bound; loops
`WaitStatus`/`Since` rather than sleeping; returns the first matching event; and
on expiry fails with the whole window it observed. Replaces 2.1 wholesale. The
bound stays a hard failure - this removes sampling, not deadlines.

**4.3 A generation anchor.** "The daemon has seen the world at least once since
T" is not currently expressible: the status revision moves on job transitions,
while the generation ID advances on a published scan
([internal/discovery/discovery.go:59](../internal/discovery/discovery.go)) and
reaches the control snapshot separately. Publishing a generation should move the
status revision too, so a waiter can block until the generation ID exceeds one
it captured. That turns 2.5 from "sleep 500ms" into "wait for two further
generations, then assert nothing new was created" - a positive event anchoring
a negative assertion.

**4.4 An out-of-process waiter.** A small client in the control test package
that dials the existing socket and consumes `WatchStatus`, replacing the
`boomerangz status` poll loops in 2.2. No new CLI surface: the RPC exists, the
tests already have credentials, and shelling out per poll is the expensive half
of that loop anyway. Socket appearance (2.2's first wait) stays a poll - a
missing file has no notifier short of inotify, and the bound is short.

**4.5 Wait for the event, then assert once.** 2.3 should not become a faster
poll of ZFS. It should wait for the job transition that reports the work and
then read the pool exactly once. A failure then reports the wrong result rather
than the absence of a result, and the test stops forking `zfs` in a loop beside
a daemon it is timing.

**4.6 Drain instead of sleep.** The retired control endpoints in 2.6 should be
closed on a drain of their in-flight RPCs (gRPC's `GracefulStop`, bounded, with
the existing error log if the bound expires) rather than after a fixed 100ms.
This is the one user-visible item here: today a reload can cut an RPC that was
still running at 100ms, and can hold a replaced listener open longer than it
needs to otherwise.

## 5. What stays as it is

- **`waitForDevice`** (2.4). udev is not ours to subscribe to from here, and
  inotify on `/dev/zvol` buys milliseconds inside a 5s bound that has never been
  the problem.
- **The scanner ticker.** Periodic rediscovery is how the daemon learns about
  ZFS changes. `zpool events`/zed could in principle replace it, but that is a
  different design with a much larger blast radius - it changes what the daemon
  is guaranteed to notice, not just when. Out of scope; worth its own document.
- **Backoff and deadline timers.** `Scheduler.Next`, `Runtime.schedule`, and the
  remote backoff deadline are already time-driven by intent.
- **Unit-test sleeps that are the work** ([internal/daemon/pool_test.go:66](../internal/daemon/pool_test.go),
  [:143](../internal/daemon/pool_test.go), [internal/control/control_test.go:159](../internal/control/control_test.go)).
  A job that sleeps to stay running is a fixture, not a wait.

## 6. Chunks

Each is independently landable and independently useful.

**Chunk A - the event window.** 4.1, with unit tests for the transition rule,
ring eviction, and the dropped count. Done when `Since` is lossless up to
capacity and a progress-heavy send cannot evict a transition.

**Chunk B - the in-process waiter.** 4.2, then convert the four helpers in 2.1.
Done when no test in `test/integration/daemon/` sleeps while watching status,
and a forced failure prints the observed transitions.

**Chunk C - the generation anchor.** 4.3, then convert 2.5. Done when the
restart test asserts against a generation count rather than a duration, and no
bare sleep remains in `test/integration/control/`.

**Chunk D - the out-of-process waiter.** 4.4, converting 2.2. Done when the
control suite watches the daemon over the socket it is testing rather than by
re-running the CLI.

**Chunk E - the listener drain.** 4.6. Done when a reload closes replaced
listeners on drain, with a test that a call in flight across a reload completes.

**Chunk F - ZFS waits behind events.** 4.5, converting 2.3. Last deliberately:
it depends on B, and it is the one that changes what the tests assert rather
than how they wait.

## 7. Risks

- **A silent drop is worse than a poll.** A waiter that misses the window and
  says nothing asserts on a gap. `Since` reports the drop and the waiter fails
  on it; this is the single most important detail in chunk A.
- **A wait can hang where a poll timed out.** Every waiter keeps a bound, and
  the bound stays a `t.Fatal`. None of the existing timeouts should be raised
  while converting: they are also a crude performance bound, and this week's
  failure is what one of them caught.
- **Tests coupled to the status vocabulary.** Converting ZFS assertions to event
  waits ties more tests to job IDs and state names. That vocabulary is already
  public - the CLI prints it and the coverage ledger names it - but chunk F
  should not invent new states to make a wait convenient.
- **Losing an independent check.** Waiting for "the daemon says it snapshotted"
  and then asserting the snapshot exists is still two facts. Replacing the
  second with the first would not be.
