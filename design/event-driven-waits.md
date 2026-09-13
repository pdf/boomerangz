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
revision, re-sending after a client-chosen interval if nothing moves
([internal/control/service.go:69](../internal/control/service.go)). Section 3.6
is about what that interval is still covering for.

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

**3.1 Publish transitions to subscribers.** Both consumers are streams: one
gRPC goroutine per `--watch` client, blocked in `Send`
([internal/control/service.go:69](../internal/control/service.go)), and a test
waiter that wants the next event matching a predicate. So the store publishes
rather than retaining a history for someone to pull:

```go
// Subscribe delivers transitions recorded after the call. The channel is
// closed when the subscription ends; cancel releases it. A subscriber that
// stops reading past the queue bound has its subscription terminated rather
// than its events dropped - ErrSubscriberOverflow is delivered before the
// close, and the subscriber must resync from a snapshot.
func (s *StatusStore) Subscribe() (<-chan Event, func())
```

The producer never blocks: publishing is a non-blocking send into each
subscriber's bounded queue. That property is not negotiable, because the report
path runs on the pool's worker goroutines and the transfer progress callbacks
([internal/daemon/pool.go:280](../internal/daemon/pool.go),
[internal/daemon/runtime.go:1064](../internal/daemon/runtime.go)) - a status
consumer must never apply backpressure to replication.

Overflow stays terminal, as with any lossy edge: a consumer that falls behind is
told its sequence is broken rather than handed a gap with a counter beside it.
The difference from a shared history is that the failure is now the slow
consumer's alone, and the memory is per subscriber - 256 events each, with one
or two subscribers in practice - rather than one global buffer sized for the
worst of them.

**3.2 Bootstrapping without a gap.** Subscribe first, then take the snapshot,
then discard queued events at or before the snapshot's revision. That ordering
is why `Event` keeps the revision it was recorded at even though no caller now
passes a cursor: it is what makes the seam between the snapshot and the stream
exact rather than approximate.

A retained history would have papered over late subscription; publication does
not, and the events a subscriber missed are gone. That is a real constraint, not a
free simplification: a test must subscribe before the action it observes, where
today its helpers look back at whatever `Status()` happens to hold. It is the
better constraint, because "subscribe, act, wait" cannot silently observe the
wrong occurrence of a repeated state the way a backward look can, but it does
mean the helpers in chunk E are written around the subscription rather than
around the assertion.

**3.3 Progress is conflated, not queued.** Progress reaches a watcher today
through the same path as everything else: each sample goes through
`recordProgress` into `StatusStore.Record`
([internal/daemon/runtime.go:1064](../internal/daemon/runtime.go)), moves the
revision, and wakes `WatchStatus` into sending a full snapshot. Samples are
produced at most every 250ms per transfer, from inside the stream's write path
([internal/zfs/stream.go:259](../internal/zfs/stream.go),
[internal/replication/rpc/client.go:261](../internal/replication/rpc/client.go)),
so a watcher sees bytes, rate and ETA move at up to four updates a second for
each running transfer. The interval plays no part in that.

A transition and a progress sample are different kinds of message, and they
need different delivery. Every transition matters, in order, so transitions are
queued and overflow is terminal (3.1). Only the newest progress sample for a job
matters - a rate from two samples ago is not information, it is a wrong number -
so progress is conflated: each subscriber holds one latest-value slot per job
and a one-element wake signal. Publishing a sample overwrites the slot and
signals without blocking; a subscriber that was slow simply reads the newest
sample when it next looks. The slots are bounded by the transfers running at
once, which the transfer worker counts bound
([internal/config/config.go:16](../internal/config/config.go)), so a progress
subscription cannot overflow and needs no terminal edge.

That is also why progress cannot share the transition queue. Deduplicating
consecutive identical events to keep samples from crowding it out would erase
the second of two identical failed attempts, which is the sequence
`TestGuestDaemonRemoteBackoff` counts; and queueing samples at all spends the
queue bound on numbers that are stale by the time they are read. Separate entry
points on the store - `Record` for transitions, `RecordProgress` for samples -
match call paths that are already distinct.

**A stalled transfer currently reports nothing.** A sample is emitted only when
bytes are written, and the rate is the cumulative average since the transfer
began ([internal/zfs/stream.go:267](../internal/zfs/stream.go),
[internal/replication/rpc/client.go:203](../internal/replication/rpc/client.go)).
A transfer that stops moving emits no further samples, so its last rate and ETA
stay on screen unchanged - and today's interval re-sends exactly those numbers,
so it never covered this either. Two producer-side fixes: emit on a ticker for
as long as a transfer is active, not only on write, so a stall is visible as a
stall; and report the rate over a recent window rather than since the start, so
it falls to zero when the bytes do, instead of decaying toward an average that
hides the stall for minutes. That ticker is sampling a counter to publish a time
series - time-driven by intent, like the backoff timers - not polling for a
condition.

**3.4 `WatchStatus` sends what it receives.** Two additive fields on
`WatchStatusResponse` ([proto/boomerangz/control/v1/control.proto:30](../proto/boomerangz/control/v1/control.proto)):
the transitions observed since the previous message, and a flag marking a
message as a resync after an overflow ended the server's subscription. The
snapshot stays as it is, so existing clients are unaffected and a client joining
mid-stream still gets a self-describing message. The handler subscribes, sends
the snapshot, and then waits on both the transition queue and the progress
signal. A wake sends every queued transition in order, together with a snapshot
that by construction carries the newest progress for every running job.

Progress-only wakes are coalesced to at most one message per 250ms per stream,
matching the producer's own sample cadence. With the default of three transfer
workers that bounds a watcher at four messages a second rather than twelve, and
it never delays a transition, which sends immediately. While a `Send` is
blocked, samples keep overwriting their slots, so a slow client costs itself
freshness and nothing else. That limit is a server-side property of conflated
data; it is not a client-chosen heartbeat, and it is not the interval coming
back.

`Runtime.WaitStatus` exists for exactly this handler
([internal/control/service.go:18](../internal/control/service.go),
[:83](../internal/control/service.go)) and is retired with it, along with the
revision-waiting on `StatusStore`. The interface the control service depends on
gains `Subscribe` and loses `WaitStatus`; two test fakes follow
([internal/control/control_test.go:55](../internal/control/control_test.go),
[internal/cli/control_test.go:31](../internal/cli/control_test.go)).

**3.5 The CLI shows them.** Interactive `--watch` gains a short transition tail
under the table - the thing an operator is actually watching for is a change,
and today the only way to see one is to be looking at the right moment.
Redirected `--json` includes the transitions in each object, which is additive
for anything reading `.datasets` or `.jobs` today. `docs/reference/cli.md` and
the operations guide say what the new field is, and that a resync marks the one
case where the stream is not continuous. No flag changes, so the completions in `contrib/` are
untouched; the man page gains a sentence with the docs.

This is the part I would have cut for being bigger than the tests needed. It is
the half of the defect a user can hit.

**3.6 The watch interval retires.** `--interval` was specified before the
stream was: the plan has non-interactive watch "emit newline-delimited JSON at
`--interval`" ([PLAN.md:1025](../PLAN.md)), which is `watch(1)` - sample every N
seconds. The control plane landed with revision waiting in the same commit
(`b007c3a`), so the flag never had that meaning in practice; it became "send on
change, and at least every N", and the help text says so ("Maximum interval
between watch updates", [internal/cli/commands.go:172](../internal/cli/commands.go)).
Its original purpose is gone. What remains is a heartbeat, and the heartbeat is
quietly covering for two real gaps rather than doing a job of its own.

*State that changes without an event.* Of the non-job state a snapshot carries,
only a config reload records a transition
([internal/daemon/runtime.go:330](../internal/daemon/runtime.go)). A published
discovery generation, the set of datasets and their activation, and the
scheduler's next-snapshot deadlines all change silently. A watcher sees a newly
enabled dataset when some unrelated job happens to transition, or when the
interval expires - up to two seconds by default, indefinitely if the client
asked for an hour. That is this document's defect in another place: a state
change that is not an event. Publish them - a generation, an activation change,
a deadline change - on the same stream, and the snapshot a watcher holds is
never stale while connected. This also supplies the generation anchor 4.3 needs,
so 4.3 stops being a test-only addition.

*A peer that has gone away without saying so.* No gRPC keepalive is configured
on either side. Over the local socket that does not matter - a dead peer closes
it. Over a paired TCP listener, the periodic `Send` is incidentally the only
thing that makes the server notice a vanished client, and nothing at all makes
the client notice a vanished server: `Recv` has no deadline and the absence of a
heartbeat is never checked ([internal/cli/control.go:73](../internal/cli/control.go)).
Under publication, a silently dead client would hold its subscription until its
queue overflowed. Liveness belongs to the transport: server and client
keepalive parameters, with an enforcement policy, on the TCP listeners.

With both addressed the interval has no remaining function, and it should go
rather than linger as a knob whose effect nobody can describe. `--interval` is
removed from the CLI, `interval_milliseconds` is reserved in the proto so an
older client still sending it is not misread, and the flag leaves
`docs/reference/cli.md`, the operations guide
([docs/operations/index.md:30](../docs/operations/index.md)), the man page and
all three completions in the same change. Whether removal passes through a
release as a hidden, ignored flag first is a compatibility decision for whoever
cuts that release, not a design question; the design's position is that the
flag has no behaviour to preserve.

If a client wants to redraw on a clock - an elapsed time, a countdown to the
next snapshot - that is a client-side ticker over the snapshot it holds. It
needs nothing from the server.

## 4. What the tests then stop doing

**4.1 In-process waiter.** A helper in `internal/testutil` taking a runtime, a
predicate over `Event`, and a bound; selecting on its subscription rather than
sleeping; returning the first matching transition; and on expiry failing with
every transition it received while waiting. Replaces the four helpers in 2.3.
The bound stays a hard failure: this removes sampling, not deadlines.

**4.2 Out-of-process waiter.** A client in the control test package that dials
the socket those tests already hold credentials for and consumes `WatchStatus`,
replacing the `boomerangz status` poll loops. Socket appearance stays a poll - a
missing file has no notifier short of inotify, and the bound is short.

**4.3 A generation anchor for negative assertions.** The restart test sleeps
500ms and then asserts that no further snapshot appeared
([test/integration/control/guest_test.go:285](../test/integration/control/guest_test.go)).
The sleep is a guess at how long "nothing else happened" has to be to mean
something. With generations published as events (3.6), a waiter can block
until the generation ID exceeds one it captured
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

**Waking discovery from ZFS events.** The scanner rescans on a ticker because
nothing pushes ZFS changes to the daemon. OpenZFS does emit them: the kernel
module posts a zevent for pool history, and the upstream zedlet
`history_event-zfs-list-cacher.sh`, shipped in `zfs-utils`, already uses them to
notice `create`, `destroy`, `rename`, `finish receiving`, `set` and `inherit` -
user properties included. zed and zevents are part of OpenZFS itself rather than
a distribution addition, and this project is Linux-only
([PLAN.md:29](../PLAN.md)), so platform variation is not what shapes the
question. Three things observed on OpenZFS 2.4.4 do:

- *The daemon cannot read zevents.* `/dev/zfs` is world-writable, but the events
  ioctl refuses an unprivileged caller - `zpool events` as an ordinary user fails
  with `cannot get event: permission denied` - and `zfs allow` cannot delegate
  it. The daemon runs as the delegated service account, so it cannot be a zevent
  consumer. A root-run zedlet can: `history_event` calling the existing
  `Reconcile` RPC, which already coalesces an explicit discovery hint
  ([internal/control/service.go:112](../internal/control/service.go)), over the
  local socket the daemon already serves.
- *That makes zed a deployment dependency, and whether it runs is the
  distribution's call.* OpenZFS ships a systemd preset that enables
  `zfs-zed.service` (`/usr/lib/systemd/system-preset/50-zfs.preset`), but a
  preset only takes effect where the distribution applies presets on install.
  Arch does not enable services from packages, so on the Arch-family
  development host the unit is installed, preset to enabled, and disabled; and
  nothing in `contrib/` or the integration target enables it. That is the real
  variation - not zed's presence or behaviour, which come from OpenZFS, but
  whether anything turned it on. Requiring it is a packaging decision with
  consequences beyond this project - zed also runs the fault-handling and
  notification zedlets - and not one to take as a side effect of a latency
  improvement.
- *Delivery is a wake, never the truth.* The kernel queue is bounded
  (`zfs_zevent_len_max`, default 512, in `zfs(4)`), `zed(8)` documents missing
  events and grows that buffer to compensate, and event IDs restart when the
  module reloads. The upstream zedlet treats an event accordingly: it re-lists
  the pool rather than trusting the payload.

So the shape is settled even though the decision is not: a zedlet that nudges
`Reconcile`, with the periodic scan left underneath unchanged as the correctness
floor. Built that way it cannot make the daemon miss a change - a lost event
costs what a missed wake costs today, one interval of latency - and it does not
remove any scanning, because lengthening the interval is exactly what would turn
a lost event into a missed change. It is a latency improvement with a packaging
dependency, and nothing has yet measured discovery latency as a problem. It is
deferred on that ground: an optimisation without a demonstrated need, shipped
through a system daemon the project does not currently require.

**Resumable watching.** A client that loses its stream and reconnects gets a
fresh snapshot and the transitions from then on; whatever happened during the
disconnect is not recoverable. Making it recoverable means retained history and
a cursor the client presents on reconnect - the pull model this design started
with, on top of the push one rather than instead of it. Neither consumer wants
it today: the CLI does not reconnect at all, it returns the stream error
([internal/cli/control.go:74](../internal/cli/control.go)), and a test subscribes
for the duration of what it is watching. It becomes worth building when
something consumes this stream as a record - an exporter, an audit trail - at
which point the honest form is durable and on disk, not a larger queue.

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

**Chunk A - publication.** 3.1 through 3.3's delivery model. Unit tests for
delivery order, the non-blocking publish, overflow terminating one subscription
without touching another or the producer, and conflation: a burst of samples
leaves exactly the newest per job, and cannot overflow or displace a queued
transition. Done when a subscriber receives every transition recorded after it
subscribed, or is told its subscription ended, and always reads the newest
progress for every running job.

**Chunk B - the control plane carries transitions.** 3.4 and 3.5, retiring
`WaitStatus` with them, and the docs in the same change. Done when
`status --watch --json` emits every transition a job made while the client was
connected, and a test asserts that a sequence which collapses in the snapshot
survives in the stream.

**Chunk C - nothing a watcher holds goes stale.** 3.6: publish generations,
activation changes and next-snapshot deadlines; configure transport keepalive on
the TCP listeners; remove `--interval` and reserve its proto field, with docs,
man page and completions. And 3.3's producer fixes: progress emitted on a ticker
while a transfer is active, and a windowed rate. Done when a watcher's snapshot
reflects a newly enabled dataset without any job transitioning, a stalled
transfer's rate reaches zero within a few samples, a client over TCP detects a
vanished daemon within the keepalive bound, and nothing in the tree describes a
watch interval.

**Chunk D - the listener drain.** Section 5's last paragraph, independent of the
rest. Done when a call in flight across a reload completes, with a test.

**Chunk E - the in-process waiter.** 4.1, converting the four helpers. Done when
nothing in `test/integration/daemon/` sleeps while watching status, and a forced
failure prints the transitions observed.

**Chunk F - the out-of-process waiter.** 4.2, on chunk B's stream. Done when the
control suite watches the daemon over the socket it is testing rather than by
re-running the CLI.

**Chunk G - the generation anchor.** 4.3, on chunk C's generation events. Done
when no bare sleep remains in `test/integration/control/`.

**Chunk H - ZFS waits behind transitions.** 4.4, last because it depends on E
and changes what those tests assert rather than how they wait.

A through C are the defect: the contract, the stream that carries it, and the
state changes it was missing. D is an unrelated sleep fixed while nearby. E
through H are the cleanup the fix makes possible, and each is independently
droppable without leaving the contract half-changed.

## 7. Risks

- **A silently lossy stream is the original bug.** Overflow has to end the
  subscription at every layer that carries it - the channel, the RPC, the CLI -
  and a test waiter must fail on it rather than continue against a gap. The
  failure mode to design against is a consumer that keeps going.
- **Queue depth is bounded by evidence, not derived from it.** 256 per
  subscriber is sized against a peak of 103 transitions a minute in a
  deliberately aggressive test daemon, against consumers that read within
  milliseconds of a wake. Chunk A exposes the high-water mark; a real deployment
  approaching it is a signal to look at what is producing transitions at that
  rate before enlarging anything.
- **Subscribe-before-act is a requirement, not a convention.** A helper that
  subscribes after the action it observes waits for an event that has already
  been published, and the failure looks like the daemon never did the work.
  Chunk E's waiter should take the subscription in its constructor so the
  ordering is structural rather than remembered.
- **Additive proto fields still change output.** `--json` consumers gain a
  field; that is compatible for anything selecting known keys and not for
  anything asserting an exact object. The docs change lands with the code.
- **A lossless stream invites over-specified tests.** Being able to assert on
  every intermediate state does not mean a test should. The waiter's predicate
  should name the transition the behaviour is about, not transcribe the
  sequence, or the suite gets brittle in a new way.
- **More test code coupled to the status vocabulary.** Job IDs and state names
  become load-bearing in more places. They are already public - the CLI prints
  them - but chunk H should not invent states to make a wait convenient.
