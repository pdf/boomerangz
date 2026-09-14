# Design: event-driven waits

Status: chunks A, B, C, D, E, and J have landed; F, G, H, and I have not.

The daemon's status contract is a map of the latest event per job. Everything
that wants to know what the daemon did - a test, an operator, a log pipeline -
reads that map repeatedly and cannot tell whether it moved in between. It does
move, and what happened in between is gone.

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
([internal/control/service.go:69](../internal/control/service.go)). Section 3.8
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
who wants a sequence has no option but to read it often. That is why four waits
in the daemon stage loop over `runtime.Status()` on a 25-100ms sleep:
`waitForTransferSuccesses`
([test/integration/daemon/guest_test.go:124](../test/integration/daemon/guest_test.go)),
`waitForEvent` in `TestGuestRemoteOutageReconnection`
([:441](../test/integration/daemon/guest_test.go)), `waitForJobState`
([test/integration/daemon/native_test.go:84](../test/integration/daemon/native_test.go)),
and the inline attempt loop in `TestGuestDaemonRemoteBackoff`
([test/integration/daemon/native_test.go:316-325](../test/integration/daemon/native_test.go)),
the test 2.2 names. Two more poll the pool with `InspectState` while waiting for
the daemon: the scheduling test's `waitFor`
([test/integration/daemon/guest_test.go:199](../test/integration/daemon/guest_test.go))
and the outage test's loops for its snapshot holds
([:460-497](../test/integration/daemon/guest_test.go)). And the control suite
re-runs `boomerangz status` in a loop
([test/integration/control/guest_test.go:82](../test/integration/control/guest_test.go),
[:237](../test/integration/control/guest_test.go),
[test/integration/control/adversarial_test.go:91](../test/integration/control/adversarial_test.go)).
Those loops are a symptom. Faster polling shrinks the window and never closes
it.

`waitForSendIntervals`
([test/integration/daemon/guest_test.go:101](../test/integration/daemon/guest_test.go))
also loops, but over the test's own `observedLocalStream` fake rather than
status. Its condition is one the test process owns, so it wants
broadcast-and-recheck on the fake (1), not a status subscription.

The cost is not only correctness. The guest is 2 vCPUs and 4 GiB
([test/integration/targets/cachyos/config.sh:16](../test/integration/targets/cachyos/config.sh)),
every `InspectState` forks `zfs`, and a 100ms poll is up to ten processes a
second competing with the daemon whose timing the same test is asserting on.
The CI failure that started this work was a starved management worker.

**2.4 The snapshot is assembled from shared memory.** The map is one instance of
a wider shape: status is state that several components write under their own
locks and readers reach into. `ControlStatus` reads the store, then the
scheduler's deadlines, then takes `r.mu` and reads every queue while holding it
([internal/daemon/runtime.go:1228-1233](../internal/daemon/runtime.go)). Nothing
makes those reads one view. `applyGeneration` updates the scheduler
([internal/daemon/runtime.go:452](../internal/daemon/runtime.go)) before it
updates the active set under `r.mu` ([:484](../internal/daemon/runtime.go)), so
a snapshot taken between them shows new deadlines beside the old active set.
`Pool.emit` reads the queue twice under separate locks - `Pending` from one
`Snapshot`, `Position` from a second inside `Position`
([internal/daemon/pool.go:242](../internal/daemon/pool.go),
[internal/daemon/queue.go:221](../internal/daemon/queue.go)) - so the two fields
of one event can disagree. And `r.mu` alone guards ten unrelated fields
([internal/daemon/runtime.go:102-117](../internal/daemon/runtime.go)).

**2.5 A progress sample stands in for a transition.** `Event` has no field that
says what kind of message it is
([internal/daemonstate/status.go:8](../internal/daemonstate/status.go)). A
progress sample is an `Event` whose `State` `recordProgress` hard-codes to
`sending` ([internal/daemon/runtime.go:1064-1069](../internal/daemon/runtime.go)),
and for remote jobs that sample is the only record of the job starting to send.
The remote job's start state is `probing`
([internal/daemon/runtime.go:1006](../internal/daemon/runtime.go)); nothing
records `sending` except the first sample that reaches `recordProgress`. The
local job has the opposite fault: its start state is `sending`
([internal/daemon/runtime.go:889](../internal/daemon/runtime.go)), published
before planning, holds, and estimation, when no stream exists yet. Neither job
reports the verification and property reconciliation that follow the stream.
Section 3.4 traces where the decision to send is actually made.

## 3. The contract change

**3.1 One goroutine owns status.** Status is asynchronous results passing from
producers to consumers. The Go wiki's guidance on
[mutex or channel](https://go.dev/wiki/MutexOrChannel) lists channels for
"communicating async results" and mutexes for "state"; the defect in 2 and the
assembly in 2.4 are what sharing that memory costs. `FairQueue` and `Scheduler`
are state in the wiki's sense and keep their mutexes. What they publish becomes
messages.

`StatusStore` is replaced by a status owner: one goroutine that owns job states,
latest progress, the runtime's dataset view, deadlines, queue views, and the
subscriber set. No other goroutine reads or writes that memory. Producers send
transitions and views on one input channel, and progress samples on a second
(3.3):

```go
type statusMessage struct {
	event     *Event               // a transition (3.3)
	runtime   *runtimeView         // discovery and configuration generations; known, active, recursive datasets
	deadlines map[string]time.Time // scheduler next-snapshot deadlines
	queue     *queueView           // pool name and a detached QueueSnapshot
	subscribe *subscribeRequest    // register a subscriber; reply carries its forwarder
	snapshot  *snapshotRequest     // reply carries a detached copy of the state
}
```

Every read becomes a request on that same channel - `subscribe`, `snapshot` (for
`GetStatus`, `ListDatasets`, and `Runtime.Status()`), and unsubscribe - carrying
a reply channel:

```go
for {
	select {
	case m := <-s.input:
		switch {
		case m.subscribe != nil:
			fwd := newForwarder(s.state.clone(), m.subscribe.policy)
			s.subscribers[fwd] = struct{}{}
			m.subscribe.reply <- fwd
		case m.snapshot != nil:
			m.snapshot.reply <- s.state.clone()
		default:
			s.apply(m)  // update owned state
			s.fanOut(m) // non-blocking send to each subscriber's forwarder (3.2)
		}
	case <-ctx.Done():
		return
	}
}
```

It is one channel because `select` chooses at random among ready cases. Separate
channels for events and views would lose the order in which one producer sent
them, and a separate request channel would lose read-your-writes: a buffered
channel is first in, first out, so a request sent after a change was sent is
answered after that change is applied, while a request on its own channel could
be answered first. Today a status read after `config reload` returns sees the
new configuration generation, because `commitConfig` bumps it and records the
reload before returning ([internal/daemon/runtime.go:319-331](../internal/daemon/runtime.go),
[:353](../internal/daemon/runtime.go)); one channel keeps that true. A flush
request, which waits until the log writer has taken everything sent before it
(3.9), rides the same channel for the same reason.

Progress samples are the exception, and they take a second channel so that
replication never waits on status for one. A producer offers a sample without
blocking and drops it when that channel is full; the next sample replaces it
anyway. The owner reads the progress channel only when the input channel is
empty, with a nested `select` that checks input first. A sample therefore has no
order against the transitions around it, and does not need one: it names the
sending transition it belongs to (3.3).

The loop holds no lock and holds no reference to any other component, so it
never calls out. That is what lets a producer send while holding its own lock
without risking a deadlock, and it is a property of the loop's shape rather than
a lock-ordering rule to remember.

Ordering within a producer is the producer's job. The scheduler and the queues
change from several goroutines, so each sends its view while holding the lock
that serializes its changes: `Scheduler.mu` in `Update`, `Complete`, and `Retry`
([internal/daemon/scheduler.go:63](../internal/daemon/scheduler.go),
[:103](../internal/daemon/scheduler.go), [:117](../internal/daemon/scheduler.go)),
and `FairQueue.mu` in `Offer`, `Pop`, `RemoveScope`, and `DiscardAll`
([internal/daemon/queue.go:70](../internal/daemon/queue.go),
[:96](../internal/daemon/queue.go), [:131](../internal/daemon/queue.go),
[:165](../internal/daemon/queue.go)). `applyGeneration` still sends deadlines
and the runtime view as two messages, so a subscriber can briefly hold the new
deadlines beside the old active set, as `ControlStatus` can today (2.4).

The pool breaks that rule today. `Pool.Submit` offers the job to the queue and
only then emits `pending-<pool>`
([internal/daemon/pool.go:224-230](../internal/daemon/pool.go)), while a worker
can pop the job and emit its start state in between
([internal/daemon/pool.go:249-273](../internal/daemon/pool.go)). Read from the
code, not observed in a run: a job can record `running` - or, if it is quick, its
outcome - before `pending`, and the status map then shows a started or finished
job as pending. Two sends for one job come from different goroutines with nothing
ordering them. So `Offer` sends the queue view and the `pending-<pool>`
transition as one message under `FairQueue.mu`, the lock `Pop` must take before
the job can start. Sending `pending` before `Offer` would instead publish it for a
job a full or closed queue then rejects. This depends on `Pool.emit` no longer
reading the queue (below), since today it takes the queue lock itself.

With queue views owned here, the owner stamps `Pending` and `Position` onto job
rows from the newest queue view, and `Pool.emit` stops reading the queue. The
double read in 2.4 goes with it.

The producer guarantee needs restating precisely. The report path runs on the
pool's worker goroutines and the transfer progress callbacks
([internal/daemon/pool.go:280](../internal/daemon/pool.go),
[internal/daemon/runtime.go:1064](../internal/daemon/runtime.go)), and a status
consumer must never apply backpressure to replication. A send on the owner's
buffered input can block, but only on the owner's in-memory work: the owner never
waits on a consumer, because every send into a forwarder is non-blocking (3.2).
That is weaker than "the producer never blocks" and it is the guarantee this
design offers for transitions and views. A progress sample never blocks at all.

**3.2 Delivery: a forwarder per subscriber.** Each subscriber gets a forwarder
goroutine that decouples the owner from the consumer's pace, fed by two
channels:

```go
// Update is one delivery to a subscriber.
type Update struct {
	Transitions []Event  // every transition since the previous Update, in order
	State       Snapshot // newest state as of the last merged message
}

type forwarder struct {
	transitions chan delivery  // capacity transitionBound; a transition and the revision it produced
	state       chan *Snapshot // capacity one; conflated by the owner
	out         chan Update
}
```

The owner sends a transition before the state it produced, and sends both
without blocking. A refused transition means the subscriber has fallen behind.
State cannot be refused: the owner is the only sender on a one-slot channel, so
it takes back any state the forwarder has not read and sends the newest in its
place. A state is an immutable snapshot the owner builds once per change and
shares; the forwarder clones it into the `Update` it delivers.

The forwarder prefers transitions, with a nested `select` that drains the
transition channel first, and stops taking them while it holds
`transitionBound` undelivered, so that a consumer which stops reading fills the
channel and the owner's refused send is what ends the subscription. After
taking a state it drains the transition channel without blocking. That is sound
because the single owner sent every transition a state includes before the
state itself, so they are already in the channel. Each transition carries the
revision it produced, and the forwarder offers an `Update` only once its state's
revision covers the newest transition it holds, so an `Update` never pairs a
transition with a state from before it:

```go
for {
	if len(pending) < transitionBound {
		select {
		case d := <-f.transitions:
			take(d) // append to pending; covered = d.revision
			continue
		default:
		}
	}
	var out chan<- Update
	if state != nil && (len(pending) > 0 && state.Revision >= covered || len(pending) == 0 && stateDue()) {
		out = f.out
	}
	select {
	case d := <-f.transitions: // nil while holding a full bound
		take(d)
	case state = <-f.state:
		drainTransitions()
	case out <- Update{Transitions: pending, State: clone(state)}:
		pending = nil
	case <-f.overflowed: // closed by the owner on a refused transition
		f.fail(ErrSubscriberOverflow)
		return
	case <-f.done:
		return
	}
}
```

```go
// Subscribe registers a subscriber. The first Update carries the full state at
// registration and no transitions; each later Update carries the transitions
// since the previous one, in order, and the state as of the last of them. The
// channel is closed when the subscription ends; Err then reports why, and is
// ErrSubscriberOverflow when the subscriber fell behind. Cancelling ctx ends it.
func (s *Status) Subscribe(ctx context.Context) (*Subscription, error)

func (u *Subscription) Updates() <-chan Update
func (u *Subscription) Err() error
```

Three properties follow from the owner handling `subscribe` between two input
messages. The initial state reflects everything already applied and the
forwarder receives everything after it, so the seam between snapshot and stream
is exact. Each `Update` pairs its transitions with the state they produced in
one value, so no subscriber reconciles two channels: the forwarder does that
once, by revision. And overflow is detected at one forwarder's channel and ends
one subscription.

Overflow stays terminal, as with any lossy edge: a consumer that falls behind is
told its sequence is broken rather than handed a gap with a counter beside it.
The one exception is the daemon's own log, which cannot be ended and marks its
gaps instead (3.9).
The failure is the slow consumer's alone, and the memory is per subscriber -
256 transitions each, set from chunk B's measurement (7), with one or two
subscribers in practice - rather than one global buffer sized for the worst of them. A forwarder holds back state-only
updates to one per 250ms and sends transitions immediately (3.6).

The events a subscriber missed before subscribing are gone. That is a real
constraint, not a free simplification: a test must subscribe before the action
it observes, where today its helpers look back at whatever `Status()` happens to
hold. It is the better constraint, because "subscribe, act, wait" cannot
silently observe the wrong occurrence of a repeated state the way a backward
look can, but it does mean the helpers in chunk F are written around the
subscription rather than around the assertion.

**3.3 Events carry a kind; progress is conflated.** `Event` today is a job's
identity, state, and reason, its queue position, and its progress fields in one
struct ([internal/daemonstate/status.go:8-23](../internal/daemonstate/status.go)).
Nothing in it separates a transition from a progress sample, and no existing
field can stand in: `State` is `sending` for both a local job's start
([internal/daemon/runtime.go:889](../internal/daemon/runtime.go)) and every
sample, and `Bytes` is zero for both the pool's transition and the sample each
stream emits before it copies anything
([internal/zfs/stream.go:395](../internal/zfs/stream.go),
[internal/replication/rpc/client.go:249](../internal/replication/rpc/client.go)).
So `Event` gains a `Kind EventKind` field:

```go
type EventKind uint8

const (
	EventTransition EventKind = iota + 1 // queued, lossless, in order
	EventProgress                        // conflated to the newest per job
)
```

The zero value is invalid, so an `Event` built without a kind is rejected by the
owner rather than guessed at. The transitions a watcher receives on the wire are
only ever transitions, so the kind needs no proto field.

A transition and a progress sample need different delivery. Every transition
matters, in order, so transitions are queued and overflow is terminal (3.2).
Only the newest progress sample for a job matters - a rate from two samples ago
is not information, it is a wrong number - so progress is conflated. The owner
applies two rules. A progress sample for a job whose latest transition is not
`sending` is discarded, so a sample that arrives late can never overwrite a
finished job. A transition that moves a job out of `sending` clears that job's
progress.

`sending` alone does not identify a stream, and samples have no order against
transitions (3.1). A resume reports `sending` twice (3.4), so a first-pass
sample read after the second `sending` would pass that rule and overwrite the
new stream's progress. So the daemon numbers each `sending` transition, and
every sample carries the number of the `sending` it was taken under: the
transfer reporter takes a new number from a runtime-wide counter on each
`PhaseSending` and stamps it on the transition and on each sample until the
next one. The owner stores the number on the row and discards a sample whose
number differs. A runtime-wide counter rather than one per job keeps the number
distinct across runs of the same job too. Held progress is therefore bounded by the transfers running at once,
which the transfer worker counts bound
([internal/config/config.go:16-17](../internal/config/config.go)). A sample
reaches a subscriber only as the state it changed, on the conflated state
channel, never on the transition channel, so progress cannot overflow a
subscription however many samples a slow consumer misses.

Progress cannot share the transition queue. Deduplicating consecutive identical
events to keep samples from crowding it out would erase the second of two
identical failed attempts, which is the sequence `TestGuestDaemonRemoteBackoff`
counts; and queueing samples at all spends the queue bound on numbers that are
stale by the time they are read.

Today a progress sample reaches a watcher through the same path as everything
else: each sample goes through `recordProgress` into `StatusStore.Record`
([internal/daemon/runtime.go:1064](../internal/daemon/runtime.go)), moves the
revision, and wakes `WatchStatus` into sending a full snapshot. Samples are
produced at most every 250ms per transfer, from inside the stream's write path
([internal/zfs/stream.go:259](../internal/zfs/stream.go),
[internal/replication/rpc/client.go:261](../internal/replication/rpc/client.go)),
so a watcher sees bytes, rate and ETA move at up to four updates a second for
each running transfer. The interval plays no part in that, and nothing in this
section slows it.

**3.4 Transfers report their phases.** Once a progress sample is a kind that
never changes a job's state (3.3), the transition 2.5 describes has to come from
wherever the decision to send is made. It is made three calls below the job.

For a remote job, the worker publishes `probing` and runs
`Roadwarrior.Reconcile` with a callback into `recordProgress`
([internal/daemon/runtime.go:1006-1015](../internal/daemon/runtime.go)).
`Reconcile` returns early inside a backoff window; otherwise it calls
`engine.Apply` up to twice, the second time only when the first plan's mode was
`resume` ([internal/transfer/recovery.go:270-289](../internal/transfer/recovery.go)).
It learns whether a stream ran only when `Apply` returns. The engine is
`remoteApplier`, which opens the transport - this is the actual probe - and
hands the same callback to a `transfer.Local` built by `NewRemoteWithService`
([internal/daemon/runtime.go:41-52](../internal/daemon/runtime.go),
[internal/transfer/local.go:127](../internal/transfer/local.go)). A local job
reaches the same `Local.Apply` directly
([internal/daemon/runtime.go:903-917](../internal/daemon/runtime.go)).

`Local.Apply` plans, checks permissions, prepares receive parents, sets the
target binding, places recovery holds, reloads, and re-plans. Only then, and only
if `plan.Mode != "up-to-date"`, does it estimate and run the stream
([internal/transfer/local.go:479-484](../internal/transfer/local.go)). After the
stream it verifies GUIDs, checks the binding and references, reconciles
destination properties, and verifies again, none of which reports anything.
Both remote stream implementations emit their first sample after both ends have
started and before the first byte
([internal/zfs/stream.go:395](../internal/zfs/stream.go) through `RunPipeline`
for SSH, [internal/replication/rpc/client.go:249](../internal/replication/rpc/client.go)
for native), and their last before `Run` returns
([internal/zfs/stream.go:407](../internal/zfs/stream.go),
[internal/replication/rpc/client.go:281](../internal/replication/rpc/client.go)).

So today a remote job shows `probing` through connection, planning, holds, and
estimation; `sending` from the first sample through verification; and never
`sending` at all against an up-to-date target. A resume shows as one continuous
`sending` whose byte count falls back to zero when the second stream starts. A
local job shows `sending` from before it has planned.

The engine reports phases and progress through one ordered callback, replacing
`report func(zfs.Progress)` on `Apply`:

```go
// Report is one ordered observation from a transfer: a phase change or a
// progress sample.
type Report struct {
	Phase    Phase         // set on a phase change
	Progress *zfs.Progress // set on a sample
}
```

`Local.Apply` reports `PhaseSending` immediately before `EstimateSend` and
`stream.Run`, and `PhaseVerifying` after `Run` returns. The daemon turns a phase
into an `EventTransition` and a sample into an `EventProgress`, and
`recordProgress` stops setting `State`. One callback keeps phases and samples in
the order they happened, which is what 3.3's discard rule relies on: a stream's
last sample is emitted inside `Run`, so it always precedes `PhaseVerifying`.

A remote job then shows `probing`, `sending`, `verifying`, and its outcome, and
an up-to-date remote job shows `probing` and its outcome, which is accurate. A
resume whose second pass has newer state to send shows a second `sending`
transition, so the counter falling back to zero has a visible cause; a second
pass that finds the target up to date sends nothing and reports no second
`sending`. A local job's start state becomes `planning`. The new
states reach users through `status`; job states are not documented in `docs/`
today, and 3.7 adds the list.

Two alternatives do not work. Treating a stream's zero-byte first sample as the
start marker relies on both stream implementations happening to emit before
copying, which is the same unstated coupling as `recordProgress` hard-coding
`sending`. And `Roadwarrior` cannot decide, because it knows whether a stream ran
only after `Apply` returns.

*Identity.* A phase says what a transfer is doing, not what it is doing it to,
and neither does any other transition. An event names its job, and a job ID
names work that recurs: `snapshot:tank/data` is every snapshot of that dataset,
and `remote:tank/data:offsite` every transfer to that remote. So neither an
operator nor a test can tie a transition to an object. Which snapshot did that
`succeeded` create? Which one did this `waiting-retry` fail to send? And which
of two runs' transitions belong together, other than by order? A test that
needs those answers falls back on reading the pool at a moment it chooses, and
then on how long the daemon takes to do the next thing, and so on a cadence
or a grace period holding off the daemon's next action.

Any event tied to an identity carries that identity, as typed fields rather
than text in `Reason`, since a consumer that matches on text breaks when the
wording changes. `Event` gains a run ID and an `Identity`:

| Field | Set on | Meaning |
|---|---|---|
| `RunID` | every transition of a job | one run: its `pending-<pool>`, start state, phases, and outcome; a configuration reload is a run of its own |
| `Snapshot` | `snapshot:` `succeeded` / `scheduled` | the snapshot created / the owned snapshot that set the deadline |
| | transfer `sending` / `succeeded` | the source snapshot sent / the snapshot now on the destination |
| | transfer `waiting-retry`, `blocked`, `cancelled` | the pending snapshot a run carried, or, if it ended before taking one, the one pending then; none when cancelled still queued |
| `Base`, `Mode` | transfer `sending` | the incremental base, and the plan's mode |
| `Destination` | transfer `succeeded` | the destination dataset |
| `Marker` | `inactive:` `succeeded` | `set`, `cleared`, or `none` |
| `Destroyed`, `DestroyedCount` | `prune:`, `retire:` `succeeded` | the snapshots destroyed, and how many |
| `ConfigGeneration` | `config:reload` `succeeded` | the generation published |

The run ID comes from one process-wide counter, so it is unique across pools
and restarts with the daemon. A job takes its ID when it is built, as ordinary
data on the `Job`, so every copy carries it, including the one its own `Run`
closure holds; the queue refuses a job without one. The first cut assigned the
ID in `Submit`, and the phase transitions a transfer reports from inside `Run`
came out without it, because the closure had captured the job before `Submit`
gave the queue's copy an ID. `Send` (3.3) stays internal: it numbers streams
within a run and exists for the owner's discard rule. A transfer that leaves its pool without running -
removed on deactivation, discarded by a reload or shutdown, or popped by a
stopped pool - records `cancelled` with its run ID but names no snapshot.
The pool records that transition under the queue lock, in the message whose
queue view removes the job, and knows nothing of pending snapshots. A first cut
had a transfer job carry a function naming its pending snapshot for the pool
to call. That read the pending set's lock inside the queue lock, which is safe
only while `PendingSet` never calls out - an ordering rule nothing would
enforce, against a lock that orders `pending-<pool>` and `cancelled` against a
worker's start. Recording the snapshot on the job when it is queued would go
stale, since a newer coalesced snapshot does not replace a queued job, and
building the transition after the lock is released would give up the
same-message guarantee. The run never took a snapshot, and one pending stays
pending and is named by the next run that ends with it, so the name is left
out rather than any of those paid for it.

`Mode` is the plan's own mode - `full`, `incremental-latest`,
`incremental-all`, `resume` - rather than a coarser `incremental`, since the
planner already distinguishes them and a consumer asking which base an
incremental used wants to know which kind it was. A transfer reports it with
`PhaseSending`: `Report` carries the plan beside the phase, which is where the
decision to send is made (above).

`Destroyed` is a list cut at 64 names, with `DestroyedCount` always the total,
rather than a count alone or an unbounded list. A count cannot be checked
against a pool, which is the point of carrying identity. An unbounded list
makes one transition as large as a prune of years of snapshots, and that
transition sits in every slow subscriber's channel (3.2) and on one log line
(3.9). 64 is more than a policy's grid destroys in one pass in practice, and
a reader knows when the list was cut because the count exceeds it.

The fields reach the wire as additive `JobStatus` fields, the "worker state"
line as keys present only when set, and the interactive tail as `key=value`
pairs. The operations guide documents them. Since 3.9 makes that line a
contract, the keys are part of it.

**3.5 A stalled transfer currently reports nothing.** A sample is emitted only
when bytes are written, and the rate is the cumulative average since the
transfer began ([internal/zfs/stream.go:267](../internal/zfs/stream.go),
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

**3.6 `WatchStatus` sends what it receives.** One additive field on
`WatchStatusResponse` ([proto/boomerangz/control/v1/control.proto:30](../proto/boomerangz/control/v1/control.proto)):
the transitions observed since the previous message. The snapshot stays as it
is, so existing clients are unaffected and a client joining mid-stream still
gets a self-describing message. The handler subscribes and sends one message per
`Update`: its state as the snapshot and its transitions as the new field. The
snapshot therefore carries the newest progress for every running job by
construction.

When the subscription ends on overflow, the stream ends with it: the handler
returns `codes.Aborted` with a message naming the overflow, and sends nothing
further. There is no in-band resync. A flag on a message that otherwise looks
like every other message is exactly what a consumer selecting `.jobs` ignores,
which would reintroduce the silent gap this design removes; an ended stream
cannot be ignored. `Aborted` rather than `ResourceExhausted` because gRPC also
reports message-size limits as `ResourceExhausted`, and because `Aborted` means
retry at a higher level - here, a fresh subscription and snapshot - which is the
only honest recovery. Whether to reconnect is the client's decision.

The forwarder's 250ms limit on state-only updates (3.2) matches the producers'
own sample cadence. With the default two local and one remote transfer workers
([internal/config/config.go:16-17](../internal/config/config.go)) that bounds a
watcher at four messages a second rather than twelve, and it never delays a
transition, which sends immediately. While a `Send` is blocked, the forwarder
keeps merging, so a slow client costs itself freshness and nothing else until it
falls 256 transitions behind. That limit is a server-side property of conflated
data; it is not a client-chosen heartbeat, and it is not the interval coming
back.

`Runtime.WaitStatus` exists for exactly this handler
([internal/control/service.go:18](../internal/control/service.go),
[:83](../internal/control/service.go)) and is retired with it - in chunk B,
which removes the store it waited on - along with
`StatusStore` and its revision waiting. `StatusSnapshot.Revision` is already on
the wire and stays; the owner counts it. The interface the control service
depends on gains `Subscribe` and loses `WaitStatus`; two test fakes follow
([internal/control/control_test.go:55](../internal/control/control_test.go),
[internal/cli/control_test.go:31](../internal/cli/control_test.go)).

**3.7 The CLI shows them.** Interactive `--watch` gains a short transition tail
under the table - the thing an operator is actually watching for is a change,
and today the only way to see one is to be looking at the right moment.
Redirected `--json` includes the transitions in each object, which is additive
for anything reading `.datasets` or `.jobs` today. `docs/reference/cli.md` and
the operations guide say what the new field is, list the job states including
those 3.4 adds, and say that a watch which falls behind ends with an error
rather than continuing with a gap.

The CLI needs no new handling for that. The watch loop already returns any
stream error other than end-of-stream or its own cancellation
([internal/cli/control.go:73-78](../internal/cli/control.go)), and `cli.Run`
prints it and exits 1 ([internal/cli/cli.go:23-26](../internal/cli/cli.go)), so
both interactive and `--json` watches exit non-zero on overflow. No flag
changes, so the completions in `contrib/` are untouched; the man page gains a
sentence with the docs.

This is the part a test-only fix would omit. It is the half of the defect a user
can hit.

**3.8 The watch interval retires.** `--interval` was specified before the
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
scheduler's next-snapshot deadlines all change silently. So do the queues:
`RemoveScope` and `DiscardAll` drop work without an event
([internal/daemon/queue.go:131](../internal/daemon/queue.go),
[:165](../internal/daemon/queue.go)), and a worker's `Pop` leaves the queue
before `acquireScope`, which can block, and only then emits `running`
([internal/daemon/pool.go:249-273](../internal/daemon/pool.go)). A watcher sees a
newly enabled dataset when some unrelated job happens to transition, or when the
interval expires - up to two seconds by default, indefinitely if the client
asked for an hour. That is this document's defect in another place: a state
change that is not an event. Under 3.1 each of these is a message to the owner,
and every `Update` carries the resulting state, so the snapshot a watcher holds
is never stale while connected.

*A peer that has gone away without saying so.* No gRPC keepalive is configured
on either side. Over the local socket that does not matter - a dead peer closes
it. Over a paired TCP listener, the periodic `Send` is incidentally the only
thing that makes the server notice a vanished client, and nothing at all makes
the client notice a vanished server: `Recv` has no deadline and the absence of a
heartbeat is never checked ([internal/cli/control.go:73](../internal/cli/control.go)).
Under publication, a silently dead client would hold its subscription until its
forwarder overflowed. Liveness belongs to the transport: server and client
keepalive parameters, with an enforcement policy, on the TCP listeners.

The values are defined once in `internal/control` and used on both sides: by
`grpcServer` for TCP listeners, where it already adds TLS credentials
([internal/control/server.go:253-255](../internal/control/server.go)), and by
`DialPairingConnection` ([internal/control/client.go:443-452](../internal/control/client.go)).
Native replication dials through the same function
([internal/replication/native/client.go:65](../internal/replication/native/client.go))
and is served by the same listeners, so a native transfer to a vanished peer is
detected by the same bound. The Unix socket gets none.

| Side | Setting | Value |
|---|---|---|
| Server | `keepalive.ServerParameters` `Time` / `Timeout` | 30s / 10s |
| Server | `keepalive.EnforcementPolicy` `MinTime` | 20s |
| Server | `keepalive.EnforcementPolicy` `PermitWithoutStream` | false |
| Client | `keepalive.ClientParameters` `Time` / `Timeout` | 30s / 10s |
| Client | `keepalive.ClientParameters` `PermitWithoutStream` | false |

The client and server values are one decision. In gRPC v1.83.2 the server
counts a strike for each ping that arrives within `MinTime` of the previous one,
and after more than two strikes closes the connection with a `too_many_pings`
GOAWAY (`handlePing` and `maxPingStrikes = 2` in
`internal/transport/http2_server.go`); its default `MinTime` is five minutes, so
a client pinging every 30 seconds against a default server would be
disconnected. `MinTime` sits below the client's `Time` to leave margin rather
than at it. Neither side pings without an active RPC: a watch or a transfer is
the connection worth keeping alive, and the CLI's other calls are short. Either
side notices a vanished peer within `Time` plus `Timeout`, 40 seconds after the
last activity.

Two tests hold this. A unit test asserts, from the shared values, that the
client's `Time` is at least the server's `MinTime`. And a host-safe test in
`internal/control` runs `WatchStatus` over loopback TCP through a proxy that
stops forwarding, and asserts that the client's stream fails and the server
releases the subscription, each within 40 seconds plus a margin.

With both addressed the interval has no remaining function, and it should go
rather than linger as a knob whose effect nobody can describe. `--interval` is
removed from the CLI, `interval_milliseconds` is reserved in the proto so its
field number is never reused, and the flag leaves `docs/reference/cli.md`, the
operations guide ([docs/operations/index.md:30](../docs/operations/index.md)),
the man page and all three completions in the same change. Whether removal
passes through a release as a hidden, ignored flag first is a compatibility
decision for whoever cuts that release, not a design question; the design's
position is that the flag has no behaviour to preserve.

If a client wants to redraw on a clock - an elapsed time, a countdown to the
next snapshot - that is a client-side ticker over the snapshot it holds. It
needs nothing from the server.

**3.9 The log is a subscriber.** Transitions are logged today in one place,
`reportWorkerState`, on the producer's goroutine, and only for pool events
([internal/daemon/runtime.go:59-69](../internal/daemon/runtime.go)). The
configuration reload record and progress go to the store without it
([internal/daemon/runtime.go:330](../internal/daemon/runtime.go),
[:1069](../internal/daemon/runtime.go)), and a discovery generation is logged
separately, as "discovery complete"
([internal/daemon/runtime.go:1140](../internal/daemon/runtime.go)). The log and
the status are two writes, so they can disagree, and nothing orders one against
the other.

The owner logs what it ingests instead, through a subscriber registered in
`daemon.New`, before any producer runs. The log therefore holds every transition
from process start, in the order every other subscriber sees them. It writes
each transition - not progress, which is not logged today either - as the same
"worker state" line with the same keys and levels as now: `failed` at error,
`blocked` and `waiting-retry` at warning, everything else at info. The
configuration reload record gains a line it does not have today. Each new
discovery generation is written as "discovery complete" with its ID and dataset
count, from the runtime view (3.8), so the line follows the generation being
applied rather than the scan finishing. `reportWorkerState`'s logging and the
line at `runtime.go:1140` go. This is also the durable record 5 says a record
consumer should get: operators already read it from the journal
([docs/operations/index.md:14](../docs/operations/index.md)).

The write does not happen in the owner's loop. `slog`'s handler writes
synchronously under its own lock (`commonHandler.handle` in `log/slog`), so a
stalled stderr would stall the owner and, through it, every producer - including
queue sends made under the queue lock (3.1). The log subscriber's forwarder puts
that write on its own goroutine. Today a stalled stderr stalls whichever worker
is logging; afterwards it stalls only the log writer.

The log subscriber cannot end on overflow, because nothing would replace it. Its
forwarder marks the gap instead. It uses the same transition bound as every
other subscriber (3.2): one bound, set once from chunk B's measurement (7),
rather than a second number for the same kind of delay. When its held
When the owner's send to the log is refused, it keeps what was refused, in
order, on its record of the log subscriber: consecutive refused transitions
become one gap, counting them and the span of their times, and a refused flush
is held as itself. It offers those again, in order, on every later fan-out and
on a short retry tick while any are held, and offers nothing new past one still
refused, so the log's order holds. The gap reaches the writer in the position
of the transitions it replaces, and the writer writes it as one error-level
line - "status log dropped transitions", with the count and the span. A flush
is never closed by the owner; the writer closes it on reaching it, after every
transition and gap before it. A writer held for a long time while transitions
keep arriving can write several gap lines, one per run of refused transitions,
with the transitions it did take between them.

Two gap lines can also be written back to back, with no transition between
them. It happens in two ways, both known and left as they are. When a re-offered
gap takes the only free slot in the log's channel, the next transition finds the
channel full again and opens a new gap. And a flush held between two runs of
refused transitions separates them into two gaps, but writes no line of its own.
Nothing is lost or miscounted either way - the two counts add up to the run -
so a reader summing adjacent gap lines gets the right total. Merging them would
mean reserving a slot for the transition after a gap, and letting a gap absorb
refusals across a held flush. Neither is worth doing until a log shows it
happening outside a writer stalled on purpose in a test.

That line is the only place the log is not the complete sequence.
It is not the in-band flag 3.6 rejects: a flag rides on a message that is
otherwise read normally, while this is a line of its own at error level, and a
test reading the log fails on it. Forwarders take this policy only when the
owner registers them for the log; every other subscription ends on overflow.

Component logs that are diagnostics with no status representation - the error
and warning lines in `internal/daemon/runtime.go` such as "queue snapshot" and
"prepare remote recovery", and "daemon started" and "daemon stopped" - stay
where they are.

The event lines become something tests read (4.2), so their message and keys
are a contract. The operations guide documents them, and the gap line, with the
change.

## 4. What the tests then stop doing

**4.1 In-process waiter.** A helper in `test/integration/internal/statuswait` taking a runtime, a
predicate over `Event`, and a bound; selecting on its subscription rather than
sleeping; returning the first matching transition; failing on a subscription
error; and on expiry failing with every transition it received while waiting.
Replaces the four status loops in 2.3. The bound stays a hard failure: this
removes sampling, not deadlines.

It lives under the integration tests because only they use it, and Go's
`internal` rule makes that structural: a package under
`test/integration/internal/` can be imported only by packages under
`test/integration/`. Nothing in `internal/daemon` can import it, so the import
cycle a helper taking a `*daemon.Runtime` would otherwise risk cannot arise, and
no production package can come to depend on test code. 4.2's log waiter lives in
the same package, so both share predicates and failure reporting. The package
itself carries no build tag, so `make test`'s `go vet ./...` and `golangci-lint`
check it; only the tests that use it are tagged `integration`.

**4.2 Out-of-process waiter.** The control suite runs the packaged daemon as a
subprocess, and a subprocess starts working before a test can subscribe to it.
It binds its control socket first - `StartServerWithReplication` listens and
sets the socket mode before `Runtime.Run` is called
([internal/cli/commands.go:141-163](../internal/cli/commands.go),
[internal/control/server.go:145](../internal/control/server.go)) - but `Run`
starts the scanner, whose first scan runs at once
([internal/discovery/discovery.go:186](../internal/discovery/discovery.go)), and
the scheduler makes a newly discovered root due immediately
([internal/daemon/scheduler.go:73](../internal/daemon/scheduler.go)). Every test
that starts or restarts a daemon, including the ones that kill a daemon at a ZFS
commit and restart it against what it left, depends on work done before any
subscription could exist.

So the waiter reads the daemon's log, which the tests already capture from the
subprocess and which carries every transition from process start (3.9). It is a
writer the test attaches as the daemon's stdout and stderr: it splits complete
lines, decodes the "worker state" and "discovery complete" lines, and wakes
waiters by closing and replacing a channel - broadcast-and-recheck, because the
condition is one the test process owns (1). A wait takes a predicate over the
lines and a bound, and can name an occurrence ("the second `succeeded` for this
job") because the sequence is complete. It fails at once on a "status log
dropped transitions" line, and on expiry prints every line it decoded. Socket
appearance stays a poll - a missing file has no notifier short of inotify, and
the bound is short.

Waits on what the daemon did read the log; assertions about the control plane
itself stay on the socket, as one read after the log shows the behaviour. That
read sees what the log reports, because the log line is written only after the
owner has applied the change and a status request is answered in order behind
it (3.1). Mapped against the two tests in `guest_test.go`:

- *Dataset in control status* ([test/integration/control/guest_test.go:97-100](../test/integration/control/guest_test.go))
  becomes the first "discovery complete" line, then one `status` read that must
  list the dataset.
- *Reloaded configuration generation* (`:124-133`) becomes one `status` read
  after `config reload` returns: the reload record and the generation are
  committed before the reply (3.1), so no wait is needed.
- *Initial owned snapshot* and *completed initial snapshot job* (`:134-161`)
  become the first `succeeded` for `snapshot:<dataset>`, then one pool read.
- *Triggered owned snapshot* (`:166-169`) becomes the second `succeeded` for
  `snapshot:<dataset>` - counted, not "the next after the trigger returns",
  because the log writer can lag the reply - since a forced run skips the
  deadline check ([internal/daemon/runtime.go:641](../internal/daemon/runtime.go));
  then one pool read.
- The abrupt-restart test's two daemons are covered by 4.3.

In `TestGuestDaemonPowerLoss` the killed daemons are observed through their exit,
which stays as it is
([test/integration/control/adversarial_test.go:180-190](../test/integration/control/adversarial_test.go),
[:261-268](../test/integration/control/adversarial_test.go)). The daemons started
after a kill or a reseed wait on their own logs:

- *snapshot-commit* checks the lineage as soon as the replacement serves
  (`:213-230`), before anything shows the replacement has looked at the dataset.
  Its log gives that point: its first terminal transition for
  `snapshot:<dataset>`, which should be `scheduled` because the surviving
  snapshot sets the next deadline (4.3). The lineage and survivor checks follow.
- *receive-commit* waits for `local:<dataset>:<root>` to report `blocked` or
  `succeeded` (`:300-312`); from the log that is the first such transition, its
  reason carried on the same line.
- The reseeded daemon's convergence (`:330-342`) becomes the first `succeeded`
  for the same job, then one read for the bookmark and the destination.

The contention tests' waits (`:421`, `:463`, `:496`, `:530`, `:556`) have not been
mapped here.

A test that subscribes to `WatchStatus` remains the control plane's own
coverage, in chunk C; the suite's waits do not depend on it.

**4.3 Anchor the restart assertion on the job outcome.** The restart test sleeps
500ms and then asserts that no further snapshot appeared
([test/integration/control/guest_test.go:285](../test/integration/control/guest_test.go)).
The sleep is a guess at how long "nothing else happened" has to be to mean
something. Waiting for further discovery generations would not fix it: a scan
completing only makes the root due (`Scheduler.Update`), and the snapshot job
that could create a duplicate runs later, from the scheduler loop through the
management queue ([internal/daemon/runtime.go:1143-1152](../internal/daemon/runtime.go)).

The restarted daemon always runs that job, because its first scan makes the root
due at `now`. When an owned snapshot already sets a later deadline, the job ends
`scheduled` with the reason "existing owned snapshot sets the next deadline", and
retries at that deadline instead of creating a snapshot
([internal/daemon/runtime.go:641-643](../internal/daemon/runtime.go)). A
duplicate would end `succeeded`. So the test waits in the restarted daemon's log
for the first terminal transition of `snapshot:<dataset>`, requires it to be
`scheduled` with that reason, and counts snapshots once. The first daemon's wait
for its initial snapshot (`guest_test.go:248-256`) is the first `succeeded` for
the same job in its own log, then one pool read, before the kill.

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
a cursor the client presents on reconnect - a pull model on top of the push one
rather than instead of it. Neither consumer wants it today: the CLI does not
reconnect at all, it returns the stream error
([internal/cli/control.go:74](../internal/cli/control.go)), and a test subscribes
for the duration of what it is watching. It becomes worth building when
something consumes this stream as a record - an exporter, an audit trail - at
which point the honest form is durable and on disk, not a larger queue.

**Mixed-version watch clients.** A new CLI watching an older daemon receives
no transitions field, which reads the same as nothing having happened, and paired
remote daemons make that combination possible. boomerangz is alpha and makes no
cross-version compatibility promise, so this is not handled; it becomes a
question when one is made.

**The rest of the runtime's shared memory.** 2.4's other instances are real but
are not status. `r.mu` guarding ten unrelated fields
([internal/daemon/runtime.go:102-117](../internal/daemon/runtime.go)) is worth
splitting, and it shrinks on its own once status no longer reads it, but it is a
separate change with its own review.

**Everything else stays as it is because it is already right.** `waitForDevice`
([internal/testutil/zfstest/fixture.go:116](../internal/testutil/zfstest/fixture.go))
waits on udev, which is not ours to subscribe to. The scheduler's deadlines and
the remote backoff are time-driven by intent. Unit-test sleeps that keep a job
running are fixtures, not waits
([internal/daemon/pool_test.go:66](../internal/daemon/pool_test.go),
[internal/control/control_test.go:159](../internal/control/control_test.go)).

One unrelated sleep is worth fixing while nearby: a control reload handed
retired listeners to a goroutine that slept 100ms before draining them. The
drain itself was already there - `GracefulStop`, bounded at five seconds, then
`Stop` - so the sleep did not cut calls in flight. What it did was keep the
retired listener accepting for 100ms after `Reload` returned, which is why the
moved-socket test polled for the old socket to disappear. And the goroutine was
untracked: `Close` did not stop a drain in progress but waited it out through
the retired listener's `Serve`, up to the full bound, and a call cut when the
bound expired left no trace. A watch never ends on its own, so one connected to
a retired listener always held the drain to its bound, and a same-address
replacement drained with the server lock held, so `Reload` - and the Reload RPC
reply, and `Close` - waited the bound with it. The bound itself cut replication
transfers, which a configuration reload has no reason to stop. And every mTLS
listener was replaced on every reload, changed or not, because its client CA
pool is fixed once built.

Chunk E closes the retired listener before `Reload` returns, which also frees
its address for a same-address replacement; drains in a goroutine `Close` stops
and waits for, with no bound, so a call in flight finishes however long it
takes; ends a watch on a draining listener at once with `codes.Unavailable`; and
replaces an mTLS listener only when its definition or the contents of its client
CA changed. Without a bound, a call to a peer that vanished silently holds the
retired server until the transport notices, which over TCP is the keepalive in
3.8; until chunk D configures it, that is the kernel's own timeout. It holds
memory, not the reload or the replacement.

## 6. Chunks

**Chunk A - transfers report phases.** 3.4, on the current store. `Apply` takes
the ordered `Report` callback; the daemon records `sending` and `verifying` as
transitions and gives local jobs a `planning` start state. `recordProgress`
still writes through `Record` until chunk B, which is harmless because a phase
transition and the samples that follow it carry the same state. It lands first
because chunk B's discard rule would otherwise drop every remote sample while
the job still shows `probing`. Done when, in unit tests against a fake stream, a
remote job records `probing`, `sending`, `verifying`, and its outcome; an
up-to-date remote job records no `sending`; a resume followed by a newer send
records two `sending` transitions; and a local job records `planning` before
`sending`.

**Chunk B - the status owner.** 3.1 through 3.3: the owner goroutine, the one
input message type, `EventKind`, and forwarders. Every use of `StatusStore`
becomes a message or a request
([internal/daemon/runtime.go:60](../internal/daemon/runtime.go),
[:162](../internal/daemon/runtime.go), [:330](../internal/daemon/runtime.go),
[:1069](../internal/daemon/runtime.go), [:1228](../internal/daemon/runtime.go),
[:1246](../internal/daemon/runtime.go), [:1363](../internal/daemon/runtime.go)),
with its tests in `status_test.go` and `runtime_test.go` following, and
producers send their views under their own locks. `WatchStatus` sends a snapshot per `Update` with its
interval retained, so the CLI is unchanged. Unit tests for delivery order; a
consumer that never reads not blocking the producer; overflow ending one
subscription without touching another; conflation leaving exactly the newest
sample per job and never displacing a queued transition; a sample after a job
leaves `sending` being discarded; and the first `Update` agreeing with every
later transition. Done when a subscriber receives every transition recorded
after it subscribed, or is told its subscription ended, always reads the newest
progress for every running job, and `status --watch` behaves as before. Expose
the forwarder high-water mark here, record its peak across an unfiltered
`make integration-test`, and set the transition bound from it (7) - one bound,
shared by the log subscriber (3.9). `Offer` sends
`pending-<pool>` with its queue view (3.1), with a unit test that a job popped
immediately never records `pending` after its start state. The race is already
observable: `TestPoolSilentOutcomeLeavesTheReportedStateStanding`
([internal/daemon/pool_test.go:252](../internal/daemon/pool_test.go)) records
`probing` before `pending-transfer` within a few hundred runs of
`go test -race -count=300`, and chunk A's job-state tests ignore `pending-*` for
the same reason; both stop needing to once `Offer` sends it.

Chunk B also moves transition logging to the log subscriber (3.9):
`reportWorkerState`'s logging goes, the configuration reload record gains its
line, and the log forwarder marks gaps rather than ending. The "discovery
complete" line moves with the runtime view in chunk D. Unit tests: every
transition recorded after `daemon.New` appears in the log in owner order; a
stalled log writer does not block a producer; a log that falls past its bound
accounts, in order, for every transition it did not write with gap lines
carrying the count, and resumes; and a flush returns only once the writer has
taken what preceded it. The operations guide documents
the event lines and the gap line in the same change.

As built, chunk B took three things this plan placed later, because leaving
them would have kept a read of shared memory or a silent gap in place. The
runtime view (3.8) is a message: `ControlStatus` is a request to the owner and
no longer takes `r.mu`, and every view message reaches subscribers, so a
watcher's snapshot already reflects a newly enabled dataset or a removed queue
entry without a job transitioning. The "discovery complete" line itself still
moves in chunk D. `WaitStatus` is retired here, since the store it waited on is
gone; the handler subscribes. And a watch whose subscription ends on overflow
already returns `codes.Aborted` rather than continuing, since continuing would
be the defect; chunk C still owns the transitions field, its tests, and the
docs. A progress sample no longer moves a job's `changed` time, which is the
time of its latest transition.

**Chunk C - the control plane carries transitions.** 3.6 and 3.7, retiring
`WaitStatus` with them, and the docs in the same change. Done when
`status --watch --json` emits every transition a job made while the client was
connected, a test asserts that a sequence which collapses in the snapshot
survives in the stream, and a test asserts that a watcher which overflows
receives `codes.Aborted` and no further message.

As built, `transitions` reuses `JobStatus`, so a transition has the same fields
as a snapshot row with the progress fields unset. While the interval survives
(chunk D), a message it re-sends carries no transitions, since they were sent
with the update that brought them. Redirected output always carries the field,
as an empty array when there are none, and one-shot `status` does not. The
interactive tail holds the ten newest transitions across messages. Both
end-to-end tests run a real status owner behind the control server
(`internal/daemon/watch_test.go`); the overflow test fixes the client's
flow-control windows so that a client which stops reading holds the server's
sends at a known size, and checks that what arrives before `Aborted` is an
unbroken prefix of the recorded sequence. `status --watch --json` is tested
against a scripted subscription in `internal/cli`.

**Chunk D - nothing a watcher holds goes stale.** 3.8: runtime, deadline and
queue views reach every `Update`; configure transport keepalive on the TCP
listeners; remove `--interval` and reserve its proto field, with docs, man page
and completions. And 3.5's producer fixes: progress emitted on a ticker while a
transfer is active, and a windowed rate. Done when a watcher's snapshot reflects
a newly enabled dataset and a removed queue entry without any job transitioning,
a stalled transfer's rate reaches zero within a few samples, a client over TCP
detects a vanished daemon and the daemon releases a vanished client's
subscription within 40 seconds (3.8), and nothing in the tree describes a watch
interval.

As built, the "discovery complete" line is written by the log subscriber when
the owner applies a runtime view whose discovery generation differs from the
previous one; the view carries the generation's dataset count for it, and a
republished generation writes nothing. A discovery generation the log refuses
is counted in the gap with the transitions around it rather than held as
itself, so what the owner holds for a stalled log stays bounded however often
discovery runs, and the gap line's `count` covers both. Both streams report
progress through one `zfs.ProgressMeter`: a first sample before the first byte,
one per 250ms on a ticker whether or not bytes moved, and a last sample from
`Finish` once the ticker's goroutine has exited, so the last sample still
precedes `PhaseVerifying` (3.4). The rate is measured over the last second, so
a stalled stream reports zero within four samples. Keepalive lives in
`internal/control/keepalive.go` and is applied wherever a listener has TLS,
which is every TCP listener and no Unix socket. Its loopback test holds the
suite for the full 40 seconds, running in parallel with the rest of the
package. The view-change test drives a real queue pop and the runtime's view
publication rather than `applyGeneration`, because a newly enabled dataset
also enqueues reconciliation, whose `pending` transition would make "without
any job transitioning" untestable there.

Building this found that a job `RemoveScope` or `DiscardAll` dropped from a
queue recorded no transition, so its row kept the `pending-<pool>` state it was
queued with until the job next ran: the queue view no longer listed it, but the
row itself went stale - the same shape as 3.8's gaps, for a job rather than a
view. Fixed after chunk D: a dropped job records `cancelled`, with a reason
the caller gives - "dataset deactivated" from deactivation, "remote
configuration changed" from a reload that changes a remote, and "daemon
shutting down" from `Runtime.Run` - sent in the same message as the queue view that no
longer lists it, under `FairQueue.mu`, the way an offer sends `pending-<pool>`
(3.1). A queue message therefore carries any number of transitions, applied and
delivered in order under one revision. A worker that pops a job after its pool
was stopped records `cancelled` too, rather than dropping it silently. So
removing a queue entry is no longer a change without a transition; a pop still
is, until the worker reports the job's start (3.8).

Recording those transitions exposed a second defect in the reload path. Every
reload discarded queued remote work and every remote's recovery coordinator,
retry state included, but requeued remote work only when the remotes had
changed. A reload that changed nothing about the remotes - a worker count, say -
therefore dropped remote transfers silently, and one carrying a newly protected
snapshot waited for that dataset's next snapshot or discovery change to run.
The discard, the coordinator reset, and the requeue now happen together, and
only when a remote changed. "Changed" compares the clients a reload builds
with the ones in use - an SSH remote's setting, a native remote's pairing
bundle and root - rather than the configuration alone, so a native credential
file whose contents changed still replaces its client and coordinators, as
the unconditional reset used to. A reload that changes a remote logs
`cancelled` for each queued remote job and `pending-transfer` for the work
requeued against the new clients, which chunk G's occurrence counting has to
allow for; any other reload logs neither.

**Chunk E - the listener drain.** Section 5's last paragraph, independent of the
rest. Done when a call in flight across a reload completes, the retired listener
refuses connections once `Reload` returns, a watch on it ends at once, a
same-address replacement does not wait for its predecessor's calls, `Close`
stops a drain, and an unchanged mTLS listener survives a reload while one whose
client CA changed does not, each with a test.

**Chunk F - the in-process waiter.** 4.1, on chunk B, in
`test/integration/internal/statuswait`. `waitForTransferSuccesses`,
`waitForEvent` in `TestGuestRemoteOutageReconnection`, `waitForJobState`, and
the attempt loop in `TestGuestDaemonRemoteBackoff` wait on a subscription, taken
before `Runtime.Run` starts - in `runDaemon` and in the tests that start the
runtime themselves; the backoff test counts its three attempts
from the subscription. `waitForSendIntervals` waits on a broadcast channel in
`observedLocalStream`. Done when none of those five sleeps, and a forced failure
prints the transitions observed.

**Chunk G - the out-of-process waiter.** 4.2, on chunk B's log subscriber and
chunk D's "discovery complete" line. A log writer the tests attach to the daemon
subprocess, with predicates over decoded lines, occurrence counting, failure on a
gap line, and every decoded line printed on expiry. `TestGuestDaemonControl` and
`TestGuestDaemonPowerLoss` convert as 4.2 maps them; each status read that
remains is a single read after the log shows the behaviour. The contention
tests' waits
([test/integration/control/adversarial_test.go:421](../test/integration/control/adversarial_test.go),
[:463](../test/integration/control/adversarial_test.go),
[:496](../test/integration/control/adversarial_test.go),
[:530](../test/integration/control/adversarial_test.go),
[:556](../test/integration/control/adversarial_test.go)) are mapped the same way
first. Done when no test in `test/integration/control/` re-runs `boomerangz
status` in a loop or polls the pool while waiting for the daemon, and a forced
failure prints the log lines the waiter decoded.

**Chunk H - the restart anchor.** 4.3, on chunk G's waiter. Done when no bare
sleep remains in `test/integration/control/`, and a restart that created a
duplicate snapshot fails on the job ending `succeeded` rather than on a count
taken after a guessed delay.

**Chunk J - events carry identity.** 3.4's identity: the run ID and `Identity`
on `Event`, additive `JobStatus` fields, the log keys, the interactive tail, and
the docs and man page. It lands before I, which asserts on it. Done when each
field in the table is set on the transitions it names and on no other, a run's
transitions share one run ID including the phases reported from inside `Run`,
and a job built without a run ID is refused, each with a unit test.

**Chunk I - ZFS waits behind transitions.** 4.4, last because it depends on F
and J and changes what those tests assert rather than how they wait. It covers the
scheduling test's `waitFor` and the outage test's hold loops (2.3); for the
latter, which transition follows the hold being placed has not been traced, and
that tracing comes first.

A through D are the defect: the transitions the daemon was not recording, the
contract, the stream that carries it, and the state changes it was missing. E is
an unrelated sleep fixed while nearby. F through I are the cleanup the fix makes
possible, and each is independently droppable without leaving the contract
half-changed. J extends the contract so that what I asserts can be tied to
objects rather than to moments.

## 7. Risks

- **A silently lossy stream is the original bug.** Overflow has to end the
  subscription at every layer that carries it - the forwarder, the RPC, the CLI -
  and a test waiter must fail on it rather than continue against a gap. The
  daemon log is the one consumer that cannot end, so it marks its gap with a
  line of its own (3.9), and a log reader must fail on that line. The failure
  mode to design against is a consumer that keeps going.
- **Queue depth is set by measurement, not asserted.** Chunk B exposes the
  forwarder high-water mark as `Status.BacklogPeak`: the most transitions a
  forwarder has held undelivered, counting what it has taken and what is still
  in its channel. It is logged as `status_backlog_peak` on "daemon stopped".
  It was measured across an unfiltered `make integration-test` on 2026-09-13
  against the two-channel forwarder (3.2), run `ci-67391269765534e8`, with all
  four stages passing. Temporary instrumentation, not committed, printed the
  peak from each daemon's log writer goroutine, never from the owner, whenever
  it had risen. The control stage's subprocesses, including the ones it kills,
  were covered by copying those lines out of their captured output. The
  highest peak was 5 undelivered transitions, reached in both the daemon and
  control stages. No run wrote a "status log dropped transitions" line. An
  earlier run against the superseded mailbox forwarder
  (`ci-54d74eb7d72ee36a`) also peaked at 5. The suite runs no
  `status --watch`, so the peak is the log subscriber's: it is how far a log
  writer on the same host falls behind under the suite's bursts. The bound
  stays at 256, about fifty times that peak. The cost of the headroom is up to
  twice 256 `Event` values per slow subscriber (3.2); the cost of too little is
  a remote watcher over TCP ended by a burst it would have absorbed. A real
  deployment approaching the bound is a signal to look at what is producing
  transitions at that rate before enlarging anything.
- **The owner must never call out.** Producers send while holding their own
  locks (3.1), which is safe only because the owner's loop holds no lock and
  references no component. A change that has the owner consult the scheduler,
  a queue, or the runtime to fill in a field reintroduces the deadlock the shape
  rules out. Fields the owner needs arrive as messages.
- **A producer can wait on the owner.** The guarantee is that no consumer
  applies backpressure to replication, not that a send never blocks (3.1). An
  owner that does anything slower than in-memory work breaks it for every
  producer at once.
- **Subscribe-before-act is a requirement, not a convention.** A helper that
  subscribes after the action it observes waits for an event that has already
  been published, and the failure looks like the daemon never did the work.
  Chunk F's waiter should take the subscription in its constructor so the
  ordering is structural rather than remembered.
- **The daemon's event log lines become a contract.** The control suite waits
  on them (4.2), and operators already read them from the journal. Renaming the
  message or a key breaks the suite's waits as surely as renaming a proto field
  breaks a client; the operations guide documents them so a change is a visible
  one.
- **The log's per-job order is only as good as the producers'.** The log records
  transitions in the order the owner receives them. A producer that sends two
  events for one job from goroutines with no ordering between them - as
  `Pool.Submit` does today (3.1) - puts them in the log out of order, and a
  waiter counting occurrences reads that order as fact.
- **New job states reach users.** `planning` and `verifying` (3.4) appear in
  `status` output, and a second `sending` appears on resume. Anything matching
  on the current states sees new ones; the states list lands in the docs with
  chunk C.
- **An additive proto field still changes output.** `--json` consumers gain a
  field; that is compatible for anything selecting known keys and not for
  anything asserting an exact object. The docs change lands with the code.
- **A long-running watch can now end with an error.** Today a watch ends only on
  cancellation, a transport failure, or `codes.Unavailable` when a reload
  retires its listener (chunk E); since chunk B it also ends with
  `codes.Aborted` when it falls behind. A script that runs `status --watch
  --json` unattended has to treat that exit as "resubscribe", not as the daemon
  failing.
- **A lossless stream invites over-specified tests.** Being able to assert on
  every intermediate state does not mean a test should. The waiter's predicate
  should name the transition the behaviour is about, not transcribe the
  sequence, or the suite gets brittle in a new way.
- **More test code coupled to the status vocabulary.** Job IDs and state names
  become load-bearing in more places. They are already public - the CLI prints
  them - but chunk I should not invent states to make a wait convenient.

## 8. Open questions

Points raised in review and not yet settled. Each is resolved in this document
or removed with a reason, not left to memory.

Nothing is open.
