# Design: root actor scope

Status: proposed. Re-derived against `main` at `bbc266c`.

This replaces the model in which every descendant inheriting
`org.boomerangz:enabled=on` becomes a peer actor with one in which a single
root owns its whole subtree. It is a breaking change to ownership semantics.
There are no known users, so no migration path is provided.

## 1. The problem

### 1.1 The incident

A freshly adopted local backup target wedged with three jobs that no retry
could clear. The source was `zroot/data/home/pdf`, `local=storage/backup`,
`discard=first`, `replicate=off`, with `pdf`, `.local` and `.local/share` all
active:

- `local:zroot/data/home/pdf:storage/backup` blocked: `target identity or
  mapping differs from persistent binding; run boomerangz dataset reseed for
  this source and target`.
- `local:zroot/data/home/pdf/.local:storage/backup` blocked: `cannot open
  'storage/backup/data/home/pdf/.local': dataset does not exist`.
- `local:zroot/data/home/pdf/.local/share:storage/backup` in `waiting-retry`
  behind the above.

The trigger was a reseed of the parent. `ReseedService.Apply` destroys the
mapped destination recursively
([reseed.go:202](../internal/transfer/reseed.go)), then releases references,
clears the suspension marker and removes the binding property for
`request.Source` alone; `plan()` never enumerates descendant sources holding
their own bindings to the same target. A parent reseed therefore destroys
every descendant's replica while leaving every descendant's source-side state
pointing at datasets that no longer exist.

`.local` held a promoted binding anchored at its own mapped dataset, destroyed
underneath it. `pdf` held a fresh bootstrap binding anchored at an ancestor,
which cannot revalidate once its mapped destination exists again.

### 1.2 The root cause

`ActiveRoot` accepts activation inherited from any ancestor
([authority.go:19](../internal/lifecycle/authority.go)):

```go
if source == "" || (source != root && !strings.HasPrefix(root, source+"/")) {
```

So `enabled=on` at one dataset silently promotes every descendant into a full
peer actor with its own lineage, binding, reference set, schedule and job.
Nothing models the relationship between them, so every ordering and
consistency requirement is reconstructed downstream from string prefixes:

| Reconstruction | Location | Re-derives |
| --- | --- | --- |
| `missingMappedAncestor` | [runtime.go:935](../internal/daemon/runtime.go) | parent-before-child transfer ordering |
| `nearestReceiveLockScope` | [runtime.go:923](../internal/daemon/runtime.go) | which lock a transfer contends on |
| `prepareReceiveParents` | [local.go:381](../internal/transfer/local.go) | placeholder ancestors, which can collide with another actor's mapped destination |
| bootstrap anchor revalidation | [binding.go:104](../internal/transfer/binding.go) | that peers may populate the destination namespace |
| bootstrap vs promoted binding | [plan.go:294](../internal/transfer/plan.go) | destination authority |
| reseed destroy scope | [reseed.go:202](../internal/transfer/reseed.go) | subtree ownership, incorrectly |

Six mechanisms reconstructing one missing edge. All six are present on `main`.

### 1.3 The concept exists, gated on the wrong thing

`discovery.Entry.CoveredBy` already is a root actor, and `isSchedulable` drops
covered datasets outright ([scheduler.go:58](../internal/daemon/scheduler.go))
- no lineage, no binding, no references, no job. That is the target model.

`markReplication` only ever sets it when the parent resolves
`Send.Replicate` ([discovery.go:446](../internal/discovery/discovery.go)).
Coverage is gated on the wire format rather than on ownership, so exactly the
layouts that cannot use `-R` are the ones left in the peer-actor model.

## 2. ZFS constraints

`send -R` forces one flag set over the whole subtree: if any descendant is
encrypted the entire stream must be raw, which forces `large_blocks`,
`compressed` and `embedded_data` on. `ForReplicationScope`
([resolve.go](../internal/policy/resolve.go)) implements this. Per-dataset send
options are not expressible, `props` is not independently controllable, and
neither `zfs snapshot -r` nor `zfs send -R` accepts an exclusion. A `-R` root
therefore cannot have a sparse member set, which is why sparse layouts run
under `replicate=off` today and so land in the peer-actor model.

`zfs snapshot` accepts several snapshot names in one invocation and creates
them atomically in a single transaction group:

```
zfs snapshot tank/a@name tank/b@name tank/c@name
```

That is strictly more capable than `-r` here: atomic across an arbitrary
sparse set, with `-o` applied uniformly. It replaces `-r` in both modes, since
under `replicate=on` the enumerated subtree is deterministic where `-r` is
not, and `CreateSnapshot` already re-reads and compares the scope either side
of creation. The cost is argument length: at roughly 60 bytes per name a 2 MiB
`ARG_MAX` holds some tens of thousands of datasets, which is accepted rather
than worked around.

## 3. The model

> A dataset is an actor root if and only if it is enabled and has no enabled
> ancestor. `org.boomerangz:enabled` is a per-dataset include/exclude switch
> with normal ZFS inheritance.

An intervening `enabled=off` does not sever a deeper enabled dataset from its
root. That is deliberate and it is the opposite of today's rule. Severing
would make the deeper dataset an independent actor nested inside another
actor's subtree, with its mapped destination inside the outer root's mapped
destination - which is the overlapping-ownership problem this document exists
to remove, reintroduced one level down.

No new property is added. Activation stays a single `zfs set`, which matters
because the property is the whole interface; there is no enable command.

### 3.1 Member sets

Given `tank/data/root` enabled, `tank/data/root/first` off and
`tank/data/root/first/second` enabled, the member set is
`{root, second}` - sparse, with a hole at `first`. Today `second` resolves as
an independent peer root; here it is a member of `root`.

```mermaid
graph TD
    R["tank/data/root<br/>enabled=on<br/><b>root actor</b>"]
    F["tank/data/root/first<br/>enabled=off<br/>excluded"]
    S["tank/data/root/first/second<br/>enabled=on<br/>member"]
    R --> F --> S
    R -. owns .-> S
```

### 3.2 Modes

`replicate` selects member-set derivation and wire format. Both are root
actors.

| | `replicate=off` (default) | `replicate=on` |
| --- | --- | --- |
| Members | sparse: the subtree minus locally disabled datasets | the entire subtree, unconditionally |
| Snapshot | atomic multi-dataset over the member set | atomic multi-dataset over the subtree |
| Stream | one per member, parent-first, per-member send options | one `send -R`, uniform flags |
| Descendant `org.boomerangz:*` | honoured | ignored, warned |
| Source-side deletes | explicit member reconciliation | `recv -F` propagation |

A member keeps its own send options, targets and retention grid. Pruning is
per member against that member's grid. A member does not get its own lineage,
binding or snapshot instant; those are root-scoped.

Holes cannot exist under `replicate=on`: an excluded descendant and everything
beneath it enter the stream anyway, so it gets a warning of its own rather
than appearing as `governs differing enabled` among send-option overrides.
Copying data the operator explicitly excluded is a different class of
consequence from an overridden send flag.

### 3.3 Nested independent actors

Not expressible, by construction: a dataset with an enabled ancestor is a
member. If one is ever wanted it needs an explicit `org.boomerangz:root=on`
opt-in and the ordering edges between overlapping roots that go with it.
Deferred until there is a case. Under the rule above there are no cross-actor
edges at all, so no dependency graph is needed.

### 3.4 Partial member failure

The snapshot is atomic; the N streams under `replicate=off` are not. The
reference stays unadvanced until every member verifies, so a run that
transfers three of five members leaves the reference at the previous instant
and a retry resumes that instant rather than selecting a newer one. Per-member
endpoint verification already exists in `verifyGUIDs` over `plan.Endpoints`.

## 4. Authority

Root-ness needs the tree, so it is decided in discovery, which has it.

This collapses rather than relocates the `ActiveRoot` reconstruction. A root
has no enabled ancestor, and `policy.Resolve` inherits only locally configured
public values, so a root's `enabled` always resolves from its own local
property. The prefix walk becomes an equality:

```go
effective.Values[policy.Namespace+"enabled"].Dataset == root
```

That is necessary but not sufficient - a descendant may also set `enabled=on`
locally while having an enabled ancestor - so discovery decides root-ness and
`ActiveRoot` keeps the local check. The CLI already refuses datasets with
`CoveredBy` set ([identity.go:73](../internal/cli/identity.go)), so extending
coverage to every member makes member-targeted commands refuse without
further work.

## 5. Scheduler

The concurrency structure is unaffected: `Next` scans entries, arms one timer
and wakes on a broadcast channel, and its single consumer submits a job and
loops. Entry counts fall, because `isSchedulable` already drops covered
datasets and every non-root member is now covered.

What does not survive is one cadence per entry. `cadence` comes from
`Grid.Cadence()`, which returns `buckets[0].Span`
([grid.go:104](../internal/policy/grid.go)) - one number per root. Per-member
grids cannot pass through it: `Schedule` carries no member set, so a fired
tick cannot say which members are due, and `Complete(dataset, completed)`
advances a single deadline by a single cadence.

One `next` per root, equal to the earliest member deadline; compute the due
subset when the tick fires; `Complete` advances per-member state. `Next`, the
pending flag and the wake channel are untouched.

Collapsing a grid to its finest bucket is sound within one grid - buckets sort
ascending, so any window of length `Span` contains a point of a
`Cadence()`-spaced sequence - and `Prune` anchors windows to the newest owned
snapshot rather than to wall-clock boundaries, so there is no alignment for
drift to break. But the collapse is a cost property, not a correctness one,
and it does not survive being extended across members: a root ticking at the
minimum cadence over its members and snapshotting all of them gives a `14x1d`
member 288 snapshots a day to retain 14, and `enqueueTransfers` fires after
each successful snapshot, so it sends 288 times too. Hence per-member.

## 6. State

`state:lineage`, `state:owner`, `state:target:*` and `state:reference:*` exist
only on roots; members carry none. `Reference` already carries per-member
`Sources` under a single root property keyed by target and shared snapshot
UUID, and `plan.Endpoints` is already per-member with per-member GUID
verification - both populated today only under `replicate=on`.

`Reference.Sources` becomes unconditional and canonical rather than "the `-R`
case". Writing the root into the singular `Dataset`/`GUID` fields and setting
`Sources` only above one member makes a one-member and a multi-member root
serialize differently and stores member 0 twice with nothing checking the
copies agree. Multi-member is the normal case here, so the conditional encodes
nothing. The back-fill of `r.Dataset` for pre-authority records is deleted
outright under this document's no-users stance.

## 7. Removed

- `missingMappedAncestor` and `missingLocalDestinationAncestor`: intra-root
  ordering is internal to one actor's transfer, not reconstructed across jobs.
- The `waiting-retry` "waiting for destination ancestor" outcome.
- `recursive bool` on the snapshot seam and every `-r` snapshot path.
- Reseed's cascade: destroy scope and cleanup scope are the same member set by
  construction.

Retained:

- `ForReplicationScope`, scoped to `replicate=on`, where it is the honest
  consequence of a single stream rather than a workaround.
- `nearestReceiveLockScope`: contention between distinct roots mapping into
  one destination namespace is real and unrelated to nesting.
- `prepareReceiveParents`, unchanged in mechanism. A hole's mapped destination
  must exist for a deeper member to be received, and under this model no actor
  claims the hole's mapped name. Peer actors claiming each other's mapped
  destinations is what made the same mechanism dangerous.
- **The bootstrap versus promoted binding asymmetry at
  [plan.go:294](../internal/transfer/plan.go).** This is a change of position
  from the earlier draft of this document, which proposed removing it. The
  asymmetry refuses to let an ancestor-only bootstrap binding authorise a
  destination that has since come into existence, and that is not only about
  peers materialising each other's destinations: it also covers a destination
  destroyed and recreated outside boomerangz entirely.
  `TestGuestDaemonPowerLoss/receive-commit` pins an independent route into the
  same blocked state - a crash between a bootstrap receive committing and the
  transfer verifying - on a single root with no descendants, which this model
  does not touch. The binding contract is therefore unchanged and that test is
  not inverted.

## 8. Known gap, out of scope

Two disjoint sources can map to one destination: `zroot/a/data` and
`zroot/b/data` both map to `backup/data` under `discard=all`, and nothing
rejects it. This predates the model and is a property of name mapping, not of
ownership, but the model widens the blast radius: each side is now a root that
owns a whole subtree, so two colliding roots overlay their entire subtrees,
and a reseed of one destroys the other's replicas. Recorded in
[destination-collisions.md](destination-collisions.md).
