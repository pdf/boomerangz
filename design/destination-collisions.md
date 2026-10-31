# Design: colliding destination mappings

Status: implemented in discovery (`markCollisions`). Recorded while designing
[root-actor-scope.md](root-actor-scope.md), which widens it.

## 1. The gap

Two disjoint sources can map to the same destination. Under `discard=all` only
the final component of the source name is appended to the configured root, so

```
zroot/a/data  local=backup  discard=all  ->  backup/data
zroot/b/data  local=backup  discard=all  ->  backup/data
```

Nothing rejects this. It is discovered at receive time, as a lineage conflict
or a GUID verification failure, after a stream has already run. `discard=first`
narrows it rather than closing it: two pools with the same path below their
pool component collide the same way.

## 2. Why the root model widens it

The collision is a property of the name mapping, not of ownership, so it
predates the root model. The blast radius is what changes. Under the
peer-actor model a collision was between two individual datasets at one mapped
path; under the root model each side owns its whole subtree and every member's
replica is placed relative to the root's mapped destination, so two colliding
roots overlay their entire subtrees.

Reseed makes that concrete. `ReseedService.Apply` destroys the mapped
destination recursively, and under the root model destroy scope equals the
root's member set by construction - exactly right when one root owns that
name, and exactly wrong when two do. A reseed of one root would destroy the
other's replicas and leave its binding and reference pointing at datasets that
no longer exist, which is the shape of the incident in
root-actor-scope.md section 1 re-created between roots rather than within one.

## 3. What validation would look like

The check is on configuration, not on a stream: for one target, no two roots
may resolve to mapped destinations where either contains the other.
Containment rather than equality, because a root owning its subtree means an
overlap anywhere in the mapped path is a conflict.

It needs the set of actor roots and their resolved targets together. Discovery
knows the roots and their local targets; canonical remote targets come from
configuration, so remote collisions cannot be detected in discovery alone. The
natural split is:

- local targets validated where roots and mappings are both known, surfaced
  the way other policy errors are, so a colliding root is invalid rather than
  silently racing.
- remote targets validated where configuration is resolved, since two roots
  can only collide on a remote through the same configured remote name.

### 3.1 As implemented

Discovery checks every actor root's replica on every target its root or any
member sends to, and blocks that target for each side of an overlap, with a
reason naming the other side (`Entry.Blocked`). Only the target stops: the
root stays valid and scheduled, so its snapshots, pruning and other targets
go on, and the daemon reports the blocked target as that transfer job's
outcome without protecting anything as pending for it. `dataset reseed` and
`dataset abort-receive` refuse a blocked target. Three cases share one
containment rule:

- two roots whose replicas in one destination namespace contain one another;
- one root whose replicas on two of its own targets do, since a reseed of the
  outer one destroys the inner;
- a local replica that overlaps any actor root on this host, which would
  inherit that root's activation and be snapshotted as its members.

A replica the root has already established at the overlapping location keeps
its target: when one side's stored binding proves a replica at its mapped
dataset on that target and the other's does not, only the other is blocked,
so new configuration cannot stop a working replica. With that evidence on
neither side or both, both are blocked.

An earlier version invalidated every root involved. That stopped snapshots and
pruning along with every other target, and let a mistake in a newly added
root stop an established one; the safety property - nothing is written into an
overlapping location - needs only the target blocked.

A destination namespace is `local` for local targets, and for a remote the
transport and address its canonical target names, without the user: two
remotes reaching one host's pools share one dataset tree whatever account
they connect as. The daemon derives namespaces from the clients it builds; the
CLI derives the same without connecting. A remote whose credential cannot be
read has no known destination and goes unchecked in that scan.

A local target and a remote that reach the same pool, such as an SSH remote
pointed at this host, are not tied together by configuration. They are caught
when the remote is first reached instead: its pool GUID matches a local pool,
and the remote's replicas are revalidated in the local namespace before
anything is received. See root-actor-scope.md section 9.7. Identity recovery scans without destinations: it restores owner
markers and sends nothing, so blocked targets do not concern it.

## 4. Out of scope

Two roots on different hosts replicating into one destination through separate
installations. The target binding and lineage checks are what stand between
those, not a local mapping check.

## 5. Relationship to the bootstrap binding asymmetry

This validation is not a reason to remove the bootstrap versus promoted
binding asymmetry in `transfer.Build`. That asymmetry refuses to let an
ancestor-only bootstrap binding authorise a destination that has since come
into existence, which covers any same-named destination reappearing without
proof of identity - including one destroyed and recreated outside boomerangz.
This validation covers two configured roots resolving to one name. They
overlap only partially. root-actor-scope.md section 7 retains the asymmetry.
