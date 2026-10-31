# Manage datasets

Boomerangz reads policy from locally configured `org.boomerangz:*` ZFS user
properties. A dataset remains unmanaged until `org.boomerangz:enabled=on`
applies to it.

## Inspect before changing

List discovered datasets and inspect one effective policy:

```sh
boomerangz dataset list
boomerangz dataset inspect tank/data
```

These commands are read-only and produce human-readable output by default.
`dataset list` labels each dataset as one of:

| Status | Meaning |
| --- | --- |
| `active` | an actor root: enabled, with no enabled ancestor |
| `member` | inside a root's subtree and managed by that root |
| `excluded` | inside a root's subtree but left out of it |
| `inactive` | not enabled and not inside any root |
| `invalid` | its policy has errors; inspect it for the reported errors |

A root with a destination blocked by a collision stays `active`; `dataset list`
names the blocked destination beside it.

`dataset inspect` shows activation and validation status, the actor root the
dataset belongs to and, for a root, its members and blocked destinations,
together with the effective policy, property provenance, warnings, and
errors.

Add `--json` when a script or other structured consumer needs the stable JSON
representation:

```sh
boomerangz dataset list --json
boomerangz dataset inspect tank/data --json
```

## Set a snapshot policy

A policy is a comma-separated grid of `count x duration` tiers:

```sh
sudo zfs set org.boomerangz:policy=12x5m,24x1h,14x1d tank/data
sudo zfs set org.boomerangz:enabled=on tank/data
```

The shortest duration is the snapshot cadence. In this example Boomerangz takes
a snapshot every five minutes, keeps progressively fewer snapshots as they age,
and retains coverage through the daily tier.

Supported duration units are:

| Unit | Meaning |
| --- | --- |
| `m` | minute |
| `h` | hour |
| `d` | 24-hour day |
| `w` | seven days |
| `mo` | 30 days |
| `y` | 365 days |

These are elapsed durations rather than calendar boundaries. For example,
`12mo` is 360 days and is not the same duration as `1y`.

## Roots and members

ZFS properties normally pass from a parent dataset to its children, and
`enabled` is no exception. Enabling a dataset makes it an **actor root**, and
it owns its whole subtree:

```sh
sudo zfs set org.boomerangz:policy=24x1h,14x1d tank/home
sudo zfs set org.boomerangz:enabled=on tank/home
```

A dataset is an actor root when it is enabled and no ancestor is. Every enabled
dataset beneath a root is one of its **members**, not a root of its own: the
root's lineage, target bindings, recovery state and jobs cover all of them,
and members carry no Boomerangz state themselves.

Set `org.boomerangz:enabled=off` locally on a child to exclude it and
everything that inherits from it:

```sh
sudo zfs set org.boomerangz:enabled=off tank/home/.cache
```

An exclusion does not sever deeper datasets from the root. Setting
`enabled=on` again further down makes that dataset a member of the same root,
with a hole at the excluded dataset in between:

```sh
sudo zfs set org.boomerangz:enabled=off tank/home/projects
sudo zfs set org.boomerangz:enabled=on tank/home/projects/current
```

Nothing is snapshotted or sent for a hole, but its name is created at each
destination as an empty placeholder so the deeper member can be received
beneath it. If the hole is included again later, its own replica replaces the
placeholder and the replicas beneath move under it with their history. The
same happens when a root starts sending to a destination only its members used
before. This needs the `rename` permission on the destination.

Each member keeps its own retention grid, send options, incremental mode,
receive overrides and destinations, including destinations its root does not
use. A root snapshots exactly the members that are due, atomically, and prunes
each against its own grid, so a `14x1d` member is not snapshotted every five
minutes because its root is. The receive path mapping (`discard`) and the wire
format (`replicate`) belong to the root: a member that sets either gets a
warning and follows its root.

A member's replica lands beneath its root's, which under the default
`discard=off` is a remote's configured root itself. A remote never creates its
configured root except by receiving the root's own replica, so a member that
sends to a remote its root does not use needs that configured root to exist
already. Until it does, the member's transfer stops and says to create it on
the remote or to add the remote to the root.

Operate on the root, not on a member. `dataset adopt`, `dataset reseed` and
`trigger` refuse a member or excluded dataset and name its root. A reseed of
the root resets every member's replica and recovery state on that target at
once.

Inspect parents and children before enabling a broad root.

## Add destinations

Select configured remote destinations by name:

```sh
sudo zfs set \
  org.boomerangz:discard=first \
  org.boomerangz:remote=home-backup \
  tank/data
```

For a destination dataset on the same host:

```sh
sudo zfs set \
  org.boomerangz:discard=first \
  org.boomerangz:local=backup/data \
  tank/data
```

`first` is recommended when the configured local or remote root is a persistent
destination container. The default `off` instead treats that configured path as
the exact, replica-owned receive dataset; it must initially be absent and its
receive permissions must be delegated on the parent. Review the
[receive path mapping](/reference/properties#receive-path-mapping) before
selecting a destination.

Multiple names are comma-separated. Empty entries and unknown remote names make
the affected policy invalid. Configure and secure the destination before
selecting it.

Each root's replica on a destination must be its own. If two roots' replicas
would overlap - `tank/a/data` and `tank/b/data` both under `discard=all`, say,
or one root's two destinations nested inside each other - that destination is
blocked for the roots involved: nothing is sent there, and `dataset reseed` and
`dataset abort-receive` refuse it. Only the destination stops. Each root stays
`active`, keeps snapshotting and pruning, and keeps sending to its other
destinations. `dataset list` shows blocked destinations beside the root, and
`dataset inspect` names what each collides with.

A replica a root has already established - one its stored binding proves -
keeps its destination, and only the newly configured root that would overlap
it is blocked, so a mistake in new configuration cannot stop a working
backup. When neither side, or both, has an established replica there, both
are blocked. A local destination inside an active root is blocked the same
way, because the received replicas would become that root's members. Two
remotes collide when they reach the same host's pools, whatever account they
connect as.

A remote that turns out to receive into one of this host's own pools - an SSH
remote addressed back at this machine, say - is recognised by its pool's GUID
the first time it is reached, before anything is received. Its replicas are
then checked like a local destination's, against this host's actor roots and
local replicas, and the transfer goes ahead only if that destination is not
blocked.
`dataset list` and `dataset inspect` do not connect to remotes, so they cannot
show this case; the daemon reports it, and so does `dataset reseed`.

## Recursive replication

```sh
sudo zfs set org.boomerangz:replicate=on tank/data
```

Without `replicate`, a root sends each member as a stream of its own, with that
member's send options. With `replicate=on` the root sends its whole subtree as
one `zfs send -R` package instead: every descendant becomes a member, on the
root's grid and with the root's send options, and descendants'
`org.boomerangz:*` properties are ignored with a warning.

`replicate=on` cannot exclude anything. A descendant with `enabled=off` is
still in the package, and gets a warning of its own saying so. Recursive
replication can also carry snapshots not created by Boomerangz, and receive
behavior can remove destination snapshots that are absent from the source
package. Enable it only after reviewing the complete hierarchy and testing the
destination mapping.

Turning `replicate=on` for a root that already replicated member by member needs
a [reseed](/operations/recovery#reseed-a-target) of each of its targets: holes
and excluded datasets were never sent, so the recursive package has no common
base to continue from. The transfer says so before changing anything. Turning
`replicate` off again needs nothing: every member's replica already shares a
snapshot with its source, and each continues incrementally on its own stream.
Replicas of datasets that are no longer members stay on the destination
untouched.

## Deactivate a dataset

```sh
sudo zfs set org.boomerangz:enabled=off tank/data
```

Deactivating a root stops new management of its whole subtree but does not
immediately remove recovery state. By default, the root remains recoverable for
24 hours before automatic retirement becomes eligible; retirement then covers
every dataset beneath it. Change that delay with
`daemon.inactive_grace_period`; set it to `0s` to disable automatic retirement.

Re-enable the root during the grace period to resume management without
discarding its established history.

Excluding a member instead only stops new snapshots of it. It has no recovery
state of its own to retire: the snapshots it already holds stay under its own
grid, keeping the newest few, and go when its root retires. Re-enabling it makes
it a member again with its history intact.

See the [property reference](/reference/properties) for every available setting.
