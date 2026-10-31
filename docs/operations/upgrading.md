# Upgrade notes

Most releases need nothing from you beyond upgrading the package. A release
that does has a section here, newest first, saying what changed, what to do,
and how to check it is done. Read every section between the version you run
and the one you are installing. The GitHub release for such a version leads
with the same notes, and the Arch package prints a reminder when an upgrade
crosses it.

Upgrade through the same package channel you installed, as described in
[Installation](/getting-started/installation#upgrade-or-remove).

## v0.3.0

This release changes how recovery proofs and target bindings are stored, and
does not read the ones earlier releases wrote. Stop the daemon before
upgrading, and clear the old records on every actor root before starting it
again.

### Clear old recovery proofs and target bindings

A recovery proof, `org.boomerangz:state:reference:*`, no longer lists every
snapshot it protects, so a recursive stream's proof no longer outgrows a ZFS
property. A target binding, `org.boomerangz:state:target:*`, now stores only
the destination's identity and the dataset it maps to. With either left in the old form, every transfer,
clean and retirement of the root refuses with `invalid reference proof` or
`invalid target binding`.

For each actor root, here `tank/data`, remove the old proofs and bindings and
the holds and bookmarks the proofs recorded:

```sh
root=tank/data
zfs get -H -o property -s local all "$root" | grep -E '^org\.boomerangz:state:(reference|target):' | grep -v ':suspended$' | xargs -r -I{} sudo zfs inherit {} "$root"
zfs list -H -t snapshot -o name -r "$root" | xargs -r zfs holds -H | awk '$2 ~ /^boomerangz-/ { print $2, $1 }' | while read -r tag snapshot; do sudo zfs release "$tag" "$snapshot"; done
zfs list -H -t bookmark -o name -r "$root" | grep '#boomerangz-' | xargs -r -n1 sudo zfs destroy
```

Suspension markers, `org.boomerangz:state:target:*:suspended`, are unchanged
and stay. Then start the daemon and run each root's transfers with
`boomerangz trigger`. A transfer to an existing replica that still shares a
snapshot with its source records a new binding and continues incrementally. One
that reports that the destination has no common base needs
`boomerangz dataset reseed`, as in [Reseed a target](/operations/recovery#reseed-a-target).

Check the result with:

```sh
zfs get -H -o property,value -s local all tank/data | grep 'state:\(reference\|target\):'
```

Each binding's value now reads like `{"version":2,"pool_guid":...,"mapped":"backup/archive","anchor_guid":...}`,
each proof's like `{"created":"..."}`, and `boomerangz status` shows each
target's last transfer verified.

## v0.2.0

This release replaces per-dataset actors with [actor roots](/guide/datasets).
Stop the daemon before upgrading, and check the points below before starting
it again.

### Grant `rename` on every destination

Boomerangz now renames a placeholder dataset aside when a dataset gets a
replica of its own where it had only held other replicas, so destinations need
the `rename` permission in addition to what they had. Add it wherever you
delegated permissions to the receiving account, for local destinations and on
each remote:

```sh
sudo zfs allow -u boomerangz rename backup/boomerangz
```

Use the account and dataset from your own `zfs allow` setup, as shown in
[Security](/guide/security#destination-datasets), and check the result with
`zfs allow backup/boomerangz`. Transfers that do not replace a placeholder
keep working without it; one that does stops with a permission preflight error
naming what is missing.

The daemon now also checks every destination for the permissions it may
need, `rename` included, when it first reaches it, and reports anything
missing as a warning in `boomerangz status` and in its log, naming the
destination, the account and the permissions. After starting the upgraded
daemon, run `boomerangz status` once a transfer has reached each destination;
it lists no `WARNINGS` when every destination grants what it needs. A remote
is checked only once its endpoint opens. After granting a missing permission,
run `boomerangz config reload` to have the daemon check again. See
[Destination permissions](/operations/#destination-permissions).

### Review how your datasets are grouped

An enabled dataset with no enabled ancestor is now an actor root, and every
enabled dataset beneath it is one of its members rather than a root of its
own. Before starting the daemon, run:

```sh
boomerangz dataset list
```

A dataset that used to replicate on its own and now reads `member` is
snapshotted, pruned and sent by its root, on its own retention grid and with
its own send options. `trigger`, `dataset adopt` and `dataset reseed` refuse a
member and name its root; operate on the root instead. If a root's transfer
then reports that an existing destination has no common base, reseed that
root's target as described in [Recovery](/operations/recovery#reseed-a-target).

A dataset set to `enabled=off` beneath a root is excluded from it. A root with
`replicate=on` still sends its whole subtree as one `zfs send -R` package.

### Check for blocked destinations

When two roots' replicas would overlap on a destination, that destination is
now blocked for the roots involved, rather than replication failing at receive
time. `dataset list` names blocked destinations beside each root, and
`dataset inspect` says what each collides with. A root keeps snapshotting,
pruning and sending to its other destinations. Fix the mapping - usually with
`discard` or a different destination - and the block clears on the next scan.

### Worth knowing

- `status` and the job log report a new state, `waiting-lock`, for a transfer
  held back by another job's lock on the same destination. Anything that
  matches job states should treat it as running.
- `dataset abort-receive` discards an interrupted receive that keeps failing to
  resume. See [Recovery](/operations/recovery#discard-an-interrupted-receive).
- Turning `replicate=on` for a root that replicated member by member needs a
  reseed of its targets; turning it off again does not.
