# Upgrade notes

Most releases need nothing from you beyond upgrading the package. A release
that does has a section here, newest first, saying what changed, what to do,
and how to check it is done. Read every section between the version you run
and the one you are installing. The GitHub release for such a version leads
with the same notes, and the Arch and Ubuntu packages print a reminder when an
upgrade crosses it.

Upgrade through the same package channel you installed, as described in
[Installation](/getting-started/installation#upgrade-or-remove).

## v0.3.2

Earlier releases could prune a replica's newest snapshot. That happened when
snapshots were closer together than the policy's shortest tier, most often
right after a policy moved to a coarser grid, such as from `12x5m,24x1h` to
`24x1h`. The newest snapshot is the base the next incremental is sent from.
Without it the replica holds data newer than its newest remaining snapshot,
and every transfer to it fails, for a local target with
`cannot receive incremental stream: destination ... has been modified since
most recent snapshot`, or for a remote one with a broken pipe and no reason
from the receiver. Each failed run then lost the next-newest replica
snapshot the same way. This release always keeps the newest snapshot, and a
remote transfer now reports the remote `zfs receive` error.

The release does not repair a replica that is already in this state.

### Find affected replicas

On each destination, look for replicas under the target's root that have
data written since their newest snapshot:

```sh
zfs get -r -H -o name,value written backup/flechette | awk '$2 != "0"'
```

A replica listed there whose target shows `blocked` in `boomerangz status`
with one of the errors above is affected. A completed receive leaves
`written` at `0`, so unless something else mounts the replica and writes to
it, a non-zero value on a replica whose transfers fail means this.

### Roll each one back

Roll each affected replica back to its newest snapshot. This discards only
data the pruned snapshots carried, which the source still holds under its
recovery references:

```sh
zfs list -H -t snapshot -o name -s createtxg backup/flechette/data/.local | tail -1
sudo zfs rollback backup/flechette/data/.local@boomerangz-...
```

Run the rollback on the destination host, as a user allowed to roll the
replica back. The next transfer sends from that snapshot and catches the
replica up. Check it with `boomerangz status`: the target's last transfer
reads `succeeded`, and `written` on the replica reads `0` again.

## v0.3.0

This release changes how boomerangz stores recovery proofs and target
bindings, and stops reading the old form: each source root now records the
format of its state in `org.boomerangz:state:version`, and a root without the
marker is in the earlier format. Until it is migrated, the daemon snapshots,
prunes and replicates nothing for it, and reports
`state on tank/data is format v1, and this build reads v2; stop the daemon
and run boomerangz migrate tank/data`.

### Migrate every source root

Stop the daemon before upgrading. After upgrading, preview the migration of
every dataset that keeps boomerangz state:

```sh
sudo systemctl stop boomerangz.service
sudo -u boomerangz -H boomerangz migrate --all
```

Each root is listed with `Status: pending` and the properties it rewrites:
every recovery proof, `org.boomerangz:state:reference:*`, shrinks to a fixed
size; every target binding, `org.boomerangz:state:target:*`, keeps only the
destination's identity and the dataset it maps to; and the version marker is
written last. Holds, bookmarks and snapshots are not touched, so transfers
continue incrementally afterwards. A root listed as `Status: blocked` names what
cannot be carried over; resolve it, or forget that target with
`boomerangz dataset clean --forget-target`, and preview again. Apply it, then
start the daemon:

```sh
sudo -u boomerangz -H boomerangz migrate --all --apply
sudo systemctl start boomerangz.service
```

A migration that is interrupted leaves the root in its old format and can be
run again; it finishes what is left. Snapshots taken before the upgrade keep
an `org.boomerangz:state:created` property, which this release no longer
reads or writes; it goes with the snapshots as retention prunes them.

Check the result with:

```sh
sudo -u boomerangz -H boomerangz migrate --all
```

Every root now reads `Status: current`, and `boomerangz status` shows each
target's next transfer verified.

### Administrative commands print text

`dataset adopt`, `dataset clean`, `dataset reseed`, `dataset abort-receive`,
`identity recover` and `migrate` now print a readable plan by default rather
than JSON, matching `dataset list` and `dataset inspect`. A script that parses
their output must add `--json`, which prints exactly the JSON these commands
printed before:

```sh
boomerangz dataset clean tank/data --json
```

Check a script by running its command once by hand with `--json` and
confirming the output is the JSON it expects; without the flag the output
begins with `Dataset:` or `Current identity:` instead.

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
