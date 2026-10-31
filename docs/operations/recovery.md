# Recovery and maintenance

Administrative commands preview their changes before they apply them. Read the
complete preview, resolve blockers, and save the output with your change record
before using `--apply`. The preview is printed for reading; add `--json` for
the structured form when a script consumes it, as described in the
[CLI reference](/reference/cli#dataset-lifecycle).

## Interrupted or unavailable destination

When a destination is temporarily unavailable, Boomerangz keeps the source
state needed for a later retry. Restore connectivity and inspect status:

```sh
boomerangz status
boomerangz trigger tank/data
```

Do not manually delete snapshots that appear retained during an interruption.
They may be needed to continue replication without retransmitting the complete
dataset.

## Discard an interrupted receive

An interrupted receive is resumed on the next transfer, and a transfer never
discards one on its own. If resuming keeps failing - the transfer error says
the interrupted receive cannot be resumed - discard it explicitly. Stop the
daemon, then preview:

```sh
sudo systemctl stop boomerangz.service
sudo -u boomerangz -H boomerangz dataset abort-receive tank/data backup/archive
```

The preview lists every receive on that target that would be discarded, across
the root and all of its members. Apply it:

```sh
sudo -u boomerangz -H boomerangz dataset abort-receive tank/data backup/archive --apply
sudo systemctl start boomerangz.service
```

Only the partially received data is discarded. The replica, its snapshots, the
target binding and the source's replication history all stay, so the next
transfer plans from what the destination already holds and sends the discarded
part again. The destination needs the `destroy`, `mount` and `receive:append`
permissions on the affected datasets.

## Reseed a target

Use a reseed when Boomerangz reports that an existing destination has no common
base or that its persistent binding no longer matches the mapped destination.
Stop the daemon first, then preview the exact managed source and target:

```sh
sudo systemctl stop boomerangz.service
sudo -u boomerangz -H boomerangz dataset reseed tank/data backup/archive
```

For a remote target, use its configured remote name instead of its destination
dataset:

```sh
sudo -u boomerangz -H boomerangz dataset reseed tank/data home-backup
```

The preview lists the persistent or currently resolved binding, the surviving
receive parent, every mapped destination object that will be destroyed,
resumable receive state that will be abandoned, and source references that will
be released. Save and review that output before applying it:

```sh
sudo -u boomerangz -H boomerangz dataset reseed tank/data backup/archive --apply
sudo systemctl start boomerangz.service
```

Applying a reseed permanently destroys the mapped replica subtree. When
`discard=off`, that subtree is the configured destination root itself. Do not
apply the plan if that root contains data which is not disposable replica data.
This is required because Boomerangz performs non-forcing full receives into the
exact mapped destination: OpenZFS creates that dataset from the full stream and
will not treat an existing dataset as a fresh seed. Receive property overrides
and exclusions do not change that rule.
The effective destination account must have the normal receive permissions on
the displayed `receive_anchor`; for `discard=off`, delegate those permissions on
the destination root's parent so that the root can be created again.
The source dataset and its snapshots are retained; only recovery references for
the selected target are released. A later daemon reconciliation starts a fresh
full transfer.

If a replica is mounted for recovery, keep it read-only and avoid modifying it.
An ordinary incremental receive does not remount an already mounted dataset,
but local changes can make a non-forcing receive fail. Unmount the mapped
dataset and its descendants before applying a reseed because they will be
destroyed.

The command operates on one actor root, and resets every member of that root
on the target at once: the root's replica subtree holds every member's
replica, and the root holds every member's recovery state and the target's
binding. Run it against the root; it refuses a member and names the root to
use. The target may be one only a member sends to.

Reseed is retryable after partial failure. It refuses to proceed if the bound
pool identity, configured mapping, source authority, or effective target has
changed during preflight.

A reseed replaces the replica where the binding says it is, so it refuses a
target whose mapping has changed since the binding was recorded, for instance
after a new `discard` or destination root: transfers to it report that the
target mapping changed. Restore the previous mapping, or
[forget the target](#forget-a-target) so the next transfer seeds it at the new
mapping, and destroy the old replica yourself once you no longer need it.

## Deactivation and automatic retirement

Disable management of an actor root and its whole subtree by setting
`enabled=off` on the root:

```sh
sudo zfs set org.boomerangz:enabled=off tank/data
```

The default 24-hour inactive grace period allows accidental deactivation to be
reversed. If you re-enable the root during the grace period, management
continues using its existing history. Retirement covers every dataset beneath
the root.

Setting `enabled=off` on a member instead excludes it from its root without
deactivating anything: it stops being snapshotted and sent, and what it already
holds is pruned under its own grid until the root retires.

After the grace period, the daemon retires only state it can prove belongs to
this installation. An unavailable destination or unresolved transfer keeps the
retirement pending rather than discarding recovery state.

## Explicit clean

`dataset clean` removes Boomerangz management state from local datasets. It
does not contact another host to clean that host.

Preview an exact dataset:

```sh
boomerangz dataset clean tank/data
```

Include descendants, then apply the reviewed plan:

```sh
boomerangz dataset clean tank/data --recursive
boomerangz dataset clean tank/data --recursive --apply
```

Select every local dataset explicitly:

```sh
boomerangz dataset clean --all
```

Snapshots remain by default. Delete only snapshots positively identified as
owned and no longer required:

```sh
boomerangz dataset clean tank/data --destroy-owned-snapshots
boomerangz dataset clean tank/data --destroy-owned-snapshots --apply
```

The operation stops when identity, destination, or recovery evidence is
ambiguous. It can report partial progress if the environment changes during an
apply. Review the output before retrying.

## Forget a target

A plain clean checks every target the dataset keeps recovery state for, and is
blocked while one cannot be checked. When a target is gone for good, such as a
remote that was decommissioned, or when its mapping has changed, forget that
one target instead. Stop the daemon or leave it running, then preview:

```sh
boomerangz dataset clean tank/data --forget-target home-backup
```

Name the target as configuration names it: a remote's name, or a local
destination root such as `backup/archive`. A remote removed from configuration
can only be named by its target ID, which is the 64-character hex string in a
blocker like `target 3f9c...e1: recorded target is not configured`, and in the
`org.boomerangz:state:reference:` and `org.boomerangz:state:target:` property
names on the root.

The preview lists the target's recovery proofs, the holds and bookmarks they
record on the root's and members' snapshots, its binding and its suspension
marker, and nothing else. The target is not contacted, and nothing on it
changes. Apply it:

```sh
boomerangz dataset clean tank/data --forget-target home-backup --apply
```

Ownership, the snapshots and every other target's state stay. A forgotten
target that is still configured is bound afresh by the next transfer: one to
an existing replica that still shares a snapshot with its source continues
incrementally, one to a new mapping seeds it, and one that reports that the
destination has no common base needs a [reseed](#reseed-a-target). To clean the whole dataset after forgetting an
unreachable target, run a plain `dataset clean` as usual.

Forgetting takes exact source roots: it cannot be combined with `--recursive`,
`--all` or `--destroy-owned-snapshots`, and a root that keeps no state for the
named target blocks the preview, which also catches a misspelled name.

## Adopt a moved or restored dataset

Use adoption when an existing managed lineage has intentionally moved under
the authority of this installation:

```sh
boomerangz dataset adopt tank/data
boomerangz dataset adopt tank/data --apply
```

The preview reports the dataset policy and every configured destination. An
unreachable remote remains suspended until you rerun adoption with the remote
available and review its identity.

Adoption is exact-dataset only. Conflicting lineage or unresolved receive state
blocks it, and so does recovery state written by an older release: run
`boomerangz migrate` on the dataset first, as in the
[upgrade notes](/operations/upgrading).

The same command promotes a replica, for instance after losing the source. A
replica received with `replicate=on` or `props=on` arrives with the sending
installation's owner and recovery state as received properties, and none of
its policy. Give it a policy of its own on the host that will manage it, then
adopt it:

```sh
sudo zfs set org.boomerangz:enabled=on org.boomerangz:local=backup/next backup/data
boomerangz dataset adopt backup/data
boomerangz dataset adopt backup/data --apply
```

The promoted dataset continues the source's lineage, so its existing
snapshots stay owned and later transfers build on them. The source's received
state is kept as a record of where the replica came from and is otherwise
ignored.

## Recover an installation identity

If the identity directory is lost but the original managed pools remain,
preview identity recovery:

```sh
boomerangz identity recover
```

If more than one owner is found, choose the intended UUID from the preview:

```sh
boomerangz identity recover --owner INSTALLATION_UUID
boomerangz identity recover --owner INSTALLATION_UUID --apply
```

Do not use recovery to clone one active installation's authority onto another.

## Revoke a pairing

```sh
boomerangz pairing list
boomerangz pairing revoke PAIRING_ID
```

Revocation disables credentials managed by Boomerangz. A client certificate
issued by an external certificate authority must also be revoked through that
external PKI.
