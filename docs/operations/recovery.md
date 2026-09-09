# Recovery and maintenance

Administrative commands preview their changes before they apply them. Read the
complete preview, resolve blockers, and save the output with your change record
before using `--apply`.

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
This is required because Boomerangz performs non-forcing full receives: without
`-d` or `-e`, OpenZFS creates the configured dataset from the full stream and
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

The command operates on one managed source dataset. If the status output lists
blocked descendant source datasets separately, reseed each listed source for
the same target. Destroying an ancestor replica may leave no destination objects
for the later commands, but those commands are still required to release each
source dataset's independent recovery state and binding.

Reseed is retryable after partial failure. It refuses to proceed if the bound
pool identity, configured mapping, source authority, or effective target has
changed during preflight.

## Deactivation and automatic retirement

Disable management by setting `enabled=off`:

```sh
sudo zfs set org.boomerangz:enabled=off tank/data
```

The default 24-hour inactive grace period allows accidental deactivation to be
reversed. If you re-enable the dataset during the grace period, management
continues using its existing history.

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
blocks it.

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
