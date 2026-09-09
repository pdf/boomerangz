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
