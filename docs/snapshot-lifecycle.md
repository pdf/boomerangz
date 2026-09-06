# Snapshot lifecycle

The administrative commands and automatic daemon lifecycle described below are
available. Live status and control usage are documented in
[Status and remote control](control-api.md).

## Ownership and snapshots

Each installation has a random UUID in `<identity_dir>/installation-id`. First
management creates a cryptographically random lineage UUID and writes both that
lineage and the installation owner locally on the dataset or replication root.
Snapshot creation atomically attaches lineage, snapshot UUID,
and UTC creation timestamp with `zfs snapshot -o`. A recursive operation uses the
same metadata across the snapshot set. Descendants are governed by the replication
root; inherited or received owner/lineage values are provenance, not authority.

Mutation additionally requires locally configured activation, an explicitly local
root lineage, and an explicitly local owner matching the installation ID. A valid
different owner is dormant: snapshot, prune, transfer and recovery-state mutation
are refused. Snapshot ownership requires matching name, explicitly stored metadata,
lineage and a nonzero ZFS GUID. A familiar name alone is insufficient.

The grid is anchored at the youngest owned snapshot. Each adjacent window keeps
its oldest snapshot; foreign snapshots do not fill windows. Holds, clones and
resume dependencies prevent pruning. Pruning rechecks dataset identity, snapshot
GUID, ownership and eligibility before each exact deletion. Missed snapshot
schedules are not backfilled. See [the policy reference](dataset-policy.md).

## Adoption

```sh
boomerangz dataset adopt pool/data
boomerangz dataset adopt pool/data --apply
```

The first command previews transfer of an existing consistent lineage to this
installation. `--apply` is the explicit confirmation in non-interactive use and
changes both missing lineage recovery state and the owner only after revalidation.
Received lineage is provenance and hidden conflicting values are never overwritten.
Multiple candidate lineages or resume state block adoption. Adoption is
exact-dataset only. Every preview shows the effective policy and target set. Local
targets include their mapped dataset, stored binding, current pool/dataset GUID
identity, and verification status; unavailable or mismatched local targets block
apply. Remote targets are probed during adoption. An unreachable target is
recorded as suspended and cannot receive work; rerun adoption when it is
reachable to review its identity and mapping and clear the suspension.

If the installation identity directory is lost while the original pools remain,
the separate preview-first recovery command inventories activated roots:

```sh
boomerangz identity recover
boomerangz identity recover --owner <installation-uuid> --apply
```

Automatic selection requires exactly one valid owner. Recovery verifies local
root lineage and snapshot/reference evidence, refuses when the current fresh ID
already owns any lineage or a lifecycle operation is active, and atomically
replaces only the expected identity file.

## Holds and bookmarks

Target-specific holds protect snapshots needed for recovery. A recursive snapshot
set uses one root reference proof containing every protected source member, avoiding
colliding properties for the shared snapshot UUID. Versioned bookmarks record
replication checkpoints. Old references remain until their dependencies are
resolved; interrupted operations retain recovery metadata for retry.

Clean verifies reference records, snapshot metadata, and GUIDs before releasing
holds or deleting bookmarks. Unknown boomerangz-looking references block clean;
foreign references are never claimed. All internal property names and record
contents are documented in [the property reference](dataset-policy.md).

## Deactivation

Disabling management must preserve snapshots, holds, bookmarks and resume tokens.
The default `inactive_grace_period` keeps an owned dataset recoverable for 24
hours before it becomes eligible for automatic retirement; setting the period
to zero disables automatic retirement. The daemon records the durable inactive
marker, waits until the deadline, and then applies ownership-safe retirement. An
inaccessible target or unresolved receive state keeps retirement pending for a
bounded retry rather than discarding recovery evidence.

Snapshot deadlines are maintained independently of dataset discovery. A delayed
wakeup creates one current snapshot rather than backfilling every missed interval.
Local and SSH transfers have separate worker limits, and an unavailable remote
retains only the newest pending snapshot for each source and target while
preserving resumable ZFS state.

## Explicit clean

```sh
boomerangz dataset clean pool/data
boomerangz dataset clean pool/data --recursive --apply
boomerangz dataset clean --all
boomerangz dataset clean pool/data --destroy-owned-snapshots --apply
```

Named scopes are exact unless `--recursive` is specified. `--all` explicitly
selects all local datasets recursively and cannot be combined with names. Scope
selection never implies snapshot destruction. Default preview performs no writes.
Output lists exact actions, blockers, warnings, and the number of applied actions.

Apply removes only proven references and selected namespace properties. Snapshots
and data remain by default; their effective ownership metadata is cleared.
`--destroy-owned-snapshots` additionally deletes proven snapshots without unresolved
dependencies. Foreign snapshots and non-boomerangz properties are preserved,
including target settings previously applied through `set_prop`.

Apply re-inventories before the first mutation and checks every subsequent
transition against its intended effect. Failures stop further operations and
report partial progress; clean does not roll back or implicitly abandon receives.
Blockers include resume state, unproven references, conflicting lineage,
unavailable target verification, and exposing inherited `enabled=on` from outside
the selected scope. The clean operation is local; it never silently cleans
another host.

The daemon holds `<paths.socket_path>.lifecycle.lock` for its lifetime, while a
standalone apply holds it for the operation. Clean coordinates through the
daemon's control socket when it is running. The current adoption and identity
recovery commands refuse to run while that socket exists. Adoption probes
configured local and SSH targets just in time; an unreachable SSH target is
recorded as suspended and must be verified before replication resumes.
Standalone and daemon-coordinated clean perform the same just-in-time
configured-target checks.
Use the same configured socket path for all cooperating processes.

ZFS CLI operations do not provide atomic compare-and-swap against independent
administrative commands. Avoid concurrent external ZFS changes during lifecycle
administration; observed changes cause refusal but cannot eliminate every race.

### Received-property limitation

Plain `zfs inherit` removes local values but only masks received values. `zfs inherit -S` can restore them. Hidden dynamic names
can disappear from `zfs get all`; explicit known ownership-key queries still reveal
their received values. The clean operation reports this limitation and never claims permanent
erasure of received values. Restoring hidden snapshot metadata externally
can restore ownership evidence; received public properties still never affect
boomerangz policy.
