# Snapshot lifecycle

Phase 3 provides the lifecycle service and explicit administrative commands.
Automatic scheduling, live daemon status, and transfer orchestration remain in
their separately planned phases. Configuration alone does not yet start jobs.

## Ownership and snapshots

First management creates a cryptographically random lineage UUID on the dataset
or replication root. Snapshot creation atomically attaches lineage, snapshot UUID,
and UTC creation timestamp with `zfs snapshot -o`. A recursive operation uses the
same metadata across the snapshot set. Inventory and ownership are verified after
creation; conflicting descendant lineages require explicit resolution.

Ownership requires matching name, explicitly stored metadata, lineage and a
nonzero ZFS GUID. A familiar name alone is insufficient. Missing dataset lineage
with existing internal metadata requires adoption or explicit cleanup, rather
than silently creating a new lineage.

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

The first command previews the unique lineage recoverable from proven snapshots.
`--apply` restores it locally after revalidation. Existing, conflicting or hidden
received dataset lineage is never overwritten. Multiple candidate lineages or
resume state block adoption. Adoption is exact-dataset only.

## Holds and bookmarks

The service records a local per-target/per-snapshot reference proof before taking
a target-specific hold. A checkpoint requires a matching destination GUID from
the caller, then creates a versioned bookmark. The transfer phases will supply
actual destination verification; the lifecycle hook does not perform transfers.
Old checkpoints and holds remain until explicitly released after dependencies
are resolved. A failed operation leaves reconstructable records for retry.

Cleanup verifies reference records, snapshot metadata, and GUIDs before releasing
holds or deleting bookmarks. Unknown boomerangz-looking references block cleanup;
foreign references are never claimed. All internal property names and record
contents are documented in [the property reference](dataset-policy.md).

## Deactivation

The lifecycle gate rejects new work for disabled scopes. Disabling cancels queued
tickets and running transfers; cancelled queued tickets cannot start even after
re-enabling. Running short management operations may finish and reconstruct their
results before releasing their tickets. Quiescence waits for those operations and
cancelled transfers to actually finish. ZFS snapshots, holds, bookmarks and resume
tokens are untouched by deactivation. Scheduler/worker integration and persistent
recovery status reconstruction remain in the daemon and transfer phases.

## Explicit cleanup

```sh
boomerangz dataset cleanup pool/data
boomerangz dataset cleanup pool/data --recursive --apply
boomerangz dataset cleanup --all
boomerangz dataset cleanup pool/data --destroy-owned-snapshots --apply
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
report partial progress; cleanup does not roll back or implicitly abandon receives.
Blockers include resume state, unproven references, conflicting lineage,
unavailable target verification, and exposing inherited `enabled=on` from outside
the selected scope. Cleanup is local; it never silently cleans another host.

Standalone applies hold an exclusive `<paths.socket_path>.lifecycle.lock` for
their duration. An existing control socket causes refusal: daemon coordination is
not implemented yet. The future daemon must participate in the same lock protocol.
Target probing is also not implemented yet, so the CLI retains target references
and reports a blocker rather than assuming the target has no recovery dependency.
The service accepts explicit quiescence and target-verification hooks for that
integration. Use the same configured socket path for all cooperating processes.

ZFS CLI operations do not provide atomic compare-and-swap against independent
administrative commands. Avoid concurrent external ZFS changes during lifecycle
administration; observed changes cause refusal but cannot eliminate every race.

### Received-property limitation

Plain `zfs inherit` removes local values but only masks received values. On the
tested OpenZFS 2.4.3 guest, `zfs inherit -S` restores them. Hidden dynamic names
can disappear from `zfs get all`; explicit known ownership-key queries still reveal
their received values. Cleanup reports this limitation and never claims permanent
erasure or uses direct libzfs APIs. Restoring hidden snapshot metadata externally
can restore ownership evidence; received public properties still never affect
boomerangz policy.
