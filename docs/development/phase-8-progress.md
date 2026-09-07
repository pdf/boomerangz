# Phase 8 progress checkpoint

Phase 8 is in progress. Packaging is no longer part of this phase.

## Verified in the disposable guest

The `phase8-260907a` CachyOS/OpenZFS 2.4.3 guest passed:

- the delegated snapshot, bookmark, hold, full/incremental/recursive send,
  receive-property, interrupted-receive, and resume matrix;
- lifecycle and CLI adoption/clean integration;
- local full, `-i`, `-I`, bookmark, recursive mapping, namespace isolation,
  receive-property, target-binding, and foreign-history tests;
- direct SSH and gRPC-over-`ssh-shell` transfers using a guest-only key;
- daemon scheduling, deactivation, automatic retirement, and shutdown;
- Unix control socket mode, status, trigger, and graceful daemon shutdown under
  the unprivileged account;
- installed systemd execution as `boomerangz`, `NoNewPrivileges`, closed device
  policy, restricted address families, socket ownership, and local status.

The guarded pool cleanup completed before the transient guest shut down. No host
ZFS command ran.

## Hardening changes in progress

- Stream cancellation tests now cover direct SSH and gRPC over `ssh-shell`.
- Daemon guest fixtures use a unique source dataset.
- The test bootstrap explicitly grants fixture-only permissions separately from
  the documented deployment baseline.
- Bootstrap version 2 is required before guest tests execute ZFS commands.
- The refreshed standalone read-only base is
  `/storage/libvirt/qcow2/boomerangz-cachyos-260809-base-v2.qcow2`; `qemu-img
  check` reported no errors and no backing file.
- Packaged configuration paths were found to require `boomerangz` group access;
  the tmpfiles definition now fixes the config directory and primary file modes.

## Remaining Phase 8 work

- run the remaining controlled daemon/process interruption and restart recovery
  cases against the refreshed base;
- exercise two-guest outage and reconnection behavior;
- finish the supported-version and delegated-permission record;
- run the complete Go, race, vet, lint, Buf, systemd, and diff verification set;
- write the Phase 8 handoff and commit the completed phase.

The packaging draft is preserved separately at `feat/phase-10-packaging`
commit `cff963d`. See `phase-10-packaging-notes.md` for confirmed decisions.
