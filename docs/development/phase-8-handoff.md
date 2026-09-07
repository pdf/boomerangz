# Phase 8 hand-off

Phase 8 is complete. Phase 9 has not started.

## Delivered

- Every real-ZFS Go test is isolated behind the `integration` build tag while
  remaining colocated with the package it exercises. Ordinary `go test ./...`
  neither compiles nor runs guest-only test code.
- Controlled transfer interruption verifies durable receive-token recovery and
  source hold release after restart. Controlled daemon termination verifies
  reconstruction from ZFS state instead of process memory.
- Newly created snapshots are protected with target-specific ZFS references and
  holds before source pruning is queued. A disconnected target coalesces to the
  newest generation while an in-flight generation remains independently
  protected.
- Successful older work cannot release a newer pending target reference. Once
  the newer generation verifies, older references are retired and its completed
  hold is released.
- A remote destination with no currently visible ancestor is treated as
  temporarily unavailable. This covers an unimported destination pool and keeps
  roadwarrior recovery in bounded retry rather than permanently blocking it.
- The two-guest outage test can select either SSH endpoint mode. The completed
  run used gRPC over `ssh-shell`, a dedicated guest-only key, and a destination
  pool export lasting longer than one natural snapshot cadence.
- User documentation explains that the configured destination leaf may be
  created by first replication and that an unavailable pool retains protected
  work for retry.

## Safety and support boundary

OpenZFS 2.4.3 is the minimum verified version for the initial CachyOS support
baseline. The completed guest matrix ran Linux `6.18.42-1-cachyos-lts`,
`zfs-utils 2.4.3-2`, and ZFS module `2.4.3-1`. Other platforms remain unverified
until the same guarded matrix passes there.

The production source delegation is
`bookmark,destroy,hold,mount,release,send,snapshot,userprop`. The production
destination baseline is
`compression,create,destroy,mount,mountpoint,readonly,receive,receive:append,userprop`,
plus each native property configured for receive. Test-only `create` on the
source and `snapshot` on the destination exist solely to construct fixtures.

The matrix did not justify a privileged helper or any Linux capability. Real
ZFS operations remained inside guarded disposable guests; no host `zfs` or
`zpool` command ran. Host-to-guest and guest-to-guest access used dedicated test
keys with agent forwarding disabled.

## Integration evidence

The refreshed CachyOS/OpenZFS 2.4.3 guest matrix passed delegated snapshots,
bookmarks, holds, pruning, full and incremental sends, recursive streams,
receive-property behavior, interrupted receive recovery, lifecycle adoption and
clean, target binding, daemon scheduling and retirement, service-account control
access, and systemd execution.

The two-guest outage run kept the target pool exported across a second scheduled
snapshot. The daemon reported retry rather than a permanent block, retained the
newest target-specific held reference through pruning, and resumed after import.
The destination leaf was initially absent, the received snapshot GUID matched
the newest source snapshot, destination pruning succeeded, and the recovery hold
was absent after verification.

The outage run used `ssh-shell` because it preserves one SSH connection per
attempt. Direct SSH is verified by the transfer matrix, but the nested
guest-to-host-to-guest `passt` port-forward topology intermittently stalls its
sequence of short SSH connections and is not a valid direct-SSH network test.

## Verification

The phase boundary passed:

```sh
go test ./...
CGO_ENABLED=1 go test -race ./...
go vet ./...
go tool golangci-lint run
go tool buf format --diff --exit-code
go tool buf lint
go tool buf generate
go tool buf breaking --against '.git#branch=main'
git diff --check
```

Integration-tag compilation also passed for the lifecycle, transfer, daemon,
and CLI packages. The guarded guest runs passed the controlled interrupted
receive, abrupt daemon restart, and two-guest outage/reconnection cases.

## Phase 9 entry point

Phase 9 should carry the existing scoped remote endpoint over authenticated
native gRPC and benchmark stream replication against the stable SSH transports.
It must preserve target identity binding, target-specific recovery holds,
just-in-time remote inspection, bounded retries, and the direct SSH fallback for
destinations where boomerangz cannot be installed.
