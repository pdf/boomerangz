# Phase 5 hand-off

Phase 5 is complete. Phase 6 has not started.

## Delivered

- SSH replication supports direct remote ZFS execution without requiring a
  remote boomerangz installation.
- The optional `ssh-shell` endpoint carries the typed remote API and receive
  stream over gRPC on one constrained SSH command channel. `auto` falls back to
  direct mode only when the remote command is unavailable, not when the host is
  unreachable.
- SSH invocation is non-interactive, uses strict host-key checking, rejects
  arbitrary options and shell fragments, scopes operations to the configured
  destination root, and bounds command output.
- Remote inventory and adoption checks run just in time. Unavailable targets are
  suspended, and a later idempotent adoption clears suspension only after
  identity and mapping verification.
- Transfer state distinguishes source and destination inventory, reconstructs
  resumable sends from durable ZFS state, coalesces pending snapshots per
  source-target pair, and applies bounded jittered retry.
- Buf manages the versioned protobuf schema and generated Go API. Buf and its Go
  generators are pinned as module tools, and CI rejects formatting, lint, or
  generation drift.
- `inactive_grace_period` defaults to 24 hours and accepts zero to disable
  automatic retirement. The lifecycle layer durably records inactive authority,
  clears it on reactivation, and produces an ownership-safe retirement plan once
  the grace period has elapsed.
- `clean` is the canonical explicit decommissioning name in the CLI and internal
  lifecycle API. Delayed automatic reclamation is called retirement; it reuses
  clean safety proofs while preserving public policy and foreign snapshots.

## Safety boundary

Retirement planning fails closed on changed authority, active or ambiguous
policy, unavailable target checks, receive resume state, unproven references,
holds, clones, or other snapshot dependencies. Phase 6 must supply live worker
quiescence and target-side retirement coordination through `CleanSafety` before
applying a due plan.

All real ZFS validation remained inside disposable libvirt/QEMU guests. The
two-guest CachyOS/OpenZFS 2.4.3 run verified both direct SSH and gRPC
`ssh-shell` transfers, each with a 33,637,624-byte stream and matching received
snapshot GUIDs. Guest pools were destroyed and the transient domain exited. The
run overlays remain under
`/storage/libvirt/qcow2/boomerangz-runs/run-remote-260906a` because the host
harness does not yet implement validated artifact deletion.

## Verification

The phase boundary was verified with:

```sh
go test ./...
go test -race ./...
go vet ./...
go tool golangci-lint run
go tool buf format --diff --exit-code
go tool buf lint
go tool buf generate
git diff --check
```

The generated protobuf files retained these SHA-256 hashes after regeneration:

```text
f30d0d2c6cbff7f69588de6406c27ced85a358417b32b5c7511695bcfb5b4195  internal/replication/rpc/remote.pb.go
4cd5bb60071fad61cc6345cc0676e972faf7388d5a224e7d32840c3c287e5f15  internal/replication/rpc/remote_grpc.pb.go
```

## Phase 6 entry point

Phase 6 should construct the daemon reconciliation loop around immutable
discovery generations, then add independent bounded management, local-transfer,
and remote-transfer workers. It must wire activation transitions to
`ReconcileInactive`, enqueue due `Retire` plans, retain per-target roadwarrior
state across retries, and provide the live `CleanSafety` implementation shared
by retirement and explicit clean. Graceful shutdown and systemd integration
complete the phase.
