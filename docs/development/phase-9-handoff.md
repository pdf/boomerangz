# Phase 9 handoff

Phase 9 is complete. Phase 10 has not started.

## Completed scope

- The shared remote RPC service is available over authenticated native TLS
  listeners and the constrained `ssh-shell` command channel.
- Token, managed mTLS, external mTLS, CA verification, and optional certificate
  pinning are represented by listener configuration and pairing bundles.
- Boomerangz-managed server and client certificates are issued, persisted,
  rotated, listed, and revoked through the unified pairing lifecycle.
- The public pairing commands are `boomerangz pairing create`, `import`, `list`,
  and `revoke`; no redundant `auth` namespace remains.
- The CLI and VM harness use Kong command structs, `Run` methods, and validation
  hooks. Fixed-set pairing scopes are documented and enforced as CLI enums.
- Direct SSH requires no remote boomerangz installation and now multiplexes its
  command and receive channels over one authenticated connection per opened
  endpoint. Agent forwarding is explicitly disabled.
- Native target bindings and recovery validation use the same transport-neutral
  safety rules as SSH targets.

## Verification

- `go test ./...`
- `go tool buf lint`
- `go tool golangci-lint run`
- Integration-tag compilation for `internal/transfer`
- Guarded CachyOS/OpenZFS guest run `phase9mux-260908a`: three fresh 33,637,624
  byte transfers per transport, with destination GUID verification on every
  sample. Median end-to-end totals were 1.842 seconds for multiplexed direct
  SSH, 1.825 seconds for SSH-shell gRPC, and 1.669 seconds for native TLS gRPC.

The guest bootstrap revalidated the run marker, pool names, scratch-disk
serials, and vdev parents before destroying both test pools. The transient
domain was then stopped. Host access and guest-loopback SSH used distinct
dedicated test keys with agent forwarding disabled.

## Phase 10 entry point

Phase 10 should adapt the guarded VM workflow to standard free GitHub-hosted
runners, make that CI-compatible path the canonical local integration harness,
and introduce the initial extensible CachyOS/OpenZFS build-and-test matrix.
