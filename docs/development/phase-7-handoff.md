# Phase 7 hand-off

Phase 7 is complete. Phase 8 has not started.

## Delivered

- A versioned protobuf control API exposes status snapshots and watches, managed
  dataset listing, immediate snapshot triggers, reconciliation hints, and
  daemon-coordinated explicit clean.
- The daemon always serves the control API on the configured Unix socket. The
  socket is created with mode `0660`, stale sockets are handled conservatively,
  peer credentials must be available, and shutdown removes only the socket
  inode created by that server.
- `boomerangz status` renders a stable terminal view or one JSON object when
  redirected. Watch mode redraws interactively or emits NDJSON, responds to
  status revisions, refreshes at the requested interval, and degrades cleanly
  on narrow terminals.
- Status snapshots contain discovery generation, active and inactive roots,
  next snapshot deadlines, detached queue pressure, latest job state, blocker
  reasons, queue position, and transfer bytes, estimate, rate, and ETA when
  available.
- `boomerangz trigger` requests immediate snapshots without bypassing active-root
  checks, queue deduplication, worker limits, lifecycle gates, or downstream
  replication safety.
- `boomerangz dataset clean` uses the daemon when its control socket exists.
  Daemon-coordinated clean serializes lifecycle administration, waits for
  related work to become quiescent, previews every selected scope before any
  apply, and re-runs the normal safety checks during apply.
- Standalone clean uses the same just-in-time verifier as daemon clean. Both
  re-resolve the stored pool and anchor GUID binding, reject same-named target
  replacement, inspect the bound mapped dataset for receive-resume state, and
  retain recovery references when verification fails.
- Optional TCP listeners require TLS 1.3 or newer and support `token`, `mtls`,
  and `mtls+token`. Client-certificate modes require an explicit client CA.
- Scoped 256-bit bearer tokens support independent status, trigger, and admin
  authorization. Only token identifiers and SHA-256 verifiers are stored;
  secrets appear only in the pairing bundle. Store updates are file-locked,
  restrictive, and atomic.
- Pairing bundles select one explicit trust mode: system CA roots, an embedded
  CA, or a server-name-checked public-key pin. Imported credentials are
  validated, stored with mode `0600`, and never overwrite an existing name.
- TCP server certificate and key files are reloaded together for new
  handshakes. An incomplete renewal retains the last complete pair and logs the
  reload failure without weakening client-side validity checks.
- User documentation covers local status and control, structured output,
  triggers, daemon-coordinated clean, TCP listener hardening, token lifecycle,
  trust selection, and mTLS setup.

## Safety boundary

The control API delegates to the existing runtime rather than issuing ZFS
commands independently. Remote requests therefore use the same discovery,
scheduling, ownership, target-binding, resume-state, and lifecycle boundaries
as locally scheduled work. A name alone never authorizes a target.

Unix access remains a host-administration boundary enforced by directory and
socket permissions. TCP is opt-in and has no unauthenticated mode. Token call
credentials require TLS, and server trust is resolved before a token is sent.
The token secret is not logged or retained in the verifier store.

The current adoption and installation-identity recovery command surface still
requires the daemon socket to be absent. This is an implementation limitation,
not a decision about their eventual daemon-coordination model.

No host ZFS state was changed during Phase 7 verification. Control, TLS, token,
CLI, rendering, coordination, and target-verification tests use loopback
listeners, temporary files, and fake ZFS backends. Real ZFS fault and privilege
coverage remains in the guarded Phase 8 matrix.

## Verification

The phase boundary was verified with:

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

Generated protobuf files are committed and regeneration leaves no drift.

## Phase 8 entry point

Phase 8 should execute the planned hardening, privilege, fault-injection, and
destructive disposable-VM matrix. It should validate the
Phase 7 Unix and TCP control paths under service-account permissions and network
failure without changing the approved authentication, lifecycle, or target
authority design unless the user confirms such a change.

Packaging was subsequently deferred until after native transport. The confirmed
release decisions and preserved draft are recorded in
`phase-10-packaging-notes.md`.
