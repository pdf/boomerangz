# Phase 10 handoff

Phase 10 is complete. Phase 11 has not started.

## Completed scope

- Standard free GitHub-hosted `ubuntu-24.04` runners execute the OpenZFS
  integration suite with KVM-backed direct QEMU; no self-hosted or paid runner
  is required.
- The canonical host harness is target-neutral. It owns only QEMU lifecycle,
  ephemeral disks and credentials, SSH access, artifacts, diagnostics, and
  guarded teardown.
- The initial `cachyos` adapter owns its pinned base image, CachyOS/OpenZFS
  provisioning, reboot, guest device conventions, and installed-system checks.
  Additional OpenZFS targets can be added without introducing their package or
  service managers into the generic host layer.
- Real-ZFS guest suites are isolated under `test/integration` and excluded from
  ordinary application-package tests by the `integration` build tag.
- Every run generates separate host and guest-loopback SSH keys, explicitly
  selects them, disables agent forwarding, and creates only ephemeral guest
  credentials.
- CI publishes diagnostics even when the guarded suite fails. The ordinary CI
  workflow also checks Go tests, the race detector, vet, golangci-lint, Buf
  formatting/lint/generation, Actionlint, ShellCheck, and generated-file drift.
- The hosted daemon test exposed and now covers a startup ordering race:
  lifecycle admission is enabled before immediately due schedules are
  published, and a pre-queue rejection restores the schedule to retryable
  state.

## Verification

Commit `fca78f6` passed both hosted workflows:

- CI run `34217019941` completed successfully.
- OpenZFS integration run `34217019925` completed successfully in 6 minutes 31
  seconds on the `cachyos` matrix entry.
- The guest ran kernel `6.18.48-1-cachyos-lts`, ZFS module `2.4.4-1`, and
  `zfs-2.4.4-1` userspace.
- Lifecycle, local transfer, interrupted-transfer recovery, direct SSH,
  `ssh-shell`, native TLS gRPC, daemon scheduling/deactivation/retirement,
  daemon control, abrupt restart, and installed-system checks passed.
- The daemon scheduling/deactivation/retirement regression completed in 7.93
  seconds, including the path that previously lost its initial schedule.
- Three 32 MiB hosted-run samples per remote transport produced medians of
  5.657 seconds for direct SSH, 5.972 seconds for `ssh-shell`, and 6.070 seconds
  for native TLS gRPC. These loopback values guard against material regressions
  and are not real-network throughput claims.

Local checks also passed:

```sh
go test ./...
CGO_ENABLED=1 go test -race ./...
go vet ./...
go tool golangci-lint run
go tool actionlint
go test -tags=integration ./test/integration/...
```

ShellCheck was unavailable on the development host; the successful hosted CI
run executed it against every integration shell script.

## Phase 11 entry point

Resume the preserved packaging draft from branch `feat/phase-11-packaging` at
commit `cff963d`, rebase it onto the completed implementation, and follow
`phase-11-packaging-notes.md`. Validate both Arch packages from actual `v0.1.0`
release artifacts and run their install, upgrade, service-account, tmpfiles,
sysusers, and systemd checks through the supported guest matrix.
