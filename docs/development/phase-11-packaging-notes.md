# Phase 11 packaging notes

Packaging work is isolated on `feat/phase-11-packaging` after completion of the
feature, native-transport, and CI integration/E2E phases.

Confirmed release decisions:

- the initial version is `v0.1.0`;
- the project license is MIT;
- Arch receives separate `boomerangz` and `boomerangz-bin` PKGBUILDs;
- `boomerangz` builds from a deterministic source archive published as a
  GitHub Release artifact, not from Git;
- `boomerangz-bin` installs CI-built GitHub Release binaries;
- normal and release builds disable CGO; the race detector may enable it only
  for testing;
- generic release binaries use the normal static Go executable build mode so
  they do not bind to the build host's libc; only the source AUR package applies
  Arch's PIE policy and declares `glibc`, which supplies the ELF interpreter
  requested by Go's Linux PIE output even when CGO is disabled;
- GoReleaser v2 is the source of truth for binary and source archives,
  checksums, release metadata, and generated `boomerangz` and `boomerangz-bin`
  AUR recipes;
- CI publishes amd64 and arm64 binary archives, the source archive, checksums,
  and rendered PKGBUILDs containing the real archive checksums;
- downloaded release archives must never use `SKIP` integrity checks.

Both generated PKGBUILDs were validated from the exact draft-release artifacts
in a fresh disposable CachyOS guest. The validation covered archive checksums,
source and binary builds, clean installation, protected-configuration upgrades,
service-account creation, tmpfiles and sysusers behavior, systemd startup,
package removal, and expected executable linkage.

AUR publication is gated by the repository `PUBLISH_AUR` variable and a
dedicated AUR SSH credential so release validation cannot publish a broken
recipe.
