# Phase 10 packaging notes

Packaging is deferred until the feature and native-transport phases are
complete. The current draft is preserved on branch
`feat/phase-10-packaging` at commit `cff963d` and is not part of `main`.

Confirmed release decisions:

- the initial version is `v0.1.0`;
- the project license is MIT;
- Arch receives separate `boomerangz` and `boomerangz-bin` PKGBUILDs;
- `boomerangz` builds from a deterministic source archive published as a
  GitHub Release artifact, not from Git;
- `boomerangz-bin` installs CI-built GitHub Release binaries;
- normal and release builds disable CGO; the race detector may enable it only
  for testing;
- CI publishes amd64 and arm64 binary archives, the source archive, checksums,
  and rendered PKGBUILDs containing the real archive checksums;
- downloaded release archives must never use `SKIP` integrity checks.

Before Phase 10 is completed, rebase the draft onto the finished implementation,
refresh installed documentation and completions, validate both PKGBUILDs from
the published artifacts, and exercise clean install, protected-config upgrade,
service-account, tmpfiles, sysusers, and systemd behavior in a fresh disposable
guest.
