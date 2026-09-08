# Installation

Arch users can choose between two AUR packages:

- `boomerangz` downloads the release source archive and builds the binary
  locally with CGO disabled and Arch's PIE hardening;
- `boomerangz-bin` downloads a portable, statically linked release binary for
  the current `x86_64` or `aarch64` system.

Choose one recipe. Both install the same executable, configuration, systemd,
sysusers, tmpfiles, manual-page, and shell-completion paths, and the packages
conflict with one another.

Install the selected package with an AUR helper, or download its generated
`PKGBUILD` from the corresponding GitHub release, save it as `PKGBUILD` in an
empty directory, inspect it, then build and install it as a normal user:

```sh
makepkg --syncdeps --install
```

Every downloaded archive has a SHA-256 checksum in the recipe. The release's
`SHA256SUMS` file provides the same checksums for independent verification.

Installation creates the `boomerangz` service account and its configuration,
state, credential, and runtime directories through systemd-sysusers and
systemd-tmpfiles. It does not enable or start the service. The package marks
`/etc/boomerangz/config.toml` as a protected configuration file, so local
changes are preserved across package upgrades.

Review `/etc/boomerangz/config.toml`, configure ZFS delegation and any remote
credentials, and validate the result before starting the daemon:

```sh
boomerangz config check
sudo systemctl enable --now boomerangz.service
```

The installation identity and managed TLS/private-key material are generated
at runtime beneath `/var/lib/boomerangz`; they are never included in a package.
Preserve that directory when upgrading or restoring the same installation, and
do not copy it to another active host.

See [Daemon and service account](daemon.md), [Configuration schema](configuration.md),
and [SSH destinations](remote-ssh.md) before activating managed datasets.
