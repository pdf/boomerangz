# Install

## Supported platforms

The currently supported target is CachyOS with OpenZFS. The Arch User Repository
packages may also work on compatible Arch Linux systems, but those systems are
not currently supported.

## Choose a package

::: code-group

```text [Arch Linux / derivatives]
boomerangz      Build from the release source archive
boomerangz-bin  Install a prebuilt static binary
```

:::

The packages conflict and install the same executable, systemd unit,
configuration, restricted SSH login-shell wrapper, manual pages, and shell
completions. The source package builds with CGO disabled and Arch's PIE
hardening. The binary package supports `x86_64` and `aarch64`.

Install one package with your preferred AUR workflow. Always inspect an AUR
build recipe before running it.

::: code-group

```sh [Arch Linux / derivatives]
# Example with an installed AUR helper:
paru -S boomerangz

# Or choose the prebuilt release:
paru -S boomerangz-bin
```

:::

Alternatively, download the generated `boomerangz-PKGBUILD` or
`boomerangz-bin-PKGBUILD` from the matching GitHub release, place it in an empty
directory as `PKGBUILD`, inspect it, and build as your normal user:

::: code-group

```sh [Arch Linux / derivatives]
makepkg --syncdeps --install
```

:::

Release downloads are covered by SHA-256 checksums in both the package recipe
and the release's `SHA256SUMS` file.

## Prepare the service

Before starting it:

1. configure the required [ZFS permissions](/guide/security#zfs-delegation);
2. review `/etc/boomerangz/config.toml`;
3. install any [remote credentials](/guide/remotes) with restrictive permissions;
4. validate the merged configuration.

```sh
boomerangz config check
```

Then continue with [Getting started](./).

## Upgrade or remove

Upgrade through the same package channel you installed. Preserve the configured
`identity_dir` (by default `/var/lib/boomerangz/identity`) when upgrading or
restoring the same installation; never clone that identity directory to another
active host.

Removing the package does not destroy datasets, snapshots, or replicated data.
Use the preview-first [clean procedure](/operations/recovery#explicit-clean)
before removal only when you intentionally want to retire Boomerangz metadata.
