# Install

## Supported platforms

The currently supported targets, each with OpenZFS, are:

- CachyOS;
- Ubuntu 26.04 LTS;
- Ubuntu 26.10.

Every release is tested on each of them before it is published. The Arch User
Repository packages may also work on compatible Arch Linux systems, and the
Ubuntu package on other Ubuntu releases and on Debian, but those systems are
not currently supported.

## Ubuntu

Install from the Boomerangz apt repository, which carries the latest release
for `amd64` and `arm64`. The package is the same on every Ubuntu release, so
the repository has a single suite, `stable`.

Add the repository's signing key and source, then install:

```sh
sudo install -d -m 0755 /etc/apt/keyrings
curl -fsSL https://boomerangz.org/apt/boomerangz-archive-keyring.gpg |
  sudo tee /etc/apt/keyrings/boomerangz.gpg >/dev/null
sudo tee /etc/apt/sources.list.d/boomerangz.sources >/dev/null <<'SOURCES'
Types: deb
URIs: https://boomerangz.org/apt
Suites: stable
Components: main
Signed-By: /etc/apt/keyrings/boomerangz.gpg
SOURCES
sudo apt update
sudo apt install boomerangz
```

The package depends on `zfsutils-linux` and recommends `openssh-client` for
SSH destinations. It installs a static executable, the systemd unit,
configuration, restricted SSH login-shell wrapper, manual pages, and shell
completions, and creates the `boomerangz` account. It does not enable or start
the service.

Each release's `.deb` files are also attached to its GitHub release and covered
by its `SHA256SUMS`, for installing a specific version with
`sudo apt install ./boomerangz_<version>_<arch>.deb`.

## Arch Linux

### Choose a package

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

Upgrade through the same package channel you installed, after reading the
[upgrade notes](/operations/upgrading) for every release between your version
and the new one; most need nothing. Preserve the configured
`identity_dir` (by default `/var/lib/boomerangz/identity`) when upgrading or
restoring the same installation; never clone that identity directory to another
active host.

Removing the package does not destroy datasets, snapshots, or replicated data,
and neither removing nor purging it removes the `boomerangz` account or
`/var/lib/boomerangz`, which holds the installation identity. On Ubuntu,
`apt remove` keeps `/etc/boomerangz/config.toml` and `apt purge` removes it.
Use the preview-first [clean procedure](/operations/recovery#explicit-clean)
before removal only when you intentionally want to retire Boomerangz metadata.
