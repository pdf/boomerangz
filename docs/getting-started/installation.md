# Install

## Supported platforms

The currently supported targets are CachyOS, Ubuntu 26.04 LTS and Ubuntu 26.10,
each with OpenZFS. The packages may also work on compatible Arch Linux, Ubuntu
and Debian systems, but those are not currently supported.

## Choose a package

::: code-group

```text [Arch Linux / derivatives]
boomerangz      Build from the release source archive
boomerangz-bin  Install a prebuilt static binary
```

```text [Ubuntu]
boomerangz      Install a prebuilt static binary
```

:::

Every package installs the same executable, systemd unit, configuration,
restricted SSH login-shell wrapper, manual pages, and shell completions, and
creates the `boomerangz` account. None enables or starts the service. Prebuilt
packages support `x86_64` and `aarch64`.

## Install the package

::: code-group

```sh [Arch Linux / derivatives]
# Install one with your preferred AUR workflow, and always inspect an AUR
# build recipe before running it. The source package builds with CGO
# disabled and Arch's PIE hardening; the two packages conflict.
# Example with an installed AUR helper:
paru -S boomerangz

# Or choose the prebuilt release:
paru -S boomerangz-bin
```

```sh [Ubuntu]
# Add the repository's signing key and source. The repository carries the
# latest release in a single suite, stable, for every Ubuntu release.
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

:::

Alternatively, install from the files attached to the matching GitHub release:

::: code-group

```sh [Arch Linux / derivatives]
# Download boomerangz-PKGBUILD or boomerangz-bin-PKGBUILD, place it in an
# empty directory as PKGBUILD, inspect it, and build as your normal user:
makepkg --syncdeps --install
```

```sh [Ubuntu]
# Download the .deb for your architecture:
sudo apt install ./boomerangz_<version>_<arch>.deb
```

:::

Release downloads are covered by the release's `SHA256SUMS` file, and the Arch
package recipes carry the same checksums.

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
and leaves the `boomerangz` account and `/var/lib/boomerangz` in place. Use the
preview-first [clean procedure](/operations/recovery#explicit-clean) before
removal only when you intentionally want to retire Boomerangz metadata.

::: code-group

```sh [Arch Linux / derivatives]
# A modified configuration is kept as /etc/boomerangz/config.toml.pacsave.
sudo pacman -R boomerangz    # or boomerangz-bin
```

```sh [Ubuntu]
# remove keeps /etc/boomerangz/config.toml; purge also removes it.
sudo apt remove boomerangz
sudo apt purge boomerangz
```

:::
