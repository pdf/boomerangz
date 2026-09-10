# boomerangz

<img src="docs/public/brand/boomerangz-lockup.svg" alt="boomerangz logo" width="420">

> A ZFS snapshot/replication manager to help make sure your data comes back to you.

[![Quality](https://github.com/pdf/boomerangz/actions/workflows/quality.yml/badge.svg?branch=main&event=push)](https://github.com/pdf/boomerangz/actions/workflows/quality.yml)
[![Release](https://img.shields.io/github/v/release/pdf/boomerangz)](https://github.com/pdf/boomerangz/releases/latest)
[![License](https://img.shields.io/github/license/pdf/boomerangz)](LICENSE)
[![AUR](https://img.shields.io/aur/version/boomerangz?logo=archlinux)](https://aur.archlinux.org/packages/boomerangz)

Boomerangz manages OpenZFS snapshots and local or remote replication, including
systems whose backup destinations are not always online.

The initial supported platform is CachyOS with OpenZFS. Release packaging
provides source-built `boomerangz` and prebuilt `boomerangz-bin` AUR packages
for `x86_64` and `aarch64`.

## Get started

Read the [getting-started guide](https://boomerangz.org/getting-started/) for
prerequisites, installation, secure ZFS delegation, initial configuration, and
verification. Published builds and package recipes appear on the
[GitHub Releases](https://github.com/pdf/boomerangz/releases) page.

Once installed:

```sh
boomerangz config check
sudo systemctl enable --now boomerangz.service
boomerangz status
```

Do not enable datasets until you have reviewed the
[dataset](https://boomerangz.org/guide/datasets.html) and
[security](https://boomerangz.org/guide/security.html) guidance.

## Contributing

The project uses Go 1.26.8 and keeps real OpenZFS tests inside disposable
virtual machines.

[ARCHITECTURE.md](ARCHITECTURE.md) maps the repository: entrypoints, the
package-by-package layout, and the property, state, and scheduling contracts.
[AGENTS.md](AGENTS.md) records the conventions every change is held to, and
[PLAN.md](PLAN.md) covers the original design intent.

Run the same host-safe validation suite as the primary CI workflow:

```sh
make test
```

The Go module pins its Go-based test tools. Install `shellcheck` separately for
the integration-harness shell validation.

Run `make integration-test` for the guarded VM integration suite. It requires
QEMU/KVM, `cloud-image-utils`, OpenSSH, and sufficient local storage.

## AI disclosure

Boomerangz’s design, technical guidance, and verification were performed by a
human. The majority of the implementation code was written by a large language
model.

## License

Boomerangz is released under the [MIT License](LICENSE).
