#!/usr/bin/env bash

set -euo pipefail

readonly repo_url=https://mirror.cachyos.org/cachyos-repo.tar.xz
readonly repo_sha256=e2c224dfc0db2cc6ef54f4765ca6ceac78761d34ce513fbaf8b021281f65d4b4
readonly work=/var/tmp/boomerangz-cachyos-repo

[[ ${EUID:-$(id -u)} -eq 0 ]] || {
	printf 'CachyOS provisioner must run as root\n' >&2
	exit 1
}

rm -rf -- "$work"
install -d -m 0700 "$work"
curl --fail --location --silent --show-error "$repo_url" --output "$work/cachyos-repo.tar.xz"
printf '%s  %s\n' "$repo_sha256" "$work/cachyos-repo.tar.xz" | sha256sum --check
tar -xJf "$work/cachyos-repo.tar.xz" -C "$work"

# The upstream helper is interactive; make only its package transactions
# non-interactive after verifying the downloaded archive.
sed -i 's/^    pacman -U /    pacman --noconfirm -U /' "$work/cachyos-repo/cachyos-repo.sh"
sed -i 's/^    pacman -Syu$/    pacman --noconfirm -Syu/' "$work/cachyos-repo/cachyos-repo.sh"
(
	cd "$work/cachyos-repo"
	./cachyos-repo.sh --install
)
pacman --noconfirm -S --needed \
	linux-cachyos-lts \
	linux-cachyos-lts-zfs \
	openssh \
	zfs-utils
grub-mkconfig -o /boot/grub/grub.cfg
systemctl enable sshd.service
rm -rf -- "$work"
