#!/usr/bin/env bash

set -euo pipefail

readonly repo_url=https://mirror.cachyos.org/cachyos-repo.tar.xz
readonly work=/var/tmp/boomerangz-cachyos-repo

[[ ${EUID:-$(id -u)} -eq 0 ]] || {
	printf 'CachyOS provisioner must run as root\n' >&2
	exit 1
}

rm -rf -- "$work"
install -d -m 0700 "$work"
# CachyOS publishes no checksum or signature for this archive and replaces it
# in place, so it is trusted over HTTPS as their install instructions do.
curl --fail --location --silent --show-error "$repo_url" --output "$work/cachyos-repo.tar.xz"
tar -xJf "$work/cachyos-repo.tar.xz" -C "$work"

# The upstream helper is interactive; make only its package transactions
# non-interactive. The archive is unpinned, so fail rather than hang on a
# prompt if upstream changes the lines these edits expect.
helper="$work/cachyos-repo/cachyos-repo.sh"
for pattern in '^    pacman -U ' '^    pacman -Syu$'; do
	grep -q -- "$pattern" "$helper" || {
		printf 'cachyos-repo.sh no longer matches %s; update provision.sh\n' "$pattern" >&2
		exit 1
	}
done
sed -i 's/^    pacman -U /    pacman --noconfirm -U /' "$helper"
sed -i 's/^    pacman -Syu$/    pacman --noconfirm -Syu/' "$helper"
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
