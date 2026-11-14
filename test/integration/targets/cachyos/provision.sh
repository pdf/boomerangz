#!/usr/bin/env bash
# Turns the Arch Linux cloud image into a CachyOS guest: trusts the CachyOS
# keyring, adds the baseline x86-64 cachyos repository, and installs the
# CachyOS LTS kernel with its OpenZFS module.
#
# The trust anchor is HTTPS to mirror.cachyos.org. The keyring package is
# found through the repository database rather than by a pinned file name or
# key, so key rotations arrive through the keyring package itself. Only the
# baseline repository is used - not the x86-64-v3, x86-64-v4 or Zen 4/5 ones -
# so the guest is the same on every host CPU it boots with.

set -euo pipefail

readonly mirror=https://mirror.cachyos.org/repo
readonly db_url=$mirror/x86_64/cachyos/cachyos.db
readonly work=/var/tmp/boomerangz-cachyos-provision

fail() {
	printf 'CachyOS provisioner: %s\n' "$*" >&2
	exit 1
}

# fetch URL FILE downloads over HTTPS only, redirects included.
fetch() {
	curl --fail --location --silent --show-error --proto '=https' --proto-redir '=https' \
		--output "$2" "$1"
}

# field NAME DESC prints the first value of %NAME% in a repository desc file.
field() {
	awk -v key="%$1%" '$0 == key { if ((getline value) > 0) print value; exit }' "$2"
}

[[ ${EUID:-$(id -u)} -eq 0 ]] || fail 'must run as root'

rm -rf -- "$work"
install -d -m 0700 "$work" "$work/db"

fetch "$db_url" "$work/cachyos.db" || fail "cannot download $db_url"
bsdtar -xf "$work/cachyos.db" -C "$work/db" 2>/dev/null ||
	fail "$db_url is not a repository database"

desc=
for candidate in "$work"/db/cachyos-keyring-*/desc; do
	if [[ -f $candidate && $(field NAME "$candidate") == cachyos-keyring ]]; then
		desc=$candidate
		break
	fi
done
[[ -n $desc ]] || fail "$db_url has no cachyos-keyring package"
keyring_file=$(field FILENAME "$desc")
keyring_sha256=$(field SHA256SUM "$desc")
[[ $keyring_file =~ ^cachyos-keyring-[A-Za-z0-9._:+-]+\.pkg\.tar\.[a-z0-9]+$ ]] ||
	fail "cannot resolve the cachyos-keyring file name from $db_url (got '$keyring_file')"
[[ $keyring_sha256 =~ ^[0-9a-f]{64}$ ]] ||
	fail "cannot resolve the $keyring_file checksum from $db_url (got '$keyring_sha256')"

keyring_url=$mirror/x86_64/cachyos/$keyring_file
fetch "$keyring_url" "$work/$keyring_file" || fail "cannot download $keyring_url"
# The mirror has answered missing files with an HTML page and HTTP 200; the
# database checksum catches that as well as a truncated download.
(cd "$work" && sha256sum --check --quiet <<<"$keyring_sha256  $keyring_file") ||
	fail "$keyring_url does not match the checksum in $db_url"

# The cloud image initialises its pacman keyring at first boot; let that
# finish rather than race it, then initialise here if it has not.
if systemctl cat pacman-init.service >/dev/null 2>&1; then
	systemctl start pacman-init.service
fi
pacman-key --init
pacman --noconfirm -U "$work/$keyring_file"
pacman-key --populate cachyos

if ! grep -qx '\[cachyos\]' /etc/pacman.conf; then
	grep -qx '\[core\]' /etc/pacman.conf || fail '/etc/pacman.conf has no [core] section'
	# shellcheck disable=SC2016 # $arch and $repo are pacman's, not the shell's.
	sed -i '/^\[core\]$/i [cachyos]\nServer = https://mirror.cachyos.org/repo/$arch/$repo\n' \
		/etc/pacman.conf
fi

pacman --noconfirm -Syu
pacman --noconfirm -S --needed \
	linux-cachyos-lts \
	linux-cachyos-lts-zfs \
	openssh \
	zfs-utils
grub-mkconfig -o /boot/grub/grub.cfg
systemctl enable sshd.service
rm -rf -- "$work"
