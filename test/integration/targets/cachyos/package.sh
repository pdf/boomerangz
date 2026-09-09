#!/usr/bin/env bash

set -euo pipefail

readonly release_dir=${1:?missing release directory}
readonly run_id=${2:?missing run ID}
readonly source_device=${3:?missing source device}
readonly destination_device=${4:?missing destination device}
readonly expected_hostname=boomerangz-$run_id

[[ $(uname -n) == "$expected_hostname" ]] || {
	printf 'packaging test guest hostname mismatch\n' >&2
	exit 1
}
serial_prefix=$(printf '%s' "$run_id" | sha256sum | cut -c1-12)
readonly serial_prefix
[[ $(lsblk -dn -o SERIAL -- "$source_device") == "bz-$serial_prefix-src" ]]
[[ $(lsblk -dn -o SERIAL -- "$destination_device") == "bz-$serial_prefix-dst" ]]

version=$(sed -n 's/.*"version":"\([^"]*\)".*/\1/p' "$release_dir/metadata.json")
readonly version
commit=$(sed -n 's/.*"commit":"\([0-9a-f]*\)".*/\1/p' "$release_dir/metadata.json")
readonly commit
[[ $version =~ ^[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z.-]+)?$ ]]
[[ $commit =~ ^[0-9a-f]{40}$ ]]
readonly pkgver=${version//-/_}

(
	cd "$release_dir"
	sha256sum --check SHA256SUMS
)

ln -sf "boomerangz-v$version-source.tar.gz" "$release_dir/boomerangz_$pkgver.tar.gz"
ln -sf "boomerangz-v$version-linux-amd64.tar.gz" "$release_dir/boomerangz-bin_${pkgver}_x86_64.tar.gz"

sudo pacman --noconfirm -S --needed base-devel go

work=$(mktemp -d)
readonly work
trap 'rm -rf -- "$work"' EXIT
install -d "$work/packages"

build_recipe() {
	local recipe=$1 directory=$2 install_script
	install -d "$directory"
	install -m 0644 "$release_dir/$recipe" "$directory/PKGBUILD"
	install_script=$(sed -n 's/^install=//p' "$directory/PKGBUILD")
	[[ $install_script =~ ^[A-Za-z0-9._+-]+$ ]]
	install -m 0644 "$release_dir/aur/$install_script" "$directory/$install_script"
	(
		cd "$directory"
		makepkg --printsrcinfo >.SRCINFO
		grep -Fx $'\tbackup = etc/boomerangz/config.toml' .SRCINFO
		if grep -Eq '(^|[^A-Za-z])SKIP([^A-Za-z]|$)' PKGBUILD .SRCINFO; then
			printf 'generated package metadata contains SKIP\n' >&2
			exit 1
		fi
		SRCDEST="$release_dir" makepkg --verifysource
		SRCDEST="$release_dir" PKGDEST="$work/packages" makepkg --cleanbuild --force --noconfirm --syncdeps
	)
}

package_path() {
	local package_name=$1 package_release=$2
	local path
	path=$(find "$work/packages" -maxdepth 1 -type f -name "$package_name-$pkgver-$package_release-*.pkg.tar.zst" -print -quit)
	[[ -n $path ]] || {
		printf 'built package not found: %s pkgrel %s\n' "$package_name" "$package_release" >&2
		exit 1
	}
	printf '%s\n' "$path"
}

verify_install() {
	local package_name=$1 expected_version=$2 linkage=$3
	local reported
	reported=$(boomerangz version --json)
	if [[ -n $expected_version ]]; then
		[[ $reported == *\"version\":\"$expected_version\"* ]]
	else
		[[ $reported != *\"version\":\"devel\"* ]]
	fi
	[[ $reported == *\"commit\":\"$commit\"* ]]
	case "$linkage" in
		static)
			! readelf -l /usr/bin/boomerangz | grep -q 'Requesting program interpreter'
			! readelf -d /usr/bin/boomerangz | grep -q '(NEEDED)'
			;;
		pie)
			readelf -h /usr/bin/boomerangz | grep -Eq 'Type:[[:space:]]+DYN'
		;;
	esac
	pacman -Q "$package_name"
	[[ $(sudo stat -c '%U:%G:%a' /etc/boomerangz) == root:boomerangz:750 ]]
	[[ $(sudo stat -c '%U:%G:%a' /etc/boomerangz/config.toml) == root:boomerangz:640 ]]
	[[ $(sudo stat -c '%U:%G:%a' /var/lib/boomerangz) == boomerangz:boomerangz:750 ]]
	[[ $(sudo stat -c '%U:%G:%a' /etc/boomerangz/credentials.d) == root:boomerangz:750 ]]
	[[ -x /usr/lib/boomerangz/boomerangz-shell ]]
	[[ $(getent passwd boomerangz | cut -d: -f7) == /usr/lib/boomerangz/boomerangz-shell ]]
	grep -Fx '/usr/lib/boomerangz/boomerangz-shell' /etc/shells
	sudo test ! -e /var/lib/boomerangz/identity/installation-id
	if systemctl is-enabled boomerangz.service >/dev/null 2>&1; then
		printf 'package unexpectedly enabled boomerangz.service\n' >&2
		exit 1
	fi
	sudo systemctl start boomerangz.service
	for _ in {1..100}; do
		if sudo test -s /var/lib/boomerangz/identity/installation-id; then
			break
		fi
		if ! sudo systemctl is-active --quiet boomerangz.service; then
			sudo systemctl status --no-pager boomerangz.service || true
			sudo journalctl --no-pager -u boomerangz.service || true
			return 1
		fi
		sleep 0.1
	done
	if ! sudo test -s /var/lib/boomerangz/identity/installation-id; then
		sudo systemctl status --no-pager boomerangz.service || true
		sudo journalctl --no-pager -u boomerangz.service || true
		return 1
	fi
	sudo systemctl stop boomerangz.service
}

exercise_recipe() {
	local package_name=$1 recipe=$2 directory=$3 expected_version=$4 linkage=$5
	build_recipe "$recipe" "$directory"
	sudo pacman --noconfirm -U "$(package_path "$package_name" 1)"
	verify_install "$package_name" "$expected_version" "$linkage"
	printf '\n# boomerangz-package-upgrade-marker\n' | sudo tee -a /etc/boomerangz/config.toml >/dev/null
	sed -i 's/^pkgrel=1$/pkgrel=2/' "$directory/PKGBUILD"
	(
		cd "$directory"
		SRCDEST="$release_dir" PKGDEST="$work/packages" makepkg --cleanbuild --force --noconfirm --syncdeps
	)
	sudo pacman --noconfirm -U "$(package_path "$package_name" 2)"
	sudo grep -Fx '# boomerangz-package-upgrade-marker' /etc/boomerangz/config.toml
	sudo pacman --noconfirm -R "$package_name"
	sudo grep -Fx '# boomerangz-package-upgrade-marker' /etc/boomerangz/config.toml.pacsave
	! grep -Fx '/usr/lib/boomerangz/boomerangz-shell' /etc/shells
	sudo rm -f -- /etc/boomerangz/config.toml.pacsave /var/lib/boomerangz/identity/installation-id
}

exercise_recipe boomerangz aur/boomerangz.pkgbuild "$work/boomerangz" "v$version" pie
expected_binary_version="v$version"
if [[ $version == *-SNAPSHOT-* ]]; then
	expected_binary_version=
fi
exercise_recipe boomerangz-bin aur/boomerangz-bin.pkgbuild "$work/boomerangz-bin" "$expected_binary_version" static
printf 'packaging=pass\n'
