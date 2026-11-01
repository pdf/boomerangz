#!/usr/bin/env bash
# Installs the release's .deb the way users do: from an apt repository built
# by the same script the release workflow publishes with, signed with a
# throwaway key. Then upgrades across a version with upgrade notes, removes,
# purges and reinstalls it.

set -Eeuo pipefail

# Most checks here are bare tests; name the one that failed.
trap 'printf "package.sh: line %s failed: %s\n" "$LINENO" "$BASH_COMMAND" >&2' ERR

readonly release_dir=${1:?missing release directory}
readonly run_id=${2:?missing run ID}
readonly source_device=${3:?missing source device}
readonly destination_device=${4:?missing destination device}
readonly expected_hostname=boomerangz-$run_id
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
readonly script_dir
readonly shell_path=/usr/lib/boomerangz/boomerangz-shell
readonly sources=/etc/apt/sources.list.d/boomerangz-test.sources
# Older than every release, so installing it and then the release crosses
# every version in the package's action_versions.
readonly old_version=0.0.1

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
# nfpm turns a prerelease's hyphen into a tilde, so it sorts before the release.
readonly deb_version=${version/-/\~}
expected_binary_version=v$version
if [[ $version == *-SNAPSHOT-* ]]; then
	expected_binary_version=
fi
readonly expected_binary_version

(
	cd "$release_dir"
	sha256sum --check SHA256SUMS
)
mapfile -t packages < <(find "$release_dir" -maxdepth 1 -type f -name "boomerangz_${deb_version}_*.deb" | sort)
((${#packages[@]} == 2)) || {
	printf 'expected amd64 and arm64 packages for %s, found: %s\n' "$deb_version" "${packages[*]}" >&2
	exit 1
}
native=$(find "$release_dir" -maxdepth 1 -type f -name "boomerangz_${deb_version}_$(dpkg --print-architecture).deb" -print -quit)
readonly native
[[ -n $native ]]

export DEBIAN_FRONTEND=noninteractive
apt_get() {
	sudo DEBIAN_FRONTEND=noninteractive apt-get --yes "$@" </dev/null
}
apt_get update
apt_get install --no-install-recommends apt-utils binutils gpg

work=$(mktemp -d)
readonly work
trap 'gpgconf --kill gpg-agent || true; sudo rm -f -- "$sources"; rm -rf -- "$work"' EXIT
# apt reads a file: repository as its unprivileged _apt user.
chmod 0755 "$work"
export GNUPGHOME=$work/gnupg
install -d -m 0700 "$GNUPGHOME"
gpg --batch --passphrase '' --quick-generate-key 'boomerangz package test' ed25519 sign never
key=$(gpg --batch --with-colons --list-secret-keys | awk -F: '$1 == "fpr" { print $10; exit }')
readonly key

# Publishes PACKAGE... as the test repository and refreshes apt's view of it.
publish() {
	bash "$script_dir/apt-repository.sh" "$work/apt" "$key" "$@"
	[[ -s $work/apt/dists/stable/InRelease && -s $work/apt/boomerangz-archive-keyring.gpg ]]
	sudo tee "$sources" >/dev/null <<SOURCES
Types: deb
URIs: file://$work/apt
Suites: stable
Components: main
Signed-By: $work/apt/boomerangz-archive-keyring.gpg
SOURCES
	apt_get update
}

verify_install() {
	local expected_version=$1
	local reported
	[[ $(dpkg-query -W -f='${Status} ${Version}' boomerangz) == "install ok installed $expected_version" ]]
	reported=$(boomerangz version --json)
	if [[ -n $expected_binary_version ]]; then
		[[ $reported == *\"version\":\"$expected_binary_version\"* ]]
	else
		[[ $reported != *\"version\":\"devel\"* ]]
	fi
	[[ $reported == *\"commit\":\"$commit\"* ]]
	if readelf -l /usr/bin/boomerangz | grep -q 'Requesting program interpreter' ||
		readelf -d /usr/bin/boomerangz | grep -q '(NEEDED)'; then
		printf 'packaged executable is not static\n' >&2
		exit 1
	fi
	[[ $(sudo stat -c '%U:%G:%a' /etc/boomerangz) == root:boomerangz:750 ]]
	[[ $(sudo stat -c '%U:%G:%a' /etc/boomerangz/config.toml) == root:boomerangz:640 ]]
	[[ $(sudo stat -c '%U:%G:%a' /var/lib/boomerangz) == boomerangz:boomerangz:750 ]]
	[[ $(sudo stat -c '%U:%G:%a' /etc/boomerangz/credentials.d) == root:boomerangz:750 ]]
	[[ -x $shell_path ]]
	[[ $(getent passwd boomerangz | cut -d: -f7) == "$shell_path" ]]
	grep -Fx "$shell_path" /etc/shells
	if systemctl is-enabled boomerangz.service >/dev/null 2>&1; then
		printf 'package unexpectedly enabled boomerangz.service\n' >&2
		exit 1
	fi
	if systemctl is-active --quiet boomerangz.service; then
		printf 'package unexpectedly started boomerangz.service\n' >&2
		exit 1
	fi
}

# Starts the packaged service and waits for it to create its identity.
exercise_service() {
	sudo systemctl start boomerangz.service
	for _ in {1..100}; do
		if sudo test -s /var/lib/boomerangz/identity/installation-id; then
			break
		fi
		if ! sudo systemctl is-active --quiet boomerangz.service; then
			break
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

# The release package relabelled as an older version, standing in for the
# installed release an upgrade starts from.
old_package=$work/old/boomerangz_${old_version}_$(dpkg --print-architecture).deb
readonly old_package
install -d "$work/old/tree"
dpkg-deb --raw-extract "$native" "$work/old/tree"
sed -i "s/^Version: .*/Version: $old_version/" "$work/old/tree/DEBIAN/control"
dpkg-deb --root-owner-group --build "$work/old/tree" "$old_package"

publish "$old_package"
sudo test ! -e /var/lib/boomerangz/identity/installation-id
apt_get install boomerangz
verify_install "$old_version"
exercise_service
installation_id=$(sudo cat /var/lib/boomerangz/identity/installation-id)
readonly installation_id

# Upgrade to the release through the repository, with an operator edit to the
# configuration that the upgrade must keep without asking.
printf '\n# boomerangz-package-upgrade-marker\n' | sudo tee -a /etc/boomerangz/config.toml >/dev/null
publish "${packages[@]}"
upgrade_output=$(apt_get install boomerangz 2>&1) || {
	printf '%s\n' "$upgrade_output"
	exit 1
}
printf '%s\n' "$upgrade_output"
verify_install "$deb_version"
sudo grep -Fx '# boomerangz-package-upgrade-marker' /etc/boomerangz/config.toml
sudo test ! -e /etc/boomerangz/config.toml.dpkg-new
# The upgrade crossed every action version up to the release, and names
# exactly those.
read -r -a action_versions < <(sed -n 's/^action_versions="\(.*\)"$/\1/p' /var/lib/dpkg/info/boomerangz.postinst)
((${#action_versions[@]} > 0))
for action_version in "${action_versions[@]}"; do
	notice="https://boomerangz.org/operations/upgrading#v${action_version//./-}"
	if dpkg --compare-versions "$deb_version" ge "$action_version"; then
		grep -Fq -- "$notice" <<<"$upgrade_output" || {
			printf 'upgrade across %s printed no notice for it\n' "$action_version" >&2
			exit 1
		}
	elif grep -Fq -- "$notice" <<<"$upgrade_output"; then
		printf 'upgrade to %s printed a notice for later %s\n' "$deb_version" "$action_version" >&2
		exit 1
	fi
done
exercise_service
[[ $(sudo cat /var/lib/boomerangz/identity/installation-id) == "$installation_id" ]]

# Removal stops a running service and unregisters the shell, but keeps the
# configuration, account and identity.
sudo systemctl start boomerangz.service
apt_get remove boomerangz
if systemctl is-active --quiet boomerangz.service; then
	printf 'removing the package left boomerangz.service running\n' >&2
	exit 1
fi
[[ ! -e /usr/bin/boomerangz ]]
if grep -Fxq "$shell_path" /etc/shells; then
	printf 'removed package left its restricted shell registered\n' >&2
	exit 1
fi
sudo grep -Fx '# boomerangz-package-upgrade-marker' /etc/boomerangz/config.toml
getent passwd boomerangz >/dev/null
[[ $(sudo cat /var/lib/boomerangz/identity/installation-id) == "$installation_id" ]]

# Purge removes the configuration it owns, never the identity.
apt_get purge boomerangz
sudo test ! -e /etc/boomerangz/config.toml
[[ $(sudo cat /var/lib/boomerangz/identity/installation-id) == "$installation_id" ]]

# A reinstall over what removal left continues as the same installation.
apt_get install boomerangz
verify_install "$deb_version"
sudo test ! -e /etc/boomerangz/config.toml.dpkg-dist
exercise_service
[[ $(sudo cat /var/lib/boomerangz/identity/installation-id) == "$installation_id" ]]
apt_get purge boomerangz
sudo rm -rf -- /var/lib/boomerangz/identity
printf 'packaging=pass\n'
