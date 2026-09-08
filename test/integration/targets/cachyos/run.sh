#!/usr/bin/env bash

set -euo pipefail

readonly artifact_dir=${1:?missing artifact directory}
readonly run_id=${2:?missing run ID}
readonly source_device=${3:?missing source device}
readonly destination_device=${4:?missing destination device}

uname -a
cat /etc/os-release
pacman -Q linux-cachyos-lts linux-cachyos-lts-zfs zfs-utils
modinfo -F version zfs

if [[ -d $artifact_dir/release ]]; then
	"$artifact_dir/target/package.sh" "$artifact_dir/release" "$run_id" "$source_device" "$destination_device"
fi

"$artifact_dir/run-common.sh" "$artifact_dir" "$run_id" "$source_device" "$destination_device"

# Installed-system behavior is target-specific. CachyOS uses systemd and the
# Linux service-account assets that will later be consumed by packaging.
sudo install -m 0755 "$artifact_dir/boomerangz" /usr/bin/boomerangz
sudo install -m 0644 "$artifact_dir/boomerangz.service" /usr/lib/systemd/system/boomerangz.service
sudo install -m 0644 "$artifact_dir/boomerangz.sysusers" /usr/lib/sysusers.d/boomerangz.conf
sudo install -m 0644 "$artifact_dir/boomerangz.tmpfiles" /usr/lib/tmpfiles.d/boomerangz.conf
sudo systemd-sysusers
sudo systemd-tmpfiles --create
sudo install -m 0640 -o root -g boomerangz /dev/null /etc/boomerangz/config.toml
sudo systemctl daemon-reload
sudo systemctl start boomerangz.service
sudo systemctl is-active boomerangz.service
if sudo systemctl is-enabled boomerangz.service >/dev/null 2>&1; then
	printf 'integration install unexpectedly enabled the service\n' >&2
	exit 1
fi
sudo systemctl stop boomerangz.service
printf 'installed_system=pass\n'
