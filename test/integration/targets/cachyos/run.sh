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

if [[ ${BOOMERANGZ_INTEGRATION_MODE:-test} == package ]]; then
	exit 0
fi

"$artifact_dir/installed-system.sh" "$artifact_dir" "$run_id" "$source_device" "$destination_device"
