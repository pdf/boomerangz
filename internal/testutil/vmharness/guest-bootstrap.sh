#!/usr/bin/env bash

set -euo pipefail

readonly marker_path=/run/boomerangz-vmtest/guest-marker
readonly version_path=/run/boomerangz-vmtest/bootstrap-version
readonly bootstrap_version=2
readonly pool_prefix=boomerangz-test-
readonly test_user=boomerangz

fail() {
	printf 'boomerangz guest bootstrap: %s\n' "$*" >&2
	exit 1
}

disk_serial() {
	local run_id=$1
	local role=$2
	printf 'bz-%s-%s\n' "$(printf '%s' "$run_id" | sha256sum | cut -c1-12)" "$role"
}

verify_disk() {
	local run_id=$1
	local role=$2
	local device=$3
	local allow_children=${4:-no}
	local resolved serial device_type child_count

	resolved=$(readlink -f -- "$device")
	[[ $resolved == /dev/* ]] || fail "device does not resolve below /dev: $device"
	device_type=$(lsblk -dn -o TYPE -- "$resolved")
	[[ $device_type == disk ]] || fail "$resolved is not a whole disk"
	serial=$(lsblk -dn -o SERIAL -- "$resolved")
	[[ $serial == "$(disk_serial "$run_id" "$role")" ]] ||
		fail "$resolved has unexpected serial $serial"
	child_count=$(lsblk -nr -o NAME -- "$resolved" | wc -l)
	[[ $allow_children == yes || $child_count -eq 1 ]] || fail "$resolved has child block devices"
	findmnt -rn -S "$resolved" | grep -q . && fail "$resolved is mounted"
	printf '%s\n' "$resolved"
}

verify_run_id() {
	local run_id=$1
	[[ $run_id =~ ^[a-z0-9][a-z0-9-]{5,47}$ ]] || fail "invalid run ID"
}

verify_marker() {
	local run_id=$1
	[[ -f $marker_path ]] || fail "guest marker is missing"
	[[ $(<"$marker_path") == "$run_id" ]] || fail "guest marker does not match run ID"
}

verify_pool_name() {
	local run_id=$1
	local pool=$2
	[[ $pool == "$pool_prefix$run_id"-* ]] || fail "pool name is outside this run: $pool"
}

setup() {
	[[ $# -eq 3 ]] || fail "usage: $0 setup RUN_ID SOURCE_DISK DESTINATION_DISK"
	local run_id=$1
	verify_run_id "$run_id"

	install -d -m 0755 "$(dirname "$marker_path")"
	printf '%s\n' "$run_id" >"$marker_path"
	printf '%s\n' "$bootstrap_version" >"$version_path"
	chmod 0644 "$marker_path"
	chmod 0644 "$version_path"
	verify_marker "$run_id"

	local source_device destination_device source_pool destination_pool
	source_device=$(verify_disk "$run_id" src "$2")
	destination_device=$(verify_disk "$run_id" dst "$3")
	[[ $source_device != "$destination_device" ]] || fail "source and destination disks are identical"

	source_pool="$pool_prefix$run_id-src"
	destination_pool="$pool_prefix$run_id-dst"
	verify_pool_name "$run_id" "$source_pool"
	verify_pool_name "$run_id" "$destination_pool"
	! zpool list -H -o name "$source_pool" >/dev/null 2>&1 || fail "$source_pool already exists"
	! zpool list -H -o name "$destination_pool" >/dev/null 2>&1 || fail "$destination_pool already exists"

	zpool create -m none "$source_pool" "$source_device"
	zpool create -m none "$destination_pool" "$destination_device"
	zfs create -o mountpoint=none "$source_pool/data"
	zfs create -o mountpoint=none "$source_pool/data/child"
	zfs create -s -V 256M -b 128K "$source_pool/data/payload"
	local payload_device="/dev/zvol/$source_pool/data/payload"
	udevadm settle
	for _ in {1..50}; do
		[[ -e $payload_device ]] && break
		sleep 0.1
	done
	[[ -e $payload_device ]] || fail "zvol device was not created: $payload_device"
	dd if=/dev/urandom of="$payload_device" bs=1M count=32 status=none
	zfs create -o mountpoint=none "$destination_pool/data"
	# create on the source and snapshot on the destination are fixture-only
	# permissions used by the integration suites. They are not deployment
	# requirements. userprop on both sides is required by boomerangz metadata.
	zfs allow -u "$test_user" bookmark,create,destroy,hold,mount,release,send,snapshot,userprop "$source_pool/data"
	zfs allow -u "$test_user" compression,create,destroy,mount,mountpoint,readonly,receive,receive:append,snapshot,userprop "$destination_pool/data"

	printf 'source_pool=%s\ndestination_pool=%s\n' "$source_pool" "$destination_pool"
}

cleanup_pool() {
	local run_id=$1
	local pool=$2
	local device=$3
	local vdev parent
	local -a vdevs
	verify_marker "$run_id"
	verify_pool_name "$run_id" "$pool"
	mapfile -t vdevs < <(zpool status -LP "$pool" | awk '$1 ~ "^/dev/" {print $1}')
	[[ ${#vdevs[@]} -gt 0 ]] || fail "$pool has no absolute device vdevs"
	for vdev in "${vdevs[@]}"; do
		parent=$(lsblk -dn -o PKNAME -- "$vdev")
		if [[ -n $parent ]]; then
			parent=/dev/$parent
		else
			parent=$(readlink -f -- "$vdev")
		fi
		[[ $parent == "$(readlink -f -- "$device")" ]] ||
			fail "$pool contains unexpected vdev $vdev"
	done
	zpool destroy "$pool"
}

cleanup() {
	[[ $# -eq 3 ]] || fail "usage: $0 cleanup RUN_ID SOURCE_DISK DESTINATION_DISK"
	local run_id=$1
	verify_run_id "$run_id"
	local source_device destination_device
	source_device=$(verify_disk "$run_id" src "$2" yes)
	destination_device=$(verify_disk "$run_id" dst "$3" yes)
	cleanup_pool "$run_id" "$pool_prefix$run_id-src" "$source_device"
	cleanup_pool "$run_id" "$pool_prefix$run_id-dst" "$destination_device"
	wipefs --all --force "$source_device" "$destination_device"
	blockdev --rereadpt "$source_device"
	blockdev --rereadpt "$destination_device"
	udevadm settle
	rm -f -- "$marker_path"
	rm -f -- "$version_path"
}

[[ ${EUID:-$(id -u)} -eq 0 ]] || fail "must run as root"
[[ $# -ge 1 ]] || fail "missing command"
command=$1
shift
case $command in
setup) setup "$@" ;;
cleanup) cleanup "$@" ;;
*) fail "unsupported command: $command" ;;
esac
