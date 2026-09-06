#!/usr/bin/env bash
# Probe whether public-property exclusion and inheritance erase received values.
set -euo pipefail

fail() { printf 'boomerangz property-layer probe: %s\n' "$*" >&2; exit 1; }
[[ $# -eq 3 ]] || fail "usage: $0 RUN_ID SOURCE_DISK DESTINATION_DISK"
readonly run_id=$1
[[ $run_id =~ ^[a-z0-9][a-z0-9-]{5,47}$ ]] || fail "invalid run ID"
readonly marker=/run/boomerangz-vmtest/guest-marker
[[ -f $marker && $(<"$marker") == "$run_id" ]] || fail "guest marker mismatch"
readonly source_pool=boomerangz-test-$run_id-src
readonly destination_pool=boomerangz-test-$run_id-dst

verify_pool() {
	local pool=$1 disk=$2 role=$3 parent vdev serial
	local -a vdevs
	disk=$(readlink -f -- "$disk")
	[[ $disk == /dev/* && $(lsblk -dn -o TYPE -- "$disk") == disk ]] || fail "not a whole guest disk"
	serial="bz-$(printf '%s' "$run_id" | sha256sum | cut -c1-12)-$role"
	[[ $(lsblk -dn -o SERIAL -- "$disk") == "$serial" ]] || fail "disk serial mismatch"
	mapfile -t vdevs < <(zpool status -LP "$pool" | awk '$1 ~ "^/dev/" {print $1}')
	[[ ${#vdevs[@]} -gt 0 ]] || fail "pool has no absolute vdev paths"
	for vdev in "${vdevs[@]}"; do
		parent=$(lsblk -dn -o PKNAME -- "$vdev")
		if [[ -n $parent ]]; then parent=/dev/$parent; else parent=$(readlink -f -- "$vdev"); fi
		[[ $parent == "$disk" ]] || fail "pool contains unexpected vdev"
	done
}
verify_pool "$source_pool" "$2" src
verify_pool "$destination_pool" "$3" dst

readonly source=$source_pool/data/child
readonly target=$destination_pool/data/property-layers
readonly property=org.boomerangz:clean-probe
readonly metadata=org.boomerangz:state:probe
! zfs list -H "$target" >/dev/null 2>&1 || fail "probe target already exists"
zfs version
zfs set "$property=on" "$source"
zfs snapshot -o "$metadata=probe" "$source@property-layers"
zfs send -p "$source@property-layers" | zfs receive -u -x "$property" "$target"

show() {
	printf 'case=%s\n' "$1"
	zfs get -H -p -o name,property,value,received,source "$property" "$target"
}
show receive-exclusion
zfs inherit -S "$property" "$target"
show restore-received
zfs set "$property=off" "$target"
show local-override
zfs inherit "$property" "$target"
show inherit
zfs inherit -S "$property" "$target"
show restore-after-inherit
printf 'case=snapshot-metadata\n'
zfs get -H -p -o name,property,value,received,source "$metadata" "$target@property-layers"
zfs inherit "$metadata" "$target@property-layers"
printf 'case=snapshot-metadata-after-inherit\n'
zfs get -H -p -o name,property,value,received,source "$metadata" "$target@property-layers"
printf 'property_layer_probe=complete\n'
