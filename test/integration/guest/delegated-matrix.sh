#!/usr/bin/env bash

set -euo pipefail

fail() {
	printf 'boomerangz delegated matrix: %s\n' "$*" >&2
	exit 1
}

[[ $# -eq 3 ]] || fail "usage: $0 RUN_ID SOURCE_DISK DESTINATION_DISK"
readonly run_id=$1
source_disk=$(readlink -f -- "$2")
readonly source_disk
destination_disk=$(readlink -f -- "$3")
readonly destination_disk
readonly marker_path=/run/boomerangz-vmtest/guest-marker
readonly version_path=/run/boomerangz-vmtest/bootstrap-version
readonly bootstrap_version=2
readonly source_pool=boomerangz-test-$run_id-src
readonly destination_pool=boomerangz-test-$run_id-dst
readonly source=$source_pool/data
readonly destination=$destination_pool/data

disk_serial() {
	local role=$1
	printf 'bz-%s-%s\n' "$(printf '%s' "$run_id" | sha256sum | cut -c1-12)" "$role"
}

verify_pool_disk() {
	local pool=$1
	local expected_disk=$2
	local expected_serial=$3
	local vdev parent
	local -a vdevs

	[[ $pool == boomerangz-test-$run_id-* ]] || fail "pool name is outside this run: $pool"
	mapfile -t vdevs < <(zpool status -LP "$pool" | awk '$1 ~ "^/dev/" {print $1}')
	[[ ${#vdevs[@]} -gt 0 ]] || fail "$pool has no absolute device vdevs"
	for vdev in "${vdevs[@]}"; do
		parent=$(lsblk -dn -o PKNAME -- "$vdev")
		if [[ -n $parent ]]; then
			parent=/dev/$parent
		else
			parent=$(readlink -f -- "$vdev")
		fi
		[[ $parent == "$expected_disk" ]] || fail "$pool contains unexpected vdev $vdev"
		[[ $(lsblk -dn -o SERIAL -- "$parent") == "$expected_serial" ]] ||
			fail "$parent has an unexpected serial"
	done
}

[[ $run_id =~ ^[a-z0-9][a-z0-9-]{5,47}$ ]] || fail "invalid run ID"
[[ -f $marker_path && $(<"$marker_path") == "$run_id" ]] || fail "guest marker mismatch"
[[ -f $version_path && $(<"$version_path") == "$bootstrap_version" ]] || fail "guest bootstrap version mismatch"
verify_pool_disk "$source_pool" "$source_disk" "$(disk_serial src)"
verify_pool_disk "$destination_pool" "$destination_disk" "$(disk_serial dst)"

printf 'kernel=%s\n' "$(uname -r)"
pacman -Q linux-cachyos-lts linux-cachyos-lts-zfs zfs-utils
printf 'zfs_module=%s\n' "$(modinfo -F version zfs)"

zfs snapshot -o org.boomerangz:state:snapshot=matrix "$source@lifecycle"
zfs hold boomerangz-matrix "$source@lifecycle"
if zfs destroy "$source@lifecycle" 2>/dev/null; then
	fail "held snapshot was destroyed"
fi
zfs release boomerangz-matrix "$source@lifecycle"
zfs bookmark "$source@lifecycle" "$source#lifecycle"
zfs destroy "$source#lifecycle"
zfs destroy "$source@lifecycle"
printf 'lifecycle=pass\n'

zfs snapshot "$source@full-a"
zfs send "$source@full-a" | zfs receive -u "$destination/full"
zfs snapshot "$source@full-b"
zfs send -i "$source@full-a" "$source@full-b" | zfs receive -u "$destination/full"
zfs snapshot "$source@full-c"
zfs snapshot "$source@full-d"
zfs send -I "$source@full-b" "$source@full-d" | zfs receive -u "$destination/full"
[[ $(zfs list -H -t snapshot -o name -r "$destination/full" | wc -l) -eq 4 ]] ||
	fail "incremental receive did not preserve the expected snapshots"
printf 'full_and_incremental=pass\n'

zfs snapshot -r "$source@recursive-a"
zfs send -R "$source@recursive-a" | zfs receive -u "$destination/recursive"
[[ $(zfs get -H -o value mountpoint "$destination/recursive") == none ]] ||
	fail "recursive receive did not retain mountpoint=none"
zfs list -H -o name "$destination/recursive/child" "$destination/recursive/payload" >/dev/null
printf 'recursive=pass\n'

zfs snapshot "$source@properties-a"
zfs send -p "$source@properties-a" |
	zfs receive -u -o readonly=on -x compression "$destination/properties"
[[ $(zfs get -H -o value readonly "$destination/properties") == on ]] ||
	fail "readonly receive override was not applied"
[[ $(zfs get -H -o source compression "$destination/properties") != received ]] ||
	fail "compression receive exclusion was not applied"
printf 'receive_properties=pass\n'

zfs snapshot -r "$source@resume-a"
set +e
zfs send -R "$source@resume-a" | head -c 4194304 |
	zfs receive -s -u "$destination/resume"
interrupted_status=$?
set -e
[[ $interrupted_status -ne 0 ]] || fail "receive interruption unexpectedly completed"

token_line=$(zfs get -H -r -o name,value receive_resume_token "$destination/resume" |
	awk '$2 != "-" {print; exit}')
[[ -n $token_line ]] || fail "interrupted receive did not retain a resume token"
token_dataset=${token_line%%$'\t'*}
token=${token_line#*$'\t'}
zfs send -t "$token" | zfs receive -s -u "$token_dataset"
[[ $(zfs get -H -o value receive_resume_token "$token_dataset") == - ]] ||
	fail "resume token remains after successful recovery"
printf 'resumable=pass\n'

printf 'delegated_matrix=pass\n'
