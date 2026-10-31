#!/usr/bin/env bash
# Prints the key a target's provisioned guest image is cached under: the host
# CPU class, then a digest of the target's provisioning inputs.
#
# The guest boots with the host's CPU, and provisioning may install packages
# built for the CPU it finds - CachyOS picks x86-64-v3, x86-64-v4 or Zen 4/5
# builds - so an image provisioned on one host can fail to boot on another
# with fewer instruction set extensions. The CPU class keeps those apart: the
# x86-64 level the host's dynamic loader supports, with the CPU vendor and
# family, which separates Intel AVX-512 from Zen 4 and Zen 5 hosts.

set -euo pipefail

readonly target=${1:?usage: image-key.sh TARGET}
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
readonly script_dir
readonly target_dir=$script_dir/../targets/$target
[[ $target =~ ^[a-z0-9][a-z0-9_-]*$ && -f $target_dir/config.sh && -f $target_dir/provision.sh ]] || {
	printf 'image-key.sh: unknown target %q\n' "$target" >&2
	exit 1
}

level=x86-64-v1
supported=$(/lib64/ld-linux-x86-64.so.2 --help 2>/dev/null || true)
for candidate in x86-64-v2 x86-64-v3 x86-64-v4; do
	if grep -qF "$candidate (supported" <<<"$supported"; then
		level=$candidate
	fi
done
vendor=$(awk -F': *' '/^vendor_id/ { print $2; exit }' /proc/cpuinfo)
family=$(awk -F': *' '/^cpu family/ { print $2; exit }' /proc/cpuinfo)
[[ -n $vendor && -n $family ]] || {
	printf 'image-key.sh: cannot read the CPU vendor and family\n' >&2
	exit 1
}
digest=$(cat "$target_dir/config.sh" "$target_dir/provision.sh" | sha256sum | cut -c1-16)
printf '%s-%s-%s-%s\n' "$level" "$vendor" "$family" "$digest"
