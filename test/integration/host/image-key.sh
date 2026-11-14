#!/usr/bin/env bash
# Prints the key a target's provisioned guest image is cached under: a digest
# of the target's provisioning inputs, so changing config.sh or provision.sh
# reprovisions the guest.
#
# The key carries nothing about the host. The guest boots with the host's CPU,
# so a target must provision only packages built for the baseline x86-64 ISA -
# CachyOS uses its baseline repository, not the x86-64-v3, x86-64-v4 or Zen 4/5
# ones - and one image then serves every host and CI runner.

set -euo pipefail

readonly target=${1:?usage: image-key.sh TARGET}
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
readonly script_dir
readonly target_dir=$script_dir/../targets/$target
[[ $target =~ ^[a-z0-9][a-z0-9_-]*$ && -f $target_dir/config.sh && -f $target_dir/provision.sh ]] || {
	printf 'image-key.sh: unknown target %q\n' "$target" >&2
	exit 1
}

digest=$(cat "$target_dir/config.sh" "$target_dir/provision.sh" | sha256sum | cut -c1-16)
printf '%s\n' "$digest"
