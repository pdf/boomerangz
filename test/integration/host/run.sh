#!/usr/bin/env bash
# shellcheck disable=SC2154 # Target values come from the sourced, validated adapter.

set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
readonly script_dir
repository=$(cd -- "$script_dir/../../.." && pwd)
readonly repository
readonly target=${1:?usage: run.sh TARGET}
readonly release_dir=${BOOMERANGZ_RELEASE_DIR:-}
readonly integration_mode=${BOOMERANGZ_INTEGRATION_MODE:-test}

[[ $target =~ ^[a-z0-9][a-z0-9_-]*$ ]] || {
	printf 'boomerangz integration: invalid target %q\n' "$target" >&2
	exit 1
}
[[ $integration_mode == test || $integration_mode == package || $integration_mode == benchmark ]] || {
	printf 'boomerangz integration: invalid mode %q\n' "$integration_mode" >&2
	exit 1
}
readonly target_dir=$repository/test/integration/targets/$target
[[ -f $target_dir/config.sh ]] || {
	printf 'boomerangz integration: unknown target %q\n' "$target" >&2
	exit 1
}
# shellcheck source=/dev/null
source "$target_dir/config.sh"

readonly ssh_port=${BOOMERANGZ_CI_SSH_PORT:-22222}
run_id=ci-$(printf '%s-%s-%s' "${GITHUB_RUN_ID:-local-$(date +%s)-$RANDOM}" "${GITHUB_RUN_ATTEMPT:-1}" "$target" | sha256sum | cut -c1-16)
readonly run_id
readonly run_root=${RUNNER_TEMP:-/tmp}/boomerangz-integration/$run_id
readonly diagnostics=$run_root/diagnostics
readonly artifacts=$run_root/artifacts
readonly private_key=$run_root/id_ed25519
readonly ssh_target=$target_guest_user@127.0.0.1
readonly system_image=$run_root/system.img
readonly source_image=$run_root/source.qcow2
readonly destination_image=$run_root/destination.qcow2
readonly seed_image=$run_root/seed.img
readonly guest_artifacts=/var/tmp/boomerangz-integration-$run_id
# Base images are shared across runs rather than re-downloaded per run ID.
# On CI this is a per-job directory and so always a miss, which is fine - the
# point is local iteration, where the run root is fresh every time.
readonly image_cache=${BOOMERANGZ_IMAGE_CACHE:-${XDG_CACHE_HOME:-${HOME:-/tmp}/.cache}/boomerangz-integration}
readonly base_image=$image_cache/$target_image_name

qemu_pid=
boot_index=0

fail() {
	printf 'boomerangz integration: %s\n' "$*" >&2
	if [[ ${GITHUB_ACTIONS:-false} == true ]]; then
		printf '::error title=Integration harness::%s\n' "$*"
	fi
	exit 1
}

required_target_value() {
	local name=$1
	[[ -n ${!name:-} ]] || fail "target $target does not define $name"
}

record_failure() {
	local status=$?
	trap - ERR
	printf 'failed_command=%q\nfailed_line=%s\nexit_status=%s\n' "$BASH_COMMAND" "$1" "$status" >"$diagnostics/failure.txt"
	return "$status"
}

stop_qemu() {
	if [[ -n ${qemu_pid:-} ]] && kill -0 "$qemu_pid" 2>/dev/null; then
		kill "$qemu_pid" 2>/dev/null || true
		for _ in {1..30}; do
			kill -0 "$qemu_pid" 2>/dev/null || break
			sleep 1
		done
		kill -KILL "$qemu_pid" 2>/dev/null || true
	fi
	qemu_pid=
	rm -f -- "$run_root/qemu.pid"
}

cleanup() {
	local status=$?
	stop_qemu
	if [[ -f $system_image ]]; then
		qemu-img info --output=json "$system_image" >"$diagnostics/system-image.json" 2>&1 || true
	fi
	df -hT "$run_root" >"$diagnostics/disk-usage.txt" 2>&1 || true
	exit "$status"
}

mkdir -p "$diagnostics" "$artifacts"
trap 'record_failure "$LINENO"' ERR
trap cleanup EXIT

ssh_guest() {
	ssh \
		-i "$private_key" \
		-o BatchMode=yes \
		-o ConnectTimeout=5 \
		-o ForwardAgent=no \
		-o IdentitiesOnly=yes \
		-o StrictHostKeyChecking=no \
		-o UserKnownHostsFile=/dev/null \
		-p "$ssh_port" \
		"$ssh_target" "$@"
}

copy_to_guest() {
	scp \
		-i "$private_key" \
		-o BatchMode=yes \
		-o ForwardAgent=no \
		-o IdentitiesOnly=yes \
		-o StrictHostKeyChecking=no \
		-o UserKnownHostsFile=/dev/null \
		-P "$ssh_port" \
		-r "$@" "$ssh_target:/home/$target_guest_user/integration/"
}

wait_for_ssh() {
	for _ in {1..180}; do
		if ssh_guest true 2>/dev/null; then
			return
		fi
		kill -0 "$qemu_pid" 2>/dev/null || fail "target $target exited before SSH became ready"
		sleep 1
	done
	fail "target $target SSH did not become ready"
}

disk_serial() {
	local role=$1
	printf 'bz-%s-%s\n' "$(printf '%s' "$run_id" | sha256sum | cut -c1-12)" "$role"
}

start_qemu() {
	stop_qemu
	boot_index=$((boot_index + 1))
	local console=$diagnostics/console-$boot_index.log
	: >"$console"
	"$target_qemu_binary" \
		-machine "$target_machine" \
		-cpu "$target_cpu" \
		-smp "$target_vcpus" \
		-m "$target_memory_mib" \
		-nodefaults \
		-no-reboot \
		-display none \
		-serial "file:$console" \
		-device virtio-rng-pci \
		-drive "if=none,id=system,file=$system_image,format=$target_image_format,cache=none" \
		-device "$target_system_device",drive=system \
		-drive "if=none,id=source,file=$source_image,format=qcow2,cache=none" \
		-device "$target_data_device",drive=source,serial="$(disk_serial src)" \
		-drive "if=none,id=destination,file=$destination_image,format=qcow2,cache=none" \
		-device "$target_data_device",drive=destination,serial="$(disk_serial dst)" \
		-drive "if=none,id=seed,file=$seed_image,format=$target_seed_format,readonly=on" \
		-device virtio-blk-pci,drive=seed \
		-netdev user,id=net0,hostfwd=tcp:127.0.0.1:"$ssh_port"-:22 \
		-device "$target_network_device",netdev=net0 \
		-daemonize \
		-pidfile "$run_root/qemu.pid"
	qemu_pid=$(<"$run_root/qemu.pid")
	wait_for_ssh
}

for name in \
	target_image_name target_image_url target_image_sha256 target_image_format \
	target_system_size target_guest_user target_source_device target_destination_device \
	target_system_device target_data_device target_network_device target_memory_mib \
	target_vcpus target_qemu_binary target_machine target_cpu target_seed_format \
	target_poweroff_command target_provision_command; do
	required_target_value "$name"
done
for adapter in prepare-image.sh seed.sh provision.sh run.sh package.sh; do
	[[ -x $target_dir/$adapter ]] || fail "target $target is missing executable $adapter"
done
if [[ -n $release_dir ]]; then
	[[ $release_dir == /* && -d $release_dir ]] || fail "BOOMERANGZ_RELEASE_DIR must be an absolute directory"
fi
if [[ $integration_mode == package && -z $release_dir ]]; then
	fail 'package mode requires BOOMERANGZ_RELEASE_DIR'
fi
[[ $ssh_port =~ ^[0-9]+$ ]] || fail "invalid SSH port"
[[ -c /dev/kvm && -r /dev/kvm && -w /dev/kvm ]] || fail "/dev/kvm is not usable"
for tool in curl qemu-img "$target_qemu_binary" scp ssh ssh-keygen; do
	command -v "$tool" >/dev/null || fail "missing host tool: $tool"
done

uname -a >"$diagnostics/host-uname.txt"
"$target_qemu_binary" --version >"$diagnostics/qemu-version.txt"
# The adapter pins the base image's sha256, so that check doubles as the cache
# validator: a cached file is used only while it still matches, and a version
# bump changes both the name and the hash. prepare-image.sh makes system.img a
# qcow2 backing-file reference to this path, so it must stay readable for the
# whole run rather than be consumed like scratch.
mkdir -p "$image_cache"
if printf '%s  %s\n' "$target_image_sha256" "$base_image" | sha256sum --status --check - 2>/dev/null; then
	printf 'boomerangz integration: reusing cached %s\n' "$target_image_name"
else
	download=$(mktemp "$image_cache/.$target_image_name.XXXXXX")
	curl --fail --location --silent --show-error "$target_image_url" --output "$download" || {
		rm -f -- "$download"
		fail "could not download $target_image_url"
	}
	printf '%s  %s\n' "$target_image_sha256" "$download" | sha256sum --check || {
		rm -f -- "$download"
		fail "base image checksum mismatch for $target_image_name"
	}
	chmod 0444 "$download"
	# Rename inside the cache directory so a concurrent run sees either the
	# previous file or the complete new one, never a partial download.
	mv -f -- "$download" "$base_image"
fi
"$target_dir/prepare-image.sh" "$base_image" "$system_image" "$target_system_size"
qemu-img create -f qcow2 "$source_image" 2G
qemu-img create -f qcow2 "$destination_image" 2G

ssh-keygen -q -t ed25519 -N '' -f "$private_key"
"$target_dir/seed.sh" "$seed_image" "$(<"$private_key.pub")" "boomerangz-$run_id" "$target_guest_user"

start_qemu
ssh_guest "mkdir -p /home/$target_guest_user/integration/target"
copy_to_guest "$target_dir/provision.sh"
ssh_guest "mv /home/$target_guest_user/integration/provision.sh /home/$target_guest_user/integration/target/provision.sh"
ssh_guest "$target_provision_command"
ssh_guest "$target_poweroff_command" || true
for _ in {1..60}; do
	kill -0 "$qemu_pid" 2>/dev/null || break
	sleep 1
done
stop_qemu

start_qemu
ssh_guest 'uname -a; cat /etc/os-release; lsblk -o NAME,SIZE,TYPE,SERIAL' >"$diagnostics/guest-environment.txt"

(
	cd "$repository"
	if [[ $integration_mode != package ]]; then
		CGO_ENABLED=0 go build -trimpath -o "$artifacts/boomerangz" ./cmd/boomerangz
		CGO_ENABLED=0 go test -tags=integration -c -o "$artifacts/lifecycle.test" ./test/integration/lifecycle
		CGO_ENABLED=0 go test -tags=integration -c -o "$artifacts/transfer.test" ./test/integration/transfer
		CGO_ENABLED=0 go test -tags=integration -c -o "$artifacts/daemon.test" ./test/integration/daemon
		CGO_ENABLED=0 go test -tags=integration -c -o "$artifacts/control.test" ./test/integration/control
	fi
)
if [[ $integration_mode != package ]]; then
	cp "$repository/test/integration/guest/bootstrap.sh" "$artifacts/"
	cp "$repository/test/integration/guest/delegated-matrix.sh" "$artifacts/"
	cp "$repository/test/integration/guest/run-common.sh" "$artifacts/"
	cp "$repository/contrib/systemd/boomerangz.service" "$artifacts/"
	cp "$repository/contrib/boomerangz-shell" "$artifacts/"
	cp "$repository/contrib/sysusers.d/boomerangz.conf" "$artifacts/boomerangz.sysusers"
	cp "$repository/contrib/tmpfiles.d/boomerangz.conf" "$artifacts/boomerangz.tmpfiles"
fi
if [[ -n $release_dir ]]; then
	mkdir -p "$artifacts/target"
	cp "$target_dir/package.sh" "$artifacts/target/package.sh"
	cp -R -- "$release_dir" "$artifacts/release"
fi
copy_to_guest "$artifacts" "$target_dir/run.sh"
ssh_guest "mv /home/$target_guest_user/integration/run.sh /home/$target_guest_user/integration/target/run.sh"
ssh_guest "sudo mv /home/$target_guest_user/integration/artifacts $guest_artifacts"
ssh_guest "BOOMERANGZ_INTEGRATION_RUN=$run_id BOOMERANGZ_INTEGRATION_MODE=$integration_mode /home/$target_guest_user/integration/target/run.sh $guest_artifacts $run_id $target_source_device $target_destination_device" | tee "$diagnostics/test-output.txt"

ssh_guest "$target_poweroff_command" || true
for _ in {1..60}; do
	kill -0 "$qemu_pid" 2>/dev/null || break
	sleep 1
done
stop_qemu
