#!/usr/bin/env bash

set -euo pipefail

readonly artifact_dir=${1:?missing artifact directory}
readonly run_id=${2:?missing run ID}
readonly source_device=${3:?missing source device}
readonly destination_device=${4:?missing destination device}
readonly socket=$artifact_dir/integration.sock
readonly config=$artifact_dir/config.toml
readonly loopback_key=$artifact_dir/loopback_ed25519
export BOOMERANGZ_INTEGRATION_SOURCE_DEVICE=$source_device
export BOOMERANGZ_INTEGRATION_DESTINATION_DEVICE=$destination_device

cleanup() {
	local status=$?
	sudo "$artifact_dir/bootstrap.sh" cleanup "$run_id" "$source_device" "$destination_device" || {
		printf 'guarded guest cleanup failed; preserving pool state\n' >&2
		status=1
	}
	exit "$status"
}
trap cleanup EXIT

chmod 0755 "$artifact_dir"/*.sh "$artifact_dir/boomerangz" "$artifact_dir"/*.test
sudo modprobe zfs
sudo "$artifact_dir/bootstrap.sh" setup "$run_id" "$source_device" "$destination_device"
"$artifact_dir/delegated-matrix.sh" "$run_id" "$source_device" "$destination_device"
"$artifact_dir/property-layers.sh" "$run_id" "$source_device" "$destination_device"

cat >"$config" <<EOF
[paths]
credentials_dir = "$artifact_dir/credentials"
identity_dir = "$artifact_dir/identity"
socket_path = "$socket"
EOF
install -d -m 0700 "$artifact_dir/credentials" "$artifact_dir/identity"

# This identity exists only inside the disposable guest and is distinct from
# the host-to-guest key created by the QEMU harness.
ssh-keygen -q -t ed25519 -N '' -f "$loopback_key"
install -d -m 0700 "$HOME/.ssh"
cat "$loopback_key.pub" >>"$HOME/.ssh/authorized_keys"
chmod 0600 "$HOME/.ssh/authorized_keys"

BOOMERANGZ_LIFECYCLE_GUEST_RUN="$run_id" \
BOOMERANGZ_LIFECYCLE_GUEST_CLI="$artifact_dir/boomerangz" \
BOOMERANGZ_LIFECYCLE_GUEST_CONFIG="$config" \
	"$artifact_dir/lifecycle.test" -test.v -test.run '^TestGuestLifecycle$'

BOOMERANGZ_TRANSFER_GUEST_RUN="$run_id" \
	"$artifact_dir/transfer.test" -test.v -test.run '^TestGuest(LocalTransfer|InterruptedTransferRecovery)$'

BOOMERANGZ_REMOTE_GUEST_RUN="$run_id" \
BOOMERANGZ_REMOTE_GUEST_KEY="$loopback_key" \
BOOMERANGZ_REMOTE_GUEST_CLI="$artifact_dir/boomerangz" \
	"$artifact_dir/transfer.test" -test.v -test.run '^TestGuestSSHTransfer$'

BOOMERANGZ_DAEMON_GUEST_RUN="$run_id" \
	"$artifact_dir/daemon.test" -test.v -test.run '^TestGuestDaemonSchedulingAndRetirement$'

BOOMERANGZ_CONTROL_GUEST_RUN="$run_id" \
BOOMERANGZ_CONTROL_GUEST_CLI="$artifact_dir/boomerangz" \
BOOMERANGZ_CONTROL_GUEST_CONFIG="$config" \
BOOMERANGZ_CONTROL_GUEST_SOCKET="$socket" \
	"$artifact_dir/control.test" -test.v -test.run '^TestGuestDaemon(Control|AbruptRestart)$'
