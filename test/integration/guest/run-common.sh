#!/usr/bin/env bash

set -euo pipefail

readonly artifact_dir=${1:?missing artifact directory}
readonly run_id=${2:?missing run ID}
readonly source_device=${3:?missing source device}
readonly destination_device=${4:?missing destination device}
readonly socket=$artifact_dir/integration.sock
readonly config=$artifact_dir/config.toml
readonly loopback_key=$artifact_dir/loopback_ed25519
readonly service_user=boomerangz
readonly direct_ssh_user=${BOOMERANGZ_INTEGRATION_DIRECT_SSH_USER:?missing direct SSH user}
readonly integration_mode=${BOOMERANGZ_INTEGRATION_MODE:-test}
export BOOMERANGZ_INTEGRATION_SOURCE_DEVICE=$source_device
export BOOMERANGZ_INTEGRATION_DESTINATION_DEVICE=$destination_device

[[ $integration_mode == test || $integration_mode == benchmark ]] || {
	printf 'invalid integration mode %q\n' "$integration_mode" >&2
	exit 1
}

run_as_service() {
	sudo -u "$service_user" -H env \
		BOOMERANGZ_INTEGRATION_SOURCE_DEVICE="$source_device" \
		BOOMERANGZ_INTEGRATION_DESTINATION_DEVICE="$destination_device" \
		"$@"
}

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
sudo "$artifact_dir/bootstrap.sh" setup "$run_id" "$source_device" "$destination_device" "$direct_ssh_user"
run_as_service "$artifact_dir/delegated-matrix.sh" "$run_id" "$source_device" "$destination_device"
run_as_service "$artifact_dir/property-layers.sh" "$run_id" "$source_device" "$destination_device"

sudo chown -R "$service_user:$service_user" "$artifact_dir"
run_as_service tee "$config" >/dev/null <<EOF
[paths]
credentials_dir = "$artifact_dir/credentials"
identity_dir = "$artifact_dir/identity"
socket_path = "$socket"
EOF
run_as_service install -d -m 0700 "$artifact_dir/credentials" "$artifact_dir/identity"

# This identity exists only inside the disposable guest and is distinct from
# the host-to-guest key created by the QEMU harness.
run_as_service ssh-keygen -q -t ed25519 -N '' -f "$loopback_key"
sudo install -d -m 0700 -o "$direct_ssh_user" -g "$direct_ssh_user" "/var/lib/$direct_ssh_user/.ssh"
sed 's/^/restrict /' "$loopback_key.pub" |
	sudo tee "/var/lib/$direct_ssh_user/.ssh/authorized_keys" >/dev/null
sudo chown "$direct_ssh_user:$direct_ssh_user" "/var/lib/$direct_ssh_user/.ssh/authorized_keys"
sudo chmod 0600 "/var/lib/$direct_ssh_user/.ssh/authorized_keys"
sudo install -d -m 0700 -o "$service_user" -g "$service_user" "/var/lib/$service_user/.ssh"
sed 's/^/restrict /' "$loopback_key.pub" |
	sudo tee "/var/lib/$service_user/.ssh/authorized_keys" >/dev/null
sudo chown "$service_user:$service_user" "/var/lib/$service_user/.ssh/authorized_keys"
sudo chmod 0600 "/var/lib/$service_user/.ssh/authorized_keys"

if [[ $integration_mode == benchmark ]]; then
	run_as_service \
	BOOMERANGZ_REMOTE_GUEST_RUN="$run_id" \
	BOOMERANGZ_REMOTE_GUEST_KEY="$loopback_key" \
	BOOMERANGZ_REMOTE_GUEST_CLI="/usr/bin/boomerangz" \
	BOOMERANGZ_REMOTE_DIRECT_SSH_USER="$direct_ssh_user" \
	BOOMERANGZ_REMOTE_GUEST_USER="$service_user" \
		"$artifact_dir/transfer.test" -test.run '^$' -test.bench '^BenchmarkGuestRemoteTransfer$'
	exit 0
fi

run_as_service \
BOOMERANGZ_LIFECYCLE_GUEST_RUN="$run_id" \
BOOMERANGZ_LIFECYCLE_GUEST_CLI="$artifact_dir/boomerangz" \
BOOMERANGZ_LIFECYCLE_GUEST_CONFIG="$config" \
	"$artifact_dir/lifecycle.test" -test.v -test.run '^TestGuestLifecycle$'

run_as_service \
BOOMERANGZ_TRANSFER_GUEST_RUN="$run_id" \
	"$artifact_dir/transfer.test" -test.v -test.run '^TestGuest(LocalTransfer|InterruptedTransferRecovery)$'

run_as_service \
BOOMERANGZ_REMOTE_GUEST_RUN="$run_id" \
BOOMERANGZ_REMOTE_GUEST_KEY="$loopback_key" \
BOOMERANGZ_REMOTE_GUEST_CLI="/usr/bin/boomerangz" \
BOOMERANGZ_REMOTE_DIRECT_SSH_USER="$direct_ssh_user" \
BOOMERANGZ_REMOTE_GUEST_USER="$service_user" \
	"$artifact_dir/transfer.test" -test.v -test.run '^TestGuestSSHTransfer$'

run_as_service \
BOOMERANGZ_DAEMON_GUEST_RUN="$run_id" \
	"$artifact_dir/daemon.test" -test.v -test.run '^TestGuestDaemonSchedulingAndRetirement$'

run_as_service \
BOOMERANGZ_CONTROL_GUEST_RUN="$run_id" \
BOOMERANGZ_CONTROL_GUEST_CLI="$artifact_dir/boomerangz" \
BOOMERANGZ_CONTROL_GUEST_CONFIG="$config" \
BOOMERANGZ_CONTROL_GUEST_SOCKET="$socket" \
	"$artifact_dir/control.test" -test.v -test.run '^TestGuestDaemon(Control|AbruptRestart)$'
