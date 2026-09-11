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
# Pool names are derived by bootstrap.sh from the run ID; recomputing them here
# keeps the outage remote's root inside the run's own destination pool.
readonly destination_pool=boomerangz-test-$run_id-dst
# The outage remote reaches sshd through a second listener that starts life
# down. Nothing listens here until restore_outage_link starts it, so the
# daemon's first attempts fail the way a real transport outage does.
readonly outage_port=2222
readonly outage_pid_file=$artifact_dir/outage-sshd.pid
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

restore_outage_link() {
	printf 'restoring the outage SSH link on port %s\n' "$outage_port" >&2
	sudo /usr/bin/sshd -p "$outage_port" -o "PidFile=$outage_pid_file" </dev/null
}

stop_outage_link() {
	[[ -f $outage_pid_file ]] || return 0
	sudo pkill -F "$outage_pid_file" || true
	sudo rm -f -- "$outage_pid_file"
}

# Forwards a stage's output verbatim while acting on the handshake line that
# TestGuestRemoteOutageReconnection prints once it has observed the outage.
# The test is still blocked waiting for the link when this fires, so the
# stream cannot be buffered.
stage_filter() {
	local line
	while IFS= read -r line; do
		printf '%s\n' "$line"
		if [[ $line == BOOMERANGZ_REMOTE_OUTAGE_OBSERVED ]]; then
			restore_outage_link
		fi
	done
}

# Runs one test binary package-wide and asserts it reported at least
# min_passes top-level results. Package-wide runs mean a new test is picked up
# without editing this script; the pass floor means a test that vanishes -
# skipped by an unset environment guard, renamed, deleted - fails the run
# instead of passing silently.
run_stage() {
	local name=$1
	local min_passes=$2
	shift 2
	local status=0
	local passes skipped log
	# Scratch for the pass floor below, owned by the invoking user: this
	# function does not run as the service account and $artifact_dir does.
	# Nothing collects this file - its contents are already on stdout, which
	# the host harness tees into the run's diagnostics.
	log=$(mktemp)
	run_as_service "$@" -test.v 2>&1 | stage_filter | tee "$log" || status=$?
	passes=$(grep -c '^--- PASS: ' "$log" || true)
	skipped=$(grep '^--- SKIP: ' "$log" || true)
	rm -f -- "$log"
	if [[ $status -ne 0 ]]; then
		printf 'integration stage %s failed with status %d\n' "$name" "$status" >&2
		return "$status"
	fi
	if [[ $passes -lt $min_passes ]]; then
		printf 'integration stage %s reported %d top-level passes, expected at least %d\n' \
			"$name" "$passes" "$min_passes" >&2
		printf '%s\n' "${skipped:-no skipped tests were reported}" >&2
		return 1
	fi
	printf 'integration stage %s: %d top-level passes\n' "$name" "$passes"
}

cleanup() {
	local status=$?
	stop_outage_link
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

# Seed both host key entries before any stage runs, so no stage depends on
# another having populated known_hosts first. The outage listener presents the
# same host key as sshd on port 22, so its entry is that keyscan with the host
# field rewritten - it can be recorded while the link is still down.
host_key=$(ssh-keyscan -t ed25519 -p 22 127.0.0.1)
{
	printf '%s\n' "$host_key"
	printf '%s\n' "$host_key" | sed "s/^127\.0\.0\.1 /[127.0.0.1]:$outage_port /"
} | sudo tee "/var/lib/$service_user/.ssh/known_hosts" >/dev/null
sudo chown "$service_user:$service_user" "/var/lib/$service_user/.ssh/known_hosts"
sudo chmod 0600 "/var/lib/$service_user/.ssh/known_hosts"

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

run_stage lifecycle 1 \
BOOMERANGZ_LIFECYCLE_GUEST_RUN="$run_id" \
BOOMERANGZ_LIFECYCLE_GUEST_CLI="$artifact_dir/boomerangz" \
BOOMERANGZ_LIFECYCLE_GUEST_CONFIG="$config" \
	"$artifact_dir/lifecycle.test"

run_stage transfer-local 4 \
BOOMERANGZ_TRANSFER_GUEST_RUN="$run_id" \
	"$artifact_dir/transfer.test"

run_stage transfer-remote 2 \
BOOMERANGZ_REMOTE_GUEST_RUN="$run_id" \
BOOMERANGZ_REMOTE_GUEST_KEY="$loopback_key" \
BOOMERANGZ_REMOTE_GUEST_CLI="/usr/bin/boomerangz" \
BOOMERANGZ_REMOTE_DIRECT_SSH_USER="$direct_ssh_user" \
BOOMERANGZ_REMOTE_GUEST_USER="$service_user" \
	"$artifact_dir/transfer.test"

run_stage daemon 6 \
BOOMERANGZ_DAEMON_GUEST_RUN="$run_id" \
BOOMERANGZ_REMOTE_GUEST_HOST=127.0.0.1 \
BOOMERANGZ_REMOTE_GUEST_PORT="$outage_port" \
BOOMERANGZ_REMOTE_GUEST_ROOT="$destination_pool/data/outage" \
BOOMERANGZ_REMOTE_GUEST_KEY="$loopback_key" \
BOOMERANGZ_REMOTE_GUEST_USER="$service_user" \
BOOMERANGZ_REMOTE_GUEST_ENDPOINT=ssh-shell \
BOOMERANGZ_REMOTE_GUEST_CLI="/usr/bin/boomerangz" \
	"$artifact_dir/daemon.test"

run_stage control 3 \
BOOMERANGZ_CONTROL_GUEST_RUN="$run_id" \
BOOMERANGZ_CONTROL_GUEST_CLI="$artifact_dir/boomerangz" \
BOOMERANGZ_CONTROL_GUEST_CONFIG="$config" \
BOOMERANGZ_CONTROL_GUEST_SOCKET="$socket" \
	"$artifact_dir/control.test"
