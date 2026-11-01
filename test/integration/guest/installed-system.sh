#!/usr/bin/env bash
# Installs the build under test as a package would and runs the suite against
# that installed system: the account, unit, sysusers and tmpfiles assets, and
# the restricted shell. Every target uses systemd, so this is shared; a
# target's run.sh reports its own package versions and runs its package test
# first.

set -euo pipefail

readonly artifact_dir=${1:?missing artifact directory}
readonly run_id=${2:?missing run ID}
readonly source_device=${3:?missing source device}
readonly destination_device=${4:?missing destination device}
readonly direct_ssh_user=boomerangz-replication

# Direct SSH intentionally uses a distinct, non-administrative login account.
# Its shell is needed only to execute the bounded ZFS commands sent by the
# fallback transport; the key itself disables forwarding, PTY allocation, and
# user startup files.
sudo install -d -m 0755 /run/sysusers.d
printf 'u %s - "boomerangz direct SSH replication" /var/lib/%s /bin/sh\n' \
	"$direct_ssh_user" "$direct_ssh_user" |
	sudo tee /run/sysusers.d/boomerangz-replication.conf >/dev/null
sudo systemd-sysusers /run/sysusers.d/boomerangz-replication.conf
sudo install -d -m 0700 -o "$direct_ssh_user" -g "$direct_ssh_user" "/var/lib/$direct_ssh_user"
[[ $(id -nG "$direct_ssh_user") == "$direct_ssh_user" ]]
if sudo -u "$direct_ssh_user" sudo -n true >/dev/null 2>&1; then
	printf 'direct SSH replication account unexpectedly has sudo access\n' >&2
	exit 1
fi

# Install the unit and service-account assets the packages ship.
sudo install -m 0755 "$artifact_dir/boomerangz" /usr/bin/boomerangz
sudo install -Dm755 "$artifact_dir/boomerangz-shell" /usr/lib/boomerangz/boomerangz-shell
sudo install -m 0644 "$artifact_dir/boomerangz.service" /usr/lib/systemd/system/boomerangz.service
sudo install -m 0644 "$artifact_dir/boomerangz.sysusers" /usr/lib/sysusers.d/boomerangz.conf
sudo install -m 0644 "$artifact_dir/boomerangz.tmpfiles" /usr/lib/tmpfiles.d/boomerangz.conf
sudo systemd-sysusers
sudo systemd-tmpfiles --create
if ! grep -qxF '/usr/lib/boomerangz/boomerangz-shell' /etc/shells; then
	printf '%s\n' '/usr/lib/boomerangz/boomerangz-shell' | sudo tee -a /etc/shells >/dev/null
fi
printf '[ssh_shell]\nreplication_roots = ["boomerangz-test-%s-dst/data"]\n' "$run_id" |
	sudo tee /etc/boomerangz/config.toml >/dev/null
sudo chown root:boomerangz /etc/boomerangz/config.toml
sudo chmod 0640 /etc/boomerangz/config.toml

BOOMERANGZ_INTEGRATION_DIRECT_SSH_USER="$direct_ssh_user" \
	"$artifact_dir/run-common.sh" "$artifact_dir" "$run_id" "$source_device" "$destination_device"

if [[ ${BOOMERANGZ_INTEGRATION_MODE:-test} == benchmark ]]; then
	exit 0
fi

# Pair through a TCP listener the way docs/guide/remotes.md tells an
# operator to: both commands through sudo, before the service has started.
# Root writes the token verifiers and managed certificates into the
# service's identity directory and the bundle into credentials.d; the
# service must still start, serve the listener, and read the bundle.
printf '\n[listeners.replication]\nnetwork = "tcp"\naddress = "127.0.0.1:17443"\nadvertised_address = "localhost:17443"\nauth_mode = "mtls+token"\n' |
	sudo tee -a /etc/boomerangz/config.toml >/dev/null
pairing_dir=$(mktemp -d)
# shellcheck disable=SC2024 # The operator's shell writes the bundle, as in the guide.
(umask 077 && sudo boomerangz pairing create --listener replication --scope status >"$pairing_dir/bundle.json")
sudo boomerangz pairing import native-check "$pairing_dir/bundle.json"
rm -rf "$pairing_dir"

sudo systemctl daemon-reload
sudo systemctl start boomerangz.service
sudo systemctl is-active boomerangz.service
# The unit is Type=simple, so start returns once the process is forked and
# is-active says so before the daemon has necessarily created its control
# socket. ExecReload is a client of that socket, so reloading immediately
# makes this assertion a race against startup rather than a check of reload -
# it has been seen to lose. sudo because RuntimeDirectoryMode is 0750 and the
# harness account is not in the service group.
for _ in $(seq 1 60); do
	if sudo test -S /run/boomerangz/boomerangz.sock; then
		break
	fi
	sleep 1
done
if ! sudo test -S /run/boomerangz/boomerangz.sock; then
	printf 'packaged service did not create its control socket\n' >&2
	exit 1
fi
# The checks docs/operations/troubleshooting.md gives for a service that
# will not start: nothing the pairing commands wrote may be left to root.
if [[ -n $(sudo find /var/lib/boomerangz/identity ! -user boomerangz) ]] ||
	[[ -n $(sudo find /etc/boomerangz/credentials.d -name '*.json' \( ! -group boomerangz -o ! -perm -g=r \)) ]]; then
	printf 'pairing through sudo left files the service account cannot use:\n' >&2
	sudo find /var/lib/boomerangz/identity /etc/boomerangz/credentials.d -printf '%u:%g %m %p\n' >&2
	exit 1
fi
sudo -u boomerangz -H boomerangz status --credential native-check >/dev/null
sudo systemctl reload boomerangz.service
sudo systemctl is-active boomerangz.service
if sudo systemctl is-enabled boomerangz.service >/dev/null 2>&1; then
	printf 'integration install unexpectedly enabled the service\n' >&2
	exit 1
fi
sudo systemctl stop boomerangz.service
printf 'installed_system=pass\n'
