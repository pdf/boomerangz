#!/usr/bin/env bash

set -euo pipefail

readonly artifact_dir=${1:?missing artifact directory}
readonly run_id=${2:?missing run ID}
readonly source_device=${3:?missing source device}
readonly destination_device=${4:?missing destination device}
readonly direct_ssh_user=boomerangz-replication

uname -a
cat /etc/os-release
pacman -Q linux-cachyos-lts linux-cachyos-lts-zfs zfs-utils
modinfo -F version zfs

if [[ -d $artifact_dir/release ]]; then
	"$artifact_dir/target/package.sh" "$artifact_dir/release" "$run_id" "$source_device" "$destination_device"
fi

if [[ ${BOOMERANGZ_INTEGRATION_MODE:-test} == package ]]; then
	exit 0
fi

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

# Installed-system behavior is target-specific. CachyOS uses systemd and the
# Linux service-account assets that will later be consumed by packaging.
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
sudo systemctl reload boomerangz.service
sudo systemctl is-active boomerangz.service
if sudo systemctl is-enabled boomerangz.service >/dev/null 2>&1; then
	printf 'integration install unexpectedly enabled the service\n' >&2
	exit 1
fi
sudo systemctl stop boomerangz.service
printf 'installed_system=pass\n'
