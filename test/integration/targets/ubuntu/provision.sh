#!/usr/bin/env bash

set -euo pipefail

[[ ${EUID:-$(id -u)} -eq 0 ]] || {
	printf 'Ubuntu provisioner must run as root\n' >&2
	exit 1
}

export DEBIAN_FRONTEND=noninteractive

# Background apt runs would contend for the dpkg lock with the package test,
# and an unattended upgrade mid-run would change what is under test.
systemctl disable --now apt-daily.timer apt-daily-upgrade.timer
apt-get purge --yes unattended-upgrades

# The kernel ships the ZFS module; zfsutils-linux is the userland. Upgrading
# keeps both on the release's current updates, the same week-long freshness
# the provisioned-image cache gives every target.
apt-get update
apt-get --yes full-upgrade
apt-get install --yes --no-install-recommends zfsutils-linux
apt-get --yes autoremove --purge
apt-get clean
