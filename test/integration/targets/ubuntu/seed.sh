#!/usr/bin/env bash

set -euo pipefail

readonly output=${1:?missing output path}
readonly public_key=${2:?missing public key}
readonly hostname=${3:?missing hostname}
readonly guest_user=${4:?missing guest user}
readonly seed_work=$output.files

# No network-config: Ubuntu names the interface by its PCI slot rather than
# eth0, and cloud-init's fallback DHCP on it is what QEMU user networking
# expects anyway.
install -d -m 0700 "$seed_work"
cat >"$seed_work/user-data" <<USERDATA
#cloud-config
users:
  - name: $guest_user
    groups: [sudo]
    shell: /bin/bash
    sudo: ALL=(ALL) NOPASSWD:ALL
    ssh_authorized_keys:
      - $public_key
ssh_pwauth: false
growpart:
  mode: auto
  devices: ['/']
USERDATA
cat >"$seed_work/meta-data" <<METADATA
instance-id: $hostname
local-hostname: $hostname
METADATA
cloud-localds "$output" "$seed_work/user-data" "$seed_work/meta-data"
