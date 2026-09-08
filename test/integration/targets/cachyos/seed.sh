#!/usr/bin/env bash

set -euo pipefail

readonly output=${1:?missing output path}
readonly public_key=${2:?missing public key}
readonly hostname=${3:?missing hostname}
readonly guest_user=${4:?missing guest user}
readonly seed_work=$output.files

install -d -m 0700 "$seed_work"
cat >"$seed_work/user-data" <<EOF
#cloud-config
users:
  - name: $guest_user
    groups: [wheel]
    shell: /bin/bash
    sudo: ALL=(ALL) NOPASSWD:ALL
    ssh_authorized_keys:
      - $public_key
ssh_pwauth: false
growpart:
  mode: auto
  devices: ['/']
EOF
cat >"$seed_work/meta-data" <<EOF
instance-id: $hostname
local-hostname: $hostname
EOF
cat >"$seed_work/network-config" <<'EOF'
version: 2
ethernets:
  eth0:
    addresses:
      - 10.0.2.15/24
    routes:
      - to: default
        via: 10.0.2.2
    nameservers:
      addresses:
        - 10.0.2.3
EOF
cloud-localds \
	--network-config="$seed_work/network-config" \
	"$output" \
	"$seed_work/user-data" \
	"$seed_work/meta-data"
