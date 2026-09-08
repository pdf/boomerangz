#!/usr/bin/env bash

set -euo pipefail

readonly base_image=${1:?missing base image}
readonly system_image=${2:?missing system image}
readonly system_size=${3:?missing system size}

qemu-img create -f qcow2 -F qcow2 -b "$base_image" "$system_image"
qemu-img resize "$system_image" "$system_size"
