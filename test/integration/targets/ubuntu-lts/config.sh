#!/usr/bin/env bash

# Target adapter consumed by the generic QEMU host harness: the current Ubuntu
# LTS release. The scripts are shared with ubuntu-current through ../ubuntu.
# shellcheck disable=SC2034
target_image_name=ubuntu-26.04-server-cloudimg-amd64-20261016.img
target_image_url=https://cloud-images.ubuntu.com/releases/resolute/release-20261016/ubuntu-26.04-server-cloudimg-amd64.img
target_image_sha256=5645d0318ad21f643ebb1ac4567514620e9893380b07b0201fda163baa5b7eb5
target_image_format=qcow2
target_system_size=16G
target_guest_user=boomerangz-test
target_source_device=/dev/vdb
target_destination_device=/dev/vdc
target_system_device=virtio-blk-pci
target_data_device=virtio-blk-pci
target_network_device=virtio-net-pci
target_memory_mib=4096
target_vcpus=2
target_qemu_binary=qemu-system-x86_64
target_machine=q35,accel=kvm
target_cpu=host
target_seed_format=raw
target_poweroff_command='sudo poweroff'
target_provision_command="sudo /home/$target_guest_user/integration/target/provision.sh"
