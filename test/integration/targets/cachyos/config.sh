#!/usr/bin/env bash

# Target adapter consumed by the generic QEMU host harness.
# shellcheck disable=SC2034
target_image_name=Arch-Linux-x86_64-cloudimg-20260901.583572.qcow2
target_image_url=https://geo.mirror.pkgbuild.com/images/v20260901.583572/Arch-Linux-x86_64-cloudimg-20260901.583572.qcow2
target_image_sha256=e3e688f97a71b265ce202905a504253f60f3680cf57d011a45411c43bedfa930
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
