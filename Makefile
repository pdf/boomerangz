INTEGRATION_TARGET ?= cachyos

.DEFAULT_GOAL := help

.PHONY: help test integration-test-compile integration-test

help:
	@printf '%s\n' \
		'make test                      Run ordinary host-safe tests' \
		'make integration-test-compile Compile integration tests without running them' \
		'make integration-test          Run real-ZFS tests in a disposable QEMU guest'

test:
	go test ./...

integration-test-compile:
	go test -run '^$$' -tags=integration ./test/integration/...

integration-test:
	test/integration/host/run.sh $(INTEGRATION_TARGET)
