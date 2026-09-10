INTEGRATION_TARGET ?= cachyos
SHELLCHECK_SOURCES := $(wildcard \
	.github/scripts/*.sh \
	test/integration/host/*.sh \
	test/integration/guest/*.sh \
	test/integration/targets/*/*.sh)

.DEFAULT_GOAL := help

.PHONY: help test integration-test-compile integration-test integration-package-test integration-benchmark

help:
	@printf '%s\n' \
		'make test                      Run the host-safe CI validation suite' \
		'make integration-test-compile Compile integration tests without running them' \
		'make integration-test          Run real-ZFS tests in a disposable QEMU guest' \
		'make integration-package-test  Validate release packages in a disposable QEMU guest' \
		'make integration-benchmark     Benchmark remote transfers in a disposable QEMU guest'

test:
	go test ./...
	CGO_ENABLED=1 go test -race ./...
	go vet ./...
	go tool golangci-lint run
	go tool buf format --diff --exit-code
	go tool buf lint
	go tool buf generate
	go tool actionlint
	shellcheck $(SHELLCHECK_SOURCES)

integration-test-compile:
	go test -run '^$$' -tags=integration ./test/integration/...

integration-test:
	test/integration/host/run.sh $(INTEGRATION_TARGET)

integration-package-test:
	BOOMERANGZ_INTEGRATION_MODE=package test/integration/host/run.sh $(INTEGRATION_TARGET)

integration-benchmark:
	BOOMERANGZ_INTEGRATION_MODE=benchmark test/integration/host/run.sh $(INTEGRATION_TARGET)
