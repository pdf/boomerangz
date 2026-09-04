# boomerangz

`boomerangz` is a property-driven ZFS snapshot and replication manager for
continuously connected and intermittently connected systems.

The project is in its initial delivery phase. The current scaffold provides the
configuration contract, command-line entry point, and direct ZFS executor
boundary. It does not yet manage datasets.

## Development

The supported toolchain is Go 1.26.8 or newer within the Go 1.26 series.

```sh
go test ./...
go test -race ./...
go vet ./...
go tool golangci-lint run
```

Real `zfs` and `zpool` commands must only be run by the integration harness in
a disposable virtual machine. Development-host tests use fake command runners.

The VM harness can check prerequisites, create isolated copy-on-write disks,
and launch a transient guest:

```sh
go run ./cmd/boomerangz-vmtest preflight \
  --base-image /absolute/path/cachyos-base.qcow2 \
  --work-dir /absolute/path/vm-runs \
  --ssh-port 22022
```

See [PLAN.md](PLAN.md) for the architecture and [docs/configuration.md](docs/configuration.md)
for the phase-one configuration schema. VM setup and safety requirements are in
[docs/integration-testing.md](docs/integration-testing.md); the first delegated
OpenZFS result is recorded in
[docs/integration-spike-cachyos-260809.md](docs/integration-spike-cachyos-260809.md).
