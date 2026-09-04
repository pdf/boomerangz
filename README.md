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

See [PLAN.md](PLAN.md) for the architecture and [docs/configuration.md](docs/configuration.md)
for the phase-one configuration schema.

