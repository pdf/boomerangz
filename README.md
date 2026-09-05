# boomerangz

`boomerangz` is a property-driven ZFS snapshot and replication manager for
continuously connected and intermittently connected systems.

The current implementation provides configuration loading, sparse discovery,
local policy inheritance, normalized retention grids, snapshot lifecycle services,
and preview-first adoption and cleanup commands. It does not yet automatically
schedule snapshots or run transfers.

On a ZFS system (use the disposable guest for development), inspect datasets:

```sh
boomerangz dataset --config /etc/boomerangz/config.toml list
boomerangz dataset --config /etc/boomerangz/config.toml inspect pool/data
```

Both commands emit JSON. Inspection includes property provenance, requested and
effective send behavior, retained received properties, errors, and replication
coverage. See [docs/dataset-policy.md](docs/dataset-policy.md) for the policy contract.
Adoption, cleanup, ownership checks, and current safety boundaries are documented
in [docs/snapshot-lifecycle.md](docs/snapshot-lifecycle.md).

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
for the configuration reference. Developer-only implementation and test notes
live under [docs/development](docs/development/README.md). VM setup and safety requirements are in
[docs/development/integration-testing.md](docs/development/integration-testing.md); the first delegated
OpenZFS result is recorded in
[docs/development/integration-spike-cachyos-260809.md](docs/development/integration-spike-cachyos-260809.md).
