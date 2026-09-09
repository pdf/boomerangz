# boomerangz

`boomerangz` is a property-driven ZFS snapshot and replication manager for
continuously connected and intermittently connected systems.

The current implementation provides configuration loading, sparse discovery,
local policy inheritance, normalized retention grids, automatic snapshot and
replication workers, preview-first adoption and clean commands, and a versioned
local control API with optional authenticated TLS access.

Run the manager in the foreground:

```sh
boomerangz daemon
```

The supplied systemd integration runs the same command as a dedicated service
account. See [docs/installation.md](docs/installation.md) and
[docs/daemon.md](docs/daemon.md) before enabling it.

Inspect or watch a running daemon and request an immediate snapshot:

```sh
boomerangz status
boomerangz status --watch
boomerangz trigger pool/data
```

See [docs/control-api.md](docs/control-api.md) for output behavior, local access,
authenticated pairing, and optional TLS listeners.

On a ZFS system (use the disposable guest for development), inspect datasets:

```sh
boomerangz dataset --config /etc/boomerangz/config.toml list
boomerangz dataset --config /etc/boomerangz/config.toml inspect pool/data
```

Both commands produce readable output by default; add `--json` for structured
output. Inspection includes property provenance, requested and effective send
behavior, retained received properties, errors, and replication coverage. See
[docs/dataset-policy.md](docs/dataset-policy.md) for the policy contract.
Adoption, clean behavior, ownership checks, and current safety boundaries are documented
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

The canonical harness verifies a target image, creates isolated copy-on-write
disks, provisions a disposable guest, and runs the real-ZFS suites:

```sh
make integration-test
```

This is not equivalent to running `go test` with the `integration` build tag:
the tagged Go packages contain guest-side tests which remain guarded against
execution on the development host. Use `make integration-test-compile` to
compile that code without provisioning a guest. Use `make integration-test`
to execute the real scenarios; it invokes `test/integration/host/run.sh` with
the `cachyos` target by default.

See [PLAN.md](PLAN.md) for the architecture and [docs/configuration.md](docs/configuration.md)
for the configuration reference. Developer-only implementation and test notes
live under [docs/development](docs/development/README.md). VM setup and safety requirements are in
[docs/development/integration-testing.md](docs/development/integration-testing.md); the first delegated
OpenZFS result is recorded in
[docs/development/integration-spike-cachyos-260809.md](docs/development/integration-spike-cachyos-260809.md).
