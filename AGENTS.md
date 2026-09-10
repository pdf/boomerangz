# boomerangz - agent instructions

`boomerangz` is a property-driven OpenZFS snapshot and send/receive manager.
Go 1.26, module `github.com/pdf/boomerangz`. This file is always in context;
it holds only what applies to every change. Detail lives in the documents
listed under "Where to look" below.

## Tests and verification

Run `make test`. It is host-safe (no ZFS, no root) and is the standard this
repo is held to - `go test`, a `CGO_ENABLED=1 -race` build, `go vet`,
`golangci-lint`, `buf format`/`lint`/`generate` diff-checks, `actionlint`,
and `shellcheck`. A bare `go test ./...` is a narrower bar; do not report
work as verified against it.

Integration tests under `test/integration/` carry the `integration` build tag
and are never part of `go test ./...`. Run them with `make integration-test`,
which builds a disposable QEMU guest, creates real pools inside it, and
destroys them on the way out - it needs QEMU and `/dev/kvm`, but not root on
the host and not a pool on the host. `make integration-package-test` and
`make integration-benchmark` drive the same harness in their other modes.

Run the suite for any change under `test/integration/`, and for changes to
replication, transfer, or lifecycle behavior that a real kernel module would
exercise. It takes a guest boot per run, so `make integration-test-compile`
is the quick check that they still build - it is a compile gate, not
verification, and work is not verified against it.

`test/integration/README.md` carries the coverage ledger: one line per
behavior axis, naming the test that owns it, and naming the axes nothing owns
yet. A change that adds, moves, or removes an integration behavior updates the
ledger in the same change - it is how "is this tested?" gets answered without
reading the suite, and it is worth reading only while it is true. Adding a
test to a package whose stage already runs also means raising that stage's
pass floor in `guest/run-common.sh`.

Generated protobuf `.pb.go` files are committed. Regenerate with
`go tool buf generate` (covered by `make test`'s diff-check) rather than
editing them.

A change that materially affects behavior lands with a test in the same
change. A bug fix gets a test that fails before it. Nothing in CI enforces
this - there is no coverage gate - so it is on the author.

Needing ZFS is not a reason to skip a unit test. `zfs.Executor` is the typed
seam for every ZFS operation, and the established idiom is a test struct that
embeds `zfs.Executor` and overrides only the methods the test exercises (see
`localBackend` in `internal/transfer/local_test.go`); anything left
unimplemented panics rather than silently passing. Reserve
`test/integration/` for behavior that genuinely needs a real pool - it is
gated behind a build tag and does not run in `make test`, so it is not a
substitute for unit coverage.

Where a change genuinely does not warrant a test - rewording terminal output,
say - say so when reporting the work rather than leaving it unmentioned.

## Changes that reach users

A user-visible CLI change is not complete until it also updates:

- `docs/` - the VitePress site published to boomerangz.org. `docs/reference/cli.md`
  is the flag-by-flag reference; the `docs/guide/` and `docs/operations/` pages
  carry the prose that mentions the affected command.
- `contrib/` - Arch packaging inputs. Shell completions list flags per
  subcommand and must be updated in all three (`boomerangz.bash`,
  `_boomerangz`, `boomerangz.fish`), as must `contrib/man/boomerangz.1`.

Follow the conventions already in `internal/cli/commands.go` rather than
inventing new ones; kong struct tags drive the whole CLI surface, and
structured output is spelled `JSON bool` with `help:"Emit JSON."`.

## Commits

Scoped conventional commits, e.g. `docs(agents): ...`, `feat(cli): ...`,
`fix(transfer): ...`. The scope is the package or area touched, and is omitted
when a change is not tied to one; check `git log` for the scopes in use. No
tooling or assistant attribution in commit messages or PR descriptions.

## Where to look

- [ARCHITECTURE.md](ARCHITECTURE.md) - the repository map: entrypoints,
  package-by-package layout, the property-to-send/receive mapping, the
  `org.boomerangz:state:*` contract, and scheduler behavior. Read the relevant
  section before working in a package you are not already familiar with. It is
  long; map it with its headings rather than reading from the top.
- [PLAN.md](PLAN.md) - original design intent. Reliable for the *why* behind
  properties, ownership, and lifecycle semantics; its Section 12 package tree
  is stale. Prefer the code and ARCHITECTURE.md for current structure.
- `internal/cli/commands.go` - the whole CLI surface at a glance.
- [test/integration/README.md](test/integration/README.md) - what the
  integration suite owns, what it does not, and how a run is structured.
