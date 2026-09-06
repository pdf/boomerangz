# Development notes

User-facing documentation is in the parent directory. These documents record
implementation details and test evidence, not instructions for configuring a
production system.

- [Architecture and delivery plan](../../PLAN.md)
- [Implementation notes](implementation-notes.md)
- [Integration test harness](integration-testing.md)
- [Initial CachyOS test results](integration-spike-cachyos-260809.md)
- [Phase 5 hand-off](phase-5-handoff.md)

## Protobuf APIs

The protobuf source lives under `proto/`. Buf and both Go generators are pinned
as Go tools, so no separately installed `buf` or `protoc` binary is required.

Run these checks after changing a schema:

```sh
go tool buf format --diff --exit-code
go tool buf lint
go tool buf generate
```

Generated Go files are committed. CI regenerates them and rejects any drift.
