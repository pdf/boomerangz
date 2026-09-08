# Development notes

User-facing documentation is in the parent directory. These documents record
implementation details and test evidence, not instructions for configuring a
production system.

- [Architecture and delivery plan](../../PLAN.md)
- [Implementation notes](implementation-notes.md)
- [Integration test harness](integration-testing.md)
- [Initial CachyOS test results](integration-spike-cachyos-260809.md)
- [Phase 5 hand-off](phase-5-handoff.md)
- [Phase 6 hand-off](phase-6-handoff.md)
- [Phase 7 hand-off](phase-7-handoff.md)
- [Phase 8 hand-off](phase-8-handoff.md)
- [Phase 9 hand-off](phase-9-handoff.md)
- [Phase 10 hand-off](phase-10-handoff.md)
- [Phase 11 packaging notes](phase-11-packaging-notes.md)

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
