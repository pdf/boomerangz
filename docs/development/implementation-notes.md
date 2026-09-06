# Implementation notes

These are developer-facing notes, not user configuration instructions.

## Discovery


Discovery queries filesystems and volumes globally, then stored activation
properties. It retrieves all stored properties only for active datasets, their
ancestors, covered replication descendants, and explicitly retained or
requested datasets. Property arguments default to at most 128 names and 32 KiB
of dataset arguments per query. Output is capped at 32 MiB per command; the
tabular parser consumes rows with a 1 MiB line limit and preserves value spaces.
This implementation uses the tabular fallback; JSON capability detection is
not yet implemented.

The `inspected` field distinguishes complete policy inspection from sparse
inventory. Consumers must require it before acting on policy. `covered_by`
identifies descendants governed by a replication root; they must not receive
independently scheduled jobs. Differences in descendant configuration are
reported as warnings. An encrypted descendant requires raw mode on its
replication root. If `raw` is unspecified, discovery selects effective raw mode
automatically and explains why in inspection warnings, including any implied
flag changes. Explicit local or inherited `raw=off` is an error for that scope.
Receive-property compatibility is checked across the entire package, including
encrypted descendants below a plaintext root. Incompatible settings invalidate
the root's policy.

Each successful scan atomically publishes an immutable generation. Returned
inspection records are detached copies. Failed queries, malformed inventory,
unexpected rows, and activation changes between sparse and detailed queries
leave the last complete generation unchanged. ZFS does not provide an atomic
snapshot across these commands: other concurrent property edits converge on
the next successful scan. Concurrent scans are serialized, reconciliation
hints coalesce, and the coordinator accepts a configurable periodic interval.
Pending and resumable work is represented by the transfer recovery layer; the
daemon phase will feed that state into discovery and scheduling.

Generation comparison reports changed and removed datasets. Disabling a dataset
is visible in the next generation. The [lifecycle service](../snapshot-lifecycle.md)
provides cancellation/quiescence gates and preview/apply clean; wiring those
gates to live workers and exposing reconstructed recovery status remain in the
daemon phase. Namespace exclusion during actual receives belongs to the transfer
phase.

## Lifecycle integration boundaries

The lifecycle service and cancellation gate are implemented. The future daemon
must wire activation transitions to the gate, hold the same lifecycle lock as
standalone commands, and reconstruct recovery status. Queued cancelled tickets
must be discarded; running management operations finish before quiescence.
Transfer implementations must supply destination GUID verification and safe target
probes before references can be released. The transfer engine now enforces that
boundary for local and SSH targets; standalone clean still fails closed where
live target coordination is unavailable.

## Test-only properties and received layers

`org.boomerangz:clean-probe` and `org.boomerangz:state:probe` are disposable
VM fixtures, not supported configuration or production metadata. OpenZFS 2.4.3
retained hidden received values after plain inherit; `inherit -S` restored them.
No direct libzfs removal path is pursued. See the integration test notes.
