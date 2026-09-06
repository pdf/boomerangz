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
daemon feeds that state into discovery and scheduling.

Generation comparison reports changed and removed datasets. Disabling a dataset
is visible in the next generation. The [lifecycle service](../snapshot-lifecycle.md)
provides cancellation/quiescence gates and preview/apply clean; wiring those
gates to live workers and reconstruction of sparse lifecycle state are provided
by the daemon. Namespace exclusion during receives is enforced by the transfer
engine.

## Lifecycle integration boundaries

The daemon wires activation transitions to the lifecycle gate, holds the same
lifecycle lock as standalone commands, and reconstructs sparse lifecycle state
after restart. Queued cancelled tickets are discarded; running management
operations finish before quiescence. The transfer engine supplies destination
GUID verification and just-in-time target probes before references can be
released for local and SSH targets. Standalone clean still fails closed where
live target coordination is unavailable until the control API can coordinate
with the daemon.

## Test-only properties and received layers

`org.boomerangz:clean-probe` and `org.boomerangz:state:probe` are disposable
VM fixtures, not supported configuration or production metadata. OpenZFS 2.4.3
retained hidden received values after plain inherit; `inherit -S` restored them.
No direct libzfs removal path is pursued. See the integration test notes.
