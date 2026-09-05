# Dataset discovery and policy

`boomerangz dataset list` and `boomerangz dataset inspect <dataset>` are read-only
commands that emit JSON. The `dataset` command accepts `--config` and
`--config-dir` with the same defaults as `config check`. An explicit inspection
includes inactive datasets. A policy error is reported in the dataset's
`policy.errors`; a failed inventory or query fails the command.

Only local public `org.boomerangz:*` properties participate in policy.
Children inherit locally configured ancestor values, and each value records
the ancestor that supplied it. Received public properties are retained under
`stored` for inspection but never activate a dataset or affect policy, even
after local promotion. Internal `org.boomerangz:state:*` metadata is kept out
of the policy map. Defaults have no supplying dataset; the default `raw` value
is computed per dataset from its encryption root.

`org.boomerangz:discard` accepts `none` (the default), `first` (receive `-d`),
and `all` (receive `-e`). Setting `none` locally masks an ancestor's choice.
Other properties and defaults are specified in [PLAN.md](../PLAN.md).
Unknown public keys, invalid toggles, malformed lists, unknown remotes, invalid
grids, and reserved receive-property targets invalidate the affected policy.
Generic `set_prop:*` and `ignore_prop:*` directives cannot target any
`org.boomerangz:*` property, including internal state.

Grids use positive integer counts and durations with `m`, `h`, `d`, or `w`:
`12x5m,24x1h,14x1d`. Durations must increase strictly. Parsing validates the
complete retention interval against overflow without allocating each window.
The first duration supplies cadence. Snapshot selection and pruning belong to
the next delivery phase.

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
Pending/resumable dataset names are supplied by the future recovery layer.

Generation comparison reports changed and removed datasets. Disabling a dataset
is visible in the next generation; worker cancellation, retained recovery
status, namespace exclusion during actual receives, and preview/apply cleanup
will be implemented in their respective lifecycle and transfer phases.
