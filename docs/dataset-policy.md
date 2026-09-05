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
The complete property reference is below.
Unknown public keys, invalid toggles, malformed lists, unknown remotes, invalid
grids, and reserved receive-property targets invalidate the affected policy.
Generic `set_prop:*` and `ignore_prop:*` directives cannot target any
`org.boomerangz:*` property, including internal state.

Grids use positive integer counts and durations with `m`, `h`, `d`, or `w`:
`12x5m,24x1h,14x1d`. Written order does not matter: the parser sorts tiers by
duration and combines equal durations by adding counts. Thus
`14x1d,12x5m,24x1h` has identical behavior, and `1x1h,2x60m` becomes `3x1h`.
Parsing validates the complete retention interval against overflow without
allocating each window. The smallest duration supplies cadence.

Retention windows run from finer to coarser resolution, adjacent rather than
overlapping, anchored at the youngest owned snapshot. Each window keeps its
oldest contained owned snapshot. For example, `12x5m,24x1h` covers the first
hour with twelve five-minute windows, then the next 24 hours with hourly
windows. Users do not need to arrange tiers themselves.

## Property reference

Every name below has the `org.boomerangz:` prefix. Public policy is parsed now;
send/receive effects describe the contract for the later transfer phases.

| Suffix | Default / values | Purpose |
| --- | --- | --- |
| `enabled` | `off`; `on`/`off` | Opts a dataset into automatic management; local `off` masks inherited activation. Disabling preserves snapshots and recovery metadata, rather than cleaning them up. |
| `remote` | Unset; comma-separated names | Selects destinations defined in global `[remotes.NAME]` tables. No remote is supplied automatically. |
| `local` | Unset; comma-separated datasets | Selects receive destinations on this host. |
| `policy` | `12x5m,24x1h,14x1d` | Determines snapshot cadence and retention windows, as described above. |
| `large_blocks` | `on`; `on`/`off` | Requests send `-L`, preserving large blocks instead of splitting them, subject to receiver feature support. |
| `compressed` | `on`; `on`/`off` | Requests send `-c`, transferring compressed blocks to reduce stream size. |
| `raw` | Encryption-dependent; `on`/`off` | Requests send `-w`; encrypted data remains encrypted in transit without decrypting it for the send. Unspecified values adapt to encrypted replication descendants; explicit `off` is never silently overridden. Plaintext raw sends imply large-block, embedded-data and compressed-data behavior. |
| `props` | `off`; `on`/`off` | Requests send `-p` to carry dataset properties. Public boomerangz configuration is excluded from receives to avoid activating or rerouting backups. |
| `incremental` | `all`; `all`/`latest` | `all` uses send `-I` to include intermediate snapshots; `latest` uses `-i` to send directly between base and target. Both use a full send if no common base exists. `all` can include foreign intermediates. |
| `replicate` | `off`; `on`/`off` | Takes recursive snapshots and requests a replication package (`-R`). Root policy governs covered descendants, suppressing duplicate jobs. Packages can include foreign snapshots and native receive semantics can remove destination snapshots absent from the sender; this is an explicit opt-in, not automatic `-F`. |
| `discard` | `none`; `none`/`first`/`all` | Controls receive path mapping: `none` uses the specified destination, `first` (`-d`) appends the source path with its pool component removed, `all` (`-e`) appends only the final source component. |
| `set_prop:<name>` | Unset; property value | Overrides a receive-side property using `-o name=value`; each dynamic key inherits independently. The resulting target property is not itself boomerangz metadata. |
| `ignore_prop:<name>` | Unset; `on`/`off` | `on` excludes an incoming property using `-x name`; `off` masks an inherited exclusion. A matching `set_prop` wins with a warning. |

Internal metadata is not user configuration and must not be edited to claim
ownership. Names alone never prove ownership; lineage, snapshot identity,
creation timestamp, and ZFS GUID must agree.

| Full name | Purpose |
| --- | --- |
| `org.boomerangz:state:lineage` | UUID identifying a managed dataset lineage and associating snapshots with it. |
| `org.boomerangz:state:snapshot` | UUID identifying an individual managed snapshot. |
| `org.boomerangz:state:created` | Snapshot creation timestamp in UTC, matching the managed snapshot name. |
| `org.boomerangz:state:reference:<target-id>:<snapshot-uuid>` | Local JSON recovery proof binding a non-secret canonical target identity, snapshot metadata and source GUID to a target hold and versioned bookmark. `target-id` is the SHA-256 hex digest of the target identity. Written before taking a hold; retained until both hold and bookmark are released. Never configure manually or populate with credentials. |

The remainder of `org.boomerangz:state:*` is reserved for internal state, not a
public extension mechanism. `org.boomerangz:cleanup-probe` and
`org.boomerangz:state:probe` are disposable VM-test fixtures only, not supported
configuration or production metadata.

### Hidden received values

On the tested OpenZFS 2.4.3 guest, plain `zfs inherit` masks a received user
property but does not erase its received value. `zfs inherit -S` can expose it
again. A query for all properties can omit such hidden names; an explicit query
for a known name can still show its received value. Cleanup must report this
limitation, not claim complete removal. No direct libzfs removal path is planned.
Received public values remain excluded from boomerangz policy regardless.

## Discovery implementation

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
is visible in the next generation. The [lifecycle service](snapshot-lifecycle.md)
provides cancellation/quiescence gates and preview/apply cleanup; wiring those
gates to live workers and exposing reconstructed recovery status remain in the
daemon phase. Namespace exclusion during actual receives belongs to the transfer
phase.
