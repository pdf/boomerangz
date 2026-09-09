# Dataset discovery and policy

`boomerangz dataset list` and `boomerangz dataset inspect <dataset>` are read-only
commands with readable output by default. Add `--json` for the stable structured
representation. The `dataset` command accepts `--config` and `--config-dir` with
the same defaults as `config check`. An explicit inspection includes inactive
datasets. Invalid policies show their errors in readable inspection output (or
in `policy.errors` with `--json`); a failed inventory or query fails the command.

Only local public `org.boomerangz:*` properties participate in policy.
Children inherit locally configured ancestor values, and each value records
the ancestor that supplied it. Received public properties are retained under
`stored` for inspection but never activate a dataset or affect policy, even
after local promotion. Internal `org.boomerangz:state:*` metadata is kept out
of the policy map. Defaults have no supplying dataset; the default `raw` value
is computed per dataset from its encryption root.

`org.boomerangz:discard` accepts `off` (the default), `first` (receive `-d`),
and `all` (receive `-e`). Setting `off` locally masks an ancestor's choice.
The complete property reference is below.
Unknown public keys, invalid toggles, malformed lists, unknown remotes, invalid
grids, and reserved receive-property targets invalidate the affected policy.
Generic `set_prop:*` and `ignore_prop:*` directives cannot target any
`org.boomerangz:*` property, including internal state.

Grids use positive integer counts and durations with the units below:
`12x5m,24x1h,14x1d`. Written order does not matter: the parser sorts tiers by
duration and combines equal durations by adding counts. Thus
`14x1d,12x5m,24x1h` has identical behavior, and `1x1h,2x60m` becomes `3x1h`.
Parsing validates the complete retention interval against overflow without
allocating each window. The smallest duration supplies cadence.

| Duration type | Short value | Fixed elapsed duration |
| --- | --- | --- |
| Minute | `m` | 60 seconds |
| Hour | `h` | 60 minutes |
| Day | `d` | 24 hours |
| Week | `w` | 7 days |
| Month | `mo` | 30 days |
| Year | `y` | 365 days |

These are elapsed-time intervals, not calendar boundaries. Months do not vary
with month length, and years do not adjust for leap years. Consequently `12mo`
is 360 days, not `1y`. Daylight-saving changes do not change interval lengths.
For example, `12x1mo,5x1y` gives twelve 30-day windows followed by five 365-day
windows. Unit suffixes are case-sensitive; seconds (`s`) are not supported yet.
Normalized output uses the largest unit that divides a duration exactly, so
`1x30d` becomes `1x1mo` and `1x365d` becomes `1x1y`.

Retention windows run from finer to coarser resolution, adjacent rather than
overlapping, anchored at the youngest owned snapshot. Each window keeps its
oldest contained owned snapshot. For example, `12x5m,24x1h` covers the first
hour with twelve five-minute windows, then the next 24 hours with hourly
windows. Users do not need to arrange tiers themselves.

## Property reference

Every name below has the `org.boomerangz:` prefix. Send/receive settings describe
transfer behavior; automatic transfers are not available in the current release.

| Suffix | Default | Values | Purpose |
| --- | --- | --- | --- |
| `enabled` | `off` | `on`, `off` | Opts a dataset into automatic management; local `off` masks inherited activation. Disabling preserves snapshots and recovery metadata during the configured inactive grace period. |
| `remote` | Unset | Comma-separated remote names | Selects destinations defined in global `[remotes.NAME]` tables. No remote is supplied automatically. |
| `local` | Unset | Comma-separated dataset names | Selects receive destinations on this host. |
| `policy` | `12x5m,24x1h,14x1d` | Grid of positive counts and durations (`m`, `h`, `d`, `w`, `mo`, `y`) | Determines snapshot cadence and retention windows, as described above. |
| `large_blocks` | `on` | `on`, `off` | Requests send `-L`, preserving large blocks instead of splitting them, subject to receiver feature support. |
| `compressed` | `on` | `on`, `off` | Requests send `-c`, transferring compressed blocks to reduce stream size. |
| `raw` | `on` for encrypted scopes; `off` otherwise | `on`, `off` | Requests send `-w`; encrypted data remains encrypted in transit without decrypting it for the send. Unspecified values adapt to encrypted replication descendants; explicit `off` is never silently overridden. Plaintext raw sends imply large-block, embedded-data and compressed-data behavior. |
| `props` | `off` | `on`, `off` | Requests send `-p` to carry dataset properties. Public boomerangz configuration is excluded from receives to avoid activating or rerouting backups. |
| `incremental` | `all` | `all`, `latest` | `all` uses send `-I` to include intermediate snapshots; `latest` uses `-i` to send directly between base and target. Both use a full send if no common base exists. `all` can include foreign intermediates. |
| `replicate` | `off` | `on`, `off` | Takes recursive snapshots and requests a replication package (`-R`). Root policy governs covered descendants, suppressing duplicate jobs. Packages can include foreign snapshots and native receive semantics can remove destination snapshots absent from the sender; this is an explicit opt-in, not automatic `-F`. |
| `discard` | `off` | `off`, `first`, `all` | Controls receive path mapping: `off` uses the specified destination, `first` (`-d`) appends the source path with its pool component removed, `all` (`-e`) appends only the final source component. |
| `set_prop:<name>` | Unset | Receive property value | Overrides a receive-side property using `-o name=value`; each dynamic key inherits independently. The resulting target property is not itself boomerangz metadata. |
| `ignore_prop:<name>` | Unset | `on`, `off` | `on` excludes an incoming property using `-x name`; `off` masks an inherited exclusion. A matching `set_prop` wins with a warning. |

Internal metadata is not user configuration and must not be edited to claim
ownership. Names alone never prove ownership; lineage, snapshot identity,
creation timestamp, and ZFS GUID must agree.

| Full name | Default | Values | Purpose |
| --- | --- | --- | --- |
| `org.boomerangz:state:lineage` | Unset until managed | UUID | UUID identifying a managed dataset lineage and associating snapshots with it. |
| `org.boomerangz:state:owner` | Unset until managed | Installation UUID | Explicitly local source-root owner. It must match `<identity_dir>/installation-id` before automatic mutation; received and inherited values are provenance only. |
| `org.boomerangz:state:snapshot` | Unset until managed | UUID | UUID identifying an individual managed snapshot. |
| `org.boomerangz:state:created` | Unset until managed | RFC3339Nano UTC timestamp | Snapshot creation timestamp in UTC, matching the managed snapshot name. |
| `org.boomerangz:state:inactive` | Unset while active | Versioned JSON identity and UTC timestamp | Explicitly local record of when an owned source root became inactive. It binds the grace-period start to the dataset GUID, lineage, and installation owner; reactivation removes it. |
| `org.boomerangz:state:target:<target-id>` | Unset until first transfer | Versioned JSON identity | Persistent source-root binding for canonical transport, receive mapping, destination pool GUID, and existing destination or ancestor GUID. Every local job revalidates it; name, GUID, or mapping changes require explicit rebind/reseed. |
| `org.boomerangz:state:reference:<target-id>:<snapshot-uuid>` | Unset until managed | JSON recovery proof | Local JSON recovery proof binding a non-secret canonical target identity, snapshot metadata and source GUID to a target hold and versioned bookmark. `target-id` is the SHA-256 hex digest of the target identity. Written before taking a hold; retained until both hold and bookmark are released. Never configure manually or populate with credentials. |

The remainder of `org.boomerangz:state:*` is reserved for internal state, not a
public extension mechanism.

### Hidden received values

Plain `zfs inherit` can mask a received user
property but does not erase its received value. `zfs inherit -S` can expose it
again. A query for all properties can omit such hidden names; an explicit query
for a known name can still show its received value. `dataset clean` reports this
limitation rather than claiming complete removal.
Received public values remain excluded from boomerangz policy regardless.


## Inspection results

The `inspected` field distinguishes complete policy inspection from sparse
inventory. `covered_by` identifies descendants governed by a replication root;
the root policy governs their jobs. Differing descendant settings produce warnings.
An encrypted descendant automatically selects effective raw mode when raw is
unspecified. Explicit local or inherited `raw=off` is an error for that scope.
Receive-property compatibility is checked across the whole replication package.

Failed scans preserve the previous complete generation. Policy changes become
visible after a successful scan; concurrent external ZFS edits may require the
next pass to converge. Received public values never affect effective policy.
