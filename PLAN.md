# boomerangz implementation plan

## 1. Purpose

`boomerangz` is a property-driven ZFS snapshot and send/receive manager designed
for intermittently connected systems as well as continuously connected hosts.
Its primary use cases include laptops and other roadwarrior systems whose
replication targets may be unavailable for long periods.

The design has four guiding principles:

1. Dataset policy belongs in ZFS user properties attached to the datasets being
   managed.
2. TOML configuration contains only remote definitions and genuinely global
   daemon configuration.
3. Interrupted and delayed transfers recover through ZFS holds, bookmarks, and
   receive resume tokens rather than requiring a continuously available target.
4. Foreign snapshots are not pruned or otherwise modified by default.

The program will be written for Go 1.26.x. The module path will be
`github.com/pdf/boomerangz` and the CLI will use
`github.com/alecthomas/kingpin/v2`.

## 2. Initial platform scope

The architecture should avoid unnecessary operating-system assumptions and aim
to support OpenZFS wherever Go and the required ZFS operations are available.
Initial packaging and integration support will be Linux-only, with Arch Linux
first.

An operating system is not considered supported until its real ZFS integration
suite passes. CachyOS is the preferred initial integration guest because it
ships `zfs-utils` and matching precompiled ZFS modules for its kernels, while
the first package remains an Arch-family package. Container deployment is out
of scope for the initial releases.

Local integration and destructive testing must never modify the development
host operating system. It runs natively inside disposable libvirt/QEMU/KVM
guests. Containers are not part of the initial test architecture; container
deployment and container-specific testing are deferred together.

The initial Arch package should provide:

- the `boomerangz` executable;
- a systemd service;
- systemd sysusers and tmpfiles definitions where appropriate;
- a protected default configuration file;
- configuration, credentials, identity, and runtime directories;
- shell completions and man pages.

### 2.1 Privilege and delegation model

The recommended deployment runs the main `boomerangz` daemon as a dedicated,
unprivileged system user. Administrators delegate only the required operations
on explicitly selected source and destination roots with `zfs allow`. SSH
destinations likewise use a non-root account with permissions delegated on the
destination root. Running the network-facing daemon as root, or granting it
`CAP_SYS_ADMIN`, is not a supported default.

Receives use `zfs receive -u` so backup datasets are not mounted as a side
effect. The daemon does not otherwise mount, unmount, share, or change the
mount namespace during normal operation.

Linux nevertheless needs explicit validation. OpenZFS documents `create`,
`destroy`, `snapshot`, `receive`, and `receive:append` as depending on the
`mount` permission, while Linux cannot delegate `mount`. Avoiding the actual
mount operation with `receive -u` does not establish that these authorization
checks can be satisfied by delegation alone.

Implementation therefore starts with a direct, delegated ZFS executor and no
privileged helper. An early integration matrix on supported Arch/OpenZFS
versions must exercise at least:

- snapshot creation and pruning;
- bookmark creation and destruction;
- hold and release;
- full, incremental, recursive, and resumable sends;
- full, incremental, recursive, and resumable receives using `-u`;
- receive-side property overrides and exclusions.

If those tests demonstrate that Linux requires elevation, add a separate
Linux-only helper behind the same executor interface. The helper must expose
typed, narrowly scoped operations over a private local channel; authenticate
the daemon peer; independently validate operation and dataset scope; accept no
arbitrary command, shell fragment, or ZFS flags; and have no network listener.
Its systemd unit should restrict its capability bounding set and sandbox it as
far as the required OpenZFS operations permit. `CAP_SYS_ADMIN` is the candidate
capability, but its sufficiency and effective authority must be verified before
packaging because it is broad and may bypass delegated ZFS checks.

Platforms that can delegate all required operations use the direct executor
without the Linux helper. Root execution remains a diagnostic or development
fallback, not the recommended deployment model.

## 3. Property contract

All public and internal properties use the `org.boomerangz` namespace. User
properties are inherited according to normal ZFS semantics unless noted below.

| Property | Default | Meaning |
| --- | --- | --- |
| `org.boomerangz:enabled=on\|off` | `off` | Enable management of a dataset. |
| `org.boomerangz:remote=name,...` | unset | Send to named remotes defined in TOML. |
| `org.boomerangz:local=dataset,...` | unset | Send to named local destination roots. |
| `org.boomerangz:policy=<grid>` | `12x5m,24x1h,14x1d` | Define snapshot cadence and retention. |
| `org.boomerangz:large_blocks=on\|off` | `on` | Request `zfs send -L`. |
| `org.boomerangz:compressed=on\|off` | `on` | Request `zfs send -c`. |
| `org.boomerangz:raw=on\|off` | encryption-dependent | Request `zfs send -w`. |
| `org.boomerangz:props=on\|off` | `off` | Request `zfs send -p`. |
| `org.boomerangz:incremental=all\|latest` | `all` | Choose `-I` or `-i` when a base exists. |
| `org.boomerangz:replicate=on\|off` | `off` | Request recursive replication with `zfs send -R`. |
| `org.boomerangz:set_prop:<name>=<value>` | unset | Apply receive-side `-o name=value`. |
| `org.boomerangz:ignore_prop:<name>=on\|off` | unset | Apply receive-side `-x name` when enabled. |
| `org.boomerangz:discard=none\|first\|all` | `none` | Discard no path components, the first component (`-d`), or all but the last component (`-e`) on receive. |

Comma-separated lists are trimmed, deduplicated, and rejected if they contain
empty entries. Unknown remote names invalidate only the affected dataset policy;
they do not prevent the daemon from managing unrelated datasets.

### 3.1 Property source and activation

Configuration and clean discovery read only locally set and received
`org.boomerangz:*` properties. `boomerangz` resolves inheritance itself, but
only locally configured public properties participate in effective policy.

A dataset becomes active only when `enabled=on` originates from a locally set
property, either on the dataset or an ancestor. A received `enabled=on` never
activates a destination by itself. This prevents a received backup from
automatically becoming another replication source.

Received public `org.boomerangz:*` properties never participate in effective
policy, even after a destination is locally activated. An administrator who
promotes a received dataset configures it locally. Internal
`org.boomerangz:state:*` properties are handled separately as ownership and
recovery metadata.

### 3.2 Receive-side namespace isolation

Public `org.boomerangz:*` configuration properties are ignored on receive by
default. This includes activation, targets, policy, send flags, destination
mapping, and dynamic `set_prop:*` and `ignore_prop:*` keys. It prevents a backup
from inheriting source routing or becoming a replication source after an
unrelated local configuration change.

When a stream contains properties because `props=on` or `replicate=on`, the
planner inventories the public `org.boomerangz:*` keys present across the send
scope and supplies an explicit receive-side `-x` for each key. OpenZFS has no
property-prefix form of `-x`. The VM compatibility matrix must verify whether
each supported OpenZFS release retains an overridden received value internally;
if it does, post-receive reconciliation masks that received value as well.
Plain `zfs inherit` can retain hidden received values that `inherit -S` exposes
again; no direct libzfs removal path is planned.
Policy evaluation ignores it in either case.

Internal `org.boomerangz:state:*` metadata is not covered by this default
exclusion. The receiver either accepts or reconstructs the minimum lineage and
snapshot metadata needed for ownership and recovery, then verifies it against
the received snapshot GUID.

Generic `set_prop:<name>` and `ignore_prop:<name>` directives may not target the
reserved `org.boomerangz:*` namespace. Target-side `boomerangz` configuration
must be set directly on that target, making activation and routing explicit.

### 3.3 Conflict handling

If `set_prop` and `ignore_prop` address the same receive property, reconciliation
emits a warning and `set_prop` wins. Only the corresponding `-o` argument is
generated.

`discard` is a single choice: `none`, `first`, or `all`. An explicit local
`discard=none` overrides an ancestor's discard setting.

Raw mode exposes both requested and effective send flags. In particular:

- raw sends of unencrypted datasets imply OpenZFS behaviours equivalent to
  large blocks, embedded data, and compressed data;
- encrypted recursive replication requires raw mode;
- when `raw` is unspecified, an encrypted descendant automatically selects raw
  mode for the replication root, subject to receive-property compatibility;
  inspection explains that selection and any implied flag changes;
- explicit local or inherited `raw=off` is never overridden automatically;
- receive-side encryption overrides incompatible with raw streams invalidate
  the job;
- conflicts caused by raw mode produce visible warnings and are never hidden.

### 3.4 Dynamic receive-property keys

`set_prop:<name>` and `ignore_prop:<name>` remain dynamic user-property keys.
A fixed key containing a delimited map would require escaping arbitrary ZFS
property values, could grow unnecessarily large, and would make the complete
map one inherited value. Dynamic keys allow each receive property to be
inherited, overridden, or disabled independently.

The explicit `set_prop` and `ignore_prop` components are retained instead of
shorter names such as `set` or `ignore`; the longer forms make their receive-
property purpose clear in `zfs get` output and administrative tooling.

### 3.5 Incremental modes

`incremental=all` uses `zfs send -I`, transferring every intermediary snapshot
between the selected base and target. `incremental=latest` uses `zfs send -i`,
transferring directly from the base to the target snapshot.

Both are incremental modes. When no valid common base exists, the first transfer
is necessarily a full send. Receive-token recovery uses `zfs send -t` and is
independent of this property.

Because `-I` includes every intermediary snapshot, it may also transmit foreign
snapshots between two `boomerangz` snapshots. Inspection and job planning must
warn when this will occur.

Repeated forced full sends are not a policy mode. Reseeding an existing target
will be an explicit administrative workflow with destination preflight and no
implicit destruction.

### 3.6 Recursive replication

`replicate=on` makes the enabled dataset a replication root:

- snapshots are taken recursively;
- the stream uses `zfs send -R`;
- covered descendants do not also receive duplicate independently scheduled
  jobs;
- descendant filesystems, clones, properties, and snapshots may be included,
  including foreign snapshots;
- differing descendant policy or target properties produce warnings because
  the replication-root policy governs the stream.

OpenZFS replication-package receive semantics can remove destination snapshots
that are absent from the sender. Therefore `replicate=on` is an explicit opt-in
to native replication semantics and is an exception to default foreign-snapshot
coexistence. Its exact behaviour must be tested across every supported OpenZFS
version before this property is released as stable. `boomerangz` will not add
receive-side `-F` automatically.

## 4. Grid policy

The policy syntax follows the central idea of zrepl's grid policy:

```text
policy   = bucket ("," bucket)*
bucket   = count "x" duration
count    = positive integer
duration = positive integer followed by m, h, d, w, mo, or y
```

For example:

```text
12x5m,24x1h,14x1d
```

The smallest duration determines snapshot cadence, so the default creates a
snapshot every five minutes. The grid then retains approximately twelve
five-minute snapshots, twenty-four hourly representatives, and fourteen daily
representatives.

Units are fixed elapsed durations: minute (`m`, 60 seconds), hour (`h`, 60
minutes), day (`d`, 24 hours), week (`w`, 7 days), month (`mo`, 30 days), and year
(`y`, 365 days). They do not use calendar arithmetic, varying month lengths,
leap-year adjustments, or daylight-saving transitions. Twelve months are 360
days, not one year. Seconds are not currently accepted. Canonical output uses
the largest exactly dividing unit.

Policy rules are:

1. Written tier order is irrelevant: normalize by increasing duration and merge
   equal-duration tiers by adding their counts (including equivalent units).
2. Buckets are adjacent, left-inclusive, and right-exclusive.
3. The grid is positioned relative to the youngest owned snapshot.
4. Each bucket retains its oldest contained snapshot.
5. Owned snapshots older than the complete grid are eligible for pruning.
6. Foreign snapshots never participate in the grid.
7. Held snapshots and snapshots needed by a receive resume token are exempt.
8. Missed schedules are not backfilled after downtime.
9. At startup, a snapshot is due only when the newest owned snapshot is older
   than the inferred cadence.
10. A policy change takes effect on the next successful reconciliation.

The source and each receive destination apply the grid independently. Remote
pruning waits until the remote is reachable.

## 5. Snapshot ownership and ZFS-native state

Replication correctness must be reconstructable from ZFS. The initial design
does not require a dedicated state dataset or an embedded replication-state
database.

### 5.1 Lineage

On first management, `boomerangz` generates a cryptographically random lineage
UUID and records both the lineage and the responsible installation locally on
the source dataset or replication root:

```text
org.boomerangz:state:lineage=<uuid>
org.boomerangz:state:owner=<installation-uuid>
```

Created snapshots carry internal metadata such as:

```text
org.boomerangz:state:lineage=<uuid>
org.boomerangz:state:snapshot=<uuid>
org.boomerangz:state:created=<RFC3339Nano>
```

Snapshot names use an identifiable prefix plus a UTC timestamp and collision
suffix, for example:

```text
pool/data@boomerangz-20260904T143052.123456789Z-a1b2c3d4
```

Deletion requires all of the following:

- a `boomerangz` snapshot name;
- valid internal metadata;
- a lineage matching the managed dataset;
- eligibility under the grid;
- no hold or other ZFS dependency preventing deletion.

Names alone are never proof of ownership.

### 5.2 Lineage authority

Each installation has a cryptographically random, non-secret UUID stored at
`/var/lib/boomerangz/identity/installation-id` by default. The path is
configurable with the rest of the identity directory. It is independent of TLS
keys, tokens, host names, and `/etc/machine-id`. The daemon creates it atomically
when absent; packages never supply or overwrite it. Only the explicit identity
recovery workflow may replace an existing value.

The daemon reconstructs its responsibility set on every complete discovery
generation. A lineage is actionable only when all of the following hold:

- the source dataset or replication root is activated by locally configured
  public policy;
- `org.boomerangz:state:lineage` is explicitly local on that exact source root;
- `org.boomerangz:state:owner` is explicitly local on that exact source root
  and matches this installation's UUID; and
- the lineage metadata is internally consistent with the snapshots and
  recovery references the proposed operation would touch.

Received or inherited owner and lineage values are provenance, not authority.
A descendant covered by a replication root is governed by that root's matching
owner and lineage; it does not acquire independent authority through
inheritance. A valid but non-matching owner is reported as a dormant foreign
lineage. `boomerangz` performs no snapshot creation, pruning, transfer,
receive-property reconciliation, or recovery-state mutation for it.

The local ZFS property source by itself is deliberately insufficient. Moving a
pool and its disks to another host preserves locally set properties, whereas the
new host has a different installation UUID. Consequently stale `local` target
names cannot cause work on the new host before an explicit administrative
decision.

There is no separate database of assigned lineages. The installation UUID plus
the owner markers on source roots form the persistent authority record; the
daemon's responsibility set is a derived, immutable in-memory view. The local
lifecycle lock prevents two daemon processes from acting as the same
installation. Simultaneous management of the same writable datasets by
different installations is unsupported and owner mismatch fails closed.

### 5.3 Identity recovery and adoption

The owner UUID is repeated on every independently managed source root. If the
identity directory is lost but the original pools remain, `boomerangz identity
recover` can inventory those local owner markers and restore the selected UUID
to the identity file. It is preview-first and requires `--apply`; it refuses
automatic recovery when locally activated roots contain no owner, multiple
owners, invalid values, or conflicting lineage evidence. Recovery is an
operator assertion that this is a continuation of the same installation, not a
pool transfer.

`boomerangz dataset adopt` is the separate pool-transfer and promotion
workflow. After showing the existing lineage, old owner, effective policy, and
every local and remote target, `--apply` changes the source-root owner to the
current installation while preserving a consistent lineage. The preview emits
a prominent warning that transferred policy may name destinations which are
absent, unrelated, or inappropriate on the adopting host. It displays, for each
target, the configured name, transport, canonical endpoint, destination root or
dataset mapping, stored identity binding, current resolved identity, and
verification status.

The target review is part of every adoption preview. Interactive `--apply` asks
for final confirmation after displaying that preview; in non-interactive use,
`--apply` itself is the explicit confirmation and no second acknowledgement
flag is required. Local target preflight includes pool and dataset GUIDs rather
than trusting names alone. A verified identity or mapping mismatch blocks
adoption. An unreachable remote may remain configured, but is marked unverified
and suspended after adoption until its identity and destination are successfully
revalidated; it cannot receive queued work merely because it becomes reachable.
Adoption refuses ambiguous lineages and does not queue work until the owner
change commits and each target is independently eligible. A missing
dataset-level lineage may be recovered from owned snapshots only when exactly
one candidate lineage is present.

On startup, an absent installation-ID file causes a new UUID to be generated,
but any roots carrying another local owner remain dormant. `identity recover`
may replace that fresh UUID only while it owns no lineage and no work is active;
otherwise it fails closed. This keeps first startup safe on both a restored
system disk and a new host receiving physically transferred pool disks.

If neither recovery nor adoption has been explicitly completed, the daemon
leaves the lineage dormant. This makes loss of `/var/lib/boomerangz/identity`
and physical pool migration safe by default.

### 5.4 Replication cursors and interrupted transfers

For every source-target pair:

- a target-specific bookmark records the most recently verified replication
  point;
- a target-specific hold protects the exact source snapshot required by an
  active or resumable transfer;
- the destination's `receive_resume_token` records interrupted receive state;
- bookmark and hold names include a deterministic target identifier;
- successful receive is confirmed by snapshot GUID before advancing the
  bookmark or releasing the hold.

Before its first transfer, each configured target receives a persistent binding
on the source root:

```text
org.boomerangz:state:target:<target-id>=<versioned JSON identity>
```

The binding records the canonical transport identity and destination mapping.
For a local target it includes the destination pool GUID, the GUID of the
existing destination root or nearest existing ancestor, and the intended
relative dataset path. Every planned job re-resolves and verifies this binding;
a missing target, a same-named replacement with another GUID, or a changed
mapping is blocked rather than treated as a new destination. Rebinding or
reseeding is an explicit preview-first administrative operation. Thus target
safety does not depend solely on detecting whether the physical host changed.

Recovery proofs use local
`org.boomerangz:state:reference:<target-id>:<snapshot-uuid>` JSON properties on
the source root, recording the non-secret canonical target identity, source GUID,
and snapshot metadata. Target IDs are SHA-256 hex digests. Holds use
`boomerangz-<target-id>` and bookmarks use
`boomerangz-<target-id>-<snapshot-uuid>`. Versioned bookmarks avoid a destructive
replace-before-create gap. Old proofs remain until their references are released;
name prefixes alone never authorize release or destruction.

When a target is unavailable, pending work is coalesced to the newest eligible
snapshot instead of holding every scheduled snapshot. If a receive resume token
exists, the exact source snapshot required by that token remains held until
resume succeeds or the receive is explicitly abandoned.

On reconnection:

1. Probe the destination and query its resume token.
2. Resume with `zfs send -t` when possible.
3. Verify the completed snapshot GUID.
4. Advance the target bookmark.
5. Release obsolete `boomerangz` holds and bookmarks.
6. Send the newest coalesced pending snapshot if another update is due.

Transient progress and process IDs remain in memory. Errors are retained in
structured logs. After restart, the daemon reconstructs pending work from
properties, snapshots, bookmarks, holds, and destination resume tokens.

### 5.5 Deactivation, delayed retirement, and explicit clean

An active dataset becomes inactive when its resolved locally configured
`enabled` value changes away from `on`. Removing a dataset's local property does
not deactivate it if a locally configured ancestor still supplies `enabled=on`;
an explicit local `enabled=off` masks that ancestor.

On an active-to-inactive transition, `boomerangz`:

- stops scheduling snapshots, pruning, transfers, and property reconciliation;
- removes work that has not started from all worker-pool queues;
- requests cancellation of active transfers and preserves any resulting
  resumable receive state;
- allows already executing short ZFS management operations to finish, then
  records their reconstructed result; and
- exposes the dataset as disabled with retained recovery state in status output.

Immediate deactivation does not destroy snapshots or bookmarks, release holds,
abort receive resume tokens, or clear properties. This makes a temporary disable
reversible and prevents a configuration edit from silently removing the only
viable recovery path.

The daemon records the first observed inactive time durably on an owned source
root and schedules automatic retirement after `inactive_grace_period`, which
defaults to 24 hours. Zero disables automatic retirement. The exact, locally set
`org.boomerangz:state:inactive` marker is versioned and binds its UTC timestamp
to the dataset GUID, lineage, and owner; inherited or received copies are never
authority. If the daemon did not observe the transition, the grace period begins
on the first later discovery of the inactive owned root; it never infers an
earlier timestamp. Reactivation before the deadline clears the marker and
cancels retirement. Reducing the configured interval may make an existing
inactive root immediately eligible, so status shows the recorded time, deadline,
planned effects, and blockers.

After the grace period, automatic retirement uses the same ownership, GUID,
quiescence, target-identity, and resume-state proofs as explicit clean. It:

1. destroys only proven boomerangz-owned source snapshots and proven owned
   replica snapshots within recorded target bindings;
2. releases only proven holds and bookmarks and clears obsolete internal
   recovery metadata after every target dependency is resolved;
3. preserves locally configured public policy, especially an `enabled=off`
   value needed to mask an active ancestor; and
4. never destroys a live dataset, foreign snapshot, unknown reference, clone,
   or interrupted receive.

Remote target retirement is probed just in time. An unavailable target, active
resume token, or ambiguous ownership leaves retirement pending with bounded
retry and visible status; it never causes source proof to be discarded. This
means automatic retirement reclaims safely attributable snapshot space without
turning a temporary network outage into destructive abandonment.

`boomerangz dataset clean` is the explicit decommissioning workflow. It
accepts exact dataset scopes, `--recursive`, or an explicit `--all`; defaults to
a read-only preview; and requires `--apply` before changing ZFS state. It
coordinates with the daemon when one is running so the selected scope is
quiescent before cleaning. Unlike delayed retirement, explicit clean may clear
the selected public configuration and remains the immediate operator-controlled
path.

When the configured control socket exists, explicit clean runs through the
daemon so live work can be quiesced and targets verified. Standalone clean uses
the same just-in-time configured-target verifier.
Standalone applies share an exclusive `<socket_path>.lifecycle.lock`; the daemon
holds that same lock for its lifetime before accepting work.

For each selected local dataset clean:

1. inventories locally set and received `org.boomerangz:*` properties on the
   dataset and its selected descendants and snapshots;
2. identifies holds and bookmarks by both the `boomerangz` naming contract and
   matching lineage/target metadata;
3. reports active jobs, inaccessible targets, conflicting lineages, and receive
   resume tokens as blockers;
4. inherits public and internal `org.boomerangz:*` properties at the selected
   scope and releases or destroys only holds and bookmarks whose ownership is
   proven; and
5. leaves snapshots and their data intact by default, after clearing their
   effective internal ownership metadata, so they become foreign snapshots.
   Hidden received metadata may remain and can be restored externally; clean
   must report this limitation and must not promise irreversible erasure.

An additional `--destroy-owned-snapshots` option may delete only snapshots that
pass the complete ownership proof and have no holds, clones, resume dependency,
or other ZFS blocker. It is never implied by `--all` and is separately visible
in the preview.

Clean never reverts or clears non-`org.boomerangz:*` properties previously
applied through `set_prop`; their prior values are unknown and they may now be
intentional target configuration. It also never aborts a destination resume
token implicitly. An interrupted receive must first be resumed or explicitly
abandoned through the separately guarded administrative workflow. Offline or
unreachable targets are reported as incomplete and must be cleaned on their
own host; package removal does not run destructive clean automatically.

## 6. Efficient dataset discovery

Discovery avoids per-dataset subprocesses and excludes snapshots from the
normal configuration walk.

### 6.1 Global sparse inventory

At startup and each periodic reconciliation, run:

```sh
zfs list -H -p -t filesystem,volume -o name,type,encryptionroot
```

Then retrieve only explicitly stored activation properties:

```sh
zfs get -H -p \
  -s local,received \
  -t filesystem,volume \
  -o name,property,value,source \
  org.boomerangz:enabled
```

Construct an inspection set containing:

- active datasets;
- their ancestors, needed for inheritance;
- descendants covered by active replication roots;
- datasets involved in pending or resumable work.

### 6.2 Property retrieval

For the inspection set, retrieve explicitly stored properties in bounded
argument batches:

```sh
zfs get -H -p \
  -s local,received \
  -t filesystem,volume \
  -o name,property,value,source \
  all \
  <dataset...>
```

Rows are immediately filtered to `org.boomerangz:*`. Requesting `all` is
necessary to discover dynamic `set_prop:*` and `ignore_prop:*` names because
OpenZFS provides no documented property-prefix query.

The in-memory resolver walks parents before children:

1. Start with application defaults.
2. Copy the parent's effective property map.
3. Retain received properties and internal metadata separately for inspection;
   they do not participate in public policy.
4. Apply locally configured public properties defined on the dataset.
5. Parse and validate the effective policy.

The daemon never requests inherited `boomerangz` rows from ZFS.

Source filtering does not apply universally. Native and read-only operational
properties such as GUID, creation time, encryption state, and
`receive_resume_token` are requested as effective values without the
`local,received` filter or obtained through `zfs list`.

### 6.3 Reconciliation cache

Each complete discovery pass creates an immutable generation containing the
dataset tree and effective policies. It is published atomically. A failed or
partial scan never replaces the last complete generation.

The daemon compares generations and enqueues policy/recovery work for changed
or newly recoverable datasets. Global discovery runs through a single
coordinator, never concurrently, at a default interval of 60 seconds.
Explicit reconciliation requests are coalesced. Changes made by `boomerangz`
update or invalidate the affected cache entry immediately.

Local reconciliation does not routinely probe remote targets. A direct SSH
target is inspected just in time when work for it becomes due, during its
bounded reconnect/retry sequence, or after an explicit remote reconciliation
request. This avoids connections whose results would go stale before infrequent
snapshot schedules need them. An optional remote `boomerangz ssh-shell` may
serve an already validated local cache, but transfer-critical state is always
revalidated by the receiving endpoint before it accepts a stream.

Reconciliation and snapshot scheduling use independent timers. The snapshot
scheduler consumes the latest immutable policy generation and maintains per-root
deadlines; its required resolution is inferred from the minimum cadence among
active, valid, independently scheduled policies. With no active policies it need
not tick. A new generation updates deadlines without forcing a global scan on
each snapshot wakeup. Snapshot completion updates the affected deadline; missed
snapshots coalesce rather than backfill. Future second-resolution policies must
not force one-second discovery scans. Second units are not enabled by this design
change. Scheduler and worker integration are provided by the daemon runtime.

The current reconciliation loop uses a ticker and runs each scan and report
callback synchronously. When a pass overruns the configured interval, a pending
tick can start another pass immediately after completion. Missed ticks are
coalesced/dropped, not queued without bound; scans never overlap. The interval
is neither a timeout nor a mandatory pause after completion. Sustained overruns
can keep discovery continuously busy. Explicit requests coalesce independently
into one pending request, which can cause an additional follow-up pass.

Prefer JSON output when capability detection confirms `zfs get -j`; otherwise
parse `-H -p` tabular output. Both parsers are streaming, bounded, and
fuzz-tested. ZFS events may later provide reconciliation hints, but periodic
discovery remains the portable source of truth.

Snapshot, bookmark, and hold inventories are targeted and requested only for a
dataset that is due, being pruned, preparing a transfer, or recovering work.
Remote discovery is limited to deterministic destination paths.

## 7. Daemon execution model

The default deployment runs `boomerangz daemon` as the dedicated unprivileged
service user, in the foreground under systemd. It handles scheduling internally
and shuts down gracefully on `SIGTERM`. All ZFS execution goes through an
interface that supports direct delegated execution and, if Linux integration
testing proves it necessary, a privileged-helper backend.

There are three independent bounded worker pools: management, local transfers,
and remote transfers.

### 7.1 Management workers

Management workers perform:

- effective-policy reconciliation;
- snapshot creation;
- bookmark and hold maintenance;
- source and destination pruning;
- target probing;
- post-transfer verification.

They use per-dataset keyed locks and a bounded, deduplicating task queue. Global
discovery itself remains owned by the single discovery coordinator.
`management_workers=0` (the default) resolves to the logical CPU count available
to the process. Positive values set an explicit bound; negative values are invalid.

### 7.2 Transfer workers

Transfer workers perform:

- send-size estimation;
- sender and receiver process startup;
- byte-counting stream transfer;
- cancellation and cleanup;
- resume streams.

Same-host transfers use `local_transfer_workers` (default 2); network transfers
use `remote_transfer_workers` (default 1 across all remotes). Each accepts any
positive integer, including 1. Both classes retain per-destination serialization
and fair scheduling across source datasets. They do not borrow each other's
capacity. Setting both to 1 permits one local and one remote stream concurrently;
these limits are not bandwidth-rate controls. They replace `transfer_workers`.

Jobs waiting for capacity remain visible, with distinct states for management
and transfer pressure:

```text
scheduled
pending-management
snapshotting
pending-transfer
probing
estimating
sending
verifying
succeeded
waiting-retry
resumable
blocked
failed
```

Status includes the reason for pending state and, when stable enough to report,
queue position.

## 8. Transfer pipeline and progress

Commands are constructed as argument vectors with `os/exec`; streams never pass
through a shell.

The local and SSH pipelines are conceptually:

```text
zfs send -> bounded Go copy/count loop -> zfs receive
zfs send -> bounded Go copy/count loop -> ssh host zfs receive
```

Eligible destinations for a snapshot fan out concurrently through the transfer
pool. One unavailable target does not delay another target.

Before transfer, `zfs send -nP` is used where supported to estimate stream size.
The copy loop records bytes transferred, rate, and ETA. Unknown or unavailable
estimates are represented explicitly rather than guessed.

SSH is the default v1 replication transport. It uses batch mode, strict host-key
verification, argument-safe remote commands, and configurable connection
settings. Password handling and arbitrary shell snippets are not supported.

Two SSH endpoint modes are supported:

- direct SSH invokes a fixed, implementation-owned set of ZFS operations and
  requires no `boomerangz` installation on the destination;
- the optional `boomerangz ssh-shell` carries versioned gRPC services over the
  command channel's stdin and stdout for probe, receive, resume-token,
  verification, and reconciliation operations.

One SSH command channel is a private ordered duplex byte stream for the life of
a replication attempt. A small `net.Conn` adapter carries gRPC/HTTP2 over that
stream; stderr remains a separate bounded diagnostic channel. The inner gRPC
connection does not add TLS because the strictly verified SSH session already
provides transport encryption and peer authentication. It never opens a remote
TCP listener or relies on SSH port forwarding.

SSH shell and the future native transport use the same protobuf service and
message definitions, handlers, capability negotiation, and bounded streaming
RPCs. Native mode adds its configured TLS and client authorization at the
network listener. If a remote daemon is available, SSH shell may obtain
non-authoritative inventory from its validated in-memory cache. SSH shell also
works without a daemon by inspecting local ZFS state directly. Direct SSH/ZFS
remains a first-class compatibility path when the remote binary is absent.

Endpoint selection and fallback policy are explicit configuration. Automatic
capability discovery may fall back from an unavailable SSH shell to direct ZFS
only when configuration permits it; a required SSH shell never silently
downgrades. Status reports the active endpoint mode. Cached discovery is never
proof of destination identity, cursor state, or receive readiness: the remote
side rechecks those preconditions immediately before receiving data.

Document a recommended restricted SSH deployment using a dedicated account,
key restrictions, disabled forwarding and PTY features, delegated permissions
limited to configured destination roots, and an authorized-key forced command
for `boomerangz ssh-shell`. Also document direct SSH/ZFS without requiring a
remote installation. Do not recommend restricted shells or ad-hoc parsing of
`SSH_ORIGINAL_COMMAND` as a security boundary; OpenSSH key restrictions and ZFS
delegation reduce exposure but cannot enforce a ZFS-only command vocabulary
without an audited dispatcher.

ZFS data uses a bounded client-streaming or bidirectional-streaming RPC with
gRPC flow control. Benchmark its framing and copying overhead against direct
SSH/ZFS, but do not introduce a separate raw SSH-shell stream that could diverge
from the native endpoint contract.

## 9. Control API and authentication

The local control API uses versioned gRPC services over a Unix-domain socket.
The same API may optionally listen on TCP for remote status and control.

Initial services include:

```text
StatusService.GetStatus
StatusService.WatchStatus
StatusService.ListDatasets
ControlService.Trigger
ControlService.Reconcile
```

A future native transport exposes the same protobuf probe, receive,
resume-token, verification, reconciliation, and prune services as SSH shell.

### 9.1 Listener security

The Unix socket relies on filesystem permissions and local peer credentials.
TCP is disabled by default and never supports an unauthenticated mode. Available
TCP authentication modes are:

- `token`: verified TLS server identity plus a scoped token;
- `mtls`: server and client certificate authentication;
- `mtls+token`: both mechanisms when desired.

### 9.2 Token pairing

`token` mode provides a homelab-friendly, one-bundle setup without requiring the
user to operate a PKI.

Server verification is independent of client authorization. Support normal
CA-chain and hostname verification using system roots or an explicitly configured
private CA, as well as optional public-key pinning for self-managed identities.
Never skip verification or silently fall back between trust modes. A public CA
server certificate does not supply mTLS client identities.

The server may generate its identity or load externally managed certificate and
key files, including certificates renewed by Let's Encrypt/ACME tooling. Reload
the pair atomically after renewal; report failed reloads and retain the previous
identity without bypassing expiry validation. Built-in ACME issuance is not
required. Define trust and reload configuration in the TCP phase.

Creating a token emits a one-time pairing bundle containing:

- endpoint;
- explicit server trust mode: CA source and expected server name, or public-key pin;
- token identifier;
- at least 256 bits of random token secret;
- authorised scopes.

The client imports the bundle before connecting. It verifies the server using
the selected trust mode before sending the token as gRPC call credentials.
CA verification allows certificate and private-key renewal without re-pairing
while the expected name and trusted chain remain valid. Optional public-key
pinning allows renewal with the same key, but changing that key requires an
explicit trust update or re-pairing.

Issue one token per client by default so clients can have separate scopes and be
revoked independently. A shared token remains possible. Suggested scopes are
`status`, `trigger`, `replicate`, `prune`, and `admin`.

The server stores only token identifiers, verifiers, scopes, and optional expiry
metadata. Tokens are never placed in URLs or logs. Multiple valid tokens permit
rotation. Loss or rotation of a pinned server identity key requires affected
clients to update their trust or pair again; CA-verified clients are not tied to
that individual key.

## 10. Configuration and filesystem layout

The primary configuration paths are:

```text
/etc/boomerangz/config.toml
/etc/boomerangz/config.d/*.toml
```

`config.toml` is loaded first. Drop-ins are loaded in bytewise lexical filename
order. Tables merge recursively and later scalar values override earlier
values. Actual TOML arrays replace earlier arrays.

Extensible collections use keyed tables rather than arrays, allowing individual
drop-ins to add entries naturally. No remote is supplied by default; this is a
placeholder example requiring user-selected connection and destination details:

```toml
[remotes.home]
transport = "ssh"
host = "home.example.net"
root = "tank/backups"
```

Later files may override individual fields of a keyed entry. Examples of keyed
collections include `[remotes.<name>]` and `[listeners.<name>]`.

Other default paths are:

```text
/etc/boomerangz/credentials.d/
/var/lib/boomerangz/identity/
/var/lib/boomerangz/identity/installation-id
/run/boomerangz/boomerangz.sock
```

The package creates these directories with restrictive ownership and modes.
The daemon generates the installation ID and key material atomically on first
use; packages never ship or overwrite either. Paths are user-configurable.
Imported remote token bundles live in `credentials.d`; the installation ID,
server identity, and token verifiers live under the identity directory.

The Arch package treats `config.toml` as a protected configuration file so
upgrades produce normal `.pacnew` handling instead of overwriting local changes.

`boomerangz config check` validates every source and the merged result.
`boomerangz config show` displays effective configuration with secrets redacted
and source-file provenance for each value.

The complete TOML schema will be reviewed separately before implementation.

## 11. CLI and output

The initial CLI shape is:

```text
boomerangz daemon
boomerangz status [-w|--watch] [-i|--interval 2s]
boomerangz dataset list
boomerangz dataset inspect <dataset>
boomerangz dataset adopt [--apply] <dataset>
boomerangz dataset clean [--recursive] [--all] [--apply]
                          [--destroy-owned-snapshots] [<dataset>...]
boomerangz identity recover [--owner <installation-uuid>] [--apply]
boomerangz config check
boomerangz config show
boomerangz trigger [<dataset>...]
boomerangz auth token create
boomerangz auth token import
boomerangz auth token list
boomerangz auth token revoke
boomerangz target reseed <dataset> <target>
boomerangz version
```

Logging uses `log/slog`, defaults to `info`, and goes to stderr:

- interactive stderr uses `slog.TextHandler`;
- non-interactive stderr uses `slog.JSONHandler`.

Status output goes to stdout:

- an interactive one-shot status uses a human-readable table;
- interactive watch mode redraws a stable terminal display with progress bars;
- non-interactive one-shot status emits one JSON object;
- non-interactive watch mode emits newline-delimited JSON at `--interval`,
  defaulting to two seconds.

Watch mode handles terminal resize and degrades cleanly when the terminal is too
narrow for progress bars.

## 12. Repository layout

No Makefile or mandatory task runner is planned. CI and documentation use normal
`go` and `go tool` commands directly.

```text
api/
    boomerangz/v1/
cmd/
    boomerangz/
internal/
    cli/
    config/
    control/
    daemon/
    logging/
    model/
    policy/
    properties/
    replication/
        ssh/
        native/
    scheduler/
    snapshot/
    statusui/
    testutil/
        commandtest/
        zfstest/
    zfs/
.github/
    workflows/
.golangci.yml
go.mod
README.md
```

Packages should be introduced as functionality requires them rather than
pre-created empty. Interfaces belong at consumer boundaries, especially around
the ZFS command runner, clocks, remote transports, and filesystem/process
integration.

Integration tests remain colocated with relevant packages under an `integration`
build tag. Shared disposable-pool helpers live in
`internal/testutil/zfstest`; no separate top-level integration tree is needed.

## 13. Toolchain, CI, and quality

The module declares Go 1.26 language semantics and pins the supported Go 1.26
patch toolchain. Development tools are declared with `tool` directives in
`go.mod` and invoked through `go tool`, including golangci-lint, Buf, and the
gRPC code-generation plugins. Buf owns protobuf formatting, linting, breaking
change checks, and code generation through checked-in configuration.

The golangci-lint configuration uses schema version 2, begins with a sensible
standard set, and adds focused correctness checks rather than every available
stylistic linter.

GitHub Actions should run at least:

```sh
go test ./...
go test -race ./...
go vet ./...
go tool golangci-lint run
go tool buf format --diff --exit-code
go tool buf lint
go tool buf generate
```

Generated gRPC API files are committed. CI regenerates them and fails if the
working tree changes.

### 13.1 Test strategy

Unit tests cover policy resolution, command construction, scheduling, pruning,
deactivation and clean planning, lineage authority and adoption, ownership
proofs, destination mapping, authentication scopes, and configuration merging.

Fuzz tests cover:

- grid-policy parsing and arithmetic;
- ZFS JSON and tabular output parsing;
- property names and values;
- comma-separated target lists;
- snapshot, bookmark, and hold naming;
- source-to-destination mapping;
- status protocol decoding;
- malformed command output and resume tokens.

Command execution tests use helper processes rather than shell scripts wherever
practical.

All tests that execute real `zfs` or `zpool` commands run inside disposable
libvirt/QEMU/KVM guests. The host-side harness may perform read-only prerequisite
checks and create ordinary user-owned VM images, overlays, sockets, logs, and
test artifacts. It must not:

- execute host `zfs` or `zpool` commands;
- load or unload host kernel modules;
- install or remove host packages;
- alter host services, users, groups, capabilities, mounts, or firewall rules;
- create persistent system or session libvirt domains, networks, storage pools,
  or secrets;
- pass the host's `/dev/zfs`, ZFS block devices, or existing pools into a
  guest.

Prefer transient domains on `qemu:///session`, copy-on-write overlays backed by
a read-only CachyOS base image, QEMU user-mode networking, and dedicated virtual
scratch disks. Every run receives unique names and identifiers. Guest shutdown
deletes the overlays and scratch disks while leaving the reusable base image
unchanged.

The in-guest harness creates disposable pools only on virtual disks carrying
expected test serial numbers. Before any destructive command it verifies a
harness-injected guest marker, the exact pool-name prefix, and every vdev path.
A missing or mismatched guard aborts the test rather than attempting cleanup.

Tests run directly in the guests. A single guest covers local replication,
delegation, pruning, recovery, systemd, packaging, mount-namespace, and
capability behaviour. Two independent guests cover SSH and future native
transport, source and destination isolation, target outages, and reconnection.
Different read-only base images provide the kernel and OpenZFS version matrix.
The harness uses ephemeral user-space networking that requires no persistent or
privileged host network configuration.

Container tests may be added later when container deployment or OpenZFS
container-specific features become supported. They are not used merely to
simulate multiple nodes because containers would share the guest kernel module
and add a namespace layer without improving the relevant coverage.

Fault-injection coverage terminates the sender, receiver, SSH process, and daemon
at controlled points. It verifies resume-token recovery, hold preservation,
bookmark advancement, restart reconciliation, and source/destination pruning.
VM integration tests also verify public-property exclusion, local and received
property-layer cleanup, active-to-inactive transitions, preview/apply parity,
identity loss and recovery, physical-pool-transfer owner mismatch, adoption
target display and confirmation, offline-target suspension, target preflight,
and refusal to clean ambiguous or resume-dependent state.

## 14. Delivery phases

1. **Specification and scaffold**: finalize the TOML schema, minimum OpenZFS
   capabilities, module, CI, lint, package boundaries, and ZFS executor
   interface. Build the isolated VM harness and run the delegated-operation
   integration spike in a CachyOS guest before committing to a helper protocol.
2. **Discovery and policy**: implement sparse dataset discovery, inheritance,
   grid parsing, effective-policy inspection, and immutable generations.
3. **Snapshot lifecycle**: implement lineage, naming, recursive and non-recursive
   snapshots, grid pruning, holds, bookmarks, adoption, deactivation, and
   preview-first clean.
4. **Local transfer**: implement full bootstrap, `-i`, `-I`, progress,
   receive-property handling, and GUID verification.
5. **SSH and roadwarrior recovery**: implement JIT direct-ZFS probing, the
   optional versioned SSH-shell endpoint, explicit capability fallback, retry
   with jitter, resume tokens, pending coalescing, offline reconciliation,
   durable inactive grace state, and ownership-safe retirement planning.
6. **Daemon and workers**: implement internal scheduling, independent bounded
   worker pools, fairness, execution of due retirement plans, graceful shutdown,
   and systemd integration.
7. **Control plane and UI**: implement gRPC over Unix sockets, status/watch,
   terminal progress, token pairing, and optional secured TCP listeners.
8. **Hardening**: run destructive integration and fault tests, document
   delegated permissions, and implement and harden the Linux helper only if the
   integration matrix requires it.
9. **Native transport**: carry the shared remote endpoint operations over
   authenticated gRPC, then prototype and benchmark stream replication after
   SSH-based replication is stable.
10. **Packaging and release**: after all feature phases are complete, produce
    the initial Arch packages and release artifacts, then validate install,
    upgrade, protected configuration, service-account, and systemd behavior in
    the disposable guest matrix.

## 15. Pre-implementation decisions and validation

The following remain deliberate checkpoints rather than implicit assumptions:

- choose and test the minimum supported OpenZFS version and capability matrix;
- validate `-R` receive and foreign-snapshot behaviour on each supported
  OpenZFS release before stabilizing `replicate=on`;
- finalize destination-path mapping for local, SSH, and native transports;
- finalize the TOML schema and worker-count defaults;
- decide whether remote status/control ships in the first release or follows
  local gRPC control;
- define safe, explicit reseed and interrupted-receive abandonment workflows;
- verify receive-side exclusion and inheritance/masking of local and received
  `org.boomerangz:*` property layers across supported OpenZFS releases, documenting
  hidden received values rather than requiring direct libzfs removal;
- benchmark Go-mediated stream copying against direct OS pipes and SSH;
- determine the exact delegated permission sets for source and destination
  roots, including permissions needed by configured native `set_prop` values;
- verify whether delegated execution with `zfs receive -u` satisfies Linux's
  transitive `mount` permission checks on every supported OpenZFS release;
- if a Linux helper is required, threat-model and test its dataset validation,
  peer authentication, capability handling, and systemd sandbox before making
  it part of the recommended deployment.

Packaging is intentionally deferred until after native transport so package
contents and dependencies reflect the complete initial release. Version
`v0.1.0` uses the MIT license and provides two Arch PKGBUILDs: `boomerangz`
builds with CGO disabled from a deterministic source archive published as a
GitHub Release artifact, while `boomerangz-bin` installs CI-built release
binaries. Race-test jobs may enable CGO; shipped binaries and normal package
builds do not require it. Release recipes must contain real artifact checksums,
never `SKIP` for downloaded archives.

## 16. References

- [OpenZFS send and receive](https://openzfs.github.io/openzfs-docs/Basic%20Concepts/Operations/Send%20and%20Receive.html)
- [OpenZFS `zfs send`](https://openzfs.github.io/openzfs-docs/man/master/8/zfs-send.8.html)
- [OpenZFS `zfs receive`](https://openzfs.github.io/openzfs-docs/man/master/8/zfs-receive.8.html)
- [OpenZFS bookmarks](https://openzfs.github.io/openzfs-docs/man/master/8/zfs-bookmark.8.html)
- [OpenZFS holds](https://openzfs.github.io/openzfs-docs/man/master/8/zfs-hold.8.html)
- [OpenZFS user properties](https://openzfs.github.io/openzfs-docs/man/master/7/zfsprops.7.html)
- [OpenZFS delegated administration](https://openzfs.github.io/openzfs-docs/Basic%20Concepts/Operations/Delegated%20Administration.html)
- [OpenZFS `zfs allow`](https://openzfs.github.io/openzfs-docs/man/master/8/zfs-allow.8.html)
- [Linux capabilities](https://man7.org/linux/man-pages/man7/capabilities.7.html)
- [zrepl grid policy](https://zrepl.github.io/configuration/prune.html#policy-grid)
- [Kingpin](https://github.com/alecthomas/kingpin)
- [gRPC authentication](https://grpc.io/docs/guides/auth/)
- [Go module tool directives](https://go.dev/ref/mod#go-mod-file-tool)
