# Day-to-day operation

## Check the service

::: code-group

```sh [Linux / systemd]
systemctl status boomerangz.service
journalctl -u boomerangz.service
```

:::

The daemon logs to the system journal when run by systemd.

The daemon writes an info-level `daemon started` line once its control socket
is accepting connections and its worker pools are running, and a
`daemon stopped` line as it exits.

### Job state lines

Every job state change is written as one `worker state` line, in the order
the daemon recorded it. The line carries these keys:

| Key | Meaning |
|---|---|
| `run_id` | The run the change belongs to; see [job identity](#job-identity) |
| `pool` | The worker pool: `management`, `transfer`, or `configuration` for a configuration reload |
| `job` | The job, for example `snapshot:tank/data` or `remote:tank/data:offsite` |
| `scope` | The dataset the job belongs to |
| `target` | The shared resource the job locks, such as a transfer target |
| `state` | The state the job moved to; see [job states](#job-states) |
| `reason` | Why, when the state carries one |
| `pending` | Jobs waiting in the job's queue when the change was recorded |

A line also carries the [identity](#job-identity) keys its change sets:
`snapshot`, `base`, `mode`, `destination`, `marker`, `destroyed` with
`destroyed_count`, and `config_generation`. A key the change does not set is
left out of the line.

A `failed` state is logged at error level, `blocked` and `waiting-retry` at
warning level, and every other state at info level. A successful
`boomerangz config reload` is logged as job `config:reload` in pool
`configuration`. Transfer progress is not logged.

### Discovery lines

Each time the daemon applies a new discovery generation, it writes an
info-level `discovery complete` line with `generation`, the generation's
number, and `datasets`, the number of datasets it holds. The line is written
once the generation is reflected in `status`, in order with the job state lines
around it. A scan that fails writes a `discovery failed` line instead, and a
configuration reload that republishes the same generation writes no second
line.

### Destination permission lines

When the daemon finds that a destination lacks a delegated ZFS permission it
may need, it writes a warning-level
`destination lacks delegated ZFS permissions boomerangz may need; operations that need them will fail`
line with `destination`, the local destination root or the remote's name,
`remote`, `dataset`, where the check ran, `account`, the account that lacks
them, and `missing`, the missing permissions separated by commas. A check
that could not run writes a warning-level
`could not check destination ZFS permissions` line with `error` in place of
the permission keys, and a destination whose earlier warning no longer
applies writes an info-level
`destination grants every delegated ZFS permission boomerangz may need` line.
A recheck that finds the same thing writes nothing. See
[destination permissions](#destination-permissions) for when the daemon
checks.

### Gaps in the log

The log is complete from the moment the daemon starts. If writing to the log
falls far enough behind that the daemon has to discard state changes rather
than hold them, it writes an error-level `status log dropped transitions`
line in place of each run of discarded changes, with `count`, the number of
changes in the run, job state changes and discovery generations together, and
`first` and `last`, the times of the earliest and latest of them. The line appears where those changes would have been, and the
state changes that follow are written after it. Anything reading the log as a
record should treat that line as a gap in it.

When the daemon stops, the `daemon stopped` line carries
`status_backlog_peak`: the most state changes any status consumer, including
the log, has had waiting to be delivered at once.

## View status

Show one current status snapshot:

```sh
boomerangz status
```

In a terminal, status is formatted for reading. Redirected output is one JSON
object, and `--json` selects that structured output even in a terminal. Watch
continuously:

```sh
boomerangz status --watch
```

A watch receives an update whenever the daemon's status changes - a job state
change, transfer progress, a newly discovered or activated dataset, a snapshot
deadline, or work entering or leaving a queue - and nothing while status stays
the same. Transfer progress updates at most four times a second, and a
transfer's rate is measured over the last second, so a transfer that stops
moving shows a rate of zero rather than a slowly falling average.

Over a paired TCP listener, the client and the daemon each check that the other
is still there while a watch or a transfer is running. A peer that vanished
without closing the connection, such as a host that lost power or a network
path that went away, is noticed within about 40 seconds: the watch ends with an
`Unavailable` error, and the daemon releases what the call held. The local
control socket needs no such check.

In a terminal, a watch shows the status view with the most recent job state
changes beneath it, oldest first, under `RECENT TRANSITIONS`. The table shows
only each job's latest state; the list also shows the changes that happened
between two refreshes.

When watch output is redirected, or when `--json` is supplied, it becomes
newline-delimited JSON. Each object carries the snapshot fields and
`transitions`: every job state change since the previous object, in the order
the daemon recorded it, with the same fields as an entry in `jobs` apart from
transfer progress, including the [identity](#job-identity) fields it sets. A job that changed state several times between two objects
has one entry in `jobs` and one entry in `transitions` for each change. The
first object, and an object sent for a change that was not a job state change,
carry an empty `transitions` array.

A watch that falls too far behind the daemon, because the client or its
connection cannot keep up, ends with an `Aborted` error and exits with status
one rather than continuing with changes missing. Start it again to receive a
fresh snapshot; changes made while no watch was connected are not replayed. A
script that runs a watch unattended should treat that exit as a reason to
reconnect, not as the daemon failing.

### The terminal view

The terminal view puts what needs attention first:

1. A summary line: the time of the snapshot, how many active datasets there
   are, how many jobs are running, queued, or in trouble, and how many
   [receives into this host](#incoming-receives) are in progress.
2. `PROBLEMS`, shown only when there are any: destinations whose
   [permission check](#destination-permissions) found something missing or
   could not run, datasets with invalid policy, targets a root is blocked from
   sending to, and jobs that `failed`, are `blocked`, or are in
   `waiting-retry`, most severe first. A retry says how long ago it failed, how
   many attempts in a row have failed where the transfer counts them, and when
   it runs again. Problems are listed whatever the dataset filter.
3. `IN PROGRESS`, shown only when there is work: running jobs with their state,
   then receives into this host, then queued jobs in queue order. A sending
   transfer shows its share sent, bytes, rate and time left, and a progress bar
   once it knows its total. A receive is labelled `<dataset> <- <sending host>`
   and shown the same way, with bytes and rate alone when the sender had no
   estimate.
4. The datasets, in one grid: each dataset's row gives its status, its next
   snapshot, and the state of its latest snapshot job, and under it a row per
   destination gives the state of its latest transfer, so every state lines
   up in the `STATE` column. `SINCE` says how long ago that state began. A
   finished job reads `ok`. A target dataset being received into reads
   `receiving`, with its share received when the sender's estimate is known,
   or `interrupted`; a full receive's new dataset, which discovery lists only
   once it exists, gets a `target` row of its own while it is received.
5. One line summarising the queues, marking any that is nearly full.

Datasets follow the same rules as `boomerangz dataset list`: each has the same
status (`active`, `member`, `excluded`, `target`, `inactive`, or `invalid`),
inactive datasets are hidden with a line saying how many and how to show them,
and `--status STATUS` lists only datasets with that status. It takes `all` or
any status, and can be repeated or given a comma-separated list. JSON output
is filtered the same way.

Times are relative to now: `in 13m`, `47m ago`. A watch redraws every second
between updates, so they keep counting while nothing changes. Long text wraps
rather than being cut, and no line is wider than the terminal; on a terminal
too narrow for a table's columns, each row's details move onto indented lines
under it. A watch that would be taller than the terminal shows its top lines
and says how many more there are. Headings and problems are coloured; set
`NO_COLOR` to turn that off.

In JSON, each entry in `datasets` carries `status`, `covered_by` on a member
or excluded dataset (the root it lies under), `blocked` on a root (each
target it may not send to, with `target`, `remote` and `reason`), and
`active` on a root the daemon schedules.

### Incoming receives

On a host other hosts send to, `status` shows the receives into it. What it
can show depends on how the sender reaches it:

| Transport | Shown | Figures |
|---|---|---|
| `native` | Always: the daemon serves the receive itself | Bytes, rate, and with the sender's estimate a share, time left and progress bar |
| `ssh-shell` | When the `ssh-shell` process can reach the daemon's control socket, as the packaged `boomerangz` account can | As for `native` |
| `ssh` direct | From the resume token `zfs receive -s` keeps on the destination, beneath a target or replication root | None: `receiving` or `interrupted` only |

A receive over `ssh` direct is found when discovery next runs, so it can take
up to a `reconcile_interval` to appear. Its resume token changes as the
receive writes, so a token that has not changed for 15 seconds reads
`interrupted`: the receive stopped, or has stalled. An interrupted receive
stays listed until the sender resumes it or
[abort-receive](/operations/recovery#discard-an-interrupted-receive) discards
it. The first receive into a destination that discovery does not yet know as
a target is not found this way.

The JSON object carries `receives`, an array sorted by dataset, empty when
there are none:

| Field | Meaning |
|---|---|
| `dataset` | The receive's destination: the dataset, or for a receive that names its datasets from the stream, the root they land under |
| `state` | `receiving` or `interrupted` |
| `sender_host` | The sending host as it names itself; display text only |
| `transport` | `native` or `ssh-shell`; absent for a receive found by its resume token |
| `started_unix_nano` | When the receive began; absent when not known |
| `bytes`, `bytes_per_second` | What the receive has accepted so far, and how fast |
| `total_bytes`, `total_known` | The sender's estimate of the stream, when it had one |
| `eta_nanoseconds` | Time left at the current rate, when the total is known |

### Destination permissions

The daemon checks each destination's delegated ZFS permissions against
everything Boomerangz may need there, not only what the next transfer uses:
the receive permissions, `rename` for
[placeholder replacement](/guide/datasets#roots-and-members), what
[reseed](/operations/recovery#reseed-a-target) and
[abort-receive](/operations/recovery#discard-an-interrupted-receive) need, and the permission for each native
property a `set_prop` or `ignore_prop` names. Most transfers exercise only part
of that set, so a missing permission can otherwise go unnoticed until the rare
operation that needs it fails. The check reads the effective delegation for the
account that runs ZFS on the receiving side, at the destination root, or at its
nearest existing ancestor while the root does not exist yet.

A local destination root is checked when discovery first finds a dataset
sending to it; a remote is checked when the daemon first opens its endpoint
for a transfer. Each is checked again when what it must grant changes, after
every `boomerangz config reload`, and, for a remote, when its endpoint opens
after failing to. Nothing is checked on a timer, so after granting a missing
permission, reload the configuration to clear the warning.

A finding is a warning, never a failure. Transfers keep running, and an
operation that needs a missing permission still stops at its own permission
preflight with an error naming what is missing.

In a terminal, `status` lists each destination with something missing, or
whose check could not run, under `PROBLEMS`. The JSON object carries
`destinations`, an array with one entry per destination checked so far, local
destinations first:

| Field | Meaning |
|---|---|
| `destination` | The local destination root, or the remote's name |
| `remote` | `true` for a remote |
| `dataset` | Where the check ran: the root, or its nearest existing ancestor |
| `account` | The account that lacks the `missing` permissions |
| `missing` | The missing permissions, sorted; absent when nothing is missing |
| `error` | Why the check could not run; absent when it ran |
| `checked_unix_nano` | When the check ran |

The array is empty until the first check completes, and a destination that
nothing sends to any more leaves it.

### Job states

A job's `state` in `status`, in `transitions`, and in the
[job state lines](#job-state-lines) is one of:

| State | Meaning |
|---|---|
| `pending-management`, `pending-transfer` | Queued in that worker pool, waiting for a worker |
| `waiting-lock` | Taken by a worker but held back by another job's lock on the same destination, for example while the first transfer to it creates the receive ancestors both would need; `reason` names the job it waits for. It is reported again if that changes |
| `snapshotting` | Taking a scheduled or triggered snapshot |
| `pruning` | Destroying snapshots that retention no longer keeps, on the source (`prune:`) or a destination (`destination-prune:`) |
| `reconciling` | Activating or deactivating a dataset |
| `retiring` | Retiring a deactivated dataset once its inactive grace period has passed |
| `planning` | A local transfer is planning, placing holds, and preparing its destination |
| `probing` | A remote transfer is connecting to its remote and planning |
| `sending` | A transfer stream is running; the progress fields describe it |
| `verifying` | The stream has finished; the daemon is verifying the result and reconciling destination properties |
| `succeeded` | The job finished its work |
| `scheduled` | The job found nothing due yet; `reason` says why |
| `waiting-retry` | The job will be retried later, for example after a remote was unreachable; `reason` carries the cause |
| `blocked` | The job stopped on a condition it cannot resolve itself; `reason` carries the cause |
| `failed` | The job failed; `reason` carries the error |
| `cancelled` | The job was stopped, or removed from its queue before it started; `reason` says why, for example `daemon shutting down`, `dataset deactivated`, or `remote configuration changed` for queued remote transfers a reload requeues against a changed remote |

A `waiting-retry` transfer also carries `retry_unix_nano`, when the daemon next
tries it, and `attempts`, how many attempts in a row have failed. A local
transfer does not count its attempts, so it carries no `attempts`.

A transfer that resumes an interrupted stream and then has newer state to send
reports `sending` twice, once for each stream, so its byte count starting again
from zero has a visible cause. A remote transfer whose destination is already
up to date goes from `probing` to `succeeded` without `sending`.

### Job identity

A job name such as `snapshot:tank/data` names work that recurs. Each state
change also says which run of that work it belongs to and what that run acted
on, so a change can be tied to the snapshot or dataset it concerns. In JSON
output an unset field is omitted; in the job state lines an unset key is left
out.

| Field | Set on | Meaning |
|---|---|---|
| `run_id` | Every state change of a job | The run: its pending state, start state, transfer phases, and outcome share one number. A configuration reload is a run of its own. Numbers are unique while the daemon runs and start again when it restarts. |
| `snapshot` | `snapshot:` `succeeded` | The snapshot the job created |
| | `snapshot:` `scheduled` | The owned snapshot whose age set the next deadline |
| | Transfer `sending` | The source snapshot the stream sends |
| | Transfer `succeeded` | The snapshot now on the destination |
| | Transfer `waiting-retry`, `blocked`, `cancelled` | The pending snapshot the run was carrying, or, for a run that stopped before taking one, the snapshot pending when it stopped. A transfer cancelled while still queued, before it started, names none. |
| `base` | Transfer `sending` | The snapshot or bookmark an incremental stream is based on |
| `mode` | Transfer `sending` | `full`, `incremental-latest`, `incremental-all`, or `resume` |
| `destination` | Transfer `succeeded` | The destination dataset |
| `marker` | `inactive:` `succeeded` | What reconciliation did to the inactive marker: `set`, `cleared`, or `none` |
| `destroyed` | `prune:`, `retire:` `succeeded` | The snapshots destroyed, at most 64 of them |
| `destroyed_count` | `prune:`, `retire:` `succeeded` | How many snapshots were destroyed; larger than the length of `destroyed` when that list was cut |
| `config_generation` | `config:reload` `succeeded` | The configuration generation the reload published |

In a terminal, the recent transitions beneath a watch show these fields as
`key=value` pairs before the reason, with `destroyed` shown as its count and a
snapshot of the job's own dataset shown from its `@`.

## Request a snapshot

Queue an immediate snapshot for every active root:

```sh
boomerangz trigger
```

Or name one or more active scheduling roots:

```sh
boomerangz trigger tank/data tank/home
```

A trigger uses the normal queue and safety checks. It does not duplicate work
that is already queued or running.

## Confirm that protection is current

Use all three views:

1. `boomerangz status` to confirm the daemon and recent work, and that it
   reports no [destination permission](#destination-permissions) warnings;
2. `boomerangz dataset inspect DATASET` to confirm the effective policy;
3. `zfs list -t snapshot -r DATASET` on the source and `zfs list -r ROOT` on
   each destination to confirm the stored data.

For remote destinations, confirm both the most recent successful transfer and
the destination dataset itself. A successful source snapshot alone does not
prove that an independent copy exists.

## Apply configuration changes

After editing configuration files, apply them without interrupting active work:

::: code-group

```sh [Linux / systemd]
sudo systemctl reload boomerangz.service
```

```sh [Direct command]
boomerangz config reload
```

:::

The direct command's JSON result identifies fields applied live and any
`restart_required` fields retained from the previous configuration. The
systemd action writes the same result to the service journal. An invalid or
otherwise unusable candidate leaves the running configuration unchanged and
causes the reload action to fail. See
[Configure the daemon](/guide/configuration#validate-changes) for the reload
contract and non-default socket usage.

A reload leaves a control listener whose configuration is unchanged in place,
along with its connections. A listener using `mtls` or `mtls+token` also counts
as changed when the contents of its client CA file changed, because the CA is
read when the listener starts. A listener that is moved, replaced or removed
stops accepting before the reload returns, and its clients' existing
connections start no new calls. Calls already running on it, such as
replication transfers, run to completion, or until transport keepalive finds
that their peer has vanished. A `status --watch` connected to it
never completes, so it ends at once with an error saying the listener was
retired; start it again against the current socket or endpoint.

Only restart the service when the reload result requires it, such as after
changing `paths.identity_dir`, or when performing planned maintenance. Stopping
the daemon prevents new work and cancels active transfers; recoverable transfer
state is retained so work can continue after restart.

## Monitor storage

Review source snapshots and destination usage regularly:

```sh
zfs list -o name,used,available,refer,mountpoint
zfs list -t snapshot -o name,used,refer,creation -s creation
```

Retention limits snapshot history, but snapshots can remain because another ZFS
object or an incomplete replication still depends on them. Investigate rather
than deleting unfamiliar state manually.
