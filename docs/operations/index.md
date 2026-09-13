# Day-to-day operation

## Check the service

::: code-group

```sh [Linux / systemd]
systemctl status boomerangz.service
journalctl -u boomerangz.service
```

:::

The daemon logs to the system journal when run by systemd.

### Job state lines

Every job state change is written as one `worker state` line, in the order
the daemon recorded it. The line carries these keys:

| Key | Meaning |
|---|---|
| `pool` | The worker pool: `management`, `transfer`, or `configuration` for a configuration reload |
| `job` | The job, for example `snapshot:tank/data` or `remote:tank/data:offsite` |
| `scope` | The dataset the job belongs to |
| `target` | The shared resource the job locks, such as a transfer target |
| `state` | The state the job moved to; see [job states](#job-states) |
| `reason` | Why, when the state carries one |
| `pending` | Jobs waiting in the job's queue when the change was recorded |

A `failed` state is logged at error level, `blocked` and `waiting-retry` at
warning level, and every other state at info level. A successful
`boomerangz config reload` is logged as job `config:reload` in pool
`configuration`. Transfer progress is not logged.

The log is complete from the moment the daemon starts. If writing to the log
falls far enough behind that the daemon has to discard state changes rather
than hold them, it writes an error-level `status log dropped transitions`
line in place of each run of discarded changes, with `count`, the number of
changes in the run, and `first` and `last`, the times of the earliest and
latest of them. The line appears where those changes would have been, and the
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
boomerangz status --watch --interval 5s
```

In a terminal, a watch shows the status view with the most recent job state
changes beneath it, oldest first, under `RECENT TRANSITIONS`. The table shows
only each job's latest state; the list also shows the changes that happened
between two refreshes.

When watch output is redirected, or when `--json` is supplied, it becomes
newline-delimited JSON. Each object carries the snapshot fields and
`transitions`: every job state change since the previous object, in the order
the daemon recorded it, with the same fields as an entry in `jobs` apart from
transfer progress. A job that changed state several times between two objects
has one entry in `jobs` and one entry in `transitions` for each change. The
first object carries an empty `transitions` array, and so does an object sent
only because the interval passed with nothing changing.

A watch that falls too far behind the daemon, because the client or its
connection cannot keep up, ends with an `Aborted` error and exits with status
one rather than continuing with changes missing. Start it again to receive a
fresh snapshot; changes made while no watch was connected are not replayed. A
script that runs a watch unattended should treat that exit as a reason to
reconnect, not as the daemon failing.

### Job states

A job's `state` in `status`, in `transitions`, and in the
[job state lines](#job-state-lines) is one of:

| State | Meaning |
|---|---|
| `pending-management`, `pending-transfer` | Queued in that worker pool, waiting for a worker |
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
| `cancelled` | The job was stopped, for example because the daemon is shutting down |

A transfer that resumes an interrupted stream and then has newer state to send
reports `sending` twice, once for each stream, so its byte count starting again
from zero has a visible cause. A remote transfer whose destination is already
up to date goes from `probing` to `succeeded` without `sending`.

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

1. `boomerangz status` to confirm the daemon and recent work;
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
replication transfers, run to completion. A `status --watch` connected to it
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
