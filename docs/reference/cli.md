# Command-line reference

Run `boomerangz COMMAND --help` for the authoritative help installed with your
version. Commands return exit status zero on success and one on failure; errors
are written to standard error.

Commands that read configuration accept:

| Option | Default |
| --- | --- |
| `--config` | `/etc/boomerangz/config.toml` |
| `--config-dir` | `/etc/boomerangz/config.d` |

## Configuration and version

| Command | Purpose |
| --- | --- |
| `boomerangz config check` | Validate the primary file, drop-ins, and merged configuration. |
| `boomerangz config show` | Print merged TOML with private-key fields redacted. |
| `boomerangz config reload` | Validate and atomically apply the running daemon configuration. Use `--socket PATH` to select a non-default current control socket. The JSON result lists the configuration generation, fields applied live, and fields requiring restart. |
| `boomerangz version` | Show build version, commit, and date. |
| `boomerangz version --json` | Emit version information as JSON. |

## Daemon and operation

| Command | Options and arguments |
| --- | --- |
| `boomerangz daemon` | Run management in the foreground. |
| `boomerangz status` | `--credential NAME_OR_PATH`, `--watch`/`-w`, and `--json`. |
| `boomerangz trigger [DATASET...]` | Snapshot each named actor root's members now. Empty selects every active root; a member is refused with the root to use instead. `--credential NAME_OR_PATH` addresses a paired listener. |

Interactive status uses a readable terminal view; an interactive watch adds the
most recent job state changes beneath it. Redirected one-shot output is one JSON
object; redirected watch output is newline-delimited JSON. Add `--json` to emit
that structured output even when standard output is a terminal. A watch
sends an update whenever the daemon's status changes, and only then. Each watch
object also carries `transitions`, every job state change since the previous
object in the order the daemon recorded it. Each job entry names its run and
what that run acted on, such as the snapshot a transfer sent, in the job
identity fields. Status also reports each destination whose delegated ZFS
permissions lack something Boomerangz may need, under `WARNINGS` in a terminal
and in `destinations` in JSON. A watch that falls too far behind
the daemon ends with an `Aborted` error and exits with status one rather than
continuing with changes missing. See
[View status](/operations/#view-status) for the fields and the job states, and
[Job identity](/operations/#job-identity) for the identity fields, and
[Destination permissions](/operations/#destination-permissions) for the
permission check.

## Dataset lifecycle

| Command | Options and arguments |
| --- | --- |
| `boomerangz dataset list` | Print a readable inventory with each dataset's management status: `active` for an actor root, `member` or `excluded` within a root, `target` for a replica received into, `inactive`, or `invalid`, with a root's blocked destinations beside it. Inactive datasets are hidden unless asked for; `--status STATUS` shows only datasets with that status, takes `all`, `active`, `member`, `excluded`, `target`, `inactive` or `invalid`, and can be repeated or given a comma-separated list, with `all` showing every dataset. Add `--json` for structured output, filtered the same way, which carries `covered_by` and `member` on datasets within a root, `members` and `blocked` on a root, and `replica` on a target. |
| `boomerangz dataset inspect DATASET` | Print readable effective policy, provenance, activation, the actor root it belongs to or a root's members, what marks it as a replica, blocked destinations with what each collides with, warnings, and errors for one exact dataset. Add `--json` for structured output. |
| `boomerangz dataset adopt DATASET` | Preview adoption of an actor root; `--apply` performs the revalidated plan. A member is refused with its root named. |
| `boomerangz dataset reseed DATASET TARGET` | Preview destruction and fresh seeding of one actor root's replica on a local destination root or named remote that the root or any member sends to, resetting every member on it; `--apply` performs the revalidated plan. A member, or a destination blocked by a collision, is refused. |
| `boomerangz dataset abort-receive DATASET TARGET` | Preview discarding the interrupted receives that a local destination root or named remote holds for one actor root and its members; `--apply` performs the revalidated plan. The replica, its snapshots, the binding and the source's replication history stay. A member, or a destination blocked by a collision, is refused. |
| `boomerangz dataset clean [DATASET...]` | Preview decommissioning. Use `--recursive`, `--all`, `--destroy-owned-snapshots`, `--forget-target TARGET`, and `--apply` as needed. |
| `boomerangz identity recover` | Preview recovery. `--owner UUID` resolves multiple candidates; `--apply` performs the revalidated plan. |
| `boomerangz migrate [DATASET...]` | Preview moving source roots' state to the format this build reads. `--all` selects every dataset that keeps state; `--apply` performs the revalidated plan and requires the daemon to be stopped. |

`dataset adopt`, `dataset reseed`, `dataset abort-receive`, `dataset clean`,
`identity recover` and `migrate` print a readable plan: every action with the
object and property it changes, every blocker, every warning, and what was
applied, ending with a reminder that a preview changed nothing. Each takes
`--json` to print the same plan as indented JSON instead, which is the form for
scripts. The JSON carries the same fields as before these commands had a
readable form; text output is for reading and may change between releases.

`dataset clean --all` cannot be combined with dataset arguments. Named datasets
are exact unless `--recursive` is supplied. Snapshot destruction is never
implied by selecting a scope.

`dataset clean --forget-target TARGET` narrows the clean to one target's state
on each named source root and does not check the target: its recovery proofs,
the holds and bookmarks they record, its binding and its suspension marker.
`TARGET` is a configured remote's name, a local destination root, or a target
ID, which is how a remote no longer configured is named; a clean blocked on it
reports the ID. Repeat the flag to forget more than one target. It cannot be
combined with `--recursive`, `--all` or `--destroy-owned-snapshots`, and the
preview is blocked when a named root keeps no state for the target.

## Pairing

| Command | Options and arguments |
| --- | --- |
| `boomerangz pairing create` | `--listener NAME`, repeatable `--scope VALUE`, `--expires-in DURATION`, and paired `--client-cert PATH`/`--client-key PATH`. |
| `boomerangz pairing import NAME BUNDLE` | Store a received bundle under a local credential name. |
| `boomerangz pairing list` | List credentials issued or imported by this installation. |
| `boomerangz pairing revoke ID` | Revoke one issued pairing. |

Allowed scope values are `status`, `trigger`, `replicate`, and `admin`.
`replicate` covers every replication operation, including pruning and retiring
replicas on the destination. The default is `status`. An expiry of `0s` means no expiry.
`--client-cert` and `--client-key` must be supplied together.

## Restricted SSH command

```text
boomerangz ssh-shell
```

This command serves a restricted receiving endpoint over standard input and
output. The packaged `/usr/lib/boomerangz/boomerangz-shell` login shell invokes
it and ignores SSH command arguments. Destination roots are supplied through
the replication API and checked against `ssh_shell.replication_roots`; there is
no root command-line option. Administrators do not normally invoke this hidden
command directly.
