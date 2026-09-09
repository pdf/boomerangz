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
| `boomerangz status` | `--credential NAME_OR_PATH`, `--watch`/`-w`, and `--interval DURATION` (default `2s`). |
| `boomerangz trigger [DATASET...]` | Empty selects every active root; `--credential NAME_OR_PATH` addresses a paired listener. |

Interactive status uses a readable terminal view. Redirected one-shot output is
one JSON object; redirected watch output is newline-delimited JSON.

## Dataset lifecycle

| Command | Options and arguments |
| --- | --- |
| `boomerangz dataset list` | Print a readable inventory with each dataset's management status. Add `--json` for structured output. |
| `boomerangz dataset inspect DATASET` | Print readable effective policy, provenance, activation, coverage, warnings, and errors for one exact dataset. Add `--json` for structured output. |
| `boomerangz dataset adopt DATASET` | Preview adoption; `--apply` performs the revalidated plan. |
| `boomerangz dataset reseed DATASET TARGET` | Preview destruction and fresh seeding of one configured local destination root or named remote; `--apply` performs the revalidated plan. |
| `boomerangz dataset clean [DATASET...]` | Preview decommissioning. Use `--recursive`, `--all`, `--destroy-owned-snapshots`, and `--apply` as needed. |
| `boomerangz identity recover` | Preview recovery. `--owner UUID` resolves multiple candidates; `--apply` performs the revalidated plan. |

`dataset clean --all` cannot be combined with dataset arguments. Named datasets
are exact unless `--recursive` is supplied. Snapshot destruction is never
implied by selecting a scope.

## Pairing

| Command | Options and arguments |
| --- | --- |
| `boomerangz pairing create` | `--listener NAME`, repeatable `--scope VALUE`, `--expires-in DURATION`, and paired `--client-cert PATH`/`--client-key PATH`. |
| `boomerangz pairing import NAME BUNDLE` | Store a received bundle under a local credential name. |
| `boomerangz pairing list` | List credentials issued or imported by this installation. |
| `boomerangz pairing revoke ID` | Revoke one issued pairing. |

Allowed scope values are `status`, `trigger`, `replicate`, `prune`, and
`admin`. The default is `status`. An expiry of `0s` means no expiry.
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
