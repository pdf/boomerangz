# ZFS property reference

All public names begin with `org.boomerangz:`. Values configured locally on a
dataset can be inherited by its descendants. Received copies do not activate a
destination or control its policy.

| Suffix | Default | Valid values | Effect |
| --- | --- | --- | --- |
| `enabled` | `off` | `on`, `off` | Enable or mask management. |
| `remote` | Unset | Comma-separated configured remote names | Select network destinations. |
| `local` | Unset | Comma-separated ZFS dataset names | Select destinations on this host. |
| `policy` | `12x5m,24x1h,14x1d` | Positive grid tiers using `m`, `h`, `d`, `w`, `mo`, or `y` | Set snapshot cadence and retention. |
| `large_blocks` | `on` | `on`, `off` | Preserve large blocks when supported by the receiver. |
| `compressed` | `on` | `on`, `off` | Send compressed blocks. |
| `raw` | Encryption-dependent | `on`, `off` | Send encrypted data without decrypting it for transfer. |
| `props` | `off` | `on`, `off` | Carry dataset properties while excluding Boomerangz policy from the receive. |
| `incremental` | `all` | `all`, `latest` | Include intermediate snapshots or send directly from the common base. |
| `replicate` | `off` | `on`, `off` | Replicate the complete descendant package. |
| `discard` | `off` | `off`, `first`, `all` | Disable path rewriting, append the source path without its pool component, or append only the final component. |
| `set_prop:NAME` | Unset | Native ZFS property value | Set a property on receive. |
| `ignore_prop:NAME` | Unset | `on`, `off` | Exclude an incoming property; `off` masks an inherited exclusion. |

A matching `set_prop` takes precedence over `ignore_prop` and produces a
warning. Neither dynamic form may target an `org.boomerangz:*` property.

## Receive path mapping

Given source `tank/projects/code` and destination root `backup/archive`:

| `discard` | Result |
| --- | --- |
| `off` (default) | `backup/archive` |
| `first` | `backup/archive/projects/code` |
| `all` | `backup/archive/code` |

`off` means that Boomerangz passes no `-d` or `-e` path-mapping option to
`zfs receive`; the configured destination is therefore the exact receive root.
It does not append the complete source name. Descendants of a recursive stream
are preserved relative to that root.

Choose the destination layout and delegation point together:

| `discard` | Recommended layout | Delegate permissions on | Operational consequence |
| --- | --- | --- | --- |
| `off` | Reserve one absent dataset path for one source. | Its existing parent. | The full receive creates the configured dataset. Boomerangz treats that dataset as replica-owned, and reseed destroys and recreates it. |
| `first` | Create a persistent container root. This is the general recommendation. | The container root. | The source path below its pool is preserved beneath the container. Reseed removes only that mapped subtree. |
| `all` | Create a persistent container root for sources with deliberately unique leaf names. | The container root. | Only the source leaf name is appended. Reseed removes only that leaf subtree; equal leaf names collide. |

For example, with `discard=off`, reserve `backup/archive` but create and
delegate on `backup`; `backup/archive` must not exist before the first full
receive. With `first` or `all`, create and delegate on `backup/archive` itself.
The configured `set_prop:*` and `ignore_prop:*` values change received
properties, but they do not change this creation and ownership model.

Received filesystems default to `canmount=noauto`. They may be mounted
explicitly for recovery, preferably read-only. An already mounted destination
is not remounted by an incremental receive, but do not modify it: Boomerangz
does not use `zfs receive -F`, and local changes can prevent the next receive.
Unmount a mapped dataset before reseeding it because reseed destroys that
dataset.

Preview and verify mappings before the first transfer. Boomerangz binds a
source to the verified destination identity; replacing a pool or dataset with
the same name does not silently authorize the replacement.

## Reserved state

Properties beginning with `org.boomerangz:state:` are managed recovery and
ownership records. Do not create, copy, or edit them manually. Use
`dataset adopt`, `dataset clean`, or `identity recover` when an administrative
transition is required.
