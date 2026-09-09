# Manage datasets

Boomerangz reads policy from locally configured `org.boomerangz:*` ZFS user
properties. A dataset remains unmanaged until `org.boomerangz:enabled=on`
applies to it.

## Inspect before changing

List discovered datasets and inspect one effective policy:

```sh
boomerangz dataset list
boomerangz dataset inspect tank/data
```

These commands are read-only and produce human-readable output by default.
`dataset list` labels each dataset as `active`, `inactive`, `covered`, or
`invalid`; when a policy is invalid, inspect that dataset for the reported
errors. `dataset inspect` shows activation and validation status, effective
policy, property provenance, recursive-replication coverage, warnings, and
errors.

Add `--json` when a script or other structured consumer needs the stable JSON
representation:

```sh
boomerangz dataset list --json
boomerangz dataset inspect tank/data --json
```

## Set a snapshot policy

A policy is a comma-separated grid of `count x duration` tiers:

```sh
sudo zfs set org.boomerangz:policy=12x5m,24x1h,14x1d tank/data
sudo zfs set org.boomerangz:enabled=on tank/data
```

The shortest duration is the snapshot cadence. In this example Boomerangz takes
a snapshot every five minutes, keeps progressively fewer snapshots as they age,
and retains coverage through the daily tier.

Supported duration units are:

| Unit | Meaning |
| --- | --- |
| `m` | minute |
| `h` | hour |
| `d` | 24-hour day |
| `w` | seven days |
| `mo` | 30 days |
| `y` | 365 days |

These are elapsed durations rather than calendar boundaries. For example,
`12mo` is 360 days and is not the same duration as `1y`.

## Inheritance

ZFS properties normally pass from a parent dataset to its children. This is
useful when one policy should cover a hierarchy:

```sh
sudo zfs set org.boomerangz:policy=24x1h,14x1d tank/home
sudo zfs set org.boomerangz:enabled=on tank/home
```

Set `org.boomerangz:enabled=off` locally on a child to exclude it. Inspect
parents and children before enabling a broad root.

## Add destinations

Select configured remote destinations by name:

```sh
sudo zfs set org.boomerangz:remote=home-backup tank/data
```

For a destination dataset on the same host:

```sh
sudo zfs set org.boomerangz:local=backup/data tank/data
```

Multiple names are comma-separated. Empty entries and unknown remote names make
the affected policy invalid. Configure and secure the destination before
selecting it.

## Recursive replication

```sh
sudo zfs set org.boomerangz:replicate=on tank/data
```

Recursive replication explicitly includes all descendant datasets and their
snapshots. This includes a descendant with `enabled=off`: that setting excludes
the descendant from independent management, but it does not exclude the
dataset from an ancestor's recursive ZFS replication package. Recursive
replication can also carry snapshots not created by Boomerangz, and receive
behavior can remove destination snapshots that are absent from the source
package. Enable it only after reviewing the complete hierarchy and testing the
destination mapping.

## Deactivate a dataset

```sh
sudo zfs set org.boomerangz:enabled=off tank/data
```

Deactivation stops new management but does not immediately remove recovery
state. By default, the dataset remains recoverable for 24 hours before automatic
retirement becomes eligible. Change that delay with
`daemon.inactive_grace_period`; set it to `0s` to disable automatic retirement.

Re-enable the dataset during the grace period to resume management without
discarding its established history.

See the [property reference](/reference/properties) for every available setting.
