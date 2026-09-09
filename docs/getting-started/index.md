# Getting started

This guide takes you from an installed package to a managed ZFS dataset. You do
not need previous Boomerangz experience, but you should already have a working
OpenZFS pool and know which dataset you want to protect.

## Before you begin

You need:

- a platform listed as supported in the [installation guide](./installation);
- OpenZFS and its `zfs` command-line tools;
- administrative access for installing the package and delegating narrowly
  scoped ZFS permissions;
- a source dataset that contains the data you want to protect;
- enough space for retained snapshots and any local or remote copies.

::: warning Snapshots are not an independent backup
A snapshot shares the source pool's hardware. Configure replication to another
pool or host if you need protection from loss of the source pool.
:::

## 1. Install Boomerangz

Follow the [installation guide](./installation). Leave the service stopped until
you have delegated its ZFS access and checked the configuration.

## 2. Delegate access to the source dataset

Run this as a ZFS administrator on the source host, replacing `tank/data` with
your dataset:

```sh
sudo zfs allow -u boomerangz \
  bookmark,destroy,hold,mount,release,send,snapshot,userprop \
  tank/data
```

The permission applies to the selected dataset and, according to normal
OpenZFS delegation rules, its descendants. Select the narrowest suitable root.
Do not give the service account unrestricted `sudo`.

## 3. Review and validate the daemon configuration

The packaged defaults are safe for local use without a network listener:

```sh
sudoedit /etc/boomerangz/config.toml
boomerangz config check
boomerangz config show
```

See [Configure the daemon](/guide/configuration) before adding destinations or
network access.

## 4. Grant local control access

Add your administrative user to the `boomerangz` group so it can use the local
status and control socket without running Boomerangz commands through `sudo`:

```sh
sudo usermod --append --groups boomerangz "$USER"
```

Sign out and back in for the new group membership to take effect. Membership
grants access to all local control operations, so add only users who are trusted
to administer the running daemon. See [Local control access](/guide/security#local-control-access)
for the socket permissions and security boundary.

## 5. Enable the dataset

If you enable a dataset without setting a policy, Boomerangz uses the default
retention grid `12x5m,24x1h,14x1d`: 12 five-minute snapshots, 24 hourly
snapshots, and 14 daily snapshots.

The following example replaces that default with an hourly cadence, keeping 24
hourly snapshots followed by 14 daily snapshots:

```sh
sudo zfs set org.boomerangz:policy=24x1h,14x1d tank/data
sudo zfs set org.boomerangz:enabled=on tank/data
```

Settings are inherited by child datasets. Read [Manage datasets](/guide/datasets)
before enabling a parent that has children with different requirements.

## 6. Start the daemon

::: code-group

```sh [Linux / systemd]
sudo systemctl enable --now boomerangz.service
systemctl status boomerangz.service
```

:::

## 7. Confirm operation

Inspect the effective dataset policy, queue a snapshot immediately, and watch
the result:

```sh
boomerangz dataset inspect tank/data
boomerangz trigger tank/data
boomerangz status --watch
```

Check that a managed snapshot appears:

```sh
zfs list -t snapshot -r tank/data
```

Next, configure a [local or remote destination](/guide/remotes). Until you do,
the snapshots remain on the source pool only.
