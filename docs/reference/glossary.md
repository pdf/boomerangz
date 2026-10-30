# Glossary

**Active root**
: An actor root that is currently managed.

**Actor root**
: An enabled dataset with no enabled ancestor. It owns its whole subtree: its
  lineage, target bindings, recovery state and jobs cover every member.

**Adoption**
: An explicit, preview-first transfer of an existing managed dataset lineage
  to the current installation.

**Dataset**
: An OpenZFS filesystem or volume. Boomerangz policy is attached to datasets
  using ZFS user properties.

**Destination root**
: The ZFS dataset beneath which received data is stored.

**Excluded dataset**
: A dataset inside an actor root's subtree that is not one of its members,
  because `enabled=off` applies to it. An excluded dataset with members beneath
  it is a hole, replicated only as an empty placeholder.

**Inactive grace period**
: The delay between deactivation and automatic retirement of recoverable
  Boomerangz state. The default is 24 hours.

**Lineage**
: The persistent identity of a managed dataset's history. It prevents a
  same-named replacement from being mistaken for the original data.

**Local destination**
: A destination dataset reached on the same host.

**Member**
: An enabled dataset inside an actor root's subtree. It is snapshotted, pruned
  and sent by its root, with its own grid and send options, and carries no
  Boomerangz state of its own.

**Native destination**
: Another Boomerangz installation reached through a secured paired listener.

**Pairing**
: A credential bundle that identifies a listener, establishes server trust,
  and carries the authentication material and scopes granted to a client.

**Policy grid**
: The count-and-duration expression that sets snapshot cadence and retention,
  such as `24x1h,14x1d`.

**Replication**
: Sending a ZFS data stream to a destination dataset so it can be stored on
  separate storage or another host.

**Scheduling root**
: The actor root whose job snapshots or replicates its members.

**Snapshot**
: A read-only ZFS point-in-time view. A snapshot on the source pool is not an
  independent backup.
