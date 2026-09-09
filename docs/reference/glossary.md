# Glossary

**Active root**
: A dataset on which an effective `enabled=on` policy establishes an
  independently scheduled management root.

**Adoption**
: An explicit, preview-first transfer of an existing managed dataset lineage
  to the current installation.

**Dataset**
: An OpenZFS filesystem or volume. Boomerangz policy is attached to datasets
  using ZFS user properties.

**Destination root**
: The ZFS dataset beneath which received data is stored.

**Inactive grace period**
: The delay between deactivation and automatic retirement of recoverable
  Boomerangz state. The default is 24 hours.

**Lineage**
: The persistent identity of a managed dataset's history. It prevents a
  same-named replacement from being mistaken for the original data.

**Local destination**
: A destination dataset reached on the same host.

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
: The active dataset whose policy governs a snapshot or replication job.

**Snapshot**
: A read-only ZFS point-in-time view. A snapshot on the source pool is not an
  independent backup.
