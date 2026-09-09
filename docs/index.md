---
layout: home

hero:
  name: boomerangz
  tagline: A ZFS snapshot/replication manager to help make sure your data comes back to you.
  image:
    src: /brand/boomerangz-mark.svg
    alt: Two boomerangs forming a subtle Z
  actions:
    - theme: brand
      text: Get started
      link: /getting-started/
    - theme: alt
      text: View on GitHub
      link: https://github.com/pdf/boomerangz

features:
  - title: Deliberate snapshots
    details: Define snapshot timing and retention alongside each ZFS dataset.
  - title: Copies where you need them
    details: Replicate to another dataset, an SSH host, or another Boomerangz installation.
  - title: Ready for unreliable links
    details: Interrupted and temporarily unavailable destinations remain recoverable for a later retry.
---

Boomerangz manages snapshots and replication for OpenZFS datasets. It is
particularly useful when a destination is not always online, such as a backup
server reached by a travelling laptop.

Configuration is explicit: you choose which datasets are managed, where copies
go, how often snapshots are created, and how long they are retained.
