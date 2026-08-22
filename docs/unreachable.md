# When a node stops answering

Here is the tradeoff, in the first paragraph, because it is the thing to know
before deploying two nodes: **tapio decides a peer is gone by waiting, and
waiting cannot tell a dead peer from a slow one.** When it decides wrongly, it
tells the watchers on this node that actors which are alive have stopped. It
then stays wrong until somebody says otherwise.

This page explains what that looks like, why it is the default, and what
changes it later.

## How a peer is declared unreachable

Each association heartbeats every `heartbeat_interval`, one second by default.
When nothing has arrived from a peer for `unreachable_after`, ten seconds by
default, that association is declared unreachable and three things happen
together:

- **Every ref on that peer that anything here was watching gets a
  `Terminated`**, delivered on the system lane so supervision reacts at once.
- **The association is quarantined.** Buffered and subsequent sends become
  dead letters naming the peer, and nothing tries to reconnect.
- **A `PeerUnreachable` event is published** on the system event stream, so a
  service can log it, alarm on it, or decide to shut itself down.

From inside this node, that is indistinguishable from the peer having stopped,
and it is meant to be: the code that handles a `Terminated` is the same code
either way.

## The part that can be wrong

A partition, a long pause, an overloaded peer and a dead peer all look
identical from here. There is nothing this node can measure that separates
them, because the only evidence is the absence of messages.

So both sides of a partition declare the other dead, and both are locally
correct:

```python
--8<-- "examples/tapio_examples/partition.py"
```

That example prints both nodes' beliefs next to each other. Both are still
serving, each thinks the other is gone, and neither can tell. This is the
split-brain problem, and resolving it needs membership and a quorum, which
v0.1 does not have.

## Why fail fast is the default

Given that the guess can be wrong, there are two ways to be wrong: decide too
early, or wait forever. tapio decides.

For the request/response and work-distribution shapes it is built for, wrongly
deciding that a peer is dead costs a retry at the application level, which is
recoverable. Waiting forever costs availability, which often is not. A worker
that might be alive is not a worker you can hand the next job to.

The second half of the default matters as much: once wrong, tapio stays wrong
in a way you can see. The quarantine does not clear itself.

## Recovery is explicit

`await system.remote.reconnect(peer_address)` clears the quarantine and
re-associates. Nothing does that on its own, even after the network is
repaired, and that is the deliberate part.

Automatic re-association after a false positive is the dangerous case: the
watchers here were already told `Terminated` for actors that are alive and
carrying on. Silently resuming would leave two nodes with contradictory
beliefs about who is alive and no moment at which either could notice. An
explicit call means a human or a supervisor decided to accept the peer again,
and the application gets to re-establish whatever it needs to.

A clustered system is the one exception, and it is an informed one. A node
keeps knocking on its fellow members, quarantine and all, because a member
that has not been downed is still a member and the cluster has a membership to
consult where a single system has nothing. A peer that is not a member follows
the rule above unchanged. See
[clustering](clustering.md#one-rule-this-contradicts).

Refs held across a quarantine are **not reusable**. Their uid belongs to a
session that is over, so addressing after a reconnect goes through `resolve`
again. That is also what makes a restarted peer a different peer rather than
an impostor at the same address: a system mints a new uid per incarnation, and
an association is bound to the uid it handshook with.

## Designing for it

The thing that survives this cleanly is work you can hand to somebody else:

```python
--8<-- "examples/tapio_examples/node_failure.py"
```

The coordinator does not retry into the node that is gone. It rebuilds the
worker somewhere that answers and finishes the job there, because the job was
described by a message rather than by a location.

The shapes that struggle are the ones where a remote actor holds the only copy
of something. If that is your design, either keep the authoritative copy where
the writer is, or accept that a false positive costs you a rebuild.

## What changes this

This is a known temporary position, not a permanent one. Two things change it,
and today's defaults are chosen to survive both unchanged:

- **A phi-accrual failure detector** cuts the false-positive rate by treating a
  peer that is late as a probability rather than a deadline. It has landed for
  the cluster's failure detector, where `phi_accrual` selects it; the plain
  two-node association on this page still uses the fixed window described above.
- **Membership with a downing strategy** makes the surviving side a fact
  rather than a guess. With a lease, the side that holds it keeps working and
  the other side stops, which is a real answer rather than two contradictory
  local ones. [Membership](clustering.md) has landed; the downing strategy
  that acts on it has not, so a member that goes quiet blocks the cluster
  instead of being written off.

Two nodes is the worst case for every deterministic strategy and the best case
for a lease, which is worth saying plainly, since two nodes is where most
deployments start.

If your system cannot tolerate a false `Terminated` today, the honest options
are to design around it as above, or to wait for the lease.
