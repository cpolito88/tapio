# Clustering

Remoting lets two systems that already know about each other exchange
messages. Clustering answers the question remoting deliberately does not: who
is in this group right now.

```python
--8<-- "examples/tapio_examples/cluster_join.py"
```

Nothing here is a vote. Every node merges what it hears into what it believes,
the merge is written so that the order gossip arrives in cannot change the
result, and the leader is the first member in address order, which every node
works out for itself. There is no consensus algorithm in tapio and there is
not going to be one.

## What is in this release

Membership: joining, converging, leading, and leaving. Reachability: every
member is watched by a few others, and a member that stops answering is
reported unreachable by the nodes watching it. What is **not** here yet is
downing, which is the decision to write an unreachable member off. Until it
lands, an unreachable member blocks the leader from acting, and it goes on
blocking it. That is the honest behaviour rather than a gap somebody forgot:
deciding a member is gone has consequences, and it gets strategies of its own.

## Statuses

A member moves up this ladder and never back down it.

| Status | Meaning | Set by |
|---|---|---|
| `joining` | contacted a seed, not yet accepted | the joining node |
| `up` | a full member | the leader, on convergence |
| `leaving` | a graceful exit was asked for | the leaving node |
| `exiting` | leaving, and every node has seen it | the leader, on convergence |
| `down` | declared dead, and may not return | the leader |
| `removed` | gone, and kept as a tombstone | the leader |

The order is the merge rule. When two nodes hold different views of one
member, the merged view takes the higher status, so a node that learns of a
`down` can never un-learn it however old the gossip that told it. It is also
why `Member.with_status` refuses to move a member backwards: a transition
that contradicted the lattice would be undone by the next gossip that arrived.

Akka has a `WeaklyUp` status between `joining` and `up`, which lets a node
join while another member is unreachable. tapio does not, deliberately. It
buys availability during a partition at the cost of a member that only half
the cluster has agreed on, and every feature that places something has to know
not to place it there. It is worth adding when somebody has the problem it
solves.

## Joining

Every node is given the same seed list, in the same order, its own address
included:

```python
await cluster.join_seed_nodes([
    "tapio://orders@10.0.0.1:2551",
    "tapio://orders@10.0.0.2:2551",
    "tapio://orders@10.0.0.3:2551",
])
```

A node asks every seed to let it in, and keeps asking until it sees itself in
the gossip that comes back, because a join is delivered at most once like
every other message in tapio. A node that is not a member itself ignores a
join request, which is what stops two nodes that started together from
admitting each other into two different clusters.

**Only the first seed in the list may form a new cluster**, and only after
`seed_form_after` in which it has heard from nobody at all. That rule is the
whole of the bootstrap safety. A restarted node hears the running cluster's
gossip in answer to its own join and joins that instead of founding a second
one, so `seed_form_after` has to stay comfortably longer than the time a
running seed takes to answer.

`join_seed_nodes` returns once this node is `up`. If it times out, the node is
still asking: the error is about how long you were prepared to wait, not about
the node giving up.

## Convergence, and the leader

A view has **converged** when every member that is not `down` or `removed` is
reachable and has seen that exact version of the gossip. Convergence is not
consensus. It is the condition under which the leader is allowed to act, and
that is all it is used for.

The leader is the first member in address order whose status is `up` or
`leaving`. Every node computes it, one node acts on it, and there is no
handover protocol because there is no state to hand over. Before anybody is
`up`, which is every cluster's first moment, it falls back to the first member
in address order: somebody has to be able to accept the first join.

A leader with an unreachable member converges on nothing and therefore does
nothing. Joins wait, leaves wait, and the cluster keeps running. That blocking
is what downing will exist to resolve.

## Leaving

```python
await cluster.leave()
```

The member walks out rather than vanishing: `leaving`, then `exiting` once
every node has seen it, then `removed`. Each step needs a converged view, so
leaving takes as long as agreement takes, and every node ends up holding the
same tombstone. That gap between `leaving` and `removed` is where a handoff
goes once there is something to hand over.

The tombstone is kept rather than pruned. Dropping the record would let a peer
holding an older view put the member back, since merging two views unions the
members in them. Pruning needs a way to know that every node has seen the
removal, and it is not in this release.

Leaving does not terminate the system. Ending the process is the
application's decision.

## What a node gossips

One node picks one other member per round and sends it everything it
believes. The receiver merges, and answers immediately if its own view turns
out to be newer, which is most of why convergence takes rounds rather than
seconds. Traffic is therefore linear in the number of nodes, not quadratic.

The state itself is small and every part of it merges the same way:

- **Members**, merged pairwise by the status lattice above.
- **Reachability**, one observation per pair of nodes, each carrying the
  observer's own version so that an unreachability can be retracted by the
  node that reported it.
- **A vector clock**, merged by taking the higher count per node, which is how
  two views are ordered against each other when one is simply newer.
- **A seen set**, which is what makes convergence observable.

## Reachability

Reachability is a separate axis from membership, and conflating the two is the
classic mistake. A member can be `up` and unreachable at the same time. The
first is a decision the cluster made about it, the second is an observation
one node made about it, and only the second can be wrong.

Every node sorts the member addresses, finds itself, and watches the few that
follow it, wrapping round at the end:

```python
cluster = Cluster(system, ClusterSettings(monitored_peers=5))
cluster.monitored  # the members this node watches, in address order
```

So every member is watched by exactly that many others, whether or not
anybody has reason to send it anything, and the probing costs one message per
watched member per round rather than one per pair. All-to-all monitoring is
quadratic, and it is what makes naive implementations fall over at a few dozen
nodes.

A watcher sends a heartbeat every `heartbeat_interval`, the watched member
answers, and a member that has not answered for `unreachable_after` is
recorded unreachable **by that node**. Nothing about that is a decision by the
cluster. It is one node saying it cannot get through, it travels in gossip
like everything else, and the node that said it is the only one that can take
it back, which it does the moment an answer arrives again.

A member is unreachable to the cluster when **any** node says so, and
reachable again only when every one of them has retracted. That is
deliberately pessimistic. One node's bad link is enough to block convergence,
which is visible and recoverable, whereas ignoring a minority report is how a
half-partitioned node stays a member forever.

The transport's verdict counts as evidence too. When remoting gives up on a
link it says so on the event stream, and a watcher takes that as its member
going unreachable, because it arrives sooner than a window that has not run
out yet. The two sources are retracted separately: an answer to a probe brings
back the first, and a link coming up again brings back the second. An answer
never retracts what the transport said, since a peer that remoting is refusing
to carry frames to cannot answer at all.

A link coming up is not an answer either. It proves a process is accepting
connections, and what this node is asking is whether the daemon behind it is
still replying, so the next probe settles that one round later. Reading a
handshake as a reply would let a peer whose links churn faster than
`unreachable_after` look healthy forever without ever answering, which is the
failure the watching exists to catch.

Silence is judged behind an interface, and today it is a fixed window. A fixed
window has no opinion about how variable a network is, so
`unreachable_after` has to sit well above `heartbeat_interval` or a slow
moment reads as a dead node. Phi-accrual, which learns the spread of a peer's
timings instead of being told a number, fits behind the same interface.

### One rule this contradicts

Remoting says [recovery is never automatic](unreachable.md): a system that
gave up on a peer stays given up on until somebody calls `reconnect`. A
clustered system does not follow that rule for its own members. Each round, a
node clears the quarantine on every alive member, so nothing it might have to
talk to is left refused.

The rule is right for a single system and wrong here, and the difference is
membership. One node alone cannot tell a false alarm from a dead peer, so it
refuses to guess twice. A cluster does not have to guess: an unreachable
member has not been downed, so it is still a member, and the cluster's job is
to keep trying to reach it until a downing strategy says to stop. Only
members are forgiven, and only while nobody has decided otherwise: a peer this
system talks to but has not clustered with follows remoting's rule as before.

It covers every member and not only the ones this node watches, because the
two sets are different jobs. The ring decides who is *judged*, and it is a few
nodes per member so that heartbeat traffic stays linear. Gossip goes to any
member at all. A node therefore has to be able to dial members it does not
watch, and if only the watching node forgave a quarantine then every other
pair would stay refused for good. In a cluster larger than `monitored_peers`
that is most pairs, and a partition that healed would never converge again.

Clearing a quarantine dials nothing. It says only that this node is willing to
be associated again, so doing it every round for a member that is perfectly
reachable costs a set lookup and changes nothing.

## Addressing, and the one uid rule this bends

A cluster daemon publishes itself as a well-known name at `/system/cluster`,
so it can be addressed by a bare path with no incarnation uid. That is the
opposite of [the rule refs normally follow](remoting.md), and it is opt-in for
exactly one reason: a seed is named by an address in a configuration file, and
a joining node has no way to know which incarnation is answering over there.
Every other ref still carries its uid and still addresses one incarnation
only.
