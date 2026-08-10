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

Membership: joining, converging, leading, and leaving. What is **not** here
yet is the half that decides what to do when a node stops answering:
reachability monitoring, and downing. Until those land, an unreachable member
blocks the leader from acting, and it goes on blocking it. That is the honest
behaviour rather than a gap somebody forgot: deciding to write a member off is
a decision with consequences, and it gets strategies of its own.

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
  node that reported it. Nothing writes into it yet.
- **A vector clock**, merged by taking the higher count per node, which is how
  two views are ordered against each other when one is simply newer.
- **A seen set**, which is what makes convergence observable.

## Addressing, and the one uid rule this bends

A cluster daemon publishes itself as a well-known name at `/system/cluster`,
so it can be addressed by a bare path with no incarnation uid. That is the
opposite of [the rule refs normally follow](remoting.md), and it is opt-in for
exactly one reason: a seed is named by an address in a configuration file, and
a joining node has no way to know which incarnation is answering over there.
Every other ref still carries its uid and still addresses one incarnation
only.
