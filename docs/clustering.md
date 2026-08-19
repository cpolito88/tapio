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
reported unreachable by the nodes watching it. Downing: with a strategy
configured, the losing side of a partition is written off rather than blocking
the leader for ever, and the losing side downs itself. And the user-facing
surface an application reacts to membership through: cluster events delivered to
an actor's mailbox, roles, a cluster singleton with handoff, and a group router
over the members of a role.

The surface is deliberately small. Clustering earns its keep only when an
application reacts to membership, and reacting is easiest when a change is just
another message: an event on a mailbox, handled by behaviour switching and
supervision like everything else, rather than a callback on some other thread.

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

Every address in that list has to name a host and a port, and so does every
address that arrives in a cluster message. `tapio://orders` on its own parses,
since that is how a system with remoting switched off writes its own refs
down, but nobody can dial it, and a cluster reaches its members by dialling
them. A seed like that is refused where the list is passed, and one that
arrives on a socket is refused as a malformed frame.

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
same tombstone. Reaching `removed` is what a
[cluster singleton](#cluster-singletons) waits for before it starts a successor,
so a member that owns a singleton keeps it until the moment it is written off.

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

A heartbeat says where to send the answer, and a node answers only an address
it has a reason to believe: a member of the view it holds, or a peer it
already has a link to. A real watcher always has the second, because its
heartbeat came over that link, so a node that is ahead on membership is still
answered and does not look dead for the round or two it takes this one to
catch up. What the rule refuses is the invented address. Answering by dialling
whatever a message names would let any peer that has finished a handshake make
this node open a connection to any host and port, as often as it cared to ask.

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

## Cluster events

Membership changes reach an application as messages, delivered to an ordinary
actor's mailbox. An actor subscribes, and from then on it is told what changed:

```python
cluster.subscribe(worker, MemberUp, MemberRemoved, UnreachableMember)
```

There are six events, all carrying the member they are about:
[MemberUp][tapio.cluster.events.MemberUp],
[MemberRemoved][tapio.cluster.events.MemberRemoved],
[UnreachableMember][tapio.cluster.events.UnreachableMember],
[ReachableMember][tapio.cluster.events.ReachableMember],
[LeaderChanged][tapio.cluster.events.LeaderChanged], and
[SelfDown][tapio.cluster.events.SelfDown]. Subscribing to none of them in
particular means all of them. The subscriber has to accept the events it asks
for as part of its declared message type, because they arrive on its own
mailbox and are type-checked like anything else.

A subscriber hears the current membership the moment it subscribes, as the
events that would have carried it: an actor that starts after the cluster has
formed is told who is up before it is told what changes next. So there is no
separate "give me a snapshot" call, and no window in which a late subscriber has
missed something it can never learn.

An event is this node's view, not the truth. It is emitted when this node's own
membership state moves, so two nodes may see the same change a gossip round
apart. That is the same guarantee everything else in clustering gives.

A subscriber that stops is forgotten, because the daemon watches it. Call
[unsubscribe][tapio.cluster.cluster.Cluster.unsubscribe] only for an actor that
wants to keep running and stop listening.

## Roles

A role is what a node says it is for, fixed when it joins and part of what the
cluster agreed on when it accepted the node:

```python
cluster = Cluster(system, ClusterSettings(roles={"worker"}))
cluster.members_with_role("worker")  # the worker members, oldest first
```

Every member carries its roles in the gossip, so every node can filter on them
without asking anyone. The two features below are the filtering: a singleton
runs on the oldest member of a role, and a group router spreads work over the
members of one.

## Cluster singletons

Some work has to happen in exactly one place: a scheduler that must not fire
twice, a coordinator that owns a piece of state. A
[ClusterSingleton][tapio.cluster.singleton.ClusterSingleton] places one such
actor and moves it when its host goes away.

```python
--8<-- "examples/tapio_examples/cluster_singleton.py"
```

Spawn the same manager on every node. Each watches membership, and the manager
on the oldest member of the singleton's role, and only that one, runs the
instance. "Oldest" is the member with the lowest `up_number`, the order the
leader accepted members in, which every node computes the same way from the same
gossip. So at a converged view exactly one manager runs the instance, with no
election and no lock.

Handoff is triggered by removal. When the host is removed, whether it left
gracefully or was downed, every manager hears `MemberRemoved` and recomputes the
oldest. The new oldest starts the instance; the old host, if its system is still
running, stops the one it was holding. Starting the successor only once the host
is *removed*, rather than the moment it starts leaving, is what keeps the two
from overlapping: a crashed host runs nothing, and a host that leaves gracefully
has stopped its instance by the time it reaches `removed`.

The instance is a fresh start wherever it runs, not a move of live state. What
mattered on the old host does not cross to the new one, which is the honest
shape of a thing that has to survive its host going away. If it owns state that
has to outlive a node, that state belongs somewhere the next host can read it,
not in the actor's memory.

## Group routers

A [pool router](supervision.md) owns its routees, spawning them as its own
children. A group router owns none of them. It routes to whatever actor each
member of a role publishes at an agreed path, and the pool follows membership:

```python
proxy = ctx.spawn(Routers.group(Job, role="worker", path="/user/worker"))
```

A member that joins is added, and one that is removed or goes unreachable is
dropped within a convergence. An empty group is not the end of the router, the
way an empty pool is: members come and go, so instead of stopping it holds and
dead-letters what it is handed until a routee appears.

The routee on each node is reached by its bare path, the way the cluster daemon
itself is, so it has to be published as a well-known name there with
`system.refs.register_well_known(ref)`. That is the one piece of setup a group
router needs beyond a pool, and it is what lets a router on any node address the
routee without knowing which incarnation is answering over there. Selection
reuses the same [RoutingStrategy][tapio.actor.router.RoutingStrategy] as a pool,
so round-robin and anything written for a pool works here without a change.

The message type is named rather than read off a routee, because the routees are
on other nodes and there is no spawned child here to read it from. That is the
only difference in how the two routers are built.

## Managing a cluster

Reading a cluster's membership and downing a stuck member are things an
operator does from outside the application, so they happen over a small HTTP
port a node opens rather than through a code path the application has to carry.
A node given [ManagementSettings][tapio.settings.ManagementSettings] answers a
few requests: one reads what it believes, and the others ask it to let a member
leave or to down one.

```python
--8<-- "examples/tapio_examples/cluster_management.py"
```

The `tapio-cluster` command is the client, and the same requests are a `curl`
away for anyone who would rather script them:

```bash
tapio-cluster --port 25530 status
tapio-cluster --port 25530 leave tapio://orders@10.0.0.2:2551
tapio-cluster --port 25530 down  tapio://orders@10.0.0.3:2551
```

An operator reaches the cluster through any one node, since every node holds
the whole view. What a `status` reports is that node's view, which is the truth
once the cluster has converged and its best guess until then, the same caveat
every gossip-based answer carries. A `leave` or a `down` is answered the moment
the node has been asked, not once the member has gone: the decision travels as
gossip like every other one, so the node replies `202 Accepted` and the next
`status` shows it taking effect.

Downing over this port is the operator's version of the move a
[downing strategy][tapio.cluster.downing.DownStrategy] makes on its own. It is
for a member no strategy will reach: one that is unreachable to everyone, so no
split fires a
strategy, or a cluster running with no strategy configured at all. The member
goes to `down` exactly as a strategy would put it there, and a `down` cannot be
taken back, so the downed member hears the decision as gossip and shuts itself
down.

The port can down a member, so it is a serious surface, and it is off unless it
is configured, like remoting. When it is on it binds loopback by default.
Binding it anywhere another host can reach requires a token, the same refusal
remoting makes about a secret:

```python
Cluster(system, management=ManagementSettings(
    bind_host="0.0.0.0",
    token=SecretStr("..."),  # required beyond loopback, or the node will not start
))
```

The token is presented as `Authorization: Bearer <token>` and compared in
constant time. On loopback it is optional, since reaching the port at all
already means being on the machine.

## Addressing, and the one uid rule this bends

A cluster daemon publishes itself as a well-known name at `/system/cluster`,
so it can be addressed by a bare path with no incarnation uid. That is the
opposite of [the rule refs normally follow](remoting.md), and it is opt-in for
exactly one reason: a seed is named by an address in a configuration file, and
a joining node has no way to know which incarnation is answering over there.
Every other ref still carries its uid and still addresses one incarnation
only.
