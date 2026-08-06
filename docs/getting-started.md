# Getting started

An actor system is created inside a coroutine, spawns actors, and is terminated
when you are done with it. Everything below is a live example module: the code
on this page is included from `examples/`, and CI runs it.

```bash
uv add tapio
```

## Hello, world

Spawn an actor, send it a message, and let it reply to an address the message
carried.

```python
--8<-- "examples/tapio_examples/hello_world.py"
```

Run it:

```bash
uv run python -m tapio_examples.hello_world
```

## Two actors talking

Neither actor knows the other in advance. Each learns where to answer from the
message it receives, and `Behaviors.same()` means "keep going, unchanged".

```python
--8<-- "examples/tapio_examples/ping_pong.py"
```

## An actor with state

State is an ordinary attribute on a class-based behavior. There is no lock
anywhere. One actor handles one message at a time, which gives the mutual
exclusion a lock would have.

```python
--8<-- "examples/tapio_examples/counter.py"
```

## When an actor fails

A failing handler does not raise into the sender. The exception never leaves
the actor's own receive loop. It becomes a decision, taken by whoever declared
one, and the default is to stop. Ask for a restart when you know a failure is
transient:

```python
--8<-- "examples/tapio_examples/supervision_backoff.py"
```

Three things about a restart are worth knowing before you rely on it. The
behavior the actor was spawned with is evaluated again, so children spawned in
`setup` come back and children spawned in a message handler do not. The
mailbox survives, both lanes, so work queued behind the failure is still there
afterwards. And watchers hear nothing, because the ref, path and uid are
unchanged and only the incarnation behind them is new.

While an actor backs off it is absent rather than dead. `tell` stays total and
its mailbox keeps filling. On an unbounded mailbox that costs memory in
proportion to the inbound rate times the window, so an actor that backs off
usually wants a bounded one.

## Knowing that an actor has stopped

There is no "is it alive?" call, because the answer would be out of date
before the caller could read it. You are told instead:

```python
--8<-- "examples/tapio_examples/death_watch.py"
```

## Escalating, and shutting down

An actor that cannot fix a failure hands it to its parent, which can. If it
reaches a guardian, nobody has taken responsibility for it, so the system
terminates and re-raises the cause from `when_terminated`.

```python
--8<-- "examples/tapio_examples/escalation.py"
```

Ordinary shutdown drains the tree bottom-up against one deadline for the whole
tree, so the worst case follows `shutdown_timeout` rather than the depth of
the tree:

```python
--8<-- "examples/tapio_examples/graceful_shutdown.py"
```

## Asking for an answer

`ask` sends one message and awaits one reply. The request still carries a ref
for the answer to come back to, as it does above. What `ask` adds is that the
ref is a promise rather than an actor, so the reply can be awaited instead of
arranged for.

`expect` is required, and it matters. A promise has no cell and so no declared
message type of its own. Without `expect`, request/response would be the only
delivery in the library with no type check on it. A reply of the wrong type
raises `AskTypeError` in the caller rather than handing back a value whose
static type is a lie.

The failures are the reason to read the example. A timeout is the expensive
answer, so it is the last resort. An ask watches its target, and a target that
stops fails the ask at once instead of spending the deadline on an answer that
is never coming.

```python
--8<-- "examples/tapio_examples/ask_timeout.py"
```

Awaiting an ask inside a handler stops that actor reading its mailbox until
the reply lands. That is sometimes what you want, but usually not: an actor
that asks and waits cannot answer anyone else.

## Doing something later, and holding what you cannot do yet

A timer sends the actor a message on its own user lane. That is the whole
design. A tick is ordinary traffic, so it queues behind what is already there
and can never re-enter a handler that is still running. Timers belong to the
cell, not to the behavior, which is why they come from `Behaviors.with_timers`
and why a restart cancels them. A tick scheduled by the incarnation that just
failed must not reach the one replacing it.

`start_fixed_delay` measures the gap from one send to the next, so an actor
that falls behind simply gets fewer ticks. `start_fixed_rate` counts ticks off
a fixed schedule and sends the missed ones one after another once a stall is
over. Think twice about the second, because the catch-up burst arrives at an
actor that has just shown it is not keeping up. It is the right choice when
what you promised really is a rate:

```python
--8<-- "examples/tapio_examples/rate_limiter.py"
```

An actor that cannot answer yet has one good option: accept what arrives, put
it aside, and replay it once it can. `Behaviors.with_stash` gives it a bounded
buffer to do that with, and `stash.unstash_all(next_behavior)` switches state
and replays the backlog in one call.

The replay goes to the front of the mailbox, ahead of anything that queued up
in the meantime, so nothing is reordered. The actor also stays an ordinary
actor throughout, which is why the replay is not a loop inside the unstash. A
stop arriving mid-replay is honoured rather than queued behind work nobody
wants any more.

```python
--8<-- "examples/tapio_examples/stash_on_startup.py"
```

The capacity is required. A stash holds traffic the actor is not keeping up
with, so an unbounded one is a memory leak. Overflow raises
`StashOverflowError` in the actor that stashed, where the decision about what
to drop belongs. A restart empties the buffer, because messages held by the
state that just failed are not the new state's to answer, and what is
discarded is published as a dead letter rather than dropped.

## Talking to someone else's protocol

An actor's declared message type is a contract, which raises a question the
first time two actors written by different people have to talk. The service
you called replies with its own reply type, and that type does not belong in
your protocol. Widening yours to admit it is wrong twice over: it lets anyone
send you that message, and it puts a foreign vocabulary inside your handlers.

`ctx.message_adapter` gives you a ref to hand out instead. It accepts the
other protocol's message, translates it into one of yours, and delivers the
result onto your own user lane. There it is ordinary traffic: it queues where
it arrived, it cannot re-enter a running handler, and it is validated against
your declared type like anything else.

The translation runs in your actor rather than in the sender, which is the
reason to prefer this over translating at the call site. The function is your
code, so a mistake in it becomes your supervision decision, and a sender that
has never heard of the adapter does not have your bug raised into it. An
adapter is not an actor, so it cannot be watched or asked. Watch the actor
that owns it.

## One address, several actors

When the work is uniform and the answer to "too slow" is "more of the same
actor", a pool router puts one address in front of several of them:

```python
--8<-- "examples/tapio_examples/worker_pool.py"
```

The router is an ordinary actor whose routees are its children, and that
decides most of its behaviour. Their failures are supervised where they were
declared, so wrapping the routee behavior in `Behaviors.supervise(...)`
restarts a failed routee in place. A routee that stops leaves the pool, and
when the last one goes the router stops too, because an empty pool is an
address that silently swallows work.

A router creates no backpressure of its own. Sending to one never blocks, just
as sending anywhere never blocks. A routee that cannot take a message gets it
dead-lettered, because the router did not write that message and failing would
take a whole pool down over one busy member. Put backpressure on the router's
own mailbox instead, where a producer can `offer` into it and wait.

## Behaviors as the states of a protocol

The state is the behavior, which is what makes the illegal transitions
impossible to write rather than merely checked for:

```python
--8<-- "examples/tapio_examples/state_machine.py"
```

## Two systems on a link

Switch remoting on, turn another system's address into a ref, and send. The
actors do not change at all. Compare this with `hello_world` above: what is
different is the settings and the one `resolve`.

```python
--8<-- "examples/tapio_examples/two_nodes.py"
```

The port is bound while the system is being constructed, so the address a ref
writes down is settled before any ref is handed out, and a configuration that
would listen beyond loopback with no shared secret fails to start rather than
failing to be secure.

`resolve` dials nothing. The first send through the ref creates the
association, and the dial happens behind it, so the call does not wait for a
peer that may be down and a `tell` to one that never answers dead-letters
instead of hanging. The ref is bound to the peer and not to a link, so it
keeps working after a link fails.

What crosses a link is not quite what crosses a mailbox. Delivery is
at-most-once and FIFO per association, with no acks and no retries. A message
rebuilt from JSON is equal to what was sent and never the same object. And a
type key on a frame is a registry key, so both ends have to
`@register_message()` what they exchange. An unknown key is a dead letter
naming the key, and nothing is imported to find out what it meant.

## When a node stops answering

Watching an actor on another node is the same call as watching one next door,
and it is how you depend on something in another process without asking it
whether it is still alive:

```python
--8<-- "examples/tapio_examples/node_failure.py"
```

What the signal means is not the same, and this is the one place where the
difference is worth understanding before you need it. Locally, `Terminated`
says an actor stopped, which is a fact. Across a link it says this node
stopped hearing from that one, which is a conclusion. A partition, a long
pause and a dead process all look identical from a single node.

So tapio does what a single node can do, and says so plainly. Each association
heartbeats. When nothing has arrived for `unreachable_after`, every local
watcher of an actor over there is told `Terminated`, a `PeerUnreachable` event
is published on `system.events`, and the address is **quarantined**: sends to
it dead-letter and nothing dials it again.

Recovery is explicit, never automatic. Watchers have already been told that
live actors are gone, so a link coming quietly back would leave two nodes
holding contradictory beliefs with nothing to notice it. `clear_quarantine`
says this node is willing to talk to that peer again, and `remote.reconnect`
dials.

The example that shows all of it is the uncomfortable one, with both nodes
alive and each convinced the other has died:

```python
--8<-- "examples/tapio_examples/partition.py"
```

Both nodes are wrong, and both are locally correct. Fixing that needs enough
nodes to hold a vote, so that the minority side of a partition can discover
that it is the minority. That is clustering, and it is not in this version.
What is here instead is a default chosen to be recoverable: fail fast, freeze
the address, and let a person or a supervisor decide when to try again. For
request/response and work distribution, wrongly deciding a peer is dead costs
a retry. Waiting forever costs availability.

`ask` works across a link too, and it has one failure a local ask does not:

```python
--8<-- "examples/tapio_examples/remote_ask.py"
```

`AskTimeoutError` says the peer was there and nobody answered in time, so
asking again may work. `AskTargetUnreachable` says this node has given up on
the peer, so it will not. Both fail immediately rather than waiting out the
deadline, which is what the death watch under the ask is for.

Testing any of this needs a network that can be broken on purpose, so the
testkit ships one. `two_nodes()` starts a pair on loopback ports the OS picks,
and `partition()`, `heal()`, `drop()` and `delay()` lose frames without
breaking anything real.

## Starting an actor on another node

Everything above is location transparent: an actor holding a ref sends, asks
and watches without knowing which node the target is on. Placement is not, and
it is not by design. Starting an actor elsewhere is a different call, and it is
awaited, because a round trip is happening.

**Both nodes must be running the same code.** A behavior is a closure, and a
closure does not cross a socket. What crosses is a key naming a factory and a
model holding its arguments, and the peer looks that key up in its own
registry. Nothing is imported to find out what an unknown key might have meant,
exactly as for a message type. A key the peer has never heard of comes back as
`SpawnFailed(reason="unknown-factory")`, which is what version skew between two
deployments looks like from the requesting side.

```python
--8<-- "examples/tapio_examples/remote_spawn.py"
```

Three things in that example are the whole design.

The factory declares its own supervision, with `Behaviors.supervise(...)`
around what it returns. That is the only place it can be declared, because the
restart happens entirely on the node that runs the actor. If the local parent
supervised a remote child, every restart, stop and failure report would be a
frame on a link that can go silent halfway through the decision, and
supervision is the one thing in this library that has to be able to answer. So
the tree stays inside one node.

The requester watches instead of parenting. `Terminated` arrives when the actor
stops, when its node stops, and when the link to that node is given up on, and
all three are indistinguishable on purpose: they mean the same thing to the
requester, which is that this worker is gone and another one should be asked
for. That is a smaller contract than supervision, and it is the largest one a
network can keep.

A spawner offers named factories rather than the registry. An actor that will
start anything registered, on request, is a capability handed to whoever can
reach the port, and the port's threat model does not assume that is nobody. The
allowlist is checked when the spawner is built, so a typo in it fails where it
was written.

The one restriction worth knowing before you write an arguments model: it
cannot carry an `ActorRef`. Arguments are validated on the peer *after* the
factory key has been checked, deliberately outside the decode that resolves
refs, so a ref in them would have nothing to resolve against. Send the new
actor a message instead. You are holding a ref to it, and refs inside that
message resolve normally.

## Backpressure across a link

`await ref.offer(msg)` on a remote ref waits for room in the sending node's
outbound buffer. That is honest local backpressure against a socket that is not
draining, and it is not backpressure from the receiving actor. A worker with a
large mailbox reads every frame the moment it arrives, so the buffer stays
empty, `offer` never waits, and the backlog builds up on the other node where
this one cannot see it.

Nothing in a fire-and-forget wire protocol can do better, so flow control is
built out of messages, where the receiver is the one who knows:

```python
--8<-- "examples/tapio_examples/worker_pool_remote.py"
```

Each worker grants the producer a number of items it will have outstanding.
The producer sends that many and no more, and each finished item grants one
back. The grant is the backpressure, it is end to end, and it holds whatever
the network is doing.

## What the runtime gives you today

- `ActorSystem`, with a `/user` guardian above everything you spawn.
- `ctx.spawn` and `ctx.spawn_anonymous`, with names unique among live siblings.
- `ref.tell`, which never blocks, and validates the message against the
  recipient's declared type before it goes anywhere.
- `await ref.offer(...)` and bounded mailboxes, with `FAIL`, `DROP_NEW` and
  `DROP_OLDEST` overflow strategies.
- Dead letters, subscribable, so a message that went nowhere can be observed
  rather than guessed at.
- `Behaviors.supervise(...).on_failure(...)`: resume, restart with backoff and
  a restart window, stop, escalate.
- `ctx.watch` and `Terminated`, plus `PreRestart` and `PostStop`.
- `await ref.ask(...)`, with a required reply type, a deadline, and a fast
  failure when the target stops rather than a wait for the deadline.
- `Behaviors.with_timers(...)`: single, fixed-delay and fixed-rate timers,
  cancelled by the cell on restart and on stop.
- `Behaviors.with_stash(...)`: a bounded buffer and `unstash_all`, which
  replays in arrival order ahead of newer traffic.
- `ctx.message_adapter(...)`: a ref that translates another protocol into
  yours, in your actor, where a failed translation is your decision.
- `Routers.pool(...)`: round-robin fan-out over routees that are the router's
  own children, shrinking as they stop.
- `ctx.log`, which tags every record with the actor's path.
- `await system.terminate()`, which drains the tree bottom-up against a single
  deadline and cancels anything still wedged when it passes.
- `@register_message()`, which gives a message type the key a frame names it
  by. A key is a registry lookup and never an import path, so a type name that
  arrived on a socket can never become an import.
- `system.address` and refs that write themselves down in full, address and
  incarnation uid included, and resolve back to live refs inside
  `with system.as_deserialization_context():`.
- `system.deliver_frame(...)`, the receiving half of remoting. Everything a
  peer can get wrong is decided here and becomes a dead letter naming the
  peer: an unreadable frame, an unknown type key, a payload that will not
  validate, an actor that has stopped, a stale incarnation, and a message the
  recipient does not accept.
- `RemoteSettings`, a TCP link with a version check and a shared-secret
  handshake, optional TLS, and one association per peer with its own bounded
  outbound buffer.
- `await system.resolve(uri, expect=...)` and `ctx.resolve(...)`, which turn
  another system's address into an ordinary ref.
- `ctx.watch` on a ref that points at another node, and `await ref.ask(...)`
  across a link, with `AskTargetUnreachable` told apart from a timeout.
- A heartbeat failure detector, quarantine, `PeerUnreachable` on
  `system.events`, and `remote.reconnect` as the one way back.
- `tapio.testkit.two_nodes()`, with link faults for partitions, dropped frames
  and delays, so the failure paths are tested rather than described.
- `@remote_behavior()` and `spawner(offers=[...])`, which start an actor on
  another node without any supervision crossing the wire. The requester
  watches what it gets back, a refused request comes back as `SpawnFailed`
  with a reason, and both nodes have to be running the same code.
