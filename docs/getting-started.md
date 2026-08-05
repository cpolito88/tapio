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

Remote death watch, a failure detector and quarantine are next, and with them
an `ask` that works across a link.
