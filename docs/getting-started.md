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
anywhere: one actor handles one message at a time, which is the mutual
exclusion a lock would have given you.

```python
--8<-- "examples/tapio_examples/counter.py"
```

## When an actor fails

A failing handler does not raise into the sender. The exception never leaves
the actor's own receive loop; it becomes a decision taken by whoever declared
one, and the default decision is to stop. Restarting is what you ask for when
you know a failure is transient:

```python
--8<-- "examples/tapio_examples/supervision_backoff.py"
```

Three things about a restart are worth knowing before you rely on it. The
behavior the actor was *spawned* with is re-evaluated, so children spawned in
`setup` come back and children spawned in a message handler do not. The mailbox
survives, both lanes, so work queued behind the failure is still there
afterwards. And watchers hear nothing: the ref, path and uid are unchanged, and
only the incarnation behind them is new.

While an actor backs off it is absent rather than dead. `tell` stays total and
its mailbox keeps filling, which on an unbounded mailbox is a memory risk
proportional to inbound rate times window, so an actor that backs off usually
wants a bounded one.

## Knowing that an actor has stopped

There is no "is it alive?" predicate, because every answer one could give is
stale before the caller reads it. You are told instead:

```python
--8<-- "examples/tapio_examples/death_watch.py"
```

## Escalating, and shutting down

An actor that cannot fix a failure hands it to its parent, which can. If it
reaches a guardian, nobody has taken responsibility for it: the system
terminates and re-raises the cause from `when_terminated`.

```python
--8<-- "examples/tapio_examples/escalation.py"
```

Ordinary shutdown drains the tree bottom-up against one deadline for the whole
tree, so the worst case tracks `shutdown_timeout` rather than the depth of the
tree:

```python
--8<-- "examples/tapio_examples/graceful_shutdown.py"
```

## Asking for an answer

`ask` sends one message and awaits one reply. The request still carries a ref
for the answer to come back to, exactly as it does above; what `ask` adds is
that the ref is a promise rather than an actor, so the reply can be awaited
instead of arranged for.

`expect` is required, and it is not ceremony. A promise has no cell and so no
declared message type of its own, and without one the request/response path
would be the only delivery in the library with no type check on it. A reply of
the wrong type raises `AskTypeError` in the caller rather than handing back a
value whose static type is a lie.

The failures are the reason to read the example. A timeout is the expensive
answer, so it is the last resort: an ask watches its target, and a target that
stops fails the ask at once instead of spending the deadline on an answer that
provably is not coming.

```python
--8<-- "examples/tapio_examples/ask_timeout.py"
```

Awaiting an ask inside a handler stops that actor reading its mailbox until the
reply lands. That is occasionally what you want and usually not: an actor that
asks and waits is an actor that cannot answer.

## Doing something later, and holding what you cannot do yet

A timer sends the actor a message on its own user lane. That is the whole
design: a tick is ordinary traffic, so it queues behind what is already there
and can never re-enter a handler that is still running. Timers belong to the
cell, not to the behavior, which is why they come from
`Behaviors.with_timers` and why a restart cancels them: a tick scheduled by
the incarnation that just failed must not arrive at the one replacing it.

`start_fixed_delay` measures the gap from one send to the next, so an actor
that falls behind simply gets fewer ticks. `start_fixed_rate` counts ticks off
a fixed schedule and sends the missed ones back to back once a stall is over.
The second is the one to think twice about, since the catch-up burst arrives
at an actor that has just proved it is not keeping up, and it is the right
choice when the promise really is a rate:

```python
--8<-- "examples/tapio_examples/rate_limiter.py"
```

An actor that cannot answer yet has one good option: accept what arrives, put
it aside, and replay it once it can. `Behaviors.with_stash` gives it a bounded
buffer to do that with, and `stash.unstash_all(next_behavior)` switches state
and replays the backlog in one call.

The replay goes to the *front* of the mailbox, ahead of anything that queued
up in the meantime, so nothing is reordered. The actor also stays an ordinary
actor throughout, which is why the replay is not a loop inside the unstash: a
stop arriving mid-replay is honoured rather than queued behind work nobody
wants any more.

```python
--8<-- "examples/tapio_examples/stash_on_startup.py"
```

The capacity is required. A stash holds traffic the actor is by definition not
keeping up with, so an unbounded one is a memory leak with a good excuse.
Overflow raises `StashOverflowError` in the actor that stashed, where the
decision about what to shed belongs, and a restart empties the buffer:
messages held by the state that just failed are not the new state's to answer,
and what is discarded is published as a dead letter rather than dropped.

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
- `ctx.log`, which tags every record with the actor's path.
- `await system.terminate()`, which drains the tree bottom-up against a single
  deadline and cancels anything still wedged when it passes.

Message adapters and routers are next.
