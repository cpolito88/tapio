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
- `ctx.log`, which tags every record with the actor's path.
- `await system.terminate()`, which drains the tree bottom-up against a single
  deadline and cancels anything still wedged when it passes.

`ask`, timers, stash and routers are next.
