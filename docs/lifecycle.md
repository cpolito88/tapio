# The life of an actor

An actor is three things that always travel together: a **path**, a
**mailbox**, and a **behavior**. The path is where it sits in the tree, the
mailbox is what it has been sent, and the behavior is what it does with the
next message. Everything on this page follows from those three.

## Starting

`system.spawn(behavior, name)` starts a top-level actor, and `ctx.spawn` from
inside a behavior starts a child. Both return a ref, immediately: the actor
starts on its own, nobody waits for it, and a message sent to it in the next
line arrives after it has started.

```python
--8<-- "examples/tapio_examples/hello_world.py"
```

Names are unique among siblings, and reusing one raises `ActorNameError`
rather than silently addressing the wrong actor. Where a name does not matter,
`spawn_anonymous` generates one beginning with `$`.

Every ref carries an incarnation **uid** as well as a path. A ref to an actor
that has stopped does not become a ref to the next actor at that path, which
is what stops a stale reference from quietly addressing a stranger.

## Holding state

State lives in a closure over `Behaviors.setup`, or in the fields of an
`AbstractBehavior`. Both are ordinary Python variables, and neither needs a
lock, because one actor handles one message at a time and nothing else in the
process can be inside its handler.

```python
--8<-- "examples/tapio_examples/counter.py"
```

That is the property to keep in mind when deciding what should be an actor:
anything that would otherwise need a mutex is a candidate.

## Changing what happens next

A handler returns the behavior for the next message. `Behaviors.same()` keeps
the current one, and returning a different behavior switches to it, which is
how a protocol with states is written without a state variable and a chain of
`if`s:

```python
--8<-- "examples/tapio_examples/state_machine.py"
```

`Behaviors.stopped()` ends the actor. That is the way to stop one: send a
message its behavior answers with `stopped()`, rather than reaching for the
runtime.

## Signals

Alongside messages, an actor is told about its own lifecycle on the system
lane, which is drained ahead of ordinary traffic:

| Signal | When |
|---|---|
| `PostStop` | after the last message, whatever stopped it |
| `PreRestart` | before a restart replaces the behavior |
| `Terminated` | an actor this one watched has stopped |
| `ChildFailed` | a child failed and the failure escalated to here |

`PostStop` is where a resource an actor opened is closed. It runs for a stop,
a restart's teardown and a shutdown alike, so there is one place to write it
rather than three.

## Watching

Watching is how one actor finds out that another has stopped, without asking
and without polling. A liveness check is out of date as soon as you have the
answer; `Terminated` is not, because it is delivered by the thing that stopped.

```python
--8<-- "examples/tapio_examples/death_watch.py"
```

The pattern that page is really about is eviction. A map of live actors stays
true because the map's owner watches what it holds, so an entry is removed by
the actor stopping rather than by whoever remembered to clean up.

## Stopping the tree

`await system.terminate()` drains the tree from the leaves up: a parent is not
stopped before its children, so a child can still send to its parent while it
shuts down. `async with ActorSystem(...)` does the same thing at the end of
the block.

```python
--8<-- "examples/tapio_examples/graceful_shutdown.py"
```

Shutdown races **one deadline for the whole tree**, not one per actor, so the
worst case tracks `shutdown_timeout` rather than multiplying by depth. An
actor still inside a handler when the deadline passes is cancelled, and the
warning names its path so the slow one is identifiable rather than anonymous.

After shutdown starts, `spawn` raises `ActorSystemTerminating` and a `tell`
becomes a dead letter. Neither is silent, and neither leaks a task.

## Where messages go when nobody takes them

A `tell` never blocks and never raises about the recipient, so an undelivered
message is not an exception. It becomes a **dead letter**: an event carrying
the message, the intended recipient and a reason.

```python
--8<-- "examples/tapio_examples/dead_letters.py"
```

Subscribing to that stream is what makes an absence observable. Without it,
"the message was dropped" and "the code never ran" look identical from
outside, which is why this is a stream rather than only a log line.
