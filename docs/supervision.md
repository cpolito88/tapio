# Supervision

This is the reason to reach for an actor library instead of a dictionary of
`asyncio.Queue`s.

An actor that raises does not take the process down, does not leave a queue
with no consumer, and does not silently stop being a thing that runs. It fails,
and **its parent decides what that means**. The decision is written where the
child is started, by whoever knew what the child was for.

## The four decisions

A strategy is declared by wrapping the behavior:
`Behaviors.supervise(worker()).on_failure(SupervisorStrategy.restart(),
on=IOError)`.

| Decision | What happens | When it fits |
|---|---|---|
| `resume()` | the failure is dropped and the actor carries on with its state | the message was bad, the actor is fine |
| `restart()` | the behavior is rebuilt from scratch | the state may be wrong, the actor should start again |
| `stop()` | the actor stops, and its watchers hear about it | this actor cannot do its job any more |
| `escalate()` | the parent fails too, and its own supervisor decides | this actor's failure means the subtree is broken |

`on=` narrows a layer to a class of failure, and layers are checked from the
inside out, so a specific rule can sit inside a general one. A failure nobody
wrote a rule for is **stopped**, not restarted: an actor that failed for a
reason nobody anticipated is in a state nobody described, and restarting it in
a loop turns one bug into a busy one.

## What a restart does, exactly

This is the table worth knowing before choosing `restart()`.

| | After a restart |
|---|---|
| The mailbox | kept. Messages queued behind the failure are still delivered |
| The message that failed | dropped. It is not retried, because it is what broke the actor |
| The behavior | re-evaluated from the original one, so `setup` runs again |
| State in the closure | gone, which is the point |
| Children | stopped and respawned by the re-run `setup` |
| Timers | cancelled |
| The stash | cleared |
| Watchers | told nothing. A restart is not a stop |
| The ref | unchanged, so everyone holding one keeps working |

The last two lines are what makes a restart invisible from outside. Senders do
not need to know, and nothing has to be re-resolved.

That also means anything a restart must *not* forget has to live outside the
part that gets re-run. In practice: outside `setup`, or in another actor.

## Backoff

Restarting immediately, forever, against a dependency that is down, is a busy
loop with extra steps. `SupervisorStrategy.backoff` waits, and waits longer
each time, with jitter so a fleet of actors that failed together does not
retry in lockstep.

```python
--8<-- "examples/tapio_examples/supervision_backoff.py"
```

Two things in that example are worth noticing. Messages that arrive during the
backoff window are buffered rather than dropped, so a recovered actor sees the
work that piled up while it was waiting. And a restart window that runs out
stops the actor for good, which is how a permanently broken dependency stops
being retried forever.

## Escalation

Sometimes a child's failure means the parent is broken too. `escalate()` says
so, and the parent fails in turn, which its own supervisor then decides about.

```python
--8<-- "examples/tapio_examples/escalation.py"
```

The parent hears about it as a `ChildFailed` signal, carrying which child and
what went wrong. When a subtree restarts, the children are stopped and
respawned by the re-run `setup`, while a sibling in a different subtree is
untouched: a failure spreads exactly as far as somebody said it should.

An escalation that reaches a guardian has run out of actors willing to take
responsibility, so the system terminates and the cause comes back out of
`when_terminated()`. A crash that nobody handled ends the process rather than
leaving it running with a hole in it.

## Failure is not the same as refusal

A service that answers "no" has not failed. A payment that is declined, a
validation that rejects, a peer that refuses a request: those are answers, and
they belong in the reply type where the caller has to deal with them.

```python
--8<-- "examples/tapio_examples/order_saga.py"
```

Supervision has nothing to decide about a refusal, and the saga above never
raises. It compensates, which is a business decision written in the
application, not a lifecycle decision written in a strategy.

## Supervision and other nodes

Supervision never crosses a link. A remotely spawned actor is supervised by
the node running it, and the requester watches it instead. The reasoning is on
the [remoting page](remoting.md), and the short version is that every restart
decision would otherwise be a frame on a link that can go silent halfway
through.

## Putting it together

```python
--8<-- "examples/tapio_examples/chat_sessions.py"
```

A session per user, a model client per session, supervision at the level that
knows what the failure means, and a registry that watches. The model crashes
while it is holding a request, and the request is simply lost: **a crash is
not a reply, and no timeout makes it one**. The session asks again, the
restarted client answers, and the session's own state was never involved.
