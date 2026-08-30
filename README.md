# tapio

[![CI](https://github.com/cpolito88/tapio/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/cpolito88/tapio/actions/workflows/ci.yml?query=branch%3Amain)
[![Coverage](https://img.shields.io/endpoint?url=https%3A%2F%2Fgist.githubusercontent.com%2Fcpolito88%2Fe24a1b7f2d4f05b76fa10fb594e54347%2Fraw%2Ftapio-coverage.json)](https://github.com/cpolito88/tapio/actions/workflows/ci.yml?query=branch%3Amain)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![Typed: mypy strict](https://img.shields.io/badge/mypy-strict-2a6db2)](https://mypy-lang.org/)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

A Pekko-inspired actor toolkit for Python. Typed, asyncio-native actors with
supervision, and Pydantic models throughout.

> **Status: pre-alpha.** The runtime core runs: actor systems, spawning,
> typed `tell`, bounded mailboxes, dead letters, a deadline-based shutdown,
> supervision with backoff, death watch, `ask`, timers, stash, message
> adapters, `run_blocking` and a round-robin pool router. Two systems can also
> talk over a TCP link, with a handshake, optional TLS and messages that carry
> refs across, plus watching and asking over the link, a failure detector,
> quarantine, an explicit reconnect, and starting an actor on another node.
> Nodes can form a cluster and agree on who is in it, watch one another for
> silence, and, given a downing strategy, resolve a partition by downing the
> losing side rather than blocking on it for ever. Applications react to
> membership through cluster events delivered to an actor's mailbox, and can run
> a singleton on the oldest member of a role or route work over a group of them.

> **Why this exists.** `tapio` is a testbed for AI agentic development. The
> point of the project is to find out what coding agents can carry on their
> own over a real codebase: a non-trivial design, invariants that span
> modules, and a gate that has to stay green. Nearly all of the code, tests
> and documentation here is written by agents under human review.
>
> The library itself is meant to work, and the design decisions are argued
> rather than generated. Treat it as an experiment that happens to be
> working software, not as something to depend on in production yet.

Named for the Finnish god of the forest, because supervision hierarchies are
trees. It keeps the mythological lineage of Akka (Sámi) and Apache Pekko
(Finnish) without borrowing anyone's trademark. See the note at the bottom.

## What it is

`tapio` gives you the concurrency structure of [Apache Pekko](https://pekko.apache.org/)
(itself the ASF fork of Akka) inside a single Python process:

- **Actors**: isolated state, one mailbox, no locks
- **Supervision**: restart with backoff, escalate, stop. Failure policy is a
  first-class thing rather than scattered `try`/`except`
- **Death watch**: learn when a child dies, without polling
- **Ask, timers, stash, routers**: the patterns you would otherwise hand-roll
- **Remoting**: two systems on a TCP link, sending each other typed messages
- **Membership**: nodes gossip a view of the cluster that they all converge
  on, with a leader that is computed rather than elected
- **Downing**: nodes watch one another for silence, and, with a strategy
  configured, resolve a partition by downing the losing side (keep the
  majority, a static quorum, the oldest, down everything, or hand an even
  split to an outside lease), the losing side downing itself
- **Cluster surface**: react to membership as events on an actor's mailbox, run
  a singleton on the oldest member of a role, or spread work over a group router
  of them

It is a **library, not infrastructure**. Pip-install it into the service you
already have. Nodes find each other from a seed list you deploy with them, so
there is no broker, no coordinator and no separate cluster process to run.

## What it is not

This is *inspired by* Pekko, not a port, and shares no code with it.
Deliberately out of scope, permanently:

- **Sharding and distributed data.** Placing an entity on one node, moving it
  when that node goes away, or replicating state so that two nodes can write
  it, is a much larger problem than agreeing on who is in the cluster. If you
  need either, use [Ray](https://www.ray.io/) or put a broker between your
  nodes.
- **Competing with the JVM on throughput.** See below.

Clustering as a whole used to be on that list, and the reason given was that
reimplementing gossip and split-brain resolution in Python is a multi-year
project with a high bug-severity floor. Membership is here because the merge
two nodes run to agree is a pure function with three laws behind it, so it can
be tested as one, and it is. Downing, deciding what to do about a member that
has stopped answering, is the part with the high bug-severity floor, so it is
off unless you configure it: a node watches its peers, and a partition is
resolved by a strategy you choose, with the losing side downing itself and, if
you ask, shutting its own system down. What stays out of scope is agreement
that needs consensus, which is why an even split is handed to a lease you hold
elsewhere rather than to a Paxos or Raft this library ships.

Remoting does give you location transparency in the narrow sense: an actor
holding a ref just sends, and does not need to know which node the target is
on. What it does not change is the failure model. A network is in the middle,
delivery over a link is at-most-once, and a message that crossed one is equal
to what was sent rather than the same object.

## Where it fits

The target is **I/O-bound orchestration**: many independent, long-lived,
stateful things, each mostly *waiting* on something external, each able to fail
on its own.

Good fits:

- A session actor per user, holding conversation state and calling an LLM API
- Saga orchestration: payment, then inventory, then shipping, compensating on failure
- One actor per websocket, with behavior-switching as the protocol state machine
- Rate limiting and circuit breaking, where the mailbox is the mutex

Bad fits, use something else:

- High-volume per-record stream processing → [Bytewax](https://bytewax.io/),
  [Quix Streams](https://quix.io/)
- Anything CPU-heavy inside a handler. One blocking call stalls every actor
  sharing the event loop.

Actors are not microservices. A microservice is a unit of deployment. An actor
is a unit of concurrency. You will have tens of thousands of actors inside one
service, each an `asyncio.Task`, so the ceiling is memory rather than a
cluster: an idle actor costs about 15 KB, so a hundred thousand of them fit in
1.6 GB. `tapio` structures the inside of that service. It competes with
`asyncio.Queue` + `TaskGroup` + a `dict` + hand-rolled retries, not with your
API framework.

## A first actor

```python
import asyncio

from tapio import ActorSystem, Behavior, Behaviors, Message
from tapio.actor import ActorContext, ActorRef


class Greeted(Message):
    whom: str


class Greet(Message):
    whom: str
    reply_to: ActorRef[Greeted]


async def on_greet(ctx: ActorContext[Greet], message: Greet) -> Behavior[Greet]:
    ctx.log.info("hello, %s!", message.whom)
    message.reply_to.tell(Greeted(whom=message.whom))
    return Behaviors.same()


async def on_greeted(message: Greeted) -> Behavior[Greeted]:
    print(f"{message.whom} has been greeted")
    return Behaviors.same()


async def main() -> None:
    async with ActorSystem("hello") as system:
        listener = system.spawn(Behaviors.receive_message(on_greeted), name="listener")
        greeter = system.spawn(Behaviors.receive(on_greet), name="greeter")
        greeter.tell(Greet(whom="world", reply_to=listener))
        await asyncio.sleep(0.1)


asyncio.run(main())
```

Runnable versions of this and every other example live in `examples/`:

```bash
uv run python -m tapio_examples.hello_world
```

## Design notes

**Sending never blocks.** `ref.tell(msg)` is synchronous and fire-and-forget,
so it works from sync callbacks and signal handlers too. Backpressure belongs
to the mailbox rather than the send call. Bounded mailboxes take an overflow
strategy (`fail`, `drop_new`, `drop_oldest`). `drop_new` and `drop_oldest`
publish what they discard as a dead letter; `fail` discards nothing and raises
`MailboxFullError` in the sender instead, so the sender decides. (A `fail` send
from another thread has nobody to raise into, so it dead-letters.)
`await ref.offer(msg)` waits for capacity when you want to be throttled.

**Every message is a validated Pydantic model.** Messages subclass
`tapio.Message`, which is frozen and re-validated on delivery rather than only
at construction. What that costs depends on the model: about 30% more per
message for a one-field message, and about 3x for a ten-field one with nested
models. In an actor that spends 50 ms on an HTTP call it is invisible. In a
tight per-record loop it dominates, which is why that workload is out of scope
above. The check sits behind a single `validate_on_tell` setting. The
[numbers](#numbers) are below.

**Undeliverable messages go to dead letters.** An `ActorRef` stays valid after
its actor dies, so `tell` never raises for a dead target. The message is
published as a `DeadLetter` you can subscribe to and assert on. To know when
something died, watch it with `ctx.watch` rather than asking whether it is
alive. A liveness answer is out of date as soon as you have it.

Requires Python 3.11+ (for `asyncio.timeout()` and `typing.Self`).

## Numbers

Measured with `make bench`, `make bench-scale` and `make bench-cluster`, which
anybody can run. Each prints what it ran on, and this is what they printed:

```
Apple M1 Pro, 8 cores, 17 GB RAM
Darwin 25.5.0
CPython 3.13.2, pydantic 2.13.4
measured 2026-08-20
```

The figures below are from twenty rounds, on a laptop that was doing other
things at the time. Take the ratios seriously and the absolute numbers as an
order of magnitude: a server core running nothing else does better on the
absolute figures, so the ratios are the durable part.

**Messages per second**, one sender to one actor, ten thousand at a time:

| Message | `validate_on_tell=True` | `validate_on_tell=False` | What validation costs |
|---|---|---|---|
| one `int` field | 450,000/s | 637,000/s | 1.4x |
| ten fields, two of them nested models | 261,000/s | 618,000/s | 2.4x |

That is the design bet of this library, priced. Revalidation on delivery is
not free and it is not 10x either: it is a property of your message, and the
setting is there for the case where you have measured it and it matters.

**Everything else, locally:**

| | |
|---|---|
| Starting an actor | 24 us |
| `ask` round trip | 77 us |

**Across a link**, two systems over loopback, so the network itself is nearly
free and what is left is the codec and the socket:

| | Local | Remote | Ratio |
|---|---|---|---|
| Messages per second | 450,000/s | 24,000/s | 19x slower |
| `ask` round trip | 77 us | 410 us | 5x slower |

JSON on the wire is the cost there, and it is the reason the codec sits behind
one module with the frame format versioned: a binary codec is an additive
change rather than a rewrite.

**Resident actors**, idle but able to answer, each measured in its own process:

| Actors | RSS | Per actor | `ask` p50 | `ask` p99 |
|---|---|---|---|---|
| 1,000 | 60 MB | 15.2 KB | 54 us | 164 us |
| 10,000 | 197 MB | 14.9 KB | 48 us | 132 us |
| 100,000 | 1,583 MB | 15.0 KB | 50 us | 216 us |

Memory per actor is flat, and the median round trip to one actor among a
hundred thousand is the same as to one among a thousand: a mailbox nobody is
sending to costs nothing to have. The p99 column is the honest part of this
table. It does not grow with the number of actors, it wanders, because what
puts a tail on a round trip here is the garbage collector walking a large
heap rather than anything in the runtime.

**A cluster, at scale**, measured with `make bench-cluster` on the same
machine:

| Nodes | Converges in | Gossip frame | Per node |
|---|---|---|---|
| 5 | ~10 rounds | 1.0 KB | 1.6 KB/s |
| 20 | ~19 rounds | 3.2 KB | 4.0 KB/s |
| 50 | ~22 rounds | 9.1 KB | 9.0 KB/s |

A gossip frame carries a node's whole view, so it grows with the cluster, and
the table shows the growth is linear: about 180 bytes a member, whether there
are five or fifty. Per-node bandwidth is a few kilobytes a second, one gossip
frame and a handful of heartbeats each. That is the scale claim, as a number:
adding a node costs every other node a couple of hundred bytes a second, not a
new connection to everyone.

Convergence is counted in gossip rounds rather than seconds, and it varies from
run to run because a node gossips to a peer it picks at random. Seconds are
only rounds times the one-second gossip interval anyway, and the fifty-node
figure is the pessimistic end: all the nodes share one event loop here, which
no real deployment does, so at that size the loop is the bottleneck rather than
anything in the runtime.

## Development

Managed with [uv](https://docs.astral.sh/uv/). The `Makefile` is the entry
point and the single source of truth for what CI runs.

```bash
make            # list every target
make install    # create the venv, install deps
make check      # pre-push gate: lint + types + tests
make ci         # exactly what GitHub Actions runs
```

## Trademark note

Apache Pekko and Apache Kafka are trademarks of the Apache Software Foundation.
This project is not affiliated with, endorsed by, or derived from the Apache
Pekko codebase. It is an independent implementation inspired by its design,
and references to Pekko above are descriptive only.

## License

Apache-2.0. Copyright 2026 Carmelo Polito.
