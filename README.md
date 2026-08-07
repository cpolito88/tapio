# tapio

[![CI](https://github.com/cpolito88/tapio/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/cpolito88/tapio/actions/workflows/ci.yml?query=branch%3Amain)
[![Coverage](https://img.shields.io/endpoint?url=https%3A%2F%2Fgist.githubusercontent.com%2Fcpolito88%2Fe24a1b7f2d4f05b76fa10fb594e54347%2Fraw%2Ftapio-coverage.json)](https://github.com/cpolito88/tapio/actions/workflows/ci.yml?query=branch%3Amain)

A Pekko-inspired actor toolkit for Python. Typed, asyncio-native actors with
supervision, and Pydantic models throughout.

> **Status: pre-alpha.** The runtime core runs: actor systems, spawning,
> typed `tell`, bounded mailboxes, dead letters, a deadline-based shutdown,
> supervision with backoff, death watch, `ask`, timers, stash, message
> adapters and a round-robin pool router. Two systems can also talk over a
> TCP link, with a handshake, optional TLS and messages that carry refs
> across. Remote death watch and quarantine are still to come.

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

It is a **library, not infrastructure**. Pip-install it into the service you
already have. There is no cluster to operate.

## What it is not

This is *inspired by* Pekko, not a port, and shares no code with it.
Deliberately out of scope, permanently:

- **Clustering, sharding and distributed data.** Reimplementing gossip and
  split-brain resolution in Python is a multi-year project with a high
  bug-severity floor. Remoting here means one system dialling another it was
  told about, and nothing more. If you need a cluster, use
  [Ray](https://www.ray.io/) or put a broker between your nodes.
- **Competing with the JVM on throughput.** See below.

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
strategy (`fail`, `drop_new`, `drop_oldest`), and all three publish what they
drop as a dead letter. `await ref.offer(msg)` waits for capacity when you want
to be throttled.

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

Measured with `make bench` and `make bench-scale`, which anybody can run. The
figures below are the best of twenty rounds on an Intel i7-5600U at 2.6 GHz,
Python 3.11 and Pydantic 2.13, on a laptop that was doing other things. Take
the ratios seriously and the absolute numbers as an order of magnitude: a
current server core is considerably faster than this one.

**Messages per second**, one sender to one actor, ten thousand at a time:

| Message | `validate_on_tell=True` | `validate_on_tell=False` | What validation costs |
|---|---|---|---|
| one `int` field | 183,000/s | 241,000/s | 1.3x |
| ten fields, two of them nested models | 55,000/s | 166,000/s | 3.0x |

That is the design bet of this library, priced. Revalidation on delivery is
not free and it is not 10x either: it is a property of your message, and the
setting is there for the case where you have measured it and it matters.

**Everything else, locally:**

| | |
|---|---|
| Starting an actor | 105 us |
| `ask` round trip | 121 us |

**Across a link**, two systems over loopback, so the network itself is nearly
free and what is left is the codec and the socket:

| | Local | Remote | Ratio |
|---|---|---|---|
| Messages per second | 183,000/s | 9,200/s | 20x slower |
| `ask` round trip | 121 us | 1.2 ms | 10x slower |

JSON on the wire is the cost there, and it is the reason the codec sits behind
one module with the frame format versioned: a binary codec is an additive
change rather than a rewrite.

**Resident actors**, idle but able to answer, each measured in its own process:

| Actors | RSS | Per actor | `ask` p50 | `ask` p99 |
|---|---|---|---|---|
| 1,000 | 53 MB | 14.9 KB | 89 us | 216 us |
| 10,000 | 190 MB | 14.8 KB | 93 us | 1,260 us |
| 100,000 | 1,557 MB | 14.8 KB | 104 us | 597 us |

Memory per actor is flat, and the median round trip to one actor among a
hundred thousand is the same as to one among a thousand: a mailbox nobody is
sending to costs nothing to have. The p99 column is the honest part of this
table. It does not grow with the number of actors, it wanders, because what
puts a tail on a round trip here is the garbage collector walking a large
heap rather than anything in the runtime.

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
