# tapio

[![CI](https://github.com/cpolito88/tapio/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/cpolito88/tapio/actions/workflows/ci.yml?query=branch%3Amain)
[![Coverage](https://img.shields.io/endpoint?url=https%3A%2F%2Fgist.githubusercontent.com%2Fcpolito88%2Fe24a1b7f2d4f05b76fa10fb594e54347%2Fraw%2Ftapio-coverage.json)](https://github.com/cpolito88/tapio/actions/workflows/ci.yml?query=branch%3Amain)

A Pekko-inspired actor toolkit for Python. Typed, asyncio-native actors with
supervision, and Pydantic models throughout.

> **Status: pre-alpha.** The runtime core runs: actor systems, spawning,
> typed `tell`, bounded mailboxes, dead letters, a deadline-based shutdown,
> supervision with backoff, death watch, `ask`, timers and stash. Message
> adapters and routers are still to come.

> **Why this exists.** `tapio` is a testbed for AI agentic development. The
> point of the project is to find out what coding agents can carry on their
> own over a real codebase: a non-trivial design, invariants that span
> modules, and a gate that has to stay green. Nearly all of the code, tests
> and documentation here is written by agents under human review.
>
> The library itself is meant to work, and the design decisions are argued
> rather than generated. Treat it as an experiment that happens to be
> functional software, not as something to depend on in production yet.

Named for the Finnish god of the forest, because supervision hierarchies are
trees. It keeps the mythological lineage of Akka (Sámi) and Apache Pekko
(Finnish) without borrowing anyone's trademark. See the note at the bottom.

## What it is

`tapio` gives you the concurrency structure of [Apache Pekko](https://pekko.apache.org/)
(itself the ASF fork of Akka) inside a single Python process:

- **Actors**: isolated state, one mailbox, no locks
- **Supervision**: restart with backoff, escalate, stop; failure policy as a
  first-class thing rather than scattered `try`/`except`
- **Death watch**: learn when a child dies, without polling
- **Ask, timers, stash, routers**: the patterns you would otherwise hand-roll

It is a **library, not infrastructure**. Pip-install it into the service you
already have; there is no cluster to operate.

## What it is not

This is *inspired by* Pekko, not a port, and shares no code with it.
Deliberately out of scope, permanently:

- **Clustering, remoting, sharding, distributed data.** Reimplementing gossip
  and split-brain resolution in Python is a multi-year project with a high
  bug-severity floor. If you need distribution, use [Ray](https://www.ray.io/)
  or put a broker between your nodes.
- **Location transparency.** An `ActorRef` is local. Full stop.
- **Competing with the JVM on throughput.** See below.

## Where it fits

The target is **I/O-bound orchestration**: many independent, long-lived,
stateful things, each mostly *waiting* on something external, each able to fail
on its own.

Good fits:

- A session actor per user, holding conversation state and calling an LLM API
- Saga orchestration: payment, then inventory, then shipping, compensating on failure
- One actor per websocket, with behavior-switching as the protocol state machine
- Rate limiting and circuit breaking, where the mailbox *is* the mutex

Bad fits, use something else:

- High-volume per-record stream processing → [Bytewax](https://bytewax.io/),
  [Quix Streams](https://quix.io/)
- Anything CPU-heavy inside a handler. One blocking call stalls every actor
  sharing the event loop.

Actors are not microservices. A microservice is a unit of *deployment*; an
actor is a unit of *concurrency*. You will have tens of thousands of actors
inside one service, each an `asyncio.Task`, so the ceiling is memory rather
than a cluster. `tapio` structures the inside of that service: it competes with
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
so it works from sync callbacks and signal handlers too. Backpressure is a
property of the mailbox rather than the send call: bounded mailboxes take an
overflow strategy (`fail`, `drop_new`, `drop_oldest`, all three of which
publish what they shed as a dead letter), and
`await ref.offer(msg)` waits for capacity when you want to be throttled.

**Every message is a validated Pydantic model.** Messages subclass
`tapio.Message`, which is frozen and re-validated on delivery rather than only at
construction. That costs roughly 3–10× the per-message overhead and caps local
throughput around 10⁴–10⁵ msg/s. In an actor that spends 50 ms on an HTTP call,
validation is ~0.02% of the message's life, which is invisible. In a tight per-record
loop it dominates, which is why that workload is out of scope above. The check
sits behind a single `validate_on_tell` setting, and real benchmarks ship with
0.1.0.

**Undeliverable messages go to dead letters.** An `ActorRef` stays valid after
its actor dies, so `tell` never raises for a dead target: the message is
published as a `DeadLetter` you can subscribe to and assert on. To *know* when
something died, watch it (`ctx.watch`) rather than asking whether it is alive;
a point-in-time liveness answer is stale the moment you have it.

Requires Python 3.11+ (for `asyncio.timeout()` and `typing.Self`).

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

Apache-2.0 (planned).
