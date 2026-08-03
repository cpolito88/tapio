# tapio

A Pekko-inspired actor toolkit for Python. Typed, asyncio-native actors with
supervision — Pydantic models throughout.

> **Status: pre-alpha.** Nothing is implemented yet.

Named for the Finnish god of the forest, because supervision hierarchies are
trees. It keeps the mythological lineage of Akka (Sámi) and Apache Pekko
(Finnish) without borrowing anyone's trademark — see the note at the bottom.

## What it is

`tapio` gives you the concurrency structure of [Apache Pekko](https://pekko.apache.org/)
(itself the ASF fork of Akka) inside a single Python process:

- **Actors** — isolated state, one mailbox, no locks
- **Supervision** — restart with backoff, escalate, stop; failure policy as a
  first-class thing rather than scattered `try`/`except`
- **Death watch** — learn when a child dies, without polling
- **Ask, timers, stash, routers** — the patterns you would otherwise hand-roll

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
- Saga orchestration — payment → inventory → shipping, compensating on failure
- One actor per websocket, with behavior-switching as the protocol state machine
- Rate limiting and circuit breaking, where the mailbox *is* the mutex

Bad fits — use something else:

- High-volume per-record stream processing → [Bytewax](https://bytewax.io/),
  [Quix Streams](https://quix.io/)
- Anything CPU-heavy inside a handler. One blocking call stalls every actor
  sharing the event loop.

Actors are not microservices. A microservice is a unit of *deployment*; an
actor is a unit of *concurrency*. You will have millions of actors inside one
service. `tapio` structures the inside of that service — it competes with
`asyncio.Queue` + `TaskGroup` + a `dict` + hand-rolled retries, not with your
API framework.

## Design notes

**Sending never blocks.** `ref.tell(msg)` is synchronous and fire-and-forget,
so it works from sync callbacks and signal handlers too. Backpressure is a
property of the mailbox rather than the send call: bounded mailboxes take an
overflow strategy (`fail`, `drop_new`, `drop_oldest`, `dead_letter`), and
`await ref.offer(msg)` waits for capacity when you want to be throttled.

**Every message is a validated Pydantic model.** That costs roughly 3–10× the
per-message overhead and caps local throughput around 10⁴–10⁵ msg/s. In an
actor that spends 50 ms on an HTTP call, validation is ~0.02% of the message's
life — invisible. In a tight per-record loop it dominates, which is why that
workload is out of scope above. The check sits behind a single
`validate_on_tell` setting, and real benchmarks ship with 0.1.0.

Requires Python 3.11+ (for `asyncio.TaskGroup`).

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
Pekko codebase — it is an independent implementation inspired by its design,
and references to Pekko above are descriptive only.

## License

Apache-2.0 (planned).
