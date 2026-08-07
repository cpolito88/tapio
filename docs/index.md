# tapio

A Pekko-inspired actor toolkit for Python. Typed, asyncio-native actors with
supervision, with Pydantic models throughout.

!!! note "0.1.0, and what that means"

    Everything documented here runs and is tested. Local actors: spawning,
    typed `tell`, bounded mailboxes, dead letters, supervision with backoff,
    death watch, `ask`, timers, stash, message adapters, a round-robin router,
    `run_blocking`, and a deadline-based shutdown. Remoting: two systems open a
    TCP link, shake hands, and send each other typed messages, with refs that
    work on the other side, plus watching, asking, a failure detector,
    quarantine, an explicit reconnect, and starting an actor on another node.
    The TestKit ships with the library.

    Two things to know before you depend on it. **A `Terminated` from an
    unreachable peer can be wrong**, and staying wrong until somebody says
    otherwise is deliberate; [the page about it](unreachable.md) explains why
    and what v0.2 changes. And **there is no clustering**: no membership, no
    sharding, no distributed data. Point-to-point remoting is the whole of it.

Every code block on this site is a snippet include from `examples/`, so nothing
documented here is unexecuted.
