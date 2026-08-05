# tapio

A Pekko-inspired actor toolkit for Python. Typed, asyncio-native actors with
supervision, with Pydantic models throughout.

!!! warning "Pre-alpha"

    The runtime core runs: actor systems, spawning, typed `tell`, bounded
    mailboxes, dead letters, supervision with backoff, death watch, `ask`,
    timers, stash, message adapters, a round-robin pool router, and a
    deadline-based shutdown. Remoting runs too: two systems open a TCP link,
    shake hands, and send each other typed messages, with refs that work on
    the other side. Remote death watch and quarantine are still to come, and
    these pages fill in as they land.

Every code block on this site is a snippet include from `examples/`, so nothing
documented here is unexecuted.
