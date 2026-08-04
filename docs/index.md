# tapio

A Pekko-inspired actor toolkit for Python. Typed, asyncio-native actors with
supervision, with Pydantic models throughout.

!!! warning "Pre-alpha"

    The runtime core runs: actor systems, spawning, typed `tell`, bounded
    mailboxes, dead letters, supervision with backoff, death watch, `ask`,
    and a deadline-based shutdown. Timers, stash, message adapters and
    routers are still to come, and these pages fill in as they land.

Every code block on this site is a snippet include from `examples/`, so nothing
documented here is unexecuted.
