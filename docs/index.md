# tapio

A Pekko-inspired actor toolkit for Python. Typed, asyncio-native actors with
supervision, with Pydantic models throughout.

!!! warning "Pre-alpha"

    The runtime core runs: actor systems, spawning, typed `tell`, bounded
    mailboxes, dead letters, supervision with backoff, death watch, `ask`,
    timers, stash, message adapters, a round-robin pool router, and a
    deadline-based shutdown. Remoting has its addressing and its wire format:
    refs carry an address and an incarnation uid, messages encode to frames,
    and a frame handed to another system in the same process delivers. The
    transport that would carry one between processes is still to come, and
    these pages fill in as it lands.

Every code block on this site is a snippet include from `examples/`, so nothing
documented here is unexecuted.
