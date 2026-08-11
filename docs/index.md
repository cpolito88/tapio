# tapio

A Pekko-inspired actor toolkit for Python. Typed, asyncio-native actors with
supervision, with Pydantic models throughout.

!!! note "What runs today"

    Everything documented here runs and is tested. Local actors: spawning,
    typed `tell`, bounded mailboxes, dead letters, supervision with backoff,
    death watch, `ask`, timers, stash, message adapters, a round-robin router,
    `run_blocking`, and a deadline-based shutdown. Remoting: two systems open a
    TCP link, shake hands, and send each other typed messages, with refs that
    work on the other side, plus watching, asking, a failure detector,
    quarantine, an explicit reconnect, and starting an actor on another node.
    Clustering: nodes join from a seed list, gossip a membership they all
    converge on, and let a member leave gracefully. The TestKit ships with the
    library.

    Two things to know before you depend on it. **A `Terminated` from an
    unreachable peer can be wrong**, and staying wrong until somebody says
    otherwise is deliberate; [the page about it](unreachable.md) explains why
    and what will change it. And **clustering stops at membership**: nothing
    yet decides what to do about a member that has stopped answering, so the
    cluster blocks instead, and there is no sharding and no distributed data.
    [The page about it](clustering.md) says what membership does give you.

Every code block on this site is a snippet include from `examples/`, so nothing
documented here is unexecuted.
