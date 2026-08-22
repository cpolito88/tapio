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
    converge on, let a member leave gracefully, and, with a downing strategy
    configured, write off the losing side of a partition. It ships a cluster
    singleton, a group router over a role, and a small HTTP port an operator
    reaches it through. The TestKit ships with the library.

    Two things to know before you depend on it. **A `Terminated` from an
    unreachable peer can be wrong**, and staying wrong until somebody says
    otherwise is deliberate; [the page about it](unreachable.md) explains why
    and what will change it. And **clustering has no sharding and no
    distributed data**: it places and moves a singleton and routes over a
    role, but it gives you nowhere to put replicated state, and a cluster with
    no downing strategy configured blocks on an unreachable member rather than
    guessing which side to keep. [The page about it](clustering.md) says what a
    cluster does give you.

Every code block on this site is a snippet include from `examples/`, so nothing
documented here is unexecuted.
