# tapio

A Pekko-inspired actor toolkit for Python. Typed, asyncio-native actors with
supervision, with Pydantic models throughout.

!!! warning "Pre-alpha"

    The runtime core runs: actor systems, spawning, typed `tell`, bounded
    mailboxes, dead letters, supervision with backoff, death watch, `ask`,
    timers, stash, message adapters, a round-robin pool router, and a
    deadline-based shutdown. Remoting runs too: two systems open a TCP link,
    shake hands, and send each other typed messages, with refs that work on
    the other side. So do the parts that decide a peer has gone away: watching
    an actor on another node, asking one, a failure detector, quarantine, and
    an explicit reconnect. Actors can be started on another node too, by asking
    a spawner there, with supervision staying inside each node. The TestKit is
    here too: probes, a behavior test kit, pytest fixtures, and two nodes with
    a network you can break. What is left before 0.1.0 is the last of the
    examples, the rest of these pages, and the benchmarks.

Every code block on this site is a snippet include from `examples/`, so nothing
documented here is unexecuted.
