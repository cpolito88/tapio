"""Shutting a system down on SIGINT, and stopping the tree in the right order.

Concepts: a real signal handler on the event loop, `system.terminate()`, the
bottom-up drain, and `PostStop` as the place a resource is released.

The signal is genuine: the handler is installed with `add_signal_handler` and
the process sends itself SIGINT, so the wiring exercised here is the wiring a
deployed service uses. Nothing about it is simulated, which is the point, since
"we handle SIGINT" is exactly the claim that turns out to be false the first
time a container is stopped.

Shutdown is bottom-up and races one deadline for the whole tree rather than one
per actor: worst-case shutdown tracks `shutdown_timeout` instead of multiplying
by the depth of the tree. Each actor sees `PostStop` after its children have
already seen theirs, so a connection pool held by a parent outlives the
children still handing work back to it.

`PostStop` is best effort, and the honest word is "usually": an actor still
wedged in a handler when the deadline passes is cancelled, and may never see
it. Release what must be released there, and do not make correctness depend on
it running.

What to watch in the output: the two connections stop before the pool that
owns them, and the pool's own line is last.

Run it with:

```
uv run python -m tapio_examples.graceful_shutdown
```
"""

import asyncio
import os
import signal

from tapio import ActorSystem, Behavior, Behaviors, Message
from tapio.actor import ActorContext, PostStop, Signal

__all__ = ["Query", "main"]


class Query(Message):
    """A unit of work for a connection to handle."""

    sql: str


def connection(name: str, lines: list[str], served: asyncio.Event) -> Behavior[Query]:
    """One pooled connection, which reports when it is closed."""

    async def on_query(message: Query) -> Behavior[Query]:
        lines.append(f"{name}: ran {message.sql!r}")
        if name == "conn-2":  # the second of the two, so both have answered
            served.set()
        return Behaviors.same()

    async def on_signal(ctx: ActorContext[Query], sig: Signal) -> Behavior[Query]:
        if isinstance(sig, PostStop):
            lines.append(f"{name}: closed")
        return Behaviors.same()

    return Behaviors.receive_message(on_query, on_signal=on_signal)


def pool(lines: list[str], served: asyncio.Event) -> Behavior[Query]:
    """A pool that owns two connections and outlives both of them."""

    def build(ctx: ActorContext[Query]) -> Behavior[Query]:
        for name in ("conn-1", "conn-2"):
            ctx.spawn(connection(name, lines, served), name=name).tell(
                Query(sql="select 1")
            )

        async def on_query(message: Query) -> Behavior[Query]:
            return Behaviors.same()

        async def on_signal(ctx: ActorContext[Query], sig: Signal) -> Behavior[Query]:
            if isinstance(sig, PostStop):
                lines.append("pool: closed, after every connection in it")
            return Behaviors.same()

        return Behaviors.receive_message(on_query, on_signal=on_signal)

    return Behaviors.setup(build)


async def main() -> list[str]:
    """Run the example.

    Returns:
        One line per thing that happened, in order.
    """
    lines: list[str] = []
    served, interrupted = asyncio.Event(), asyncio.Event()
    loop = asyncio.get_running_loop()

    def on_sigint() -> None:
        # Deliberately does no work beyond waking the shutdown: a signal
        # handler runs outside every actor, and anything it touched directly
        # would be state nobody is holding the mailbox for.
        lines.append("signal: SIGINT, shutting down")
        interrupted.set()

    loop.add_signal_handler(signal.SIGINT, on_sigint)
    try:
        system = ActorSystem("graceful")
        system.spawn(pool(lines, served), name="pool")
        await served.wait()

        os.kill(os.getpid(), signal.SIGINT)
        await interrupted.wait()
        await system.terminate()
    finally:
        # Put the interpreter's own handler back, so a second Ctrl-C after
        # this example is a KeyboardInterrupt again.
        loop.remove_signal_handler(signal.SIGINT)

    for line in lines:
        print(line)
    return lines


if __name__ == "__main__":
    asyncio.run(main())
