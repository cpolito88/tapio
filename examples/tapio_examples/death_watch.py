"""Keeping a registry honest when the things in it can stop on their own.

Concepts: `ctx.watch`, the `Terminated` signal, and evicting an entry without
leaking it.

A registry of live actors is the first thing most people write, and it comes
with the same bug every time: entries for actors that have since stopped. The
obvious fix, asking a ref whether it is still alive, does not exist in tapio
and would not work if it did. The answer would be out of date as soon as the
caller read it, because the actor can stop in between. Watching turns this
around. Instead of asking, you are told, exactly once, on the system lane, and
the entry is removed where that fact arrives.

The sessions here are not the registry's children. Watching is not parenthood.
It is a one-way subscription to "this actor has stopped", which is the most
one actor can know about another it does not supervise.

What to watch in the output: the registry never checks whether a session is
alive. It is told, and the count afterwards shows the entry is gone.

Run it with:

```
uv run python -m tapio_examples.death_watch
```
"""

import asyncio

from tapio import ActorSystem, Behavior, Behaviors, Message
from tapio.actor import ActorContext, ActorRef, Signal, Terminated

__all__ = ["Census", "Close", "Register", "main"]


class Close(Message):
    """Ask a session to end itself."""


class Register(Message):
    """Tell the registry about a session it should keep track of."""

    who: str
    session: ActorRef[Close]


class Census(Message):
    """Ask the registry to report who it still holds."""


def session() -> Behavior[Close]:
    """A session that ends when asked, and says nothing on the way out."""

    async def on_close(message: Close) -> Behavior[Close]:
        return Behaviors.stopped()

    return Behaviors.receive_message(on_close)


def registry(
    lines: list[str], evicted: asyncio.Event, counted: asyncio.Event
) -> Behavior[Register | Census]:
    """A registry of live sessions that evicts by being told, not by asking."""

    def build(ctx: ActorContext[Register | Census]) -> Behavior[Register | Census]:
        live: dict[str, ActorRef[Close]] = {}

        async def on_message(
            message: Register | Census,
        ) -> Behavior[Register | Census]:
            match message:
                case Register(who=who, session=ref):
                    live[who] = ref
                    # One call, and from here on this registry cannot hold a
                    # stale entry for that session.
                    ctx.watch(ref)
                    lines.append(f"registry: registered {who}, holding {len(live)}")
                case Census():
                    lines.append(f"registry: holding {sorted(live)}")
                    counted.set()
            return Behaviors.same()

        async def on_signal(
            ctx: ActorContext[Register | Census], signal: Signal
        ) -> Behavior[Register | Census]:
            if isinstance(signal, Terminated):
                # The signal carries the ref, and a ref knows its own path, so
                # the entry is found without a second lookup table.
                who = signal.ref.path.name
                live.pop(who, None)
                lines.append(f"registry: {who} stopped, holding {len(live)}")
                evicted.set()
            return Behaviors.same()

        return Behaviors.receive_message(on_message, on_signal=on_signal)

    return Behaviors.setup(build)


async def main() -> list[str]:
    """Run the example.

    Returns:
        One line per thing the registry did, in order.
    """
    lines: list[str] = []
    evicted, counted = asyncio.Event(), asyncio.Event()

    async with ActorSystem("death-watch") as system:
        desk = system.spawn(registry(lines, evicted, counted), name="registry")
        ada = system.spawn(session(), name="ada")
        grace = system.spawn(session(), name="grace")
        desk.tell(Register(who="ada", session=ada))
        desk.tell(Register(who="grace", session=grace))

        # The session ends for its own reasons. This is the case a liveness
        # check cannot handle: nobody tells the registry, and it finds out
        # anyway.
        ada.tell(Close())
        await evicted.wait()

        desk.tell(Census())
        await counted.wait()

    for line in lines:
        print(line)
    return lines


if __name__ == "__main__":
    asyncio.run(main())
