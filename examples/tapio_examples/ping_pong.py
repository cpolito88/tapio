"""Two actors passing a message back and forth a fixed number of times.

Concepts: bidirectional messaging between two live actors, `Behaviors.same()`
as "keep going, unchanged", and stopping an actor from inside its own handler
with `Behaviors.stopped()`.

Each message carries the address of its sender, so neither actor needs to know
the other in advance: `ping` learns about `pong` from the message it receives.
Neither actor holds any state. The hop count travels in the message instead,
which is the cheapest form of "state" an actor system has.

What to watch in the output: hops strictly alternate, ping then pong, and the
last line is ping stopping itself once the rally is over.

Run it with:

```
uv run python -m tapio_examples.ping_pong
```
"""

import asyncio
from collections.abc import Callable

from tapio import ActorSystem, Behavior, Behaviors, Message
from tapio.actor import ActorContext, ActorRef

__all__ = ["Ping", "Pong", "main"]

ROUNDS = 3
"""How many times the ball crosses the net before ping calls it a day."""


class Ping(Message):
    """A hop towards the ping actor, with the address to answer."""

    hop: int
    reply_to: ActorRef["Pong"]


class Pong(Message):
    """A hop towards the pong actor, with the address to answer."""

    hop: int
    reply_to: ActorRef[Ping]


def pong(record: Callable[[str], None]) -> Behavior[Pong]:
    """Build the pong actor: it answers every hop and never stops itself."""

    async def on_pong(ctx: ActorContext[Pong], message: Pong) -> Behavior[Pong]:
        record(f"pong: hop {message.hop}")
        message.reply_to.tell(Ping(hop=message.hop + 1, reply_to=ctx.self_ref))
        return Behaviors.same()

    return Behaviors.receive(on_pong)


def ping(
    partner: ActorRef[Pong],
    record: Callable[[str], None],
    finished: asyncio.Event,
) -> Behavior[Ping]:
    """Build the ping actor: it answers until the rally is long enough."""

    async def on_ping(ctx: ActorContext[Ping], message: Ping) -> Behavior[Ping]:
        record(f"ping: hop {message.hop}")
        if message.hop >= ROUNDS * 2:
            record("ping: that is enough, stopping")
            finished.set()
            return Behaviors.stopped()
        partner.tell(Pong(hop=message.hop + 1, reply_to=ctx.self_ref))
        return Behaviors.same()

    return Behaviors.receive(on_ping)


async def main() -> list[str]:
    """Run the example.

    Returns:
        One line per hop, in the order the actors produced them.
    """
    lines: list[str] = []
    finished = asyncio.Event()

    async with ActorSystem("ping-pong") as system:
        table = system.spawn(pong(lines.append), name="pong")
        player = system.spawn(ping(table, lines.append, finished), name="ping")
        player.tell(Ping(hop=1, reply_to=table))
        await finished.wait()

    for line in lines:
        print(line)
    return lines


if __name__ == "__main__":
    asyncio.run(main())
