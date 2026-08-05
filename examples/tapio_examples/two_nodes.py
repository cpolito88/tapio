"""The same greeting as `hello_world`, with a socket in the middle.

Concepts: switching remoting on with `RemoteSettings`, turning another system's
address into a ref with `resolve`, sending across the association, and getting
the answer back through a `reply_to` that crossed the wire.

Compare it to `hello_world` line by line. The actors are the same: one greets,
one listens, and the request carries the address for the reply. What changed is
outside them, in the settings and in the one `resolve`. That is what location
transparency buys, and it is worth being precise about what it does not buy:
the failure model is different (a network is in the middle), placement is not
uniform (the greeter is started on its own node), and the message the greeter
receives is *equal* to what was sent rather than the same object, because it
was rebuilt from JSON on the other side.

Both systems run in this one process, on loopback ports the OS picks, so the
example needs no orchestration and no second machine.

What to watch in the output: the node name in front of every line. The request
is written by `home`, handled by `away`, and the reply arrives back at `home`
without either actor knowing there is a link between them.

Run it with:

```
uv run python -m tapio_examples.two_nodes
```
"""

import asyncio
from collections.abc import Callable

from tapio import (
    ActorSystem,
    Behavior,
    Behaviors,
    Message,
    RemoteSettings,
    TapioSettings,
    register_message,
)
from tapio.actor import ActorContext, ActorRef
from tapio.remote.address import format_ref

__all__ = ["Greet", "Greeted", "main"]


@register_message()
class Greeted(Message):
    """Sent back to whoever asked for the greeting."""

    whom: str


@register_message()
class Greet(Message):
    """A request to greet someone, carrying the address for the reply.

    Both message types are registered, because a type key on a frame is a
    registry key and never an import path: a peer decodes what it has been told
    about and imports nothing to find out what a name it does not know might
    have meant.
    """

    whom: str
    reply_to: ActorRef[Greeted]


def node() -> TapioSettings:
    """Settings for a system that listens on a loopback port of the OS's choosing.

    Port 0 keeps the example free of numbers somebody has to keep unique.
    Loopback is the default, and it is the one bind address that needs no
    shared secret: a port accepting frames that name actor paths and message
    types is a serious surface, so binding anywhere else without a secret
    refuses to start.

    Returns:
        The settings.
    """
    return TapioSettings(remote=RemoteSettings(bind_port=0))


def greeter(record: Callable[[str], None]) -> Behavior[Greet]:
    """Build the greeter, which is exactly the local one.

    Args:
        record: Where to write the greeting, so the example can be asserted.

    Returns:
        The behavior to spawn.
    """

    async def on_greet(ctx: ActorContext[Greet], message: Greet) -> Behavior[Greet]:
        ctx.log.info("hello, %s!", message.whom)
        record(f"away: hello, {message.whom}!")
        # An ordinary tell. The ref came off the wire, so this one crosses back
        # over the same association, and nothing here had to know that.
        message.reply_to.tell(Greeted(whom=message.whom))
        return Behaviors.same()

    return Behaviors.receive(on_greet)


async def main() -> list[str]:
    """Run the example.

    Returns:
        The lines the actors produced, in the order they produced them.
    """
    lines: list[str] = []
    done = asyncio.get_running_loop().create_future()

    async def on_greeted(message: Greeted) -> Behavior[Greeted]:
        lines.append(f"home: {message.whom} has been greeted")
        done.set_result(None)
        return Behaviors.same()

    async with (
        ActorSystem("away", node()) as away,
        ActorSystem("home", node()) as home,
    ):
        hello = away.spawn(greeter(lines.append), name="greeter")
        listener = home.spawn(Behaviors.receive_message(on_greeted), name="listener")

        # In a real deployment this string comes from configuration or from a
        # message, not from the other system's own ref: the two are in
        # different processes. Here they are not, so the address is read off
        # the ref that spawn returned.
        address = format_ref(away.address, hello.path)
        lines.append(f"home: resolving {address}")
        remote = await home.resolve(address, expect=Greet)
        remote.tell(Greet(whom="world", reply_to=listener))

        await done

    for line in lines:
        print(line)
    return lines


if __name__ == "__main__":
    asyncio.run(main())
