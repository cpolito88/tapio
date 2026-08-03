"""The smallest complete actor program: spawn, tell, reply, shut down.

Concepts: starting an `ActorSystem`, spawning a top-level actor, sending it a
message with `tell`, and carrying a return address in the message as an
`ActorRef` field.

There is no `ask` here. A reply is just another message sent to a ref the
sender put in the request, and seeing that plainly once makes `ask` read as the
sugar it is.

What to watch in the output: the greeter's line comes first, then the
listener's, because the reply is a second message and cannot overtake the
handler that sends it. Both actors stop when the system terminates.

Run it with:

```
uv run python -m tapio_examples.hello_world
```
"""

import asyncio
from collections.abc import Callable

from tapio import ActorSystem, Behavior, Behaviors, Message
from tapio.actor import ActorContext, ActorRef

__all__ = ["Greet", "Greeted", "main"]


class Greeted(Message):
    """Sent back to whoever asked for the greeting."""

    whom: str


class Greet(Message):
    """A request to greet someone, carrying the address for the reply."""

    whom: str
    reply_to: ActorRef[Greeted]


def greeter(record: Callable[[str], None]) -> Behavior[Greet]:
    """Build the greeter: it greets, replies, and stays as it is.

    Args:
        record: Where to write the greeting, so the example can be asserted.

    Returns:
        The behavior to spawn.
    """

    async def on_greet(ctx: ActorContext[Greet], message: Greet) -> Behavior[Greet]:
        ctx.log.info("hello, %s!", message.whom)
        record(f"greeter: hello, {message.whom}!")
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
        lines.append(f"listener: {message.whom} has been greeted")
        # Handing a result back out to non-actor code. A future is the honest
        # way to do that from inside a handler, and `ask` wraps this pattern up
        # once the request/response shape is familiar.
        done.set_result(None)
        return Behaviors.same()

    async with ActorSystem("hello") as system:
        listener = system.spawn(Behaviors.receive_message(on_greeted), name="listener")
        hello = system.spawn(greeter(lines.append), name="greeter")

        hello.tell(Greet(whom="world", reply_to=listener))
        await done

    for line in lines:
        print(line)
    return lines


if __name__ == "__main__":
    asyncio.run(main())
