"""Asking for an answer, and the three ways of not getting one.

Concepts: `ref.ask`, `AskTimeoutError`, `AskTargetTerminated`, and where a
reply goes once nobody is waiting for it.

`ask` is sugar over the `reply_to` field `hello_world` starts with. The
request still carries a ref for the answer to come back to. What `ask` adds is
that the ref is a promise rather than an actor, so the caller can await the
reply instead of arranging to be told about it later.

The interesting part is what happens when no reply arrives. A timeout is the
expensive answer, so tapio avoids it when it can. An ask watches its target,
and a target that stops fails the ask at once instead of making the caller
wait out the deadline for an answer that cannot come. With the five-second
default, that is the difference between failing now and failing in five
seconds.

What to watch in the output: the third line is the answer the desk produced
for a lookup that had already timed out. It did not vanish, and it resolved
nothing, because there was no future left to resolve. It was recorded as a
dead letter instead. The fourth line is the fast failure: the desk closes
while a reader is waiting, and the reader hears about it immediately even
though it asked for thirty seconds of patience.

Run it with:

```
uv run python -m tapio_examples.ask_timeout
```
"""

import asyncio
from datetime import timedelta

from tapio import (
    ActorSystem,
    AskTargetTerminated,
    AskTimeoutError,
    Behavior,
    Behaviors,
    DeadLetter,
    Message,
)
from tapio.actor import ActorContext, ActorRef

__all__ = ["Close", "Lookup", "Shelf", "main"]

TIMEOUT = timedelta(milliseconds=50)
"""Short enough to keep the example quick, and the number the reader prints."""


class Shelf(Message):
    """Where a book is, which is what a lookup is answered with."""

    title: str
    shelf: int


class Lookup(Message):
    """Ask the desk where a book is, and say where to send the answer."""

    title: str
    reply_to: ActorRef[Shelf]


class Close(Message):
    """Tell the desk to shut, with whatever it was doing unfinished."""


def desk(catalogue: dict[str, int], stuck: asyncio.Event) -> Behavior[Lookup | Close]:
    """A reference desk that answers lookups, and stalls on one of them.

    The stall is an `await` inside the handler, which is the usual way an
    actor becomes slow: it is waiting on something outside itself. While it
    waits it reads nothing else, so everything behind it in the mailbox waits
    too.
    """

    def build(ctx: ActorContext[Lookup | Close]) -> Behavior[Lookup | Close]:
        async def on_message(message: Lookup | Close) -> Behavior[Lookup | Close]:
            match message:
                case Close():
                    ctx.log.info("closing with work outstanding")
                    return Behaviors.stopped()
                case Lookup(title=title, reply_to=reply_to):
                    if title not in catalogue:
                        # The slow path. No answer until something else
                        # happens, which from the asker's side looks the same
                        # as a desk that has died.
                        await stuck.wait()
                        reply_to.tell(Shelf(title=title, shelf=0))
                    else:
                        reply_to.tell(Shelf(title=title, shelf=catalogue[title]))
            return Behaviors.same()

        return Behaviors.receive_message(on_message)

    return Behaviors.setup(build)


async def main() -> list[str]:
    """Run the example.

    Returns:
        One line per thing the reader learned, in order.
    """
    lines: list[str] = []
    stuck = asyncio.Event()
    letters: asyncio.Queue[DeadLetter] = asyncio.Queue()

    async with ActorSystem("ask-timeout") as system:
        system.dead_letters.subscribe(letters.put_nowait)
        reference = system.spawn(desk({"Dune": 3}, stuck), name="desk")

        # The happy path: one call, one reply, and a value with a type.
        found = await reference.ask(
            lambda reply_to: Lookup(title="Dune", reply_to=reply_to),
            expect=Shelf,
            timeout=TIMEOUT,
        )
        lines.append(f"reader: '{found.title}' is on shelf {found.shelf}")

        # The desk stalls on this one, so the deadline is what ends the wait.
        try:
            await reference.ask(
                lambda reply_to: Lookup(title="Ulysses", reply_to=reply_to),
                expect=Shelf,
                timeout=TIMEOUT,
            )
        except AskTimeoutError:
            seconds = TIMEOUT.total_seconds()
            lines.append(f"reader: gave up on 'Ulysses' after {seconds:g}s")

        # The desk gets unstuck and answers a lookup nobody is waiting for any
        # more. The answer is not lost. It is recorded as a dead letter.
        stuck.set()
        letter = await letters.get()
        while not isinstance(letter.message, Shelf):
            letter = await letters.get()
        lines.append(f"dead letter: {type(letter.message).__name__} ({letter.reason})")

        # Now the desk closes with a reader still waiting. The ask allowed
        # thirty seconds and uses none of them, because it is watching the
        # desk and hears that it stopped.
        reference.tell(Close())
        try:
            await reference.ask(
                lambda reply_to: Lookup(title="Dune", reply_to=reply_to),
                expect=Shelf,
                timeout=timedelta(seconds=30),
            )
        except AskTargetTerminated:
            lines.append("reader: the desk closed, so there was no point waiting")

    for line in lines:
        print(line)
    return lines


if __name__ == "__main__":
    asyncio.run(main())
