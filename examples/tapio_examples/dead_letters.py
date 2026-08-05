"""Where a message goes when nobody is there to receive it.

Concepts: the dead letter stream, subscribing to it, and the three ways a
message fails to arrive: a stopped actor, a full bounded mailbox, and a system
that has already shut down.

An `ActorRef` stays a valid handle after its actor dies, so `tell` never
raises about the recipient. An "is it alive?" check would be out of date as
soon as you had it. The price is messages with nowhere to go, and the point of
this example is that tapio accounts for every one of them instead of dropping
them quietly.

This example teaches an absence, and subscribing is what makes that possible.
Without the stream, "the message was dropped" and "this example is broken"
would look the same from outside.

What to watch in the output: three dead letters, each naming the message, the
actor it was addressed to, and why it did not arrive. The reasons differ, and
that difference is the useful part.

Run it with:

```
uv run python -m tapio_examples.dead_letters
```
"""

import asyncio

from tapio import ActorSystem, Behavior, Behaviors, Message
from tapio.actor import ActorRef, DeadLetter, MailboxConfig, OverflowStrategy

__all__ = ["Work", "main"]

CAPACITY = 2
"""How many messages the overloaded worker will hold before shedding."""


class Work(Message):
    """A unit of work, numbered so the output can be followed."""

    item: int


def busy(started: asyncio.Event, release: asyncio.Event) -> Behavior[Work]:
    """A worker that takes one item and then stalls, so its mailbox fills."""

    async def on_work(message: Work) -> Behavior[Work]:
        started.set()
        await release.wait()
        return Behaviors.same()

    return Behaviors.receive_message(on_work)


async def main() -> list[str]:
    """Run the example.

    Returns:
        One line per dead letter, in the order the system produced them.
    """
    lines: list[str] = []

    def record(letter: DeadLetter) -> None:
        lines.append(f"{letter.message!r} -> {letter.recipient} ({letter.reason})")

    system = ActorSystem("dead-letters")
    system.dead_letters.subscribe(record)

    # 1. A stopped actor. The ref is still a good handle and the send is still
    #    legal. There is simply nobody there to receive it.
    departed: ActorRef[Work] = system.spawn(Behaviors.stopped(), name="departed")
    departed.tell(Work(item=1))
    await asyncio.sleep(0)

    # 2. A bounded mailbox that overflows. DROP_OLDEST keeps the newest work
    #    and drops the oldest, which is what you want when only the latest
    #    reading matters. Whichever message it drops is accounted for.
    started, release = asyncio.Event(), asyncio.Event()
    worker = system.spawn(
        busy(started, release),
        name="worker",
        mailbox=MailboxConfig(
            capacity=CAPACITY, on_overflow=OverflowStrategy.DROP_OLDEST
        ),
    )
    worker.tell(Work(item=2))
    await started.wait()  # it is now stalled, holding item 2
    for item in (3, 4, 5):  # 3 and 4 fill the mailbox, 5 pushes 3 out
        worker.tell(Work(item=item))

    release.set()
    await system.terminate()

    # 3. A send after the system has gone. Still not an error, and still not
    #    silent. The reason names the system rather than the actor.
    worker.tell(Work(item=6))
    await asyncio.sleep(0)

    for line in lines:
        print(line)
    return lines


if __name__ == "__main__":
    asyncio.run(main())
