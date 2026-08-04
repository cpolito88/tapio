"""Where a message goes when nobody is there to receive it.

Concepts: the dead letter stream, subscribing to it, and the three ways a
message fails to arrive: a stopped actor, a full bounded mailbox, and a system
that has already shut down.

An `ActorRef` stays a valid handle after its actor dies, so `tell` never raises
about the recipient: a point-in-time "is it alive?" check is stale the moment
you have it. The price of that is messages with nowhere to go, and the whole
point of this example is that tapio accounts for every one of them rather than
dropping them quietly.

This is also the one example that teaches an *absence*. Subscribing is what
makes it teachable: without the stream, "the message was dropped" and "this
example is broken" would look exactly the same from outside.

What to watch in the output: three dead letters, each naming the message, the
actor it was addressed to, and why it did not arrive. The reasons differ, and
the difference is the useful part.

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

    # 1. A stopped actor. The ref is still a perfectly good handle, and the
    #    send is still legal; there is simply nobody home.
    departed: ActorRef[Work] = system.spawn(Behaviors.stopped(), name="departed")
    departed.tell(Work(item=1))
    await asyncio.sleep(0)

    # 2. A bounded mailbox that overflows. DROP_OLDEST keeps the newest work
    #    and sheds the stalest, which is what you want when only the latest
    #    reading matters. Whichever message it sheds is accounted for.
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

    # 3. A send after the system has gone. Still not an error, still not
    #    silent: the reason says the system rather than the actor.
    worker.tell(Work(item=6))
    await asyncio.sleep(0)

    for line in lines:
        print(line)
    return lines


if __name__ == "__main__":
    asyncio.run(main())
