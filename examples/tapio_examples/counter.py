"""An actor that holds mutable state, written in the class-based style.

Concepts: `AbstractBehavior` for an actor with fields, a union message type, and
answering a query by sending to the `reply_to` address the query carried.

The count is an ordinary attribute, mutated in place with no lock anywhere. One
actor processes one message at a time, so the mailbox already provides the
mutual exclusion a lock would.

What to watch in the output: the reply reports 3, not 1. Messages sent to one
actor from one place arrive in order, so all three increments are applied
before the query behind them is.

Run it with:

```
uv run python -m tapio_examples.counter
```
"""

import asyncio

from tapio import AbstractBehavior, ActorSystem, Behavior, Behaviors, Message
from tapio.actor import ActorContext, ActorRef

__all__ = ["Count", "Counter", "GetCount", "Increment", "main"]


class Count(Message):
    """The answer to a `GetCount`."""

    value: int


class Increment(Message):
    """Add to the count."""

    by: int = 1


class GetCount(Message):
    """Ask for the count, and say where to send it."""

    reply_to: ActorRef[Count]


class Counter(AbstractBehavior[Increment | GetCount]):
    """Counts, and reports the count when asked.

    The message type is read off the type parameter, so nothing has to repeat
    it, and a message of any other type is refused at the sender rather than
    landing in this mailbox.
    """

    def __init__(self, ctx: ActorContext[Increment | GetCount]) -> None:
        """Start at zero."""
        super().__init__(ctx)
        self._count = 0

    async def on_message(
        self, message: Increment | GetCount
    ) -> Behavior[Increment | GetCount]:
        """Apply an increment, or answer a query."""
        match message:
            case Increment(by=by):
                self._count += by
                self.ctx.log.debug("count is now %d", self._count)
            case GetCount(reply_to=reply_to):
                reply_to.tell(Count(value=self._count))
        return Behaviors.same()


async def main() -> int:
    """Run the example.

    Returns:
        The count the actor reported.
    """
    answer: asyncio.Future[int] = asyncio.get_running_loop().create_future()

    async def on_count(message: Count) -> Behavior[Count]:
        answer.set_result(message.value)
        return Behaviors.same()

    async with ActorSystem("counter") as system:
        readout = system.spawn(Behaviors.receive_message(on_count), name="readout")
        # Deferred construction: the factory runs when the actor starts, which
        # is what gives the behavior its context, and what a restart re-runs.
        counter = system.spawn(Behaviors.setup(Counter), name="counter")

        counter.tell(Increment())
        counter.tell(Increment(by=2))
        counter.tell(GetCount(reply_to=readout))
        value = await answer

    print(f"counter: {value}")
    return value


if __name__ == "__main__":
    asyncio.run(main())
