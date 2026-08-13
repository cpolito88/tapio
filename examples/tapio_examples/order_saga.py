"""Three steps that must all happen, and the unwinding when one does not.

Concepts: a saga as an actor, `ask` used as a sequence of steps, and
compensation as ordinary messages.

Payment, then inventory, then shipping. There is no transaction across the
three, because there is no database underneath them: they are separate
services, and once payment has taken the money there is no rolling it back by
not committing. What there is instead is a compensating action for each step,
and an actor whose job is to remember which steps have run.

The saga is a good fit for an actor for one reason. A transaction in flight is
state, and the mailbox means that state is touched by one message at a time.
The steps are awaited in turn, so the actor is parked while a service thinks,
and a second order waits in the queue instead of interleaving with the first.
No lock appears anywhere in here, and there is none to forget to take.

What to watch in the output: the last three lines. Shipping refused, and the
saga did not raise, did not retry, and did not stop. It walked back through
exactly the steps that had succeeded, in reverse, and reported a failed order
rather than a half-finished one. The step that never ran is not compensated,
which is why the list of what to undo is built as the saga goes rather than
written out in advance.

Run it with `uv run python -m tapio_examples.order_saga`.
"""

import asyncio
from collections.abc import Callable
from typing import TypeAlias

from tapio import ActorSystem, Behavior, Behaviors, Message
from tapio.actor import ActorRef

__all__ = [
    "Charge",
    "Order",
    "Outcome",
    "Refund",
    "Release",
    "Reserve",
    "Ship",
    "Stepped",
    "Unship",
    "main",
    "saga",
    "service",
]


class Stepped(Message):
    """What a service answers: whether it did the thing, and what it says."""

    step: str
    ok: bool
    detail: str


class Charge(Message):
    """Take the money."""

    order: str
    reply_to: ActorRef[Stepped]


class Refund(Message):
    """Give the money back. The compensation for `Charge`."""

    order: str


class Reserve(Message):
    """Hold the stock."""

    order: str
    reply_to: ActorRef[Stepped]


class Release(Message):
    """Put the stock back. The compensation for `Reserve`."""

    order: str


class Ship(Message):
    """Send the parcel."""

    order: str
    reply_to: ActorRef[Stepped]


class Unship(Message):
    """Recall the parcel. The compensation for `Ship`."""

    order: str


class Outcome(Message):
    """What became of an order, and what had to be undone to get there."""

    order: str
    ok: bool
    detail: str
    compensated: list[str]


class Order(Message):
    """Asks for the whole thing to happen, and says where the answer goes."""

    order: str
    reply_to: ActorRef[Outcome]


ServiceMessage: TypeAlias = Charge | Refund | Reserve | Release | Ship | Unship
"""Everything a stand-in service accepts: the three steps and their undos.

One behavior stands in for all three services here, so one ref type describes
all three. A real deployment would have three protocols and three refs, and
the saga below would read the same.
"""


Request: TypeAlias = Callable[[ActorRef[Stepped]], ServiceMessage]
"""Builds one step's request, given where the answer should go."""


def service(
    name: str, lines: list[str], *, refuses: bool = False
) -> Behavior[ServiceMessage]:
    """Build a stand-in for one downstream service.

    Args:
        name: What to call it in the output.
        lines: Where to write what happened.
        refuses: Whether it turns work down. A refusal, not a crash: this
            service is working correctly and the answer is no.

    Returns:
        The behavior to spawn.
    """

    async def on_message(message: ServiceMessage) -> Behavior[ServiceMessage]:
        if isinstance(message, Refund | Release | Unship):
            lines.append(f"saga: {name} undid its part of {message.order}")
            return Behaviors.same()
        if refuses:
            message.reply_to.tell(
                Stepped(step=name, ok=False, detail=f"{name} refused")
            )
            return Behaviors.same()
        lines.append(f"saga: {name} did its part of {message.order}")
        message.reply_to.tell(Stepped(step=name, ok=True, detail=f"{name} agreed"))
        return Behaviors.same()

    return Behaviors.receive_message(on_message, msg_type=ServiceMessage)


def saga(
    payments: ActorRef[ServiceMessage],
    inventory: ActorRef[ServiceMessage],
    shipping: ActorRef[ServiceMessage],
    lines: list[str],
) -> Behavior[Order]:
    """Build the actor that runs one order through the three steps.

    Each step is an `ask`, awaited in turn. That parks the saga while a
    service is thinking, which sounds like a cost and is the point: an order
    part-way through is state, and an actor that is awaiting is not reading
    its mailbox, so a second order waits in the queue rather than interleaving
    with the first. There is no lock in here and there is nothing to forget to
    take.

    Args:
        payments: The service that takes the money.
        inventory: The service that holds the stock.
        shipping: The service that sends the parcel.
        lines: Where to write what happened.

    Returns:
        The behavior to spawn.
    """

    async def on_order(message: Order) -> Behavior[Order]:
        # Built as the saga goes, never written out in advance. A step that
        # never ran has nothing to undo, and a step that refused did nothing
        # to undo either.
        done: list[str] = []
        steps: tuple[tuple[str, ActorRef[ServiceMessage], Request], ...] = (
            ("payments", payments, lambda to: Charge(order=message.order, reply_to=to)),
            (
                "inventory",
                inventory,
                lambda to: Reserve(order=message.order, reply_to=to),
            ),
            ("shipping", shipping, lambda to: Ship(order=message.order, reply_to=to)),
        )
        for name, service_ref, request in steps:
            answer = await service_ref.ask(request, expect=Stepped)
            if not answer.ok:
                # Walk back through what succeeded, newest first.
                for step in reversed(done):
                    _compensate(step, message.order, payments, inventory, shipping)
                message.reply_to.tell(
                    Outcome(
                        order=message.order,
                        ok=False,
                        detail=answer.detail,
                        compensated=list(reversed(done)),
                    )
                )
                return Behaviors.same()
            done.append(name)
        message.reply_to.tell(
            Outcome(
                order=message.order,
                ok=True,
                detail="every step agreed",
                compensated=[],
            )
        )
        return Behaviors.same()

    return Behaviors.receive_message(on_order, msg_type=Order)


def _compensate(
    step: str,
    order: str,
    payments: ActorRef[ServiceMessage],
    inventory: ActorRef[ServiceMessage],
    shipping: ActorRef[ServiceMessage],
) -> None:
    """Send one step's compensating message.

    Args:
        step: Which step to undo.
        order: Which order it was for.
        payments: The service that takes the money.
        inventory: The service that holds the stock.
        shipping: The service that sends the parcel.
    """
    if step == "payments":
        payments.tell(Refund(order=order))
    elif step == "inventory":
        inventory.tell(Release(order=order))
    else:
        shipping.tell(Unship(order=order))


async def main() -> list[str]:
    """Run the example.

    Returns:
        The lines the two orders produced, in order.
    """
    lines: list[str] = []
    async with ActorSystem("orders") as system:
        payments = system.spawn(service("payments", lines), "payments")
        inventory = system.spawn(service("inventory", lines), "inventory")
        # The one that says no. Not a crash: this service is working, and the
        # answer is no. Supervision has nothing to decide about a refusal.
        shipping = system.spawn(service("shipping", lines, refuses=True), "shipping")
        desk = system.spawn(saga(payments, inventory, shipping, lines), "saga")

        outcome = await desk.ask(
            lambda reply_to: Order(order="order-1", reply_to=reply_to), expect=Outcome
        )

        lines.append(f"saga: {outcome.order} failed because {outcome.detail}")
        lines.append(f"saga: undone, newest first: {', '.join(outcome.compensated)}")
        lines.append("saga: nothing was left half done, and nothing raised")

    for line in lines:
        print(line)
    return lines


if __name__ == "__main__":
    asyncio.run(main())
