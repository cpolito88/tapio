"""Message adapters: taking delivery of someone else's protocol.

The assertions worth reading are the ones about *where* things happen. The
translation runs in the owning actor, so a failure in it is that actor's
supervision decision and never the sender's exception; the type check on the
way in belongs to the sender, like every other send; and a message that never
arrives is reported as what its sender sent rather than as the wrapper it
travelled in.
"""

import asyncio

import pytest
from pydantic import ValidationError

from tapio import (
    ActorSystem,
    Behavior,
    Behaviors,
    DeadLetter,
    DeadLetterReason,
    MailboxConfig,
    Message,
    MessageTypeError,
    OverflowStrategy,
    SupervisorStrategy,
)
from tapio.actor import ActorContext, ActorRef, LocalActorRef
from tapio.actor.adapter import AdaptedMessage
from tapio.errors import BehaviorTypeError
from tests.failures import BoomError, eventually


class Price(Message):
    """What the pricing service replies with. Not our protocol."""

    cents: int


class Quote(Message):
    """What the pricing service accepts."""

    reply_to: ActorRef[Price]


class Quoted(Message):
    """What we translate a `Price` into, which is our protocol."""

    cents: int


class Ask(Message):
    """Tell the shop to go and get a price."""


class Boom(Message):
    """Tell the shop to fail, so a restart can be observed."""


Shop = Ask | Quoted
Restarting = Boom | Quoted


def pricing(cents: int = 100) -> Behavior[Quote]:
    """A service that answers in its own vocabulary."""

    async def on_quote(message: Quote) -> Behavior[Quote]:
        message.reply_to.tell(Price(cents=cents))
        return Behaviors.same()

    return Behaviors.receive_message(on_quote)


def shop(
    seen: list[str],
    service: ActorRef[Quote],
    *,
    translate: str = "plain",
    strategy: SupervisorStrategy | None = None,
) -> Behavior[Shop]:
    """An actor that talks to `pricing` without admitting `Price` to its protocol."""

    def build(ctx: ActorContext[Shop]) -> Behavior[Shop]:
        seen.append("ready")

        def as_quoted(price: Price) -> Quoted:
            match translate:
                case "raises":
                    raise BoomError("the translation itself failed")
                case "wrong-type":
                    # A translation that produces something this actor never
                    # declared. The cell has to catch it: an adapter is the one
                    # way onto the lane that skipped the declared type.
                    return Price(cents=price.cents)  # type: ignore[return-value]
                case _:
                    return Quoted(cents=price.cents)

        replies = ctx.message_adapter(as_quoted)

        async def on_message(message: Shop) -> Behavior[Shop]:
            match message:
                case Ask():
                    service.tell(Quote(reply_to=replies))
                case Quoted(cents=cents):
                    seen.append(f"quoted {cents}")
            return Behaviors.same()

        return Behaviors.receive_message(on_message)

    behavior: Behavior[Shop] = Behaviors.setup(build)
    if strategy is None:
        return behavior
    return Behaviors.supervise(behavior).on_failure(strategy, on=Exception)


async def test_a_reply_in_another_protocol_arrives_translated(system: ActorSystem):
    seen: list[str] = []
    service = system.spawn(pricing(), name="pricing")
    ref = system.spawn(shop(seen, service), name="shop")

    ref.tell(Ask())
    await eventually(lambda: "quoted 100" in seen)


async def test_the_adapter_refuses_what_it_does_not_accept(system: ActorSystem):
    """The check belongs to the sender, exactly as it does for any `tell`."""
    adapters: list[ActorRef[Price]] = []

    def build(ctx: ActorContext[Shop]) -> Behavior[Shop]:
        adapters.append(ctx.message_adapter(lambda p: Quoted(cents=p.cents), Price))
        return Behaviors.receive_message(_ignore)

    system.spawn(Behaviors.setup(build), name="shop")
    await eventually(lambda: len(adapters) == 1)

    with pytest.raises(MessageTypeError, match="Ask"):
        adapters[0].tell(Ask())  # type: ignore[arg-type]


async def test_a_failing_translation_is_the_owners_failure(system: ActorSystem):
    """And never the sender's, which has not heard of the adapter.

    The whole reason the translation travels with the message instead of being
    applied at the send site: it is the owner's code, so it fails where the
    owner's code fails, and a strategy the owner declared governs it.
    """
    seen: list[str] = []
    service = system.spawn(pricing(), name="pricing")
    ref = system.spawn(
        shop(seen, service, translate="raises", strategy=SupervisorStrategy.restart()),
        name="shop",
    )

    ref.tell(Ask())
    await eventually(lambda: seen.count("ready") == 2)


async def test_a_translation_to_the_wrong_type_is_caught(system: ActorSystem):
    """An adapter is the one way onto the lane that skipped the declared type."""
    seen: list[str] = []
    service = system.spawn(pricing(), name="pricing")
    ref = system.spawn(
        shop(
            seen,
            service,
            translate="wrong-type",
            strategy=SupervisorStrategy.restart(),
        ),
        name="shop",
    )

    ref.tell(Ask())
    await eventually(lambda: seen.count("ready") == 2)


async def test_a_translated_message_keeps_its_place_in_the_queue(system: ActorSystem):
    """It rides the user lane, so it is ordinary traffic and nothing more."""
    seen: list[str] = []
    order: list[str] = []

    def build(ctx: ActorContext[Shop]) -> Behavior[Shop]:
        replies = ctx.message_adapter(lambda p: Quoted(cents=p.cents), Price)
        # Sent before the two below, so it is handled before them.
        replies.tell(Price(cents=1))

        async def on_message(message: Shop) -> Behavior[Shop]:
            order.append(type(message).__name__)
            return Behaviors.same()

        return Behaviors.receive_message(on_message)

    ref = system.spawn(Behaviors.setup(build), name="shop")
    ref.tell(Ask())
    ref.tell(Ask())
    await eventually(lambda: len(order) == 3)

    assert order == ["Quoted", "Ask", "Ask"]
    assert seen == []


async def test_an_adapter_outlives_a_restart(system: ActorSystem):
    """A ref somebody else is holding must not become a dead letter.

    An adapter addresses the actor, not the incarnation that handed it out, so
    a service still holding one from before a restart keeps being heard.
    """
    seen: list[str] = []
    handed_out: list[ActorRef[Price]] = []

    def build(ctx: ActorContext[Restarting]) -> Behavior[Restarting]:
        seen.append("ready")
        handed_out.append(ctx.message_adapter(lambda p: Quoted(cents=p.cents), Price))

        async def on_message(message: Restarting) -> Behavior[Restarting]:
            if isinstance(message, Boom):
                raise BoomError("boom")
            seen.append(f"quoted {message.cents}")
            return Behaviors.same()

        return Behaviors.receive_message(on_message)

    ref = system.spawn(
        Behaviors.supervise(Behaviors.setup(build)).on_failure(
            SupervisorStrategy.restart(), on=BoomError
        ),
        name="shop",
    )
    await eventually(lambda: len(handed_out) == 1)
    first = handed_out[0]

    ref.tell(Boom())
    await eventually(lambda: seen.count("ready") == 2)

    first.tell(Price(cents=7))
    await eventually(lambda: "quoted 7" in seen)


async def test_what_an_adapter_cannot_deliver_is_reported_as_it_was_sent():
    """The wrapper is how it travelled, not what was sent."""
    letters: list[DeadLetter] = []
    handed_out: list[ActorRef[Price]] = []

    async with ActorSystem("adapter-dead-letters") as system:
        system.dead_letters.subscribe(letters.append)

        stopped = asyncio.Event()

        def build(ctx: ActorContext[Shop]) -> Behavior[Shop]:
            handed_out.append(
                ctx.message_adapter(lambda p: Quoted(cents=p.cents), Price)
            )

            async def on_message(message: Shop) -> Behavior[Shop]:
                stopped.set()
                return Behaviors.stopped()

            return Behaviors.receive_message(on_message)

        ref = system.spawn(Behaviors.setup(build), name="shop")
        await eventually(lambda: len(handed_out) == 1)
        ref.tell(Ask())
        await stopped.wait()
        await eventually(lambda: not _alive(ref))

        handed_out[0].tell(Price(cents=3))
        await eventually(lambda: len(letters) == 1)

    assert letters[0].message == Price(cents=3)
    assert letters[0].reason == DeadLetterReason.RECIPIENT_TERMINATED


async def test_an_adapter_offers_into_the_owners_mailbox(system: ActorSystem):
    """`offer` waits for the owner's capacity, as it does through any ref."""
    handled = asyncio.Event()
    handed_out: list[ActorRef[Price]] = []

    def build(ctx: ActorContext[Shop]) -> Behavior[Shop]:
        handed_out.append(ctx.message_adapter(lambda p: Quoted(cents=p.cents), Price))

        async def on_message(message: Shop) -> Behavior[Shop]:
            handled.set()
            return Behaviors.same()

        return Behaviors.receive_message(on_message)

    system.spawn(
        Behaviors.setup(build),
        name="shop",
        mailbox=MailboxConfig(capacity=1, on_overflow=OverflowStrategy.FAIL),
    )
    await eventually(lambda: len(handed_out) == 1)

    await handed_out[0].offer(Price(cents=5))
    await handled.wait()


async def test_an_adapter_needs_to_know_what_it_accepts(system: ActorSystem):
    """A lambda carries no annotation, so it has to be told."""
    failures: list[Exception] = []

    def build(ctx: ActorContext[Shop]) -> Behavior[Shop]:
        try:
            ctx.message_adapter(lambda p: Quoted(cents=p.cents))
        except BehaviorTypeError as error:
            failures.append(error)
        return Behaviors.receive_message(_ignore)

    system.spawn(Behaviors.setup(build), name="shop")
    await eventually(lambda: len(failures) == 1)

    assert "msg_type" in str(failures[0])


async def test_every_adapter_gets_its_own_address(system: ActorSystem):
    """They are addressed under their owner, and no two collide."""
    handed_out: list[ActorRef[Price]] = []

    def build(ctx: ActorContext[Shop]) -> Behavior[Shop]:
        for _ in range(2):
            handed_out.append(
                ctx.message_adapter(lambda p: Quoted(cents=p.cents), Price)
            )
        return Behaviors.receive_message(_ignore)

    ref = system.spawn(Behaviors.setup(build), name="shop")
    await eventually(lambda: len(handed_out) == 2)

    first, second = handed_out
    assert first != second
    assert first.path.parent == ref.path.parent.child("shop")
    assert first.path.name.startswith("$adapter-")


async def _ignore(message: Shop) -> Behavior[Shop]:
    """Take a message and do nothing with it."""
    return Behaviors.same()


def _alive(ref: ActorRef[Shop]) -> bool:
    """Whether the cell behind a ref is still reading its mailbox.

    Only a test asks this. Application code watches, because a liveness answer
    is stale by the time the caller reads it.
    """
    return isinstance(ref, LocalActorRef) and ref.cell.is_alive


async def test_an_adapter_is_safe_from_another_thread(system: ActorSystem):
    """Like every `tell`: validate on the calling thread, then hop."""
    seen: list[str] = []
    handed_out: list[ActorRef[Price]] = []

    def build(ctx: ActorContext[Shop]) -> Behavior[Shop]:
        handed_out.append(ctx.message_adapter(lambda p: Quoted(cents=p.cents), Price))

        async def on_message(message: Shop) -> Behavior[Shop]:
            seen.append(type(message).__name__)
            return Behaviors.same()

        return Behaviors.receive_message(on_message)

    system.spawn(Behaviors.setup(build), name="shop")
    await eventually(lambda: len(handed_out) == 1)
    adapter = handed_out[0]

    await asyncio.to_thread(lambda: adapter.tell(Price(cents=9)))
    await eventually(lambda: seen == ["Quoted"])


async def test_offering_to_an_adapter_off_the_loop_is_refused(system: ActorSystem):
    """Awaiting capacity across a thread boundary is a bridge too far."""
    handed_out: list[ActorRef[Price]] = []

    def build(ctx: ActorContext[Shop]) -> Behavior[Shop]:
        handed_out.append(ctx.message_adapter(lambda p: Quoted(cents=p.cents), Price))
        return Behaviors.receive_message(_ignore)

    system.spawn(Behaviors.setup(build), name="shop")
    await eventually(lambda: len(handed_out) == 1)
    adapter = handed_out[0]

    def off_the_loop() -> None:
        asyncio.run(adapter.offer(Price(cents=1)))

    with pytest.raises(RuntimeError, match="must run on the system's loop"):
        await asyncio.to_thread(off_the_loop)


def test_an_adapted_message_carries_a_callable():
    """The one thing the wrapper's own validation is there to say."""
    with pytest.raises(ValidationError, match="carries a callable"):
        AdaptedMessage(payload=Price(cents=1), adapt=4)  # type: ignore[arg-type]


def test_the_wrapper_and_the_adapter_render_what_they_are():
    def as_quoted(price: Price) -> Quoted:
        return Quoted(cents=price.cents)

    wrapper = AdaptedMessage(payload=Price(cents=1), adapt=as_quoted)

    assert "as_quoted" in repr(wrapper)
    assert "Price(cents=1)" in repr(wrapper)
    # And a dump renders the function by name rather than raising, which is all
    # anyone would want from one.
    assert wrapper.model_dump()["adapt"].endswith("as_quoted")
