"""What happens between `ref.tell(message)` and a handler running.

The validation claim is the one that has to hold end to end: a message is
checked against the recipient's declared type on every send, its contents are
re-checked when that is switched on, and the recipient receives the object the
sender passed either way.
"""

import asyncio
import logging

import pytest
from pydantic import ValidationError

from tapio import (
    ActorSystem,
    Behavior,
    Behaviors,
    Message,
    MessageTypeError,
    TapioSettings,
)
from tapio.actor import ActorContext, ActorRef
from tests.messages import Greet, Greeted, Increment


def collecting(received: list[Message], arrived: asyncio.Event) -> Behavior[Greet]:
    """A behavior that records what it is given."""

    async def on_message(message: Greet) -> Behavior[Greet]:
        received.append(message)
        arrived.set()
        return Behaviors.same()

    return Behaviors.receive_message(on_message)


def tampered(ref: ActorRef[Greeted]) -> Greet:
    """A `Greet` that never went through validation, with a bad field."""
    message = Greet.model_construct(whom="ada", count=1, reply_to=ref)
    object.__setattr__(message, "count", "not an int")
    return message


async def test_the_recipient_gets_the_object_the_sender_sent(system: ActorSystem):
    received: list[Message] = []
    arrived = asyncio.Event()
    target = system.spawn(collecting(received, arrived), name="target")
    sent = Greet(whom="ada", count=1, reply_to=target)

    target.tell(sent)
    await asyncio.wait_for(arrived.wait(), timeout=1)

    assert received[0] is sent


async def test_a_tampered_message_is_rejected_on_send(system: ActorSystem):
    received: list[Message] = []
    target = system.spawn(collecting(received, asyncio.Event()), name="target")

    with pytest.raises(ValidationError):
        target.tell(tampered(target))

    assert received == []


async def test_content_validation_can_be_switched_off():
    # Turning it off changes cost and nothing else: the message still arrives,
    # and it is still the sender's object.
    settings = TapioSettings(validate_on_tell=False)
    received: list[Message] = []
    arrived = asyncio.Event()
    async with ActorSystem("lax", settings) as system:
        target = system.spawn(collecting(received, arrived), name="target")
        sent = tampered(target)

        target.tell(sent)
        await asyncio.wait_for(arrived.wait(), timeout=1)

    assert received[0] is sent


async def test_the_type_check_survives_validation_being_off():
    settings = TapioSettings(validate_on_tell=False)
    async with ActorSystem("lax", settings) as system:
        target = system.spawn(collecting([], asyncio.Event()), name="target")

        with pytest.raises(MessageTypeError, match="does not match"):
            target.tell(Increment())  # type: ignore[arg-type]


async def test_a_type_mismatch_names_the_target(system: ActorSystem):
    target = system.spawn(collecting([], asyncio.Event()), name="target")

    with pytest.raises(MessageTypeError, match=str(target.path)):
        target.tell(Increment())  # type: ignore[arg-type]


async def test_tell_to_a_stopped_actor_is_a_dead_letter_not_an_error(
    system: ActorSystem, caplog: pytest.LogCaptureFixture
):
    async def stop_on_first(message: Greet) -> Behavior[Greet]:
        return Behaviors.stopped()

    target = system.spawn(Behaviors.receive_message(stop_on_first), name="target")
    target.tell(Greet(whom="ada", count=1, reply_to=target))
    await asyncio.sleep(0.01)

    with caplog.at_level(logging.WARNING, logger="tapio.runtime"):
        target.tell(Greet(whom="ada", count=2, reply_to=target))

    assert "dead letter" in caplog.text
    assert str(target.path) in caplog.text


async def test_a_handler_that_raises_stops_only_that_actor(
    system: ActorSystem, caplog: pytest.LogCaptureFixture
):
    async def explode(message: Greet) -> Behavior[Greet]:
        msg = "scripted failure"
        raise RuntimeError(msg)

    received: list[Message] = []
    arrived = asyncio.Event()
    victim = system.spawn(Behaviors.receive_message(explode), name="victim")
    bystander = system.spawn(collecting(received, arrived), name="bystander")

    with caplog.at_level(logging.ERROR, logger="tapio.actor"):
        victim.tell(Greet(whom="ada", count=1, reply_to=victim))
        await asyncio.sleep(0.01)

    bystander.tell(Greet(whom="grace", count=1, reply_to=bystander))
    await asyncio.wait_for(arrived.wait(), timeout=1)

    assert "scripted failure" in caplog.text
    assert len(received) == 1


async def test_a_returned_behavior_replaces_the_current_one(system: ActorSystem):
    seen: list[str] = []
    arrived = asyncio.Event()

    async def second(message: Greet) -> Behavior[Greet]:
        seen.append(f"second:{message.whom}")
        arrived.set()
        return Behaviors.same()

    async def first(message: Greet) -> Behavior[Greet]:
        seen.append(f"first:{message.whom}")
        return Behaviors.receive_message(second)

    target = system.spawn(Behaviors.receive_message(first), name="switcher")
    target.tell(Greet(whom="one", count=1, reply_to=target))
    target.tell(Greet(whom="two", count=2, reply_to=target))
    await asyncio.wait_for(arrived.wait(), timeout=1)

    assert seen == ["first:one", "second:two"]


async def test_every_record_from_ctx_log_carries_the_actor_path(
    system: ActorSystem, caplog: pytest.LogCaptureFixture
):
    arrived = asyncio.Event()

    async def talkative(ctx: ActorContext[Greet], message: Greet) -> Behavior[Greet]:
        ctx.log.info("handling %s", message.whom)
        arrived.set()
        return Behaviors.same()

    target = system.spawn(Behaviors.receive(talkative), name="talkative")
    with caplog.at_level(logging.INFO, logger="tapio.actor"):
        target.tell(Greet(whom="ada", count=1, reply_to=target))
        await asyncio.wait_for(arrived.wait(), timeout=1)

    records = [r for r in caplog.records if r.getMessage().startswith(str(target.path))]
    assert records, caplog.text
    assert all(r.actor_path == str(target.path) for r in records)
