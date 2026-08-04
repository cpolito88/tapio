"""Dead letters: where a message goes when nobody is there to receive it.

The point of the event stream is that an absence becomes observable. Without
it, "the message was dropped" and "the code under test never ran" look exactly
the same from outside, and every test here would be asserting nothing.
"""

import asyncio
import logging
import threading
from collections.abc import AsyncIterator
from datetime import timedelta

import pytest

from tapio.actor import (
    ActorRef,
    ActorSystem,
    Behavior,
    Behaviors,
    DeadLetter,
    DeadLetterReason,
    MailboxConfig,
    OverflowStrategy,
)
from tapio.actor.dead_letters import DeadLetterOffice
from tapio.actor.path import ActorPath
from tapio.errors import MailboxFullError, MessageTypeError
from tapio.settings import TapioSettings
from tapio.testkit import assert_no_leaked_tasks
from tests.messages import Greeted, Ping


class Clock:
    """A hand-wound monotonic clock, so throttling needs no sleeping."""

    def __init__(self) -> None:
        """Start at zero."""
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def office(
    log_first: int = 10, interval: float = 60.0
) -> tuple[DeadLetterOffice, Clock]:
    """An office with a clock the test winds by hand."""
    clock = Clock()
    return (
        DeadLetterOffice(log_first=log_first, summary_interval=interval, clock=clock),
        clock,
    )


async def accepts_anything(ping: Ping) -> Behavior[Ping]:
    """A handler that does nothing, for actors whose traffic never arrives."""
    return Behaviors.same()


def a_path() -> ActorPath:
    return ActorPath.root("sys").child("user").child("gone", uid=1)


# The office itself


def test_a_subscriber_sees_the_message_the_reason_and_the_recipient():
    sink, _ = office()
    seen: list[DeadLetter] = []
    sink.subscribe(seen.append)

    sink.publish(Ping(n=7), a_path(), DeadLetterReason.RECIPIENT_TERMINATED)

    assert len(seen) == 1
    assert seen[0].recipient == "tapio://sys/user/gone#1"
    assert seen[0].reason == DeadLetterReason.RECIPIENT_TERMINATED


def test_the_carried_message_keeps_its_type_and_its_fields():
    # A field annotated with the Message base class would otherwise be
    # revalidated *as* a bare Message, silently dropping every field of the
    # actual subclass. A dead letter that has lost its payload is worse than
    # no dead letter at all.
    sink, _ = office()
    seen: list[DeadLetter] = []
    sink.subscribe(seen.append)
    sent = Ping(n=7)

    sink.publish(sent, a_path(), DeadLetterReason.RECIPIENT_TERMINATED)

    assert seen[0].message is sent
    assert isinstance(seen[0].message, Ping)
    assert seen[0].model_dump()["message"] == {"n": 7}


def test_unsubscribing_stops_delivery():
    sink, _ = office()
    seen: list[DeadLetter] = []
    subscription = sink.subscribe(seen.append)

    sink.publish(Ping(n=1), a_path(), DeadLetterReason.RECIPIENT_TERMINATED)
    subscription.unsubscribe()
    sink.publish(Ping(n=2), a_path(), DeadLetterReason.RECIPIENT_TERMINATED)

    assert [event.message.n for event in seen] == [1]  # type: ignore[attr-defined]


def test_a_subscriber_that_raises_does_not_stop_the_others():
    sink, _ = office()
    seen: list[DeadLetter] = []

    def boom(event: DeadLetter) -> None:
        raise RuntimeError("subscriber is broken")

    sink.subscribe(boom)
    sink.subscribe(seen.append)

    sink.publish(Ping(n=1), a_path(), DeadLetterReason.RECIPIENT_TERMINATED)

    assert len(seen) == 1


def test_the_log_throttle_holds_under_a_hot_send_loop(caplog):
    sink, _clock = office(log_first=3, interval=60.0)
    with caplog.at_level(logging.WARNING):
        for n in range(1000):
            sink.publish(Ping(n=n), a_path(), DeadLetterReason.RECIPIENT_TERMINATED)

    # Three in full and no summary yet, because no time has passed.
    assert len(caplog.records) == 3
    assert sink.total == 1000


def test_a_summary_lands_once_the_interval_has_passed(caplog):
    sink, clock = office(log_first=1, interval=10.0)
    for n in range(100):
        sink.publish(Ping(n=n), a_path(), DeadLetterReason.RECIPIENT_TERMINATED)

    clock.now = 11.0
    caplog.clear()
    with caplog.at_level(logging.WARNING):
        sink.publish(Ping(n=100), a_path(), DeadLetterReason.RECIPIENT_TERMINATED)

    assert len(caplog.records) == 1
    assert "in total" in caplog.records[0].message


# The delivery paths that produce them


@pytest.fixture
async def quick(settings: TapioSettings) -> AsyncIterator[ActorSystem]:
    """A system that gives up on a wedged actor at once.

    The overflow tests deliberately wedge an actor forever, and the default
    ten-second deadline would otherwise be paid three times over in shutdown.
    """
    running = ActorSystem(
        "test", settings.model_copy(update={"shutdown_timeout": timedelta(seconds=0)})
    )
    try:
        yield running
    finally:
        await running.terminate()


async def test_a_tell_to_a_stopped_actor_dead_letters(system: ActorSystem):
    seen: list[DeadLetter] = []
    system.dead_letters.subscribe(seen.append)
    ref = system.spawn(Behaviors.stopped(), name="already-gone")

    ref.tell(Ping(n=1))
    await asyncio.sleep(0)

    assert [event.reason for event in seen] == [DeadLetterReason.RECIPIENT_TERMINATED]


async def test_a_tell_after_shutdown_says_the_system_is_gone(
    settings: TapioSettings,
):
    with assert_no_leaked_tasks():
        system = ActorSystem("test", settings)
        seen: list[DeadLetter] = []
        system.dead_letters.subscribe(seen.append)
        ref = system.spawn(
            Behaviors.receive_message(accepts_anything, msg_type=Ping),
            name="worker",
        )
        await system.terminate()

        ref.tell(Ping(n=1))

    # A stopped actor and a stopped system are different diagnoses, and the
    # sender usually cares which.
    assert [event.reason for event in seen] == [DeadLetterReason.SYSTEM_TERMINATED]


async def test_messages_still_queued_when_an_actor_stops_are_accounted_for(
    system: ActorSystem,
):
    seen: list[DeadLetter] = []
    system.dead_letters.subscribe(seen.append)
    released = asyncio.Event()

    async def wait_then_stop(ping: Ping) -> Behavior[Ping]:
        await released.wait()
        return Behaviors.stopped()

    ref = system.spawn(
        Behaviors.receive_message(wait_then_stop, msg_type=Ping), name="slow"
    )
    for n in range(4):
        ref.tell(Ping(n=n))
    await asyncio.sleep(0)
    released.set()
    await asyncio.sleep(0.05)

    # The first was handled and the actor then stopped; the rest were queued
    # behind it and must not vanish with the mailbox.
    assert [event.message.n for event in seen] == [1, 2, 3]  # type: ignore[attr-defined]


# Bounded mailboxes, end to end


def blocked(strategy: OverflowStrategy) -> MailboxConfig:
    return MailboxConfig(capacity=2, on_overflow=strategy)


async def wedged_actor(
    system: ActorSystem, strategy: OverflowStrategy
) -> ActorRef[Ping]:
    """An actor that never finishes its first message, so its mailbox fills."""
    forever = asyncio.Event()

    async def never_returns(ping: Ping) -> Behavior[Ping]:
        await forever.wait()
        return Behaviors.same()

    ref = system.spawn(
        Behaviors.receive_message(never_returns, msg_type=Ping),
        name=f"wedged-{strategy.value}",
        mailbox=blocked(strategy),
    )
    ref.tell(Ping(n=0))
    await asyncio.sleep(0)  # let it pick up the first and park
    for n in (1, 2):
        ref.tell(Ping(n=n))
    return ref


async def test_fail_raises_in_the_sender_on_the_loop(quick: ActorSystem):
    ref = await wedged_actor(quick, OverflowStrategy.FAIL)
    with pytest.raises(MailboxFullError):
        ref.tell(Ping(n=3))


async def test_drop_new_dead_letters_the_arriving_message(quick: ActorSystem):
    seen: list[DeadLetter] = []
    quick.dead_letters.subscribe(seen.append)
    ref = await wedged_actor(quick, OverflowStrategy.DROP_NEW)

    ref.tell(Ping(n=3))

    assert [(e.message.n, e.reason) for e in seen] == [  # type: ignore[attr-defined]
        (3, DeadLetterReason.MAILBOX_FULL)
    ]


async def test_drop_oldest_dead_letters_the_head_of_the_queue(quick: ActorSystem):
    seen: list[DeadLetter] = []
    quick.dead_letters.subscribe(seen.append)
    ref = await wedged_actor(quick, OverflowStrategy.DROP_OLDEST)

    ref.tell(Ping(n=3))

    assert [(e.message.n, e.reason) for e in seen] == [  # type: ignore[attr-defined]
        (1, DeadLetterReason.MAILBOX_FULL)
    ]


# The off-loop split: the message is yours, the recipient is not


async def test_a_tell_from_another_thread_is_delivered(system: ActorSystem):
    received: list[int] = []
    done = asyncio.Event()

    async def collect(ping: Ping) -> Behavior[Ping]:
        received.append(ping.n)
        done.set()
        return Behaviors.same()

    ref = system.spawn(Behaviors.receive_message(collect, msg_type=Ping), name="sink")

    thread = threading.Thread(target=lambda: ref.tell(Ping(n=99)))
    thread.start()
    thread.join()
    async with asyncio.timeout(2):
        await done.wait()

    assert received == [99]


async def test_a_wrong_typed_tell_from_another_thread_raises_in_that_thread(
    system: ActorSystem,
):
    async def accept(ping: Ping) -> Behavior[Ping]:
        return Behaviors.same()

    ref = system.spawn(Behaviors.receive_message(accept, msg_type=Ping), name="typed")
    caught: list[BaseException] = []

    def send() -> None:
        try:
            ref.tell(Greeted(whom="wrong type"))  # type: ignore[arg-type]
        except BaseException as exc:
            caught.append(exc)

    thread = threading.Thread(target=send)
    thread.start()
    thread.join()

    # Errors about the message belong to whoever wrote the message, wherever
    # they are. Validation runs before the hop, on the calling thread.
    assert len(caught) == 1
    assert isinstance(caught[0], MessageTypeError)


async def test_a_full_fail_mailbox_dead_letters_across_a_thread_boundary(
    quick: ActorSystem,
):
    seen: list[DeadLetter] = []
    quick.dead_letters.subscribe(seen.append)
    ref = await wedged_actor(quick, OverflowStrategy.FAIL)

    caught: list[BaseException] = []

    def send() -> None:
        try:
            ref.tell(Ping(n=3))
        except BaseException as exc:  # pragma: no cover - the point is it is empty
            caught.append(exc)

    thread = threading.Thread(target=send)
    thread.start()
    thread.join()
    await asyncio.sleep(0.05)

    # Nothing can be raised into a thread that has moved on, so the recipient's
    # backpressure becomes a dead letter instead.
    assert caught == []
    assert [e.reason for e in seen] == [DeadLetterReason.MAILBOX_FULL]
