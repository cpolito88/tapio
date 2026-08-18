"""A probe is an actor, and everything it asserts is asserted about a real one."""

from datetime import timedelta

import pytest

from tapio import Behavior, Behaviors, DeadLetter, Message
from tapio.actor import ActorRef, ActorSystem
from tapio.errors import MessageTypeError
from tapio.testkit import TestProbe
from tests.failures import eventually


class Greeted(Message):
    """What the greeter answers."""

    whom: str


class Greet(Message):
    """A request, carrying where the answer goes."""

    whom: str
    reply_to: ActorRef[Greeted]


class Retire(Message):
    """Tells an actor to stop through its own behavior."""


def greeter() -> Behavior[Greet]:
    """An actor that answers on the ref it was given."""

    async def on_greet(message: Greet) -> Behavior[Greet]:
        message.reply_to.tell(Greeted(whom=message.whom))
        return Behaviors.same()

    return Behaviors.receive_message(on_greet, msg_type=Greet)


def retiring() -> Behavior[Retire]:
    """An actor that stops the first time it is asked."""

    async def on_message(message: Retire) -> Behavior[Retire]:
        return Behaviors.stopped()

    return Behaviors.receive_message(on_message, msg_type=Retire)


async def test_a_probe_receives_what_it_is_sent(system: ActorSystem):
    probe: TestProbe[Greeted] = TestProbe(system, Greeted)
    hello = system.spawn(greeter(), "greeter")

    hello.tell(Greet(whom="world", reply_to=probe.ref))

    await probe.expect_message(Greeted(whom="world"))


async def test_a_probe_is_an_ordinary_actor_with_a_path(system: ActorSystem):
    probe: TestProbe[Greeted] = TestProbe(system, Greeted, name="replies")

    assert probe.path.name == "replies"
    # It is in the registry like anything else, which is what lets a ref to it
    # cross a link.
    assert system.refs.lookup(probe.path) is not None


async def test_a_message_of_the_wrong_type_dead_letters_at_a_probe(
    system: ActorSystem,
):
    letters: list[DeadLetter] = []
    system.dead_letters.subscribe(letters.append)
    probe: TestProbe[Greeted] = TestProbe(system, Greeted)

    # A probe validates on delivery like any other actor. Recording anything
    # sent to it would make the probe the one place the type contract is not
    # kept, which is the opposite of what a test helper should do.
    with pytest.raises(MessageTypeError):
        probe.ref.tell(Retire())  # type: ignore[arg-type]

    assert probe.pending == 0


async def test_expect_message_says_what_arrived_instead(system: ActorSystem):
    probe: TestProbe[Greeted] = TestProbe(system, Greeted)
    probe.tell(Greeted(whom="somebody else"))

    with pytest.raises(AssertionError, match="somebody else"):
        await probe.expect_message(Greeted(whom="world"))


async def test_expect_message_of_narrows_without_naming_the_contents(
    system: ActorSystem,
):
    probe: TestProbe[Greeted] = TestProbe(system, Greeted)
    system.spawn(greeter(), "greeter").tell(Greet(whom="world", reply_to=probe.ref))

    message = await probe.expect_message_of(Greeted)

    assert message.whom == "world"


async def test_a_probe_that_is_kept_waiting_fails_rather_than_hanging(
    system: ActorSystem,
):
    probe: TestProbe[Greeted] = TestProbe(system, Greeted)

    with pytest.raises(AssertionError, match="got nothing"):
        await probe.receive(timedelta(milliseconds=20))


async def test_expect_no_message_passes_when_nothing_is_sent(system: ActorSystem):
    probe: TestProbe[Greeted] = TestProbe(system, Greeted)

    await probe.expect_no_message(timedelta(milliseconds=20))


async def test_expect_no_message_fails_when_something_arrives(system: ActorSystem):
    probe: TestProbe[Greeted] = TestProbe(system, Greeted)
    probe.tell(Greeted(whom="world"))

    with pytest.raises(AssertionError, match="expected nothing"):
        await probe.expect_no_message(timedelta(milliseconds=20))


async def test_expect_no_message_leaves_a_later_message_for_receive(
    system: ActorSystem,
):
    """A message sent after the quiet window is still there for the next receive.

    expect_no_message consumes nothing when it passes, so the probe stays
    usable and a reply that lands just after the window closes is not lost.
    This pins the common case; a message arriving in the same instant the
    window times out is a tie this does not try to force.
    """
    probe: TestProbe[Greeted] = TestProbe(system, Greeted)

    await probe.expect_no_message(timedelta(milliseconds=20))
    probe.tell(Greeted(whom="world"))

    assert await probe.receive(timedelta(milliseconds=200)) == Greeted(whom="world")


async def test_a_probe_watches_and_expects_a_stop(system: ActorSystem):
    probe: TestProbe[Greeted] = TestProbe(system, Greeted)
    worker = system.spawn(retiring(), "worker")
    probe.watch(worker)

    worker.tell(Retire())

    signal = await probe.expect_terminated(worker)
    assert signal.ref.path == worker.path


async def test_expecting_the_wrong_actor_to_stop_says_which_one_did(
    system: ActorSystem,
):
    probe: TestProbe[Greeted] = TestProbe(system, Greeted)
    first = system.spawn(retiring(), "first")
    second = system.spawn(retiring(), "second")
    probe.watch(first)

    first.tell(Retire())

    with pytest.raises(AssertionError, match="second"):
        await probe.expect_terminated(second, timedelta(milliseconds=200))


async def test_a_probe_can_subscribe_to_dead_letters(system: ActorSystem):
    probe: TestProbe[DeadLetter] = TestProbe(system, DeadLetter)
    system.dead_letters.subscribe(probe.tell)
    worker = system.spawn(retiring(), "worker")
    worker.tell(Retire())
    await eventually_stopped(system, worker)

    worker.tell(Retire())

    # This is what makes an absence testable: the message did not arrive
    # anywhere, and the probe can say so.
    letter = await probe.expect_message_of(DeadLetter)
    assert letter.recipient == str(worker.path)


async def test_a_probe_stops_through_its_own_behavior(system: ActorSystem):
    watcher: TestProbe[Greeted] = TestProbe(system, Greeted)
    probe: TestProbe[Greeted] = TestProbe(system, Greeted)
    watcher.watch(probe.ref)

    probe.stop()

    await watcher.expect_terminated(probe.ref)


async def test_several_probes_need_no_names(system: ActorSystem):
    first: TestProbe[Greeted] = TestProbe(system, Greeted)
    second: TestProbe[Greeted] = TestProbe(system, Greeted)

    assert first.path != second.path
    assert "TestProbe(" in repr(first)


async def eventually_stopped(system: ActorSystem, ref: ActorRef[Retire]) -> None:
    """Wait until a ref no longer answers to its path and uid."""
    await eventually(lambda: system.refs.lookup(ref.path) is None)
