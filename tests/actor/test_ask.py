"""Ask: the reply that arrives, and the four ways one does not.

Most of this file is about the failures rather than the happy path, because the
happy path is one line of sugar over a `reply_to` field and the failures are
where an ask earns its keep. Three of them exist so that a caller never waits
out a timeout for an answer that provably is not coming, and the fourth is what
happens to a reply that arrives once nobody is listening.
"""

import asyncio
from datetime import timedelta

import pytest
from pydantic import ValidationError

from tapio import (
    ActorRef,
    ActorSystem,
    AskTargetTerminated,
    AskTimeoutError,
    AskTypeError,
    Behavior,
    Behaviors,
    DeadLetter,
    DeadLetterReason,
    Message,
    MessageTypeError,
    TapioSettings,
)
from tapio.actor import ActorContext, LocalActorRef
from tapio.testkit import assert_no_leaked_tasks
from tests.failures import eventually


class Answer(Message):
    """What a well-behaved responder replies with."""

    value: int


class Surprise(Message):
    """A reply of the wrong type, which is the responder's bug."""


class Query(Message):
    """Ask for an answer, and say where to send it."""

    value: int = 1
    reply_to: ActorRef[Answer]


class Misdirected(Message):
    """A request the responder does not accept, which is the asker's bug."""

    reply_to: ActorRef[Answer]


class Silence(Message):
    """Ask for nothing to happen, which is what makes a timeout testable."""

    reply_to: ActorRef[Answer]


Request = Query | Silence


def responder() -> Behavior[Request]:
    """Answers a `Query`, ignores a `Silence`, and stops when value is 0."""

    async def on_message(message: Request) -> Behavior[Request]:
        match message:
            case Query(value=0, reply_to=_):
                return Behaviors.stopped()
            case Query(value=value, reply_to=reply_to):
                reply_to.tell(Answer(value=value))
            case Silence():
                pass
        return Behaviors.same()

    return Behaviors.receive_message(on_message)


def wrong_replier() -> Behavior[Query]:
    """Replies with a `Surprise`, which is not what the asker declared."""

    async def on_message(message: Query) -> Behavior[Query]:
        # The ref is typed ActorRef[Answer], so this is a type error a checker
        # catches at the call site. The runtime has to catch it too, since a
        # responder can always be wrong at runtime.
        message.reply_to.tell(Surprise())  # type: ignore[arg-type]
        return Behaviors.same()

    return Behaviors.receive_message(on_message)


def late_replier(gate: asyncio.Event) -> Behavior[Query]:
    """Holds a reply until a test lets it go, which is after the ask is over."""

    async def on_message(message: Query) -> Behavior[Query]:
        await gate.wait()
        message.reply_to.tell(Answer(value=message.value))
        return Behaviors.same()

    return Behaviors.receive_message(on_message)


def stopper(gate: asyncio.Event) -> Behavior[Query]:
    """Stops instead of replying, once a test says so."""

    async def on_message(message: Query) -> Behavior[Query]:
        await gate.wait()
        return Behaviors.stopped()

    return Behaviors.receive_message(on_message)


def cell_of(ref: ActorRef[object]) -> object:
    """The cell behind a ref, for the tests that assert on runtime state."""
    assert isinstance(ref, LocalActorRef)
    return ref.cell


async def test_ask_returns_the_reply_object(system: ActorSystem):
    ref = system.spawn(responder(), name="responder")

    # The cells belong to the fixture, which stops them; what this block is
    # asserting is that the ask itself adds nothing to them.
    with assert_no_leaked_tasks():
        reply = await ref.ask(
            lambda reply_to: Query(value=7, reply_to=reply_to), expect=Answer
        )

    assert reply == Answer(value=7)


async def test_the_reply_is_the_object_the_responder_sent(system: ActorSystem):
    """Identity, not equality: ask is delivery, and delivery never copies."""
    sent: list[Answer] = []

    async def on_message(message: Query) -> Behavior[Query]:
        answer = Answer(value=message.value)
        sent.append(answer)
        message.reply_to.tell(answer)
        return Behaviors.same()

    ref = system.spawn(Behaviors.receive_message(on_message), name="responder")
    reply = await ref.ask(lambda reply_to: Query(reply_to=reply_to), expect=Answer)

    assert reply is sent[0]


async def test_a_timeout_names_the_target_and_the_request(system: ActorSystem):
    ref = system.spawn(responder(), name="quiet")

    with pytest.raises(AskTimeoutError) as caught:
        await ref.ask(
            lambda reply_to: Silence(reply_to=reply_to),
            expect=Answer,
            timeout=timedelta(milliseconds=20),
        )

    message = str(caught.value)
    assert "quiet" in message
    assert "Silence" in message
    assert "Answer" in message


async def test_a_timeout_is_a_builtin_timeout_error(system: ActorSystem):
    """An existing `except TimeoutError` keeps working."""
    ref = system.spawn(responder(), name="quiet")

    with pytest.raises(TimeoutError):
        await ref.ask(
            lambda reply_to: Silence(reply_to=reply_to),
            expect=Answer,
            timeout=timedelta(milliseconds=20),
        )


async def test_the_default_timeout_comes_from_settings():
    """A short default is honoured without the call site saying anything."""
    settings = TapioSettings(_env_file=None, ask_timeout=timedelta(milliseconds=20))
    async with ActorSystem("defaults", settings) as system:
        ref = system.spawn(responder(), name="quiet")

        with pytest.raises(AskTimeoutError):
            await ref.ask(lambda reply_to: Silence(reply_to=reply_to), expect=Answer)


async def test_timed_out_asks_leak_no_tasks_or_futures(system: ActorSystem):
    """The stress case: a hundred asks that all time out, and nothing left over.

    Futures are the half that a task check cannot see, so the target's watcher
    set is asserted too: one entry left behind per ask would be a leak that
    grows with traffic and never shows up as a pending task.
    """
    ref = system.spawn(responder(), name="quiet")
    cell = cell_of(ref)

    with assert_no_leaked_tasks():
        results = await asyncio.gather(
            *(
                ref.ask(
                    lambda reply_to: Silence(reply_to=reply_to),
                    expect=Answer,
                    timeout=timedelta(milliseconds=20),
                )
                for _ in range(100)
            ),
            return_exceptions=True,
        )

    assert all(isinstance(r, AskTimeoutError) for r in results)
    assert cell.watchers == ()


async def test_a_late_reply_dead_letters(system: ActorSystem):
    """The reply that arrives after the ask gave up resolves nothing.

    This is the case the promise has to be settled for: without it a future
    nobody is awaiting would be completed, and the reply would vanish instead
    of being accounted for.
    """
    seen: list[DeadLetter] = []
    system.dead_letters.subscribe(seen.append)
    gate = asyncio.Event()
    ref = system.spawn(late_replier(gate), name="slow")

    with pytest.raises(AskTimeoutError):
        await ref.ask(
            lambda reply_to: Query(value=3, reply_to=reply_to),
            expect=Answer,
            timeout=timedelta(milliseconds=20),
        )

    gate.set()
    await eventually(lambda: len(seen) == 1)

    assert seen[0].message == Answer(value=3)
    assert seen[0].reason == DeadLetterReason.ASK_SETTLED
    assert "promises" in seen[0].recipient


async def test_a_second_reply_dead_letters(system: ActorSystem):
    """One ask is one reply. The first wins and the rest are accounted for."""
    seen: list[DeadLetter] = []
    system.dead_letters.subscribe(seen.append)

    async def on_message(message: Query) -> Behavior[Query]:
        message.reply_to.tell(Answer(value=1))
        message.reply_to.tell(Answer(value=2))
        return Behaviors.same()

    ref = system.spawn(Behaviors.receive_message(on_message), name="eager")
    reply = await ref.ask(lambda reply_to: Query(reply_to=reply_to), expect=Answer)

    assert reply == Answer(value=1)
    await eventually(lambda: len(seen) == 1)
    assert seen[0].message == Answer(value=2)
    assert seen[0].reason == DeadLetterReason.ASK_SETTLED


async def test_a_target_that_stops_mid_ask_fails_fast(system: ActorSystem):
    """Well inside the timeout, which is the whole point of watching it."""
    gate = asyncio.Event()
    ref = system.spawn(stopper(gate), name="quitter")
    started = asyncio.get_running_loop().time()

    asking = asyncio.ensure_future(
        ref.ask(
            lambda reply_to: Query(reply_to=reply_to),
            expect=Answer,
            timeout=timedelta(seconds=30),
        )
    )
    await asyncio.sleep(0)
    gate.set()

    with pytest.raises(AskTargetTerminated) as caught:
        await asking

    elapsed = asyncio.get_running_loop().time() - started
    assert elapsed < 1.0
    assert "quitter" in str(caught.value)
    assert "Answer" in str(caught.value)


async def test_asking_an_already_stopped_actor_fails_at_once(system: ActorSystem):
    ref = system.spawn(responder(), name="gone")
    ref.tell(Query(value=0, reply_to=system.spawn(responder(), name="sink")))
    await eventually(lambda: not cell_of(ref).is_alive)

    with pytest.raises(AskTargetTerminated) as caught:
        await ref.ask(
            lambda reply_to: Query(reply_to=reply_to),
            expect=Answer,
            timeout=timedelta(seconds=30),
        )

    assert "gone" in str(caught.value)


async def test_a_reply_of_the_wrong_type_raises_in_the_asker(system: ActorSystem):
    """Naming both types, since the asker cannot see the responder's code."""
    ref = system.spawn(wrong_replier(), name="confused")

    with pytest.raises(AskTypeError) as caught:
        await ref.ask(
            lambda reply_to: Query(reply_to=reply_to),
            expect=Answer,
            timeout=timedelta(seconds=5),
        )

    message = str(caught.value)
    assert "Surprise" in message
    assert "Answer" in message
    assert "confused" in message


async def test_a_wrong_reply_does_not_fail_the_responder(system: ActorSystem):
    """The responder's mistake is the asker's error, not the responder's death.

    Raising into the responder would turn a caller's disappointment into a
    supervision decision about an actor that did its job as far as it knew.
    """
    ref = system.spawn(wrong_replier(), name="confused")

    with pytest.raises(AskTypeError):
        await ref.ask(lambda reply_to: Query(reply_to=reply_to), expect=Answer)

    assert cell_of(ref).is_alive


async def test_a_tampered_reply_is_rejected_on_arrival(system: ActorSystem):
    """A promise runs the same delivery-time check a cell does, contents too."""

    async def on_message(message: Query) -> Behavior[Query]:
        # Never validated, and of the right class, so only the check on
        # delivery can catch it.
        message.reply_to.tell(Answer.model_construct(value="not an int"))
        return Behaviors.same()

    ref = system.spawn(Behaviors.receive_message(on_message), name="sloppy")

    with pytest.raises(ValidationError):
        await ref.ask(lambda reply_to: Query(reply_to=reply_to), expect=Answer)


async def test_a_reply_from_another_thread_resolves_the_ask(system: ActorSystem):
    """Replying is thread-safe, like every other `tell`.

    An actor that offloads work to a thread and answers from there is the case
    this exists for: the future is resolved on the system's loop even though
    nothing about the reply happened there.
    """

    async def on_message(message: Query) -> Behavior[Query]:
        def answer_off_the_loop() -> None:
            message.reply_to.tell(Answer(value=message.value))

        await asyncio.to_thread(answer_off_the_loop)
        return Behaviors.same()

    ref = system.spawn(Behaviors.receive_message(on_message), name="threaded")
    reply = await ref.ask(
        lambda reply_to: Query(value=4, reply_to=reply_to), expect=Answer
    )

    assert reply == Answer(value=4)


async def test_offer_replies_like_tell(system: ActorSystem):
    """A promise has one future and no mailbox, so there is nothing to fill."""

    async def on_message(message: Query) -> Behavior[Query]:
        await message.reply_to.offer(Answer(value=message.value))
        return Behaviors.same()

    ref = system.spawn(Behaviors.receive_message(on_message), name="patient")
    reply = await ref.ask(
        lambda reply_to: Query(value=5, reply_to=reply_to), expect=Answer
    )

    assert reply == Answer(value=5)


async def test_a_wrong_request_raises_in_the_sender(system: ActorSystem):
    """An error about the message belongs to whoever wrote the message."""
    ref = system.spawn(responder(), name="responder")

    with pytest.raises(MessageTypeError):
        await ref.ask(
            lambda reply_to: Misdirected(reply_to=reply_to),  # type: ignore[arg-type]
            expect=Answer,
        )

    assert cell_of(ref).watchers == ()


async def test_an_unusable_reply_type_is_refused(system: ActorSystem):
    """`expect` goes through the same check a behavior's message type does."""
    ref = system.spawn(responder(), name="responder")

    with pytest.raises(MessageTypeError):
        await ref.ask(
            lambda reply_to: Query(reply_to=reply_to),
            expect=int,  # type: ignore[type-var]
        )


async def test_the_watch_is_released_when_the_ask_succeeds(system: ActorSystem):
    """A watcher per completed ask would be a leak proportional to traffic."""
    ref = system.spawn(responder(), name="responder")

    for _ in range(5):
        await ref.ask(lambda reply_to: Query(reply_to=reply_to), expect=Answer)

    assert cell_of(ref).watchers == ()


async def test_a_cancelled_ask_leaves_nothing_behind(system: ActorSystem):
    ref = system.spawn(responder(), name="quiet")
    cell = cell_of(ref)

    asking = asyncio.ensure_future(
        ref.ask(
            lambda reply_to: Silence(reply_to=reply_to),
            expect=Answer,
            timeout=timedelta(seconds=30),
        )
    )
    await asyncio.sleep(0)
    asking.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asking

    assert cell.watchers == ()


async def test_an_actor_can_ask_from_inside_a_handler(system: ActorSystem):
    """The awaiting actor stops reading its mailbox, and the ask still works."""
    answers: list[int] = []

    def asker(target: ActorRef[Request]) -> Behavior[Query]:
        def build(ctx: ActorContext[Query]) -> Behavior[Query]:
            async def on_message(message: Query) -> Behavior[Query]:
                reply = await target.ask(
                    lambda reply_to: Query(value=message.value, reply_to=reply_to),
                    expect=Answer,
                )
                answers.append(reply.value)
                return Behaviors.same()

            return Behaviors.receive_message(on_message)

        return Behaviors.setup(build)

    target = system.spawn(responder(), name="responder")
    front = system.spawn(asker(target), name="front")
    sink = system.spawn(responder(), name="sink")

    with assert_no_leaked_tasks():
        front.tell(Query(value=9, reply_to=sink))
        await eventually(lambda: answers == [9])


async def test_ask_refuses_to_run_off_the_loop(system: ActorSystem):
    """A reply is resolved on the system's loop, so it cannot be awaited off it."""
    ref = system.spawn(responder(), name="responder")

    def from_another_thread() -> None:
        async def go() -> Answer:
            return await ref.ask(
                lambda reply_to: Query(reply_to=reply_to), expect=Answer
            )

        asyncio.run(go())

    with pytest.raises(RuntimeError, match="must run on the system's loop"):
        await asyncio.to_thread(from_another_thread)


def test_the_base_ref_cannot_ask():
    """The API is on the abstract ref, and says why it cannot serve it."""
    from tapio.actor import ActorPath

    ref: ActorRef[Query] = ActorRef(ActorPath.root("sys").child("user"))

    with pytest.raises(NotImplementedError, match="cannot deliver"):
        asyncio.run(
            ref.ask(lambda reply_to: Query(reply_to=reply_to), expect=Answer)  # type: ignore[arg-type]
        )
