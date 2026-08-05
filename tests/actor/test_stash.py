"""Tests for the stash: holding messages aside, and replaying them."""

import asyncio

import pytest

from tapio import (
    ActorSystem,
    Behavior,
    Behaviors,
    DeadLetter,
    DeadLetterReason,
    Message,
    StashBuffer,
    StashOverflowError,
    SupervisorStrategy,
)
from tapio.actor import ActorContext
from tests.failures import BoomError, eventually


class Work(Message):
    """A unit of work the actor can only do once it is ready."""

    item: int


class Ready(Message):
    """Tell the actor its state has arrived."""


class Fail(Message):
    """Ask the actor to blow up, so a restart can be observed."""


Traffic = Work | Ready | Fail


def loader(
    seen: list[str],
    *,
    capacity: int = 10,
    strategy: SupervisorStrategy | None = None,
) -> Behavior[Traffic]:
    """An actor that stashes work until it is told it is ready.

    This is the shape every "load state on startup" actor has. It cannot
    answer yet, it will not drop what arrives, and it will not block its own
    receive loop waiting.
    """

    def build(stash: StashBuffer[Traffic]) -> Behavior[Traffic]:
        def setup(ctx: ActorContext[Traffic]) -> Behavior[Traffic]:
            seen.append("loading")

            async def while_loading(message: Traffic) -> Behavior[Traffic]:
                match message:
                    case Ready():
                        seen.append("ready")
                        return stash.unstash_all(Behaviors.receive_message(when_ready))
                    case Fail():
                        raise BoomError("boom")
                    case Work():
                        stash.stash(message)
                return Behaviors.same()

            async def when_ready(message: Traffic) -> Behavior[Traffic]:
                match message:
                    case Work(item=item):
                        seen.append(f"work {item}")
                    case Fail():
                        raise BoomError("boom")
                    case Ready():
                        pass
                return Behaviors.same()

            return Behaviors.receive_message(while_loading)

        return Behaviors.setup(setup)

    behavior: Behavior[Traffic] = Behaviors.with_stash(capacity, build)
    if strategy is None:
        return behavior
    return Behaviors.supervise(behavior).on_failure(strategy, on=BoomError)


async def test_stashed_work_is_replayed_in_order(system: ActorSystem):
    seen: list[str] = []
    ref = system.spawn(loader(seen), name="loader")

    for item in (1, 2, 3):
        ref.tell(Work(item=item))
    ref.tell(Ready())
    await eventually(lambda: seen.count("work 3") == 1)

    assert seen == ["loading", "ready", "work 1", "work 2", "work 3"]


async def test_replayed_work_goes_ahead_of_what_queued_since(system: ActorSystem):
    """Why replay goes to the front of the mailbox and not the back.

    Messages held while the actor was not ready arrived first, so they are
    handled first. Putting them behind newer traffic would reorder the work
    the stash exists to preserve.
    """
    seen: list[str] = []
    ref = system.spawn(loader(seen), name="loader")

    ref.tell(Work(item=1))
    ref.tell(Work(item=2))
    ref.tell(Ready())
    ref.tell(Work(item=3))
    await eventually(lambda: seen.count("work 3") == 1)

    assert seen == ["loading", "ready", "work 1", "work 2", "work 3"]


async def test_a_stop_arriving_mid_replay_is_honoured():
    """Why a replay goes through the mailbox instead of a loop.

    Handing the held messages to the behavior one after another inside the
    unstash would make the replay uninterruptible. The actor would owe the
    whole backlog before it could read its own system lane. Putting them on
    the user lane leaves it an ordinary actor, so a stop still outranks work
    it is no longer going to do.
    """
    seen: list[str] = []
    replaying = asyncio.Event()

    def build(stash: StashBuffer[Traffic]) -> Behavior[Traffic]:
        async def while_loading(message: Traffic) -> Behavior[Traffic]:
            if isinstance(message, Ready):
                return stash.unstash_all(Behaviors.receive_message(when_ready))
            stash.stash(message)
            return Behaviors.same()

        async def when_ready(message: Traffic) -> Behavior[Traffic]:
            seen.append(type(message).__name__)
            replaying.set()
            # Slow enough that the terminate below lands while the rest of the
            # replayed backlog is still queued in front of the actor.
            await asyncio.sleep(0.02)
            return Behaviors.same()

        return Behaviors.receive_message(while_loading)

    running = ActorSystem("stash-stop-mid-replay")
    ref = running.spawn(Behaviors.with_stash(20, build), name="loader")
    for item in range(20):
        ref.tell(Work(item=item))
    ref.tell(Ready())

    await replaying.wait()
    await running.terminate()

    # It stopped instead of working through all twenty, and the ones it never
    # reached are reported as undelivered.
    assert 0 < len(seen) < 20


async def test_overflow_raises_in_the_stashing_actor(system: ActorSystem):
    """And the failure is a supervision decision, like any other."""
    seen: list[str] = []
    letters: list[DeadLetter] = []
    system.dead_letters.subscribe(letters.append)
    ref = system.spawn(loader(seen, capacity=2), name="loader")

    for item in (1, 2, 3):
        ref.tell(Work(item=item))

    # The third overflows and the actor has no strategy for it, so it stops.
    # The two it was holding are reported rather than dropped.
    await eventually(lambda: len(letters) >= 2)
    reasons = {letter.reason for letter in letters}

    assert DeadLetterReason.STASH_DISCARDED in reasons


def test_a_stash_refuses_a_capacity_below_one():
    with pytest.raises(ValueError, match="at least 1"):
        StashBuffer[Work](0)


def test_the_buffer_reports_what_it_holds():
    buffer: StashBuffer[Work] = StashBuffer(2)
    assert buffer.is_empty
    assert not buffer.is_full

    buffer.stash(Work(item=1))
    buffer.stash(Work(item=2))

    assert buffer.size == 2
    assert buffer.is_full
    with pytest.raises(StashOverflowError):
        buffer.stash(Work(item=3))

    assert buffer.take_all() == (Work(item=1), Work(item=2))
    assert buffer.is_empty


def test_the_buffer_holds_the_object_it_was_given():
    """Replay is delivery, and delivery never copies."""
    buffer: StashBuffer[Work] = StashBuffer(1)
    work = Work(item=1)
    buffer.stash(work)

    assert buffer.take_all()[0] is work


async def test_a_restart_clears_the_stash(system: ActorSystem):
    """The restart rule for the stash, asserted here beside the feature.

    Messages held by the state that just failed are not the new state's to
    answer. It never saw them arrive. They are discarded, and published as
    dead letters rather than dropped.
    """
    seen: list[str] = []
    letters: list[DeadLetter] = []
    system.dead_letters.subscribe(letters.append)
    ref = system.spawn(
        loader(seen, strategy=SupervisorStrategy.restart()), name="loader"
    )

    ref.tell(Work(item=1))
    ref.tell(Work(item=2))
    ref.tell(Fail())
    await eventually(lambda: seen.count("loading") == 2)

    discarded = [x for x in letters if x.reason == DeadLetterReason.STASH_DISCARDED]
    assert [x.message for x in discarded] == [Work(item=1), Work(item=2)]

    # And the new incarnation replays nothing when it becomes ready.
    ref.tell(Ready())
    ref.tell(Work(item=3))
    await eventually(lambda: seen.count("work 3") == 1)
    assert seen == ["loading", "loading", "ready", "work 3"]


async def test_stopping_accounts_for_what_was_still_held(system: ActorSystem):
    """Nobody is left to replay them, which is not a reason to lose them."""
    seen: list[str] = []
    letters: list[DeadLetter] = []
    system.dead_letters.subscribe(letters.append)

    async with ActorSystem("stash-stop") as inner:
        inner.dead_letters.subscribe(letters.append)
        ref = inner.spawn(loader(seen), name="loader")
        ref.tell(Work(item=1))
        await eventually(lambda: seen == ["loading"])

    discarded = [x for x in letters if x.reason == DeadLetterReason.STASH_DISCARDED]
    assert [x.message for x in discarded] == [Work(item=1)]
