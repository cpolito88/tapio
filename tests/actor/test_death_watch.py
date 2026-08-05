"""Tests for death watch: how one actor learns that another has stopped."""

import asyncio

import pytest

from tapio import ActorRef, ActorSystem, Behavior, Behaviors, WatchError
from tapio.actor import ActorContext, ActorPath, Signal
from tests.failures import Job, eventually, recording


def watching(
    target: ActorRef[Job],
    seen: list[str],
    *,
    gate: asyncio.Event | None = None,
    entered: asyncio.Event | None = None,
) -> Behavior[Job]:
    """A behavior that watches one actor from its setup and records what it sees."""

    def build(ctx: ActorContext[Job]) -> Behavior[Job]:
        ctx.watch(target)

        async def on_message(message: Job) -> Behavior[Job]:
            if gate is not None and message.item == 0:
                if entered is not None:
                    entered.set()
                await gate.wait()
            if message.item < 0:
                return Behaviors.stopped()
            seen.append(f"job {message.item}")
            return Behaviors.same()

        async def on_signal(ctx: ActorContext[Job], signal: Signal) -> Behavior[Job]:
            seen.append(type(signal).__name__)
            return Behaviors.same()

        return Behaviors.receive_message(on_message, on_signal=on_signal)

    return Behaviors.setup(build)


async def test_a_watcher_is_told_when_its_target_stops(system: ActorSystem):
    seen: list[str] = []

    target = system.spawn(recording([]), name="target")
    system.spawn(watching(target, seen), name="watcher")

    target.tell(Job(item=-1))
    await eventually(lambda: seen == ["Terminated"])


async def test_a_failure_is_a_stop_like_any_other(system: ActorSystem):
    seen: list[str] = []

    target = system.spawn(recording([]), name="target")
    system.spawn(watching(target, seen), name="watcher")

    # An unsupervised failure stops the actor. A watcher hears about the stop,
    # not the failure, because the absence is what it can act on.
    target.tell(Job(fail=True))
    await eventually(lambda: seen == ["Terminated"])


async def test_watching_twice_still_fires_once(system: ActorSystem):
    seen: list[str] = []

    target = system.spawn(recording([]), name="target")

    def build(ctx: ActorContext[Job]) -> Behavior[Job]:
        ctx.watch(target)
        ctx.watch(target)
        ctx.watch(target)

        async def on_signal(ctx: ActorContext[Job], signal: Signal) -> Behavior[Job]:
            seen.append(type(signal).__name__)
            return Behaviors.same()

        async def on_message(message: Job) -> Behavior[Job]:
            return Behaviors.same()

        return Behaviors.receive_message(on_message, on_signal=on_signal)

    system.spawn(Behaviors.setup(build), name="watcher")
    target.tell(Job(item=-1))
    await eventually(lambda: seen == ["Terminated"])
    await asyncio.sleep(0.01)

    assert seen == ["Terminated"]


async def test_watching_an_actor_that_has_already_stopped_fires_at_once(
    system: ActorSystem,
):
    seen: list[str] = []

    target = system.spawn(recording([]), name="target")
    target.tell(Job(item=-1))
    await eventually(lambda: not target.cell.is_alive)

    # The race cannot be avoided, so watching answers the same way whichever
    # side of it the caller lands on.
    system.spawn(watching(target, seen), name="watcher")
    await eventually(lambda: seen == ["Terminated"])


async def test_unwatch_stops_the_signal(system: ActorSystem):
    seen: list[str] = []

    target = system.spawn(recording([]), name="target")

    def build(ctx: ActorContext[Job]) -> Behavior[Job]:
        ctx.watch(target)
        ctx.unwatch(target)

        async def on_signal(ctx: ActorContext[Job], signal: Signal) -> Behavior[Job]:
            seen.append(type(signal).__name__)
            return Behaviors.same()

        async def on_message(message: Job) -> Behavior[Job]:
            seen.append(f"job {message.item}")
            return Behaviors.same()

        return Behaviors.receive_message(on_message, on_signal=on_signal)

    watcher = system.spawn(Behaviors.setup(build), name="watcher")
    target.tell(Job(item=-1))
    await eventually(lambda: not target.cell.is_alive)

    watcher.tell(Job(item=1))
    await eventually(lambda: "job 1" in seen)
    assert seen == ["job 1"]


async def test_a_stopped_watcher_leaves_nothing_behind(system: ActorSystem):
    target = system.spawn(recording([]), name="target")
    watcher = system.spawn(watching(target, []), name="watcher")
    await eventually(lambda: len(target.cell.watchers) == 1)

    watcher.tell(Job(item=-1))
    await eventually(lambda: not watcher.cell.is_alive)

    # This is the map users would otherwise write themselves and forget to
    # clean up. Both directions are released.
    assert target.cell.watchers == ()


async def test_an_actor_cannot_watch_itself(system: ActorSystem):
    raised: list[BaseException] = []

    def build(ctx: ActorContext[Job]) -> Behavior[Job]:
        try:
            ctx.watch(ctx.self_ref)
        except WatchError as error:
            raised.append(error)

        async def on_message(message: Job) -> Behavior[Job]:
            return Behaviors.same()

        return Behaviors.receive_message(on_message)

    system.spawn(Behaviors.setup(build), name="ouroboros")
    await eventually(lambda: len(raised) == 1)

    assert "cannot watch itself" in str(raised[0])


async def test_a_ref_with_no_actor_behind_it_cannot_be_watched(system: ActorSystem):
    raised: list[BaseException] = []
    detached: ActorRef[Job] = ActorRef(ActorPath.root("elsewhere").child("user"))

    def build(ctx: ActorContext[Job]) -> Behavior[Job]:
        try:
            ctx.watch(detached)
        except WatchError as error:
            raised.append(error)

        async def on_message(message: Job) -> Behavior[Job]:
            return Behaviors.same()

        return Behaviors.receive_message(on_message)

    system.spawn(Behaviors.setup(build), name="watcher")
    await eventually(lambda: len(raised) == 1)

    assert "not a ref to a live actor" in str(raised[0])


async def test_a_signal_overtakes_a_deep_user_backlog(system: ActorSystem):
    seen: list[str] = []
    gate, entered = asyncio.Event(), asyncio.Event()

    target = system.spawn(recording([]), name="target")
    watcher = system.spawn(
        watching(target, seen, gate=gate, entered=entered), name="watcher"
    )

    watcher.tell(Job(item=0))  # blocks in the handler until the gate opens
    await entered.wait()
    for item in range(1, 51):
        watcher.tell(Job(item=item))

    target.tell(Job(item=-1))
    await eventually(lambda: not target.cell.is_alive)
    gate.set()
    await eventually(lambda: "job 50" in seen)

    # Signals drain before user messages, so a death watch fires within the
    # current handler rather than after the whole mailbox. This is the
    # ordering rule that most often surprises people, so it is asserted.
    assert seen[:3] == ["job 0", "Terminated", "job 1"]


@pytest.mark.parametrize("stop_target_first", [True, False])
async def test_the_watcher_hears_once_however_the_race_goes(
    system: ActorSystem, stop_target_first: bool
):
    seen: list[str] = []

    target = system.spawn(recording([]), name="target")
    if stop_target_first:
        target.tell(Job(item=-1))
        await eventually(lambda: not target.cell.is_alive)

    system.spawn(watching(target, seen), name="watcher")
    if not stop_target_first:
        target.tell(Job(item=-1))

    await eventually(lambda: seen == ["Terminated"])
    await asyncio.sleep(0.01)
    assert seen == ["Terminated"]
