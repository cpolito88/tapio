"""Tests for the cell's own runtime behaviour, with a live system.

The directive behaviors, `empty()` and `ignore()`, carry no signal handler of
their own. The cell keeps delivering signals to the last real behavior anyway,
so an actor that drains its work and steps back still runs its stop hook and
still hears about a death it was watching.
"""

import asyncio
import logging
from typing import Any

import pytest

from tapio import ActorSystem, Behavior, Behaviors, Message
from tapio.actor import ActorContext, ActorRef, Signal
from tapio.testkit import assert_no_leaked_tasks
from tests.failures import eventually


class Drain(Message):
    """Tells the actor to step back to a directive behavior."""


class Watch(Message):
    """Carries the actor to watch, so the test can then stop it.

    Any ref, not an `ActorRef[Message]`: watching sends nothing to the target,
    so what it receives is not this message's business.
    """

    target: ActorRef[Any]


class Hold(Message):
    """Tells the actor to sit in its handler until something cancels it."""


def _stepping_back(seen: list[str], *, to: Behavior[Drain]) -> Behavior[Drain]:
    """A behavior that records its signals and becomes a directive on `Drain`."""

    async def on_message(ctx: ActorContext[Drain], message: Drain) -> Behavior[Drain]:
        seen.append("drained")
        return to

    async def on_signal(ctx: ActorContext[Drain], signal: Signal) -> Behavior[Drain]:
        seen.append(type(signal).__name__)
        return Behaviors.same()

    return Behaviors.receive(on_message, on_signal=on_signal)


async def test_an_actor_that_becomes_empty_still_runs_its_stop_hook(
    system: ActorSystem,
):
    seen: list[str] = []

    ref = system.spawn(_stepping_back(seen, to=Behaviors.empty()), name="holder")
    ref.tell(Drain())
    await eventually(lambda: seen == ["drained"])

    await system.terminate()

    # PostStop is where a held resource is released. An actor that stepped back
    # to empty() must still run it, or the resource leaks silently.
    assert seen == ["drained", "PostStop"]


async def test_an_actor_that_becomes_ignore_still_runs_its_stop_hook(
    system: ActorSystem,
):
    seen: list[str] = []

    ref = system.spawn(_stepping_back(seen, to=Behaviors.ignore()), name="holder")
    ref.tell(Drain())
    await eventually(lambda: seen == ["drained"])

    await system.terminate()

    assert seen == ["drained", "PostStop"]


async def test_an_actor_that_becomes_empty_still_hears_a_watched_death(
    system: ActorSystem,
):
    seen: list[str] = []

    async def on_message(ctx: ActorContext[Watch], message: Watch) -> Behavior[Watch]:
        ctx.watch(message.target)
        seen.append("watching")
        return Behaviors.empty()

    async def on_signal(ctx: ActorContext[Watch], signal: Signal) -> Behavior[Watch]:
        seen.append(type(signal).__name__)
        return Behaviors.same()

    async def be_stopped(message: Drain) -> Behavior[Drain]:
        return Behaviors.stopped()

    watcher = system.spawn(
        Behaviors.receive(on_message, on_signal=on_signal), name="watcher"
    )
    victim = system.spawn(Behaviors.receive_message(be_stopped), name="victim")

    watcher.tell(Watch(target=victim))
    await eventually(lambda: seen == ["watching"])
    victim.tell(Drain())

    # The watcher switched to empty() before the death. A Terminated dropped
    # here is a watcher that waits forever for an eviction it was promised.
    await eventually(lambda: "Terminated" in seen)
    assert seen == ["watching", "Terminated"]


def _stuck(entered: asyncio.Event, unwinding: asyncio.Event) -> Behavior[Hold]:
    """An actor that wedges in its handler and then unwinds slowly."""

    async def on_message(message: Hold) -> Behavior[Hold]:
        entered.set()
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            # Taking a moment to unwind is what gives the sweep a window to be
            # cancelled in. An actor that ended at once would leave none.
            unwinding.set()
            await asyncio.Event().wait()
        return Behaviors.same()

    return Behaviors.receive_message(on_message)


async def test_a_cancelled_shutdown_sweep_stops_rather_than_carrying_on(
    caplog: pytest.LogCaptureFixture,
):
    # Nothing public cancels a caller of `ActorCell.stop`: `terminate` shields
    # the drain, and the drain is a task the system owns. That is what keeps
    # this latent, and it is why the test runs the sweep itself.
    #
    # `stop` resumes work after the wait, so a cancellation aimed at the sweep
    # has to reach it. Swallowed, the sweep logged its warning, returned, and
    # its caller went on stopping the rest of the tree after being told to
    # stop. Both halves are asserted: the cancellation comes back out, and the
    # work that follows the wait did not happen.
    entered = asyncio.Event()
    unwinding = asyncio.Event()

    with assert_no_leaked_tasks():
        system = ActorSystem("sweep")
        try:
            system.spawn(_stuck(entered, unwinding), name="wedged").tell(Hold())
            await entered.wait()

            # A deadline already in the past, so the sweep reaches the wait
            # that follows it without the test sleeping through one.
            loop = asyncio.get_running_loop()
            cell = system._user._children["wedged"]
            with caplog.at_level(logging.WARNING, logger="tapio.actor"):
                sweep = asyncio.create_task(cell.stop(loop.time()))
                await unwinding.wait()

                sweep.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await sweep

            assert "did not stop within the shutdown deadline" not in caplog.text
        finally:
            await system.terminate()
