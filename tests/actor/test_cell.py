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
from tapio.errors import BehaviorTypeError, MessageTypeError
from tapio.testkit import assert_no_leaked_tasks
from tests.failures import eventually
from tests.internals import cell_of
from tests.messages import Ping


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


async def _ignore(message: Ping) -> Behavior[Ping]:
    return Behaviors.same()


def _spawns_then_fails(
    kids: list[ActorRef[Ping]], *, returning: Behavior[Ping] | None = None
) -> Behavior[Ping]:
    """A setup that spawns a child and makes an adapter, then fails.

    With no `returning` it raises. Otherwise it returns that behavior, which
    the tests pass as one the actor cannot start with.
    """

    def build(ctx: ActorContext[Ping]) -> Behavior[Ping]:
        kids.append(ctx.spawn(Behaviors.receive_message(_ignore, msg_type=Ping), "kid"))
        ctx.message_adapter(lambda ping: ping, msg_type=Ping)
        if returning is None:
            raise RuntimeError("setup failed after spawning")
        return returning

    return Behaviors.setup(build)


async def test_a_setup_that_spawns_then_raises_leaves_nothing_running():
    kids: list[ActorRef[Ping]] = []
    with assert_no_leaked_tasks():
        system = ActorSystem("t")
        try:
            with pytest.raises(RuntimeError, match="after spawning"):
                system.spawn(_spawns_then_fails(kids), name="parent")

            kid = kids[0]
            assert system.refs.lookup(kid.path) is None
            assert not cell_of(kid).is_alive
            # Nothing under the failed actor is left in the registry either,
            # its adapter included.
            parent = kid.path.parent
            assert [
                p for p in system.refs.paths() if str(p).startswith(str(parent))
            ] == []
        finally:
            await system.terminate()
        assert not cell_of(kids[0]).is_alive


async def test_a_setup_that_spawns_then_returns_no_type_leaves_nothing_running():
    kids: list[ActorRef[Ping]] = []
    with assert_no_leaked_tasks():
        system = ActorSystem("t")
        try:
            with pytest.raises(BehaviorTypeError, match="carries no message type"):
                system.spawn(
                    _spawns_then_fails(kids, returning=Behaviors.same()), name="parent"
                )

            assert system.refs.lookup(kids[0].path) is None
            assert not cell_of(kids[0]).is_alive
        finally:
            await system.terminate()


async def test_the_name_of_an_actor_that_failed_to_start_is_free_again():
    kids: list[ActorRef[Ping]] = []
    async with ActorSystem("t") as system:
        with pytest.raises(RuntimeError):
            system.spawn(_spawns_then_fails(kids), name="parent")

        again = system.spawn(
            Behaviors.receive_message(_ignore, msg_type=Ping), "parent"
        )

        assert again.path.name == "parent"


class Other(Message):
    """A message the actors below do not declare."""


async def test_a_self_send_during_setup_is_type_checked():
    seen: list[int] = []

    async def record(message: Ping) -> Behavior[Ping]:
        seen.append(message.n)
        return Behaviors.same()

    def build(ctx: ActorContext[Ping]) -> Behavior[Ping]:
        ctx.self_ref.tell(Other())  # type: ignore[arg-type]
        return Behaviors.receive_message(record, msg_type=Ping)

    with assert_no_leaked_tasks():
        async with ActorSystem("t") as system:
            with pytest.raises(MessageTypeError, match="deferred construction"):
                system.spawn(Behaviors.setup(build), name="actor")

            await asyncio.sleep(0.01)
            assert seen == []


async def test_a_self_send_of_the_declared_type_during_setup_arrives():
    seen: list[int] = []

    async def record(message: Ping) -> Behavior[Ping]:
        seen.append(message.n)
        return Behaviors.same()

    def build(ctx: ActorContext[Ping]) -> Behavior[Ping]:
        # A message to itself from setup, to kick off its first piece of work.
        ctx.self_ref.tell(Ping(n=1))
        return Behaviors.receive_message(record, msg_type=Ping)

    async with ActorSystem("t") as system:
        system.spawn(Behaviors.setup(build), name="actor")

        await eventually(lambda: seen == [1])


async def test_a_setup_returned_from_a_handler_can_stop_the_actor(
    system: ActorSystem,
):
    seen: list[str] = []

    def nothing_to_run(ctx: ActorContext[Ping]) -> Behavior[Ping]:
        return Behaviors.stopped()

    async def on_message(ctx: ActorContext[Ping], message: Ping) -> Behavior[Ping]:
        seen.append(f"ping {message.n}")
        return Behaviors.setup(nothing_to_run)

    async def on_signal(ctx: ActorContext[Ping], signal: Signal) -> Behavior[Ping]:
        seen.append(type(signal).__name__)
        return Behaviors.same()

    actor = system.spawn(
        Behaviors.receive(on_message, Ping, on_signal=on_signal), name="actor"
    )
    actor.tell(Ping(n=1))
    actor.tell(Ping(n=2))

    await eventually(lambda: "PostStop" in seen)
    assert seen == ["ping 1", "PostStop"]
    await eventually(lambda: system.refs.lookup(actor.path) is None)


async def test_a_setup_returned_from_a_handler_can_keep_the_behavior(
    system: ActorSystem,
):
    seen: list[int] = []

    def nothing_new(ctx: ActorContext[Ping]) -> Behavior[Ping]:
        return Behaviors.same()

    async def on_message(message: Ping) -> Behavior[Ping]:
        seen.append(message.n)
        return Behaviors.setup(nothing_new)

    actor = system.spawn(
        Behaviors.receive_message(on_message, msg_type=Ping), name="actor"
    )
    actor.tell(Ping(n=1))
    actor.tell(Ping(n=2))

    await eventually(lambda: seen == [1, 2])
