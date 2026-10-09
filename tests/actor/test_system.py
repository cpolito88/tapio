"""The system: spawning, naming, and shutting the whole tree down."""

import asyncio
import logging
import time
from datetime import timedelta
from typing import Any

import pytest

from tapio import (
    ActorNameError,
    ActorSystem,
    ActorSystemTerminating,
    Behavior,
    Behaviors,
    TapioSettings,
)
from tapio.actor import ActorContext, SupervisorStrategy
from tapio.testkit import IsolatedTapioSettings, assert_no_leaked_tasks
from tests.failures import BoomError, well_inside
from tests.messages import Increment


def counting(
    counter: list[int], target: int, done: asyncio.Event
) -> Behavior[Increment]:
    """A behavior that counts what it receives and reports when it has enough."""

    async def on_message(message: Increment) -> Behavior[Increment]:
        counter.append(message.by)
        if len(counter) >= target:
            done.set()
        return Behaviors.same()

    return Behaviors.receive_message(on_message)


def idle() -> Behavior[Increment]:
    """A behavior that accepts messages and does nothing with them."""

    async def on_message(message: Increment) -> Behavior[Increment]:
        return Behaviors.same()

    return Behaviors.receive_message(on_message)


async def test_actors_spawn_under_the_user_guardian(system: ActorSystem):
    actor = system.spawn(idle(), name="worker")

    assert actor.path.elements == ("user", "worker")
    assert actor.path.system == "test"


async def test_a_thousand_actors_exchange_messages_and_shut_down_clean():
    failures: list[dict[str, Any]] = []
    asyncio.get_running_loop().set_exception_handler(
        lambda loop, context: failures.append(context)
    )
    counted: list[int] = []
    done = asyncio.Event()

    with assert_no_leaked_tasks():
        async with ActorSystem("swarm") as system:
            fanout = [
                system.spawn(counting(counted, 1000, done), name=f"worker-{i}")
                for i in range(1000)
            ]
            for actor in fanout:
                actor.tell(Increment(by=1))
            await asyncio.wait_for(done.wait(), timeout=10)

    assert len(counted) == 1000
    assert failures == []


async def test_names_are_unique_among_live_siblings(system: ActorSystem):
    system.spawn(idle(), name="worker")

    with pytest.raises(ActorNameError, match="already has a live child"):
        system.spawn(idle(), name="worker")


async def test_a_name_is_free_again_once_its_actor_stops(system: ActorSystem):
    async def stop_now(message: Increment) -> Behavior[Increment]:
        return Behaviors.stopped()

    first = system.spawn(Behaviors.receive_message(stop_now), name="worker")
    first.tell(Increment())
    await asyncio.sleep(0.01)

    second = system.spawn(idle(), name="worker")

    # Same name, new incarnation. The uid is what stops a stale ref from
    # addressing the new actor.
    assert second.path.name == "worker"
    assert second.path.uid != first.path.uid
    assert second != first


async def test_anonymous_names_cannot_collide_with_chosen_ones(system: ActorSystem):
    first = system.spawn_anonymous(idle())
    second = system.spawn_anonymous(idle())

    assert first.path.name.startswith("$")
    assert first.path.name != second.path.name


async def test_spawning_after_shutdown_raises_and_leaves_nothing_running():
    with assert_no_leaked_tasks():
        system = ActorSystem("closing")
        await system.terminate()

        with pytest.raises(ActorSystemTerminating, match="shutting down"):
            system.spawn(idle(), name="latecomer")


async def test_spawning_from_a_handler_during_shutdown_raises(system: ActorSystem):
    gate = asyncio.Event()
    outcome: list[BaseException] = []

    async def spawn_late(
        ctx: ActorContext[Increment], message: Increment
    ) -> Behavior[Increment]:
        await gate.wait()
        try:
            ctx.spawn(idle(), name="child")
        except ActorSystemTerminating as exc:
            outcome.append(exc)
        return Behaviors.same()

    actor = system.spawn(Behaviors.receive(spawn_late), name="slow")
    actor.tell(Increment())
    await asyncio.sleep(0.01)

    shutdown = asyncio.create_task(system.terminate())
    await asyncio.sleep(0.01)
    gate.set()
    await shutdown

    assert isinstance(outcome[0], ActorSystemTerminating)


async def test_a_wedged_actor_is_cancelled_at_the_deadline(
    caplog: pytest.LogCaptureFixture,
):
    shutdown_timeout = timedelta(seconds=0.2)
    settings = TapioSettings(shutdown_timeout=shutdown_timeout)
    depth = 5

    def wedged(remaining: int) -> Behavior[Increment]:
        def build(ctx: ActorContext[Increment]) -> Behavior[Increment]:
            if remaining:
                ctx.spawn(wedged(remaining - 1), name="child")

            async def on_message(message: Increment) -> Behavior[Increment]:
                await asyncio.sleep(30)
                return Behaviors.same()

            # Block every actor in the chain, not just the top one, so the
            # measurement below is about the deadline and not one slow
            # handler.
            ctx.self_ref.tell(Increment())
            return Behaviors.receive_message(on_message)

        return Behaviors.setup(build)

    loop = asyncio.get_running_loop()
    with assert_no_leaked_tasks():
        system = ActorSystem("wedged", settings)
        root = system.spawn(wedged(depth), name="chain")
        await asyncio.sleep(0.05)

        started = loop.time()
        with caplog.at_level(logging.WARNING, logger="tapio.actor"):
            await system.terminate()
        elapsed = loop.time() - started

    # One deadline for the tree, not one per cell. Depth must not multiply it,
    # so the bound is the deadline times the depth: a per-cell deadline would
    # reach it and a shared one cannot.
    assert elapsed < depth * shutdown_timeout.total_seconds()
    assert "did not stop within the shutdown deadline" in caplog.text
    assert str(root.path) in caplog.text


async def test_terminate_finishes_when_a_restart_cancels_the_same_wedged_child():
    # Two stoppers on one child: the parent's restart, against its own
    # deadline, and the system drain, against a later one. The restart cancels
    # the child first. That cancellation is not the drain's, and it used to end
    # the drain, so terminate raised CancelledError and when_terminated hung.
    wedged = asyncio.Event()

    def child() -> Behavior[Increment]:
        async def on_message(message: Increment) -> Behavior[Increment]:
            wedged.set()
            await asyncio.Event().wait()
            return Behaviors.same()

        return Behaviors.receive_message(on_message)

    def parent(ctx: ActorContext[Increment]) -> Behavior[Increment]:
        ctx.spawn(child(), "kid").tell(Increment())

        async def on_message(message: Increment) -> Behavior[Increment]:
            raise BoomError("restart, and stop the wedged child")

        return Behaviors.receive_message(on_message)

    settings = IsolatedTapioSettings(shutdown_timeout=timedelta(milliseconds=300))
    with assert_no_leaked_tasks():
        system = ActorSystem("two-stoppers", settings)
        ref = system.spawn(
            Behaviors.supervise(Behaviors.setup(parent)).on_failure(
                SupervisorStrategy.restart()
            ),
            name="parent",
        )
        await wedged.wait()
        ref.tell(Increment())
        # A clock reading, not a synchronisation: the drain's deadline has to
        # fall after the restart's, so the restart is the one that cancels.
        await asyncio.sleep(0.1)

        async with asyncio.timeout(3.0):
            await system.terminate()
            await system.when_terminated()


async def test_a_cancelled_handlers_slow_cleanup_does_not_hold_terminate(
    caplog: pytest.LogCaptureFixture,
):
    # Cancelling a handler runs its `finally`, which can await for as long as
    # it likes. Shutdown gives that a bounded grace past the deadline and then
    # stops waiting for it.
    shutdown_timeout = timedelta(milliseconds=100)
    entered = asyncio.Event()
    release = asyncio.Event()
    cleaned_up = asyncio.Event()

    async def on_message(message: Increment) -> Behavior[Increment]:
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            try:
                await release.wait()
            finally:
                cleaned_up.set()
        return Behaviors.same()

    loop = asyncio.get_running_loop()
    with assert_no_leaked_tasks():
        system = ActorSystem(
            "slow-cleanup", IsolatedTapioSettings(shutdown_timeout=shutdown_timeout)
        )
        system.spawn(Behaviors.receive_message(on_message), name="worker").tell(
            Increment()
        )
        await entered.wait()

        started = loop.time()
        with caplog.at_level(logging.WARNING, logger="tapio.actor"):
            async with asyncio.timeout(5.0):
                await system.terminate()
        elapsed = loop.time() - started

        # The abandoned task is still the cell's and still cancelled, so it
        # ends one way or the other once nothing holds it.
        release.set()
        async with asyncio.timeout(2.0):
            await cleaned_up.wait()

    assert elapsed < shutdown_timeout.total_seconds() + 1.0 + 0.5
    assert "abandoned" in caplog.text


async def test_terminate_is_idempotent_and_reported():
    system = ActorSystem("twice")
    system.spawn(idle(), name="worker")

    await asyncio.gather(system.terminate(), system.terminate())
    await system.when_terminated()

    assert system.is_terminating
    assert repr(system) == "ActorSystem('twice', terminating)"


async def test_a_cancelled_terminate_does_not_wedge_the_system():
    # __aexit__ calls terminate(), and a process being shut down is exactly
    # when the enclosing task gets cancelled: a second SIGINT, a supervising
    # TaskGroup unwinding, a timeout around the drain. The shutdown the caller
    # asked for must still finish, because the drain is the system's task and
    # not the caller's.
    settings = TapioSettings(shutdown_timeout=timedelta(seconds=0.2))

    def wedged() -> Behavior[Increment]:
        async def on_message(message: Increment) -> Behavior[Increment]:
            await asyncio.sleep(30)
            return Behaviors.same()

        return Behaviors.receive_message(on_message)

    with assert_no_leaked_tasks():
        system = ActorSystem("cancelled-drain", settings)
        actor = system.spawn(wedged(), name="worker")
        # In a handler when the stop arrives, so terminate() has real awaits to
        # be cancelled at rather than returning at once.
        actor.tell(Increment())
        await asyncio.sleep(0.05)

        shutting = asyncio.create_task(system.terminate())
        await asyncio.sleep(0)
        shutting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await shutting

        # The drain the caller abandoned still runs to completion.
        async with asyncio.timeout(15):
            await system.when_terminated()
        assert system.is_terminating


async def test_a_system_needs_a_running_loop():
    with pytest.raises(RuntimeError):
        await asyncio.to_thread(ActorSystem, "off-loop")


async def test_the_shutdown_after_a_guardian_failure_is_a_task_the_system_holds():
    # A guardian failure terminates the tree from a task nobody awaits. The
    # loop keeps only a weak reference to a task, so the system has to keep the
    # strong one or a sweep can be collected halfway through. Asserted on the
    # attribute because a garbage collection this test could force would not
    # reliably reclaim it: the point is that nothing has to.
    with assert_no_leaked_tasks():
        system = ActorSystem("escalating")
        actor = system.spawn(
            Behaviors.supervise(failing()).on_failure(SupervisorStrategy.escalate()),
            name="worker",
        )
        actor.tell(Increment())

        with pytest.raises(BoomError):
            await system.when_terminated()

        # The drain carries the sweep to the end, and when_terminated returns
        # only once the drain has set the event, so it is finished here.
        held = system._draining
        assert held is not None
        assert held.done()


def failing() -> Behavior[Increment]:
    """An actor that raises on the first message it gets."""

    async def on_message(message: Increment) -> Behavior[Increment]:
        raise BoomError("boom")

    return Behaviors.receive_message(on_message)


async def test_terminate_awaited_inside_a_handler_does_not_wait_out_the_deadline(
    caplog: pytest.LogCaptureFixture,
):
    # The actor asking for the shutdown is part of the tree being stopped, so
    # waiting for the tree would wait on itself until the deadline cut it off.
    timeout = timedelta(seconds=2)
    with assert_no_leaked_tasks():
        system = ActorSystem("t", IsolatedTapioSettings(shutdown_timeout=timeout))
        returned: list[bool] = []

        async def on_message(message: Increment) -> Behavior[Increment]:
            await system.terminate()
            returned.append(True)
            return Behaviors.same()

        ref = system.spawn(
            Behaviors.receive_message(on_message, msg_type=Increment), "quitter"
        )
        started = time.monotonic()
        ref.tell(Increment())

        await system.when_terminated()

        assert time.monotonic() - started < well_inside(timeout)
        assert returned == [True]
        assert "did not stop within the shutdown deadline" not in caplog.text


async def test_terminate_awaited_in_a_task_an_actor_started_still_waits():
    # Only the actor's own task returns early. Any other task that awaits the
    # shutdown, even one started from inside a handler, waits for all of it.
    with assert_no_leaked_tasks():
        system = ActorSystem("t")
        waited: list[bool] = []
        tasks: list[asyncio.Task[None]] = []

        async def wait_for_it() -> None:
            await system.terminate()
            waited.append(system._terminated.is_set())

        async def on_message(message: Increment) -> Behavior[Increment]:
            tasks.append(asyncio.create_task(wait_for_it()))
            return Behaviors.same()

        ref = system.spawn(
            Behaviors.receive_message(on_message, msg_type=Increment), "starter"
        )
        ref.tell(Increment())

        await system.when_terminated()
        await asyncio.gather(*tasks)
        assert waited == [True]
