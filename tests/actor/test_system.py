"""The system: spawning, naming, and shutting the whole tree down."""

import asyncio
import logging
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
from tapio.testkit import assert_no_leaked_tasks
from tests.failures import BoomError
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
    settings = TapioSettings(shutdown_timeout=timedelta(seconds=0.2))
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

    # One deadline for the tree, not one per cell. Depth must not multiply it.
    assert elapsed < depth * 0.2
    assert "did not stop within the shutdown deadline" in caplog.text
    assert str(root.path) in caplog.text


async def test_terminate_is_idempotent_and_reported():
    system = ActorSystem("twice")
    system.spawn(idle(), name="worker")

    await asyncio.gather(system.terminate(), system.terminate())
    await system.when_terminated()

    assert system.is_terminating
    assert repr(system) == "ActorSystem('twice', terminating)"


async def test_a_system_needs_a_running_loop():
    with pytest.raises(RuntimeError):
        await asyncio.to_thread(ActorSystem, "off-loop")


async def test_the_shutdown_after_a_guardian_failure_is_a_task_the_system_holds():
    # A guardian failure terminates the tree from a task nobody awaits. The
    # loop keeps only a weak reference to a task, so the system has to keep
    # the strong one or a sweep can be collected halfway through. Asserted on
    # the attribute because a garbage collection this test could force would
    # not reliably reclaim it: the point is that nothing has to.
    with assert_no_leaked_tasks():
        system = ActorSystem("escalating")
        actor = system.spawn(
            Behaviors.supervise(failing()).on_failure(SupervisorStrategy.escalate()),
            name="worker",
        )
        actor.tell(Increment())

        with pytest.raises(BoomError):
            await system.when_terminated()

        held = system._terminator
        assert held is not None
        assert held.done()


def failing() -> Behavior[Increment]:
    """An actor that raises on the first message it gets."""

    async def on_message(message: Increment) -> Behavior[Increment]:
        raise BoomError("boom")

    return Behaviors.receive_message(on_message)
