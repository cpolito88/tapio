"""Running a call that blocks, without blocking every other actor."""

import asyncio
import threading
import time
from collections.abc import Awaitable, Callable

import pytest

from tapio import Behavior, Behaviors, Message
from tapio.actor import ActorContext, ActorRef, ActorSystem
from tapio.errors import ActorSystemTerminating
from tapio.settings import TapioSettings
from tapio.testkit import assert_no_leaked_tasks, assert_no_leaked_threads
from tests.failures import eventually


class Answer(Message):
    """What the worker sends back."""

    value: str


class Work(Message):
    """Asks the worker to block for a while, then answer."""

    seconds: float = 0.0
    fail: bool = False
    reply_to: ActorRef[Answer]


class Tick(Message):
    """A message the ticker counts, to show the loop kept running."""


def blocking_worker() -> Behavior[Work]:
    """An actor that does its blocking on a thread rather than on the loop."""

    async def on_work(ctx: ActorContext[Work], message: Work) -> Behavior[Work]:
        value = await ctx.run_blocking(_sleep_then, message.seconds, message.fail)
        message.reply_to.tell(Answer(value=value))
        return Behaviors.same()

    return Behaviors.receive(on_work)


def _sleep_then(seconds: float, fail: bool) -> str:
    """Block a real thread, which is the thing being arranged."""
    time.sleep(seconds)
    if fail:
        msg = "the blocking call raised"
        raise RuntimeError(msg)
    return threading.current_thread().name


async def test_a_blocking_call_runs_on_a_pool_thread(system: ActorSystem):
    answers: list[Answer] = []
    worker = system.spawn(blocking_worker(), "worker")
    sink = system.spawn(
        Behaviors.receive_message(_collect(answers), msg_type=Answer), "sink"
    )

    worker.tell(Work(seconds=0.0, reply_to=sink))

    await eventually(lambda: len(answers) == 1)
    # The name is the proof: it ran on this system's pool, not on the loop and
    # not on the default executor every other library shares.
    assert answers[0].value.startswith("tapio-blocking-test")


async def test_the_loop_keeps_running_while_a_call_blocks(system: ActorSystem):
    answers: list[Answer] = []
    ticks: list[int] = []
    worker = system.spawn(blocking_worker(), "worker")
    sink = system.spawn(
        Behaviors.receive_message(_collect(answers), msg_type=Answer), "sink"
    )
    ticker = system.spawn(
        Behaviors.receive_message(_count(ticks), msg_type=Tick), "ticker"
    )

    worker.tell(Work(seconds=0.05, reply_to=sink))
    for _ in range(20):
        ticker.tell(Tick())

    # Every tick is handled while the worker is still inside its sleep, which
    # is the whole point of the offload.
    await eventually(lambda: len(ticks) == 20)
    assert not answers
    await eventually(lambda: len(answers) == 1)


async def test_a_failing_call_raises_in_the_actor(system: ActorSystem):
    seen: list[str] = []

    async def on_work(ctx: ActorContext[Work], message: Work) -> Behavior[Work]:
        try:
            await ctx.run_blocking(_sleep_then, 0.0, True)
        except RuntimeError as error:
            seen.append(str(error))
        return Behaviors.same()

    worker = system.spawn(Behaviors.receive(on_work), "worker")
    sink = system.spawn(_sink(), "sink")

    worker.tell(Work(fail=True, reply_to=sink))

    # Re-raised where it was awaited, so it is this actor's failure and its
    # supervisor's decision like any other.
    await eventually(lambda: seen == ["the blocking call raised"])


async def test_a_system_that_never_blocks_starts_no_threads(system: ActorSystem):
    system.spawn(_sink(), "idle")

    assert not system.blocking.is_started
    assert system.blocking.threads == ()


async def test_the_pool_is_shut_down_with_the_system():
    with assert_no_leaked_threads(), assert_no_leaked_tasks():
        settings = TapioSettings(_env_file=None, blocking_pool_size=2)
        system = ActorSystem("closing", settings)
        answers: list[Answer] = []
        worker = system.spawn(blocking_worker(), "worker")
        sink = system.spawn(
            Behaviors.receive_message(_collect(answers), msg_type=Answer), "sink"
        )
        worker.tell(Work(seconds=0.01, reply_to=sink))
        await eventually(lambda: len(answers) == 1)
        assert system.blocking.is_started

        await system.terminate()

        assert system.blocking.threads == ()


async def test_the_pool_is_bounded_by_the_setting():
    settings = TapioSettings(_env_file=None, blocking_pool_size=2)
    system = ActorSystem("bounded", settings)
    try:
        started = threading.Barrier(3, timeout=0.2)

        def hold() -> str:
            # Three of these are submitted into a pool of two, so the barrier
            # can never be reached and the third call is still queued.
            try:
                started.wait()
            except threading.BrokenBarrierError:
                return "not all three ran at once"
            return "all three ran at once"

        results = await asyncio.gather(
            *(
                system.blocking.submit(asyncio.get_running_loop(), hold)
                for _ in range(3)
            )
        )

        assert results == ["not all three ran at once"] * 3
        assert len(system.blocking.threads) == 2
    finally:
        await system.terminate()


async def test_running_a_blocking_call_during_shutdown_is_refused(
    system: ActorSystem,
):
    refused: list[str] = []

    async def on_work(ctx: ActorContext[Work], message: Work) -> Behavior[Work]:
        try:
            await ctx.run_blocking(_sleep_then, 0.0, False)
        except ActorSystemTerminating as error:
            refused.append(str(error))
        return Behaviors.same()

    worker = system.spawn(Behaviors.receive(on_work), "worker")
    await system.blocking.shutdown(0.0, now=lambda: 1.0)

    worker.tell(Work(reply_to=system.spawn(_sink(), "sink")))

    await eventually(lambda: len(refused) == 1)
    assert "shutting down" in refused[0]


async def test_shutting_the_pool_down_twice_is_harmless(system: ActorSystem):
    await system.blocking.shutdown(0.0, now=lambda: 1.0)
    await system.blocking.shutdown(0.0, now=lambda: 1.0)

    assert not system.blocking.is_accepting


async def test_a_call_still_running_at_the_deadline_is_reported(
    system: ActorSystem, caplog: pytest.LogCaptureFixture
):
    release = threading.Event()
    running = threading.Event()

    def wedged() -> None:
        running.set()
        release.wait(timeout=5.0)

    future = system.blocking.submit(asyncio.get_running_loop(), wedged)
    await eventually(running.is_set)

    # A thread cannot be interrupted, so the deadline is where tapio stops
    # waiting rather than where the call stops running.
    await system.blocking.shutdown(0.0, now=lambda: 1.0)

    assert "cannot be interrupted" in caplog.text
    release.set()
    await future


def _sink() -> Behavior[Answer]:
    """An actor that takes answers and does nothing with them."""

    async def on_message(message: Answer) -> Behavior[Answer]:
        return Behaviors.same()

    return Behaviors.receive_message(on_message, msg_type=Answer)


def _collect(answers: list[Answer]) -> Callable[[Answer], Awaitable[Behavior[Answer]]]:
    async def on_message(message: Answer) -> Behavior[Answer]:
        answers.append(message)
        return Behaviors.same()

    return on_message


def _count(ticks: list[int]) -> Callable[[Tick], Awaitable[Behavior[Tick]]]:
    async def on_message(message: Tick) -> Behavior[Tick]:
        ticks.append(len(ticks))
        return Behaviors.same()

    return on_message
