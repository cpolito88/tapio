"""Helpers shared by the failure-semantics tests.

`test_supervision.py` and `test_death_watch.py` both need the same three
things: a message that can be told to fail, a behavior that records what it
saw, and a way to wait for something without sleeping for a fixed time.

`well_inside` is here for the same reason `eventually` is: a test that means
to say something about a deadline should say it about the deadline, rather
than against a number somebody picked.
"""

import asyncio
from collections.abc import Callable
from datetime import timedelta

from tapio import Behavior, Behaviors, Message
from tapio.actor import ActorContext, Signal, SupervisorStrategy


class Job(Message):
    """A unit of work that can be asked to blow up on arrival."""

    item: int = 0
    fail: bool = False


class BoomError(RuntimeError):
    """What a failing job raises, so a strategy can be keyed on it."""


class OtherError(RuntimeError):
    """A second failure type, for testing which strategy governs what."""


def recording(
    seen: list[str],
    *,
    strategy: SupervisorStrategy | None = None,
    on: type[Exception] | tuple[type[Exception], ...] = Exception,
    error: type[Exception] = BoomError,
    on_setup: Callable[[ActorContext[Job]], None] | None = None,
) -> Behavior[Job]:
    """A behavior that writes down everything that happens to it.

    Args:
        seen: Where to append what happened, in order.
        strategy: How to supervise its failures. Unsupervised when omitted,
            which means a failure stops it.
        on: Which exceptions the strategy governs.
        error: What a failing job raises.
        on_setup: Run inside `setup`, so it runs again on every restart. This
            is where a test spawns the children a restart is supposed to
            rebuild.

    Returns:
        The behavior.
    """

    def build(ctx: ActorContext[Job]) -> Behavior[Job]:
        seen.append("setup")
        if on_setup is not None:
            on_setup(ctx)

        async def on_message(message: Job) -> Behavior[Job]:
            if message.fail:
                raise error("boom")
            if message.item < 0:
                # A negative item means "stop now", so a test can end an actor
                # that supervision would otherwise keep restarting.
                return Behaviors.stopped()
            seen.append(f"job {message.item}")
            return Behaviors.same()

        async def on_signal(ctx: ActorContext[Job], signal: Signal) -> Behavior[Job]:
            seen.append(type(signal).__name__)
            return Behaviors.same()

        return Behaviors.receive_message(on_message, on_signal=on_signal)

    behavior: Behavior[Job] = Behaviors.setup(build)
    if strategy is None:
        return behavior
    return Behaviors.supervise(behavior).on_failure(strategy, on=on)


async def eventually(
    predicate: Callable[[], bool], *, within: float = 2.0, interval: float = 0.001
) -> None:
    """Wait until a predicate holds, rather than sleeping for a guessed time.

    Args:
        predicate: Checked after every yield to the loop.
        within: How long to keep checking before giving up.
        interval: How long to sleep between checks.

    Raises:
        AssertionError: If the predicate never held.
    """
    try:
        async with asyncio.timeout(within):
            # Polling. An event would be better, but there is none to wait on:
            # this waits for runtime state a test can only observe.
            while not predicate():  # noqa: ASYNC110
                await asyncio.sleep(interval)
    except TimeoutError:
        msg = f"condition never held within {within}s"
        raise AssertionError(msg) from None


def well_inside(bound: timedelta) -> float:
    """An upper bound for "this ended on the event, not on the clock".

    A test that configures a thirty-second deadline and then asserts against
    a literal second is measuring by stopwatch what it means to state about
    the deadline. This is a tenth of whichever timeout or ceiling the test
    configured: far enough from it that waiting one out cannot pass, and
    loose enough that four Python versions sharing a runner's CPU do not
    fail it.

    Args:
        bound: The timeout or ceiling the test configured.

    Returns:
        The number of seconds the measured wait has to stay under.
    """
    return bound.total_seconds() / 10
