"""The sharpest footgun in an asyncio actor system, and the one line that fixes it.

Concepts: `ctx.run_blocking`, shown against the same code without it, with the
damage counted rather than described.

Every actor in a system shares one event loop. An actor that calls something
blocking, a database driver, `requests`, `time.sleep`, a hash function on a
large file, does not just stall itself. It stalls every other actor in the
system, because none of them can run while that thread is inside the call.
Nothing raises and nothing is logged. The system simply stops for a while, and
in production that looks like latency nobody can explain.

`await ctx.run_blocking(fn, ...)` moves the call to a bounded pool of threads
that belongs to the system. The actor is still parked, and its own mailbox
still fills up behind the call, but the loop is free and everybody else keeps
working.

What to watch in the output: the two counts. The same call, the same duration,
and the rest of the system processed nothing in one case and dozens of
messages in the other. The elapsed times are printed for interest and asserted
by nothing: they depend on how loaded the machine is, and the count does not.

Run it with:

```
uv run python -m tapio_examples.blocking_offload
```
"""

import asyncio
import time
from datetime import timedelta

from tapio import ActorSystem, Behavior, Behaviors, Message
from tapio.actor import ActorContext, ActorRef, TimerScheduler

__all__ = ["Done", "Tick", "Work", "main", "ticker", "worker"]

CALL_SECONDS = 0.06
"""How long the blocking call takes. Short, because this is a test as well."""


class Tick(Message):
    """One unit of ordinary work for the rest of the system to get on with."""


class Done(Message):
    """The answer, and how much the rest of the system managed meanwhile."""

    offloaded: bool
    processed: int
    elapsed: float


class Work(Message):
    """Asks the worker to make one blocking call.

    Args:
        offload: Whether to move the call off the loop.
    """

    offload: bool
    reply_to: ActorRef[Done]


def ticker(ticks: list[int]) -> Behavior[Tick]:
    """Build the rest of the system: an actor with steady work arriving.

    A fixed-delay timer, at the shortest interval that is still a schedule
    rather than a spin. Every tick it records is a turn of the event loop that
    happened, which is exactly what the blocking call is about to take away.

    Fixed delay rather than fixed rate on purpose: a fixed-rate timer counts
    ticks off a schedule and sends the missed ones in a burst once a stall is
    over, which would credit the loop with work it did not do while it was
    blocked.

    Args:
        ticks: Where it records the work it got through.

    Returns:
        The behavior to spawn.
    """

    def build(timers: TimerScheduler[Tick]) -> Behavior[Tick]:
        timers.start_fixed_delay("work", Tick(), timedelta(milliseconds=1))

        async def on_message(message: Tick) -> Behavior[Tick]:
            ticks.append(len(ticks))
            return Behaviors.same()

        return Behaviors.receive_message(on_message, msg_type=Tick)

    return Behaviors.with_timers(build)


def worker(ticks: list[int]) -> Behavior[Work]:
    """Build the actor that makes the blocking call, one way or the other.

    It reads the tick count either side of the call. Both actors are in this
    one process, so the example can count directly what a production system
    would only see as unexplained latency.

    Args:
        ticks: The ticker's record, read but never written here.

    Returns:
        The behavior to spawn.
    """

    async def on_work(ctx: ActorContext[Work], message: Work) -> Behavior[Work]:
        before = len(ticks)
        started = time.perf_counter()
        if message.offload:
            # One line. The call runs on a pool thread, the loop keeps
            # turning, and this actor waits for the result like any other
            # await.
            await ctx.run_blocking(time.sleep, CALL_SECONDS)
        else:
            # The whole system is inside this call. Nothing else runs, no
            # message is delivered anywhere, and nothing says so. The linter
            # flags this line, and the linter is right: the noqa is here
            # because being wrong is the point of the example.
            time.sleep(CALL_SECONDS)  # noqa: ASYNC251
        message.reply_to.tell(
            Done(
                offloaded=message.offload,
                processed=len(ticks) - before,
                elapsed=time.perf_counter() - started,
            )
        )
        return Behaviors.same()

    return Behaviors.receive(on_work)


async def main() -> list[str]:
    """Run the example.

    Returns:
        The lines the two runs produced, in order.
    """
    lines: list[str] = []
    ticks: list[int] = []
    async with ActorSystem("blocking") as system:
        answers: asyncio.Queue[Done] = asyncio.Queue()

        async def on_done(message: Done) -> Behavior[Done]:
            answers.put_nowait(message)
            return Behaviors.same()

        sink = system.spawn(Behaviors.receive_message(on_done), "sink")
        system.spawn(ticker(ticks), "ticker")
        hand = system.spawn(worker(ticks), "worker")
        # Let the timer get going, so the count either side of the call is
        # about the call and not about the start-up.
        await asyncio.sleep(0.01)

        for offload in (False, True):
            hand.tell(Work(offload=offload, reply_to=sink))
            done = await answers.get()
            how = "run_blocking" if done.offloaded else "on the loop"
            lines.append(
                f"{how}: the rest of the system processed {done.processed} "
                f"messages during the call"
            )
            # Printed, never asserted. It depends on how loaded the machine
            # is; the count above does not.
            print(f"{how}: the call itself took {done.elapsed * 1000:.0f}ms")

        # The ticker's timer is still running. Shutdown cancels it with the
        # cell that owns it, which is what makes a tick from an actor that has
        # stopped impossible rather than unlikely.

    for line in lines:
        print(line)
    return lines


if __name__ == "__main__":
    asyncio.run(main())
