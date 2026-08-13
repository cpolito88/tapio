"""A failure the actor that hit it cannot fix, handed to the one that can.

Concepts: `SupervisorStrategy.escalate()`, `ChildFailed`, a whole subtree being
rebuilt by its supervisor, and what happens when an escalation runs out of
supervisors.

A worker that cannot parse its input has no way to repair the pipeline it is
part of. Its parent, which built the pipeline, does. Escalating says that:
stop me, and make this your decision. The parent then takes its own decision,
which here is a restart, so the setup runs again and rebuilds every child
rather than only the one that broke.

Escalation is ordinary message flow, not an exception thrown across a task
boundary. The child stops itself and puts a signal on the parent's system
lane, which is why it can be ordered, observed and tested like anything else.

This example shows two more things. An actor outside the restarted subtree is
untouched, because a child failing must never cancel its siblings. That is why
the runtime uses no task group. And an escalation that reaches the guardian
has run out of actors willing to take responsibility, so the system terminates
and re-raises the cause from `when_terminated`. The service embedding tapio
then decides whether to exit or rebuild.

What to watch in the output: the ticker keeps counting across the restart, and
the second scenario ends with the original error, carrying the path it climbed
through.

Run it with `uv run python -m tapio_examples.escalation`.
"""

import asyncio

from tapio import ActorSystem, Behavior, Behaviors, Message
from tapio.actor import (
    ActorContext,
    ActorRef,
    PostStop,
    PreRestart,
    Signal,
    SupervisorStrategy,
)

__all__ = ["Parse", "Tick", "main"]


class Parse(Message):
    """A line for the worker to parse. An empty one is unparseable."""

    line: str


class Tick(Message):
    """A nudge for the ticker, which is here to keep working throughout."""


def worker(lines: list[str], parsed: asyncio.Event | None = None) -> Behavior[Parse]:
    """A parser that escalates rather than pretending it can recover."""

    def build(ctx: ActorContext[Parse]) -> Behavior[Parse]:
        lines.append("worker: ready")

        async def on_parse(message: Parse) -> Behavior[Parse]:
            if not message.line:
                lines.append("worker: cannot parse an empty line")
                msg = "empty input"
                raise ValueError(msg)
            lines.append(f"worker: parsed {message.line!r}")
            if parsed is not None:
                parsed.set()
            return Behaviors.same()

        async def on_signal(
            ctx: ActorContext[Parse], signal: Signal
        ) -> Behavior[Parse]:
            if isinstance(signal, PostStop):
                lines.append("worker: stopped")
            return Behaviors.same()

        return Behaviors.receive_message(on_parse, on_signal=on_signal)

    return Behaviors.supervise(Behaviors.setup(build)).on_failure(
        SupervisorStrategy.escalate(), on=ValueError
    )


def pipeline(
    lines: list[str],
    workers: list[ActorRef[Parse]],
    rebuilt: asyncio.Event,
    parsed: asyncio.Event,
) -> Behavior[Parse]:
    """A supervisor that builds its subtree in setup, and so rebuilds it on restart."""

    def build(ctx: ActorContext[Parse]) -> Behavior[Parse]:
        lines.append(f"pipeline: building, incarnation {len(workers) + 1}")
        # Spawned in setup, which is what makes this child come back. A child
        # spawned from a message handler would be gone until that message
        # arrives again.
        workers.append(ctx.spawn(worker(lines, parsed), name="worker"))
        if len(workers) > 1:
            rebuilt.set()

        async def on_parse(message: Parse) -> Behavior[Parse]:
            return Behaviors.same()

        async def on_signal(
            ctx: ActorContext[Parse], signal: Signal
        ) -> Behavior[Parse]:
            if isinstance(signal, PreRestart):
                lines.append("pipeline: restarting after the worker escalated")
            return Behaviors.same()

        return Behaviors.receive_message(on_parse, on_signal=on_signal)

    return Behaviors.supervise(Behaviors.setup(build)).on_failure(
        SupervisorStrategy.restart(max_restarts=3), on=ValueError
    )


def ticker(lines: list[str], ticks: list[int]) -> Behavior[Tick]:
    """An actor with no part in any of this, which is the point of it."""

    async def on_tick(ctx: ActorContext[Tick], message: Tick) -> Behavior[Tick]:
        ticks.append(len(ticks) + 1)
        lines.append(f"ticker: tick {len(ticks)}")
        return Behaviors.same()

    return Behaviors.receive(on_tick)


async def subtree_restarted_by_its_supervisor(lines: list[str]) -> None:
    """Run the first scenario: a worker escalates and its parent rebuilds."""
    workers: list[ActorRef[Parse]] = []
    ticks: list[int] = []
    rebuilt, parsed = asyncio.Event(), asyncio.Event()

    async with ActorSystem("escalation") as system:
        system.spawn(pipeline(lines, workers, rebuilt, parsed), name="pipeline")
        beat = system.spawn(ticker(lines, ticks), name="ticker")

        beat.tell(Tick())
        workers[0].tell(Parse(line=""))
        await rebuilt.wait()

        # The sibling never noticed. A failing actor stops only itself and,
        # through its supervisor's decision, that supervisor's subtree.
        beat.tell(Tick())
        workers[1].tell(Parse(line="ok"))
        await parsed.wait()


async def escalation_that_nobody_catches(lines: list[str]) -> None:
    """Run the second scenario: the escalation reaches the guardian."""
    system = ActorSystem("unsupervised")
    lonely = system.spawn(worker(lines), name="worker")
    lonely.tell(Parse(line=""))

    try:
        await system.when_terminated()
    except ValueError as error:
        lines.append(f"system: terminated by {error}")
        for note in getattr(error, "__notes__", []):
            lines.append(f"system: {note}")


async def main() -> list[str]:
    """Run the example.

    Returns:
        One line per thing that happened, in order.
    """
    lines: list[str] = []
    await subtree_restarted_by_its_supervisor(lines)
    await escalation_that_nobody_catches(lines)

    for line in lines:
        print(line)
    return lines


if __name__ == "__main__":
    asyncio.run(main())
