"""One address in front of several workers, and what happens when one dies.

Concepts: `Routers.pool`, round-robin fan-out, a bounded mailbox, and `offer`
as the way a producer is made to wait.

A pool router is the shape to reach for when the work is uniform and the answer
to "too slow" is "more of the same actor". The router is an ordinary actor: it
has a mailbox, it handles one message at a time, and all it does with each one
is hand it to the next routee in turn. The routees are its children, so their
failures are supervised the ordinary way, and their deaths shrink the pool it
routes to.

Backpressure is the part worth being precise about, because a router creates
none of it. Sending to a router never blocks, exactly as sending anywhere else
never blocks, so a fast producer against slow workers simply fills something
up. Which thing fills up is the mailbox's business, which is why the router
here has a bounded one and the producer uses `await router.offer(...)`: the
producer is throttled by the pool it is feeding rather than being allowed to
pile an unbounded backlog in front of it.

What to watch in the output: six jobs land on three workers in strict rotation,
never twice in a row on the same one. Then a worker is given a job it does not
survive, and the pool carries on with two: the router was told its routee
stopped and stopped routing to it, instead of sending work to an address nobody
reads.

Run it with:

```
uv run python -m tapio_examples.worker_pool
```
"""

import asyncio

from tapio import (
    ActorSystem,
    Behavior,
    Behaviors,
    MailboxConfig,
    Message,
    OverflowStrategy,
    Routers,
)
from tapio.actor import ActorContext, ActorRef, PostStop, Signal

__all__ = ["Done", "Job", "Stopped", "main"]

POOL_SIZE = 3
"""How many workers sit behind the one address."""

WORK_TIME = 0.005
"""How long a worker pretends each job takes."""


class Done(Message):
    """What a worker reports when it has finished a job."""

    worker: str
    item: int


class Stopped(Message):
    """What a worker reports on its way out."""

    worker: str


Report = Done | Stopped


class Job(Message):
    """A unit of work, and the address to report it to."""

    item: int
    reply_to: ActorRef[Report]
    poison: bool = False


def worker() -> Behavior[Job]:
    """One member of the pool.

    Wrapped in `Behaviors.setup` so that each routee gets its own. A pool built
    out of one already-constructed stateful behavior would share that state
    across every member, which is not a pool.
    """

    def build(ctx: ActorContext[Job]) -> Behavior[Job]:
        name = ctx.path.name
        # Where to report a stop of this worker's own choosing. An ordinary
        # shutdown is nobody's news, so it stays empty for one of those.
        dying: list[ActorRef[Report]] = []

        async def on_job(message: Job) -> Behavior[Job]:
            if message.poison:
                # A worker deciding it cannot go on. Its parent is the router,
                # which is watching it, so the pool shrinks by one.
                dying.append(message.reply_to)
                return Behaviors.stopped()
            await asyncio.sleep(WORK_TIME)
            message.reply_to.tell(Done(worker=name, item=message.item))
            return Behaviors.same()

        async def on_signal(_: ActorContext[Job], signal: Signal) -> Behavior[Job]:
            if isinstance(signal, PostStop) and dying:
                dying[0].tell(Stopped(worker=name))
            return Behaviors.same()

        return Behaviors.receive_message(on_job, on_signal=on_signal)

    return Behaviors.setup(build)


def collector(lines: list[str], marks: dict[int, asyncio.Event]) -> Behavior[Report]:
    """Writes down what every worker reported, so the run has one output.

    It also signals the points the script below waits for. An actor is the
    only thing that knows when it has handled something, so saying so beats
    sleeping for a guessed interval and hoping.
    """

    async def on_report(message: Report) -> Behavior[Report]:
        match message:
            case Done(worker=name, item=item):
                lines.append(f"{name}: job {item}")
            case Stopped(worker=name):
                lines.append(f"{name}: stopped, and left the pool")
        mark = marks.get(len(lines))
        if mark is not None:
            mark.set()
        return Behaviors.same()

    return Behaviors.receive_message(on_report)


async def main() -> list[str]:
    """Run the example.

    Returns:
        One line per thing a worker reported, in the order it happened.
    """
    lines: list[str] = []
    marks = {n: asyncio.Event() for n in (6, 7, 11)}

    async with ActorSystem("worker-pool") as system:
        reports = system.spawn(collector(lines, marks), name="collector")
        # The bounded mailbox is what gives `offer` something to wait for.
        # Unbounded, the producer below would hand over all six jobs before a
        # single one had been started.
        workers = system.spawn(
            Routers.pool(POOL_SIZE, worker()),
            name="workers",
            mailbox=MailboxConfig(capacity=2, on_overflow=OverflowStrategy.FAIL),
        )

        for item in range(1, 7):
            # Waits while the router is full, which is backpressure reaching
            # the producer from the pool it is feeding.
            await workers.offer(Job(item=item, reply_to=reports))
        await marks[6].wait()

        # Now take a worker out from under the router. The rotation is where
        # the first six jobs left it, so this lands on the first worker.
        await workers.offer(Job(item=0, reply_to=reports, poison=True))
        # A worker reports its own stop before the runtime tells its watchers,
        # and the router is one of them, so by the time this line is here the
        # router's `Terminated` is already queued ahead of anything sent next.
        await marks[7].wait()

        # The pool is two now, and the rotation carries on across what is left
        # rather than starting again.
        for item in range(7, 11):
            await workers.offer(Job(item=item, reply_to=reports))
        await marks[11].wait()

    for line in lines:
        print(line)
    return lines


if __name__ == "__main__":
    asyncio.run(main())
