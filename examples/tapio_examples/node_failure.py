"""Watching an actor on another node, and rebuilding when that node dies.

Concepts: `ctx.watch` on a ref that came from `resolve`, `Terminated` arriving
because a whole system went away, and a coordinator that reacts by rebuilding
the work somewhere it can still reach.

The watch is the same call as in `death_watch`, on a ref that happens to point
at another process. That is the whole of the API difference. The difference
that matters is in what the signal means. Locally, `Terminated` is a fact: the
actor stopped. Across a link it is a conclusion: this node stopped hearing
from that one. Here the peer really did terminate, so the conclusion is right,
but nothing in the signal says which of the two it was, and nothing can.

Supervision does not cross the wire, so the coordinator is not the remote
worker's supervisor and never sees it fail. It sees it disappear. That is the
supported way to depend on an actor somewhere else: watch it, and have a plan
for the day it stops answering.

What to watch in the output: the third line. The coordinator does not retry
against the node that is gone. It starts a worker it owns and finishes the
job there, which is the difference between a failover and a stall.

Run it with `uv run python -m tapio_examples.node_failure`.
"""

import asyncio

from tapio import (
    Behavior,
    Behaviors,
    Message,
    Signal,
    Terminated,
    register_message,
)
from tapio.actor import ActorContext, ActorRef
from tapio.remote.address import format_ref
from tapio.testkit import two_nodes

__all__ = ["Assign", "Done", "Job", "main"]


@register_message()
class Done(Message):
    """A finished job, and which node finished it."""

    item: int
    where: str


@register_message()
class Job(Message):
    """A unit of work, carrying the ref its result goes back to."""

    item: int
    reply_to: ActorRef[Done]


class Assign(Message):
    """Tell the coordinator to get one job done, wherever it can.

    Not registered, because it never leaves the node that sends it. Only what
    crosses a link needs a wire key.
    """

    item: int


def worker(where: str) -> Behavior[Job]:
    """Build an actor that does the work and says where it was done.

    Args:
        where: The node's name, so the output shows which one ran it.

    Returns:
        The behavior to spawn.
    """

    async def on_job(message: Job) -> Behavior[Job]:
        message.reply_to.tell(Done(item=message.item, where=where))
        return Behaviors.same()

    return Behaviors.receive_message(on_job, msg_type=Job)


def coordinator(
    remote: ActorRef[Job],
    lines: list[str],
    finished: asyncio.Event,
    lost: asyncio.Event,
) -> Behavior[Assign | Done]:
    """Build the actor that hands out work and survives losing its worker.

    Args:
        remote: The worker on the other node, to start with.
        lines: Where to record what happened.
        finished: Set whenever a job comes back.
        lost: Set when the remote worker is gone and a local one has replaced it.

    Returns:
        The behavior to spawn.
    """

    def build(ctx: ActorContext[Assign | Done]) -> Behavior[Assign | Done]:
        # One call, and from here on this coordinator cannot be left holding a
        # ref to a node that is no longer there without knowing it.
        ctx.watch(remote)
        current = remote
        # The coordinator takes a wider protocol than the worker knows how to
        # send, so what goes in `reply_to` is an adapter ref: an `ActorRef[Done]`
        # that delivers into this actor. It is addressable like the actor
        # behind it, so a result crossing the link finds its way back.
        answers: ActorRef[Done] = ctx.message_adapter(lambda done: done, Done)

        async def on_message(message: Assign | Done) -> Behavior[Assign | Done]:
            nonlocal current
            match message:
                case Assign(item=item):
                    # An ordinary tell. Whether it crosses a link is decided
                    # by which ref this is, and this code does not ask.
                    current.tell(Job(item=item, reply_to=answers))
                case Done(item=item, where=where):
                    lines.append(f"home: job {item} done by {where}")
                    finished.set()
            return Behaviors.same()

        async def on_signal(
            ctx: ActorContext[Assign | Done], signal: Signal
        ) -> Behavior[Assign | Done]:
            nonlocal current
            if isinstance(signal, Terminated):
                lines.append("home: the away node is gone, rebuilding here")
                # A child of this actor, so it is supervised by the one that
                # depends on it. That is what could never be true of the
                # worker on the other node.
                current = ctx.spawn(worker("home"), name="worker")
                lost.set()
            return Behaviors.same()

        return Behaviors.receive_message(on_message, on_signal=on_signal)

    return Behaviors.setup(build)


async def main() -> list[str]:
    """Run the example.

    Returns:
        The lines the coordinator produced, in order.
    """
    lines: list[str] = []
    finished, lost = asyncio.Event(), asyncio.Event()

    async with two_nodes(alpha="home", beta="away") as nodes:
        here, there = nodes.alpha, nodes.beta
        hand = there.spawn(worker("away"), name="worker")
        remote = await here.resolve(format_ref(there.address, hand.path), expect=Job)
        boss = here.spawn(coordinator(remote, lines, finished, lost), name="boss")

        boss.tell(Assign(item=1))
        await finished.wait()
        finished.clear()

        # The whole node goes away, not just the actor. Every watcher of every
        # ref on it hears the same thing.
        await there.terminate()
        await lost.wait()

        boss.tell(Assign(item=2))
        await finished.wait()

    for line in lines:
        print(line)
    return lines


if __name__ == "__main__":
    asyncio.run(main())
