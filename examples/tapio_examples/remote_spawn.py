"""Asking another node to start an actor, and letting it supervise the actor.

Concepts: `@remote_behavior` and the factory registry, a spawner actor with an
allowlist, watching what comes back, and seeing the peer restart its own child
while this node is told nothing at all.

Placement is the one part of remoting that is deliberately not transparent.
Sending, asking and watching are the same calls whichever node the target is
on. Starting an actor elsewhere is a different call, and it is awaited, because
a round trip is happening and pretending otherwise would be the kind of
transparency that lies.

The reason is supervision. If the local parent supervised a remote child, then
every restart, stop and failure report would be a frame on a link that can go
silent halfway through the decision. So the tree stays inside one node: the
spawned actor is the spawner's child, supervised over there, and this node
holds a ref and watches it. That is a smaller contract, and it is the largest
one a network can keep.

What to watch in the output: the fourth line. The worker crashed, the peer
restarted it, and the job that was queued behind the crash was answered by the
new incarnation through the very same ref. The count of jobs handled went back
to one, which is the only trace of it here. Nothing was reported, because
nothing had to be: the restart was decided a process boundary away from the
actor rather than a network away.

Run it with `uv run python -m tapio_examples.remote_spawn`.
"""

import asyncio

from tapio import (
    Behavior,
    Behaviors,
    Message,
    Spawn,
    Spawned,
    SpawnFailed,
    SpawnReply,
    register_message,
    remote_behavior,
    spawner,
)
from tapio.actor import (
    ActorContext,
    ActorRef,
    Signal,
    SupervisorStrategy,
    Terminated,
)
from tapio.remote.address import format_ref
from tapio.testkit import two_nodes

__all__ = ["Crash", "Double", "Doubled", "DoublerArgs", "Retire", "doubler", "main"]


class DoublerArgs(Message):
    """What the worker is built with.

    An arguments model, not a closure. A behavior is a closure and a closure
    does not cross a socket, so what travels is a key naming a factory and a
    model naming its arguments. Both nodes have to be running the same code for
    the key to mean anything, which is the sentence to remember about all of
    this.
    """

    factor: int = 2


@register_message()
class Doubled(Message):
    """An answer, and how much work this incarnation has done."""

    n: int
    handled: int


@register_message()
class Double(Message):
    """A number to multiply, and where the answer goes."""

    n: int
    reply_to: ActorRef[Doubled]


@register_message()
class Crash(Message):
    """Tells the worker to fail, so that somebody has to decide about it."""


@register_message()
class Retire(Message):
    """Tells the worker to stop, so that its watchers hear about it."""


@remote_behavior("doubler")
def doubler(args: DoublerArgs) -> Behavior[Double | Crash | Retire]:
    """Build a worker that multiplies, fails on request, and can be retired.

    The supervision is declared here because here is the only place it can be.
    A restart happens entirely on the node that runs the actor, so the strategy
    has to be part of what that node builds.

    Args:
        args: What to multiply by.

    Returns:
        The behavior the spawner will start.
    """

    def build(
        ctx: ActorContext[Double | Crash | Retire],
    ) -> Behavior[Double | Crash | Retire]:
        # Rebuilt on every restart, which is what makes the restart visible on
        # the other node without anything being reported.
        handled = 0

        async def on_message(
            message: Double | Crash | Retire,
        ) -> Behavior[Double | Crash | Retire]:
            nonlocal handled
            if isinstance(message, Crash):
                msg = "the doubler fell over"
                raise RuntimeError(msg)
            if isinstance(message, Retire):
                return Behaviors.stopped()
            handled += 1
            message.reply_to.tell(Doubled(n=message.n * args.factor, handled=handled))
            return Behaviors.same()

        return Behaviors.receive_message(on_message, msg_type=Double | Crash | Retire)

    return Behaviors.supervise(Behaviors.setup(build)).on_failure(
        SupervisorStrategy.restart(), on=RuntimeError
    )


def overseer(
    target: ActorRef[Message], lines: list[str], gone: asyncio.Future[None]
) -> Behavior[Retire]:
    """Build an actor that watches the worker on the other node.

    Death watch is what replaces the parent-child link, and it is the whole of
    what this node is promised. `Terminated` arrives when the worker stops,
    when its node stops, and when the link to that node is given up on. All
    three mean the same thing here: that worker is gone, ask for another.

    Args:
        target: The worker to watch.
        lines: Where to write what happened.
        gone: Resolved once the worker has been reported gone.

    Returns:
        The behavior to spawn.
    """

    def build(ctx: ActorContext[Retire]) -> Behavior[Retire]:
        ctx.watch(target)

        async def on_message(message: Retire) -> Behavior[Retire]:
            return Behaviors.same()

        async def on_signal(
            ctx: ActorContext[Retire], signal: Signal
        ) -> Behavior[Retire]:
            if isinstance(signal, Terminated):
                lines.append("orders: the worker is gone, so ask for another")
                if not gone.done():
                    gone.set_result(None)
            return Behaviors.same()

        return Behaviors.receive_message(
            on_message, msg_type=Retire, on_signal=on_signal
        )

    return Behaviors.setup(build)


async def main() -> list[str]:
    """Run the example.

    Returns:
        The lines the two nodes produced, in the order they produced them.
    """
    lines: list[str] = []
    gone: asyncio.Future[None] = asyncio.get_running_loop().create_future()
    async with two_nodes(alpha="orders", beta="compute") as nodes:
        here, there = nodes.alpha, nodes.beta
        # The spawner offers one factory and nothing else. An actor that will
        # start anything registered, on request, is a capability handed to
        # whoever can reach the port.
        desk = there.spawn(spawner(offers=["doubler"]), name="spawner")
        remote = await here.resolve(format_ref(there.address, desk.path), expect=Spawn)

        reply = await remote.ask(
            lambda reply_to: Spawn(
                factory="doubler",
                args=DoublerArgs(factor=2),
                name="doubler-1",
                reply_to=reply_to,
            ),
            # Both answers, because a refusal is news to act on rather than a
            # broken protocol. Asking for `Spawned` alone would turn one into
            # an AskTypeError.
            expect=SpawnReply,
        )
        if not isinstance(reply, Spawned):
            msg = f"compute refused to start the worker: {reply}"
            raise RuntimeError(msg)
        worker = reply.ref
        lines.append(f"orders: compute started {reply.name} at {worker.path}")

        here.spawn(overseer(worker, lines, gone), name="overseer")

        first = await worker.ask(
            lambda reply_to: Double(n=6, reply_to=reply_to), expect=Doubled
        )
        lines.append(f"orders: 6 doubled is {first.n}, job {first.handled} for it")

        # The crash and the job queue behind each other on the worker's own
        # mailbox. The crash is supervised over there, the mailbox survives it,
        # and the job is answered by the new incarnation through this same ref.
        worker.tell(Crash())
        second = await worker.ask(
            lambda reply_to: Double(n=7, reply_to=reply_to), expect=Doubled
        )
        lines.append(f"orders: 7 doubled is {second.n}, job {second.handled} for it")
        lines.append("orders: the count restarted, and nothing told me why")

        refused = await remote.ask(
            lambda reply_to: Spawn(factory="tripler", reply_to=reply_to),
            expect=SpawnReply,
        )
        if isinstance(refused, SpawnFailed):
            # What version skew looks like: a key crossed the wire and the peer
            # has never heard of it. Nothing is imported to find out what it
            # might have meant.
            lines.append(f"orders: compute cannot start 'tripler' ({refused.reason})")

        worker.tell(Retire())
        await gone

    for line in lines:
        print(line)
    return lines


if __name__ == "__main__":
    asyncio.run(main())
