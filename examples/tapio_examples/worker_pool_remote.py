"""Backpressure across a link, which the transport cannot give you.

Concepts: why `offer` is local backpressure and not end-to-end backpressure,
and the credit-based protocol that is. Workers say how much they will accept,
and the producer sends only what it has been granted.

`await ref.offer(item)` on a remote ref waits for room in *this* node's
outbound buffer. That is a real thing to wait on, and it is a socket that is
not draining rather than a worker that is falling behind. The two come apart
exactly when it matters: a worker with a huge mailbox reads every frame the
moment it arrives, so the buffer stays empty, `offer` never waits, and the
backlog piles up on the other node where this one cannot see it. Nothing in a
fire-and-forget wire protocol can do better, and pretending otherwise would be
the kind of transparency that lies.

So flow control is built out of messages, where the receiver is the one who
knows. Each worker grants the producer a number of items it is willing to have
outstanding. The producer sends that many and no more, and each finished item
grants one back. The grant is the backpressure, it is end to end, and it works
whatever the network is doing.

Compare it with `worker_pool`, which fans out over routees in one process. The
router there needs none of this: a full mailbox pushes back on the sender
directly, because the sender and the mailbox are on the same loop.

What to watch in the output: the third line. Twelve items were done and no
worker ever had more than the three it granted waiting on it. That number is
chosen by the workers and obeyed by the producer, and nothing in the transport
enforces it.

Run it with:

```
uv run python -m tapio_examples.worker_pool_remote
```
"""

import asyncio
from collections.abc import Callable

from tapio import (
    Behavior,
    Behaviors,
    Message,
    Spawn,
    Spawned,
    SpawnReply,
    register_message,
    remote_behavior,
    spawner,
)
from tapio.actor import ActorContext, ActorRef
from tapio.remote.address import format_ref
from tapio.testkit import two_nodes

__all__ = [
    "Credit",
    "Hello",
    "Item",
    "WorkerArgs",
    "main",
    "producer",
    "sink",
    "spawn_request",
]

ITEMS = 12
"""How much work there is to hand out."""

GRANT = 3
"""How many items a worker will have outstanding at once."""


class WorkerArgs(Message):
    """What a worker is built with: how much it is prepared to have in hand."""

    grant: int = GRANT


@register_message()
class Credit(Message):
    """A worker saying how much more it will accept, and how much it has done.

    It carries its own ref, so the producer knows which worker is speaking and
    has somewhere to send the next item. That ref crosses the link in this
    direction, and the items cross back through it in the other.
    """

    worker: ActorRef["Hello | Item"]
    n: int
    done: int


@register_message()
class Hello(Message):
    """Introduces the producer to a worker, which answers with its first grant."""

    reply_to: ActorRef[Credit]


@register_message()
class Item(Message):
    """One unit of work, and where to ask for the next one."""

    n: int
    reply_to: ActorRef[Credit]


@remote_behavior("sink")
def sink(args: WorkerArgs) -> Behavior[Hello | Item]:
    """Build a worker that grants credit and gives it back as it finishes.

    Args:
        args: How many items it will have outstanding at once.

    Returns:
        The behavior the spawner will start.
    """

    def build(ctx: ActorContext[Hello | Item]) -> Behavior[Hello | Item]:
        done = 0

        async def on_message(message: Hello | Item) -> Behavior[Hello | Item]:
            nonlocal done
            if isinstance(message, Hello):
                # The opening grant. Until this arrives the producer has been
                # told nothing, so it sends nothing.
                message.reply_to.tell(
                    Credit(worker=ctx.self_ref, n=args.grant, done=done)
                )
                return Behaviors.same()
            done += 1
            # One item finished, one slot free. This is the whole protocol.
            message.reply_to.tell(Credit(worker=ctx.self_ref, n=1, done=done))
            return Behaviors.same()

        return Behaviors.receive_message(on_message, msg_type=Hello | Item)

    return Behaviors.setup(build)


def producer(
    workers: list[ActorRef["Hello | Item"]],
    items: list[int],
    finished: asyncio.Future[tuple[int, list[int]]],
) -> Behavior[Credit]:
    """Build the actor that hands work out, and never outruns its grants.

    Args:
        workers: The workers on the other node.
        items: The work to hand out.
        finished: Resolved with the peak outstanding count and how much each
            worker did, once every item is done.

    Returns:
        The behavior to spawn.
    """

    def build(ctx: ActorContext[Credit]) -> Behavior[Credit]:
        queue = list(items)
        credit = {worker.path: 0 for worker in workers}
        sent = {worker.path: 0 for worker in workers}
        done = {worker.path: 0 for worker in workers}
        peak = 0

        for worker in workers:
            worker.tell(Hello(reply_to=ctx.self_ref))

        async def on_credit(message: Credit) -> Behavior[Credit]:
            nonlocal peak
            at = message.worker.path
            credit[at] += message.n
            done[at] = message.done
            while credit[at] > 0 and queue:
                message.worker.tell(Item(n=queue.pop(0), reply_to=ctx.self_ref))
                credit[at] -= 1
                sent[at] += 1
            # What the grant is actually bounding: items sent to a worker that
            # it has not reported finishing.
            peak = max(peak, max(sent[at] - done[at] for at in sent))
            if sum(done.values()) == len(items) and not finished.done():
                finished.set_result((peak, [done[worker.path] for worker in workers]))
            return Behaviors.same()

        return Behaviors.receive_message(on_credit, msg_type=Credit)

    return Behaviors.setup(build)


def spawn_request(number: int) -> Callable[[ActorRef[SpawnReply]], Spawn]:
    """Build the request for one worker, given where the answer goes.

    A named function rather than a lambda in a loop, because a lambda would
    close over the loop variable and every request would ask for the same name.

    Args:
        number: Which worker this is, which becomes its name on the peer.

    Returns:
        What `ask` calls with the ref for the reply.
    """

    def request(reply_to: ActorRef[SpawnReply]) -> Spawn:
        return Spawn(
            factory="sink",
            args=WorkerArgs(grant=GRANT),
            name=f"sink-{number}",
            reply_to=reply_to,
        )

    return request


async def main() -> list[str]:
    """Run the example.

    Returns:
        The lines the producer wrote, in the order it wrote them.
    """
    lines: list[str] = []
    finished: asyncio.Future[tuple[int, list[int]]] = (
        asyncio.get_running_loop().create_future()
    )
    async with two_nodes(alpha="orders", beta="compute") as nodes:
        here, there = nodes.alpha, nodes.beta
        desk = there.spawn(spawner(offers=["sink"]), name="spawner")
        remote = await here.resolve(format_ref(there.address, desk.path), expect=Spawn)

        workers: list[ActorRef[Hello | Item]] = []
        for number in (1, 2):
            reply = await remote.ask(spawn_request(number), expect=SpawnReply)
            if not isinstance(reply, Spawned):
                msg = f"compute refused to start a worker: {reply}"
                raise RuntimeError(msg)
            workers.append(reply.ref)
        lines.append(
            f"orders: {len(workers)} workers on compute, each granting {GRANT} "
            "items at a time"
        )

        here.spawn(producer(workers, list(range(ITEMS)), finished), name="producer")
        peak, split = await finished

        lines.append(f"orders: the work split {split[0]} and {split[1]}")
        lines.append(
            f"orders: {ITEMS} items done, and never more than {peak} "
            "outstanding at one worker"
        )
        lines.append(
            "orders: the grant is the backpressure; offer would have waited on "
            "this node's outbound buffer instead"
        )

    for line in lines:
        print(line)
    return lines


if __name__ == "__main__":
    asyncio.run(main())
