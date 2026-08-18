"""One actor across a whole cluster, moved to a new node when its host leaves.

Concepts: `ClusterSingleton`, the oldest member of a role as where a singleton
runs, and the handoff that happens when that member goes away.

Some work has to happen in exactly one place: a scheduler that must not fire
twice, a coordinator that owns a piece of state. Spawn the same
`ClusterSingleton` manager on every node, and the one on the oldest member runs
the instance while every other manager waits. "Oldest" is the member the leader
accepted first, an order every node computes the same way, so there is no
election and no lock, only a function of the membership every node already
agrees on.

When the host leaves, the next oldest member starts the instance. It is a fresh
start, not a move of live state: what mattered on the old host does not cross to
the new one, which is the honest shape of a thing that has to survive its host
going away. Here `node1` is both the first seed and the oldest member, so the
coordinator starts there; when `node1` leaves, it reappears on `node2`.

The three systems run in this one process on loopback ports the OS picks, so the
example needs no orchestration and no second machine.

What to watch in the output: the coordinator names the node it is running on
when it starts. It says `node1` first, and after `node1` leaves it says `node2`,
without anything having told the second manager to take over.

Run it with `uv run python -m tapio_examples.cluster_singleton`.
"""

import asyncio
from datetime import timedelta

from tapio import (
    ActorSystem,
    Behavior,
    Behaviors,
    Message,
    RemoteSettings,
    TapioSettings,
)
from tapio.actor import ActorContext, Signal
from tapio.actor.signals import PostStop
from tapio.cluster import Cluster, ClusterSingleton
from tapio.settings import ClusterSettings

__all__ = ["Tick", "main"]


class Tick(Message):
    """A message the coordinator does not need to act on, only to exist for."""


class Coordinators:
    """Where every coordinator instance reports which node it runs on."""

    def __init__(self) -> None:
        """Start with nobody running the coordinator."""
        self.running: set[str] = set()

    def started(self, node: str) -> None:
        """Record that the coordinator started on a node."""
        self.running.add(node)

    def stopped(self, node: str) -> None:
        """Record that the coordinator on a node stopped."""
        self.running.discard(node)


def coordinator(registry: Coordinators, node: str) -> Behavior[Tick]:
    """The singleton instance: it reports its life to the shared registry.

    Args:
        registry: Where the instance says which node it runs on.
        node: The name of the node it is running on.

    Returns:
        The behavior.
    """

    def build(ctx: ActorContext[Tick]) -> Behavior[Tick]:
        registry.started(node)

        async def on_message(message: Tick) -> Behavior[Tick]:
            return Behaviors.same()

        async def on_signal(ctx: ActorContext[Tick], signal: Signal) -> Behavior[Tick]:
            if isinstance(signal, PostStop):
                registry.stopped(node)
            return Behaviors.same()

        return Behaviors.receive_message(on_message, msg_type=Tick, on_signal=on_signal)

    return Behaviors.setup(build)


def node() -> TapioSettings:
    """Settings for one node: remoting on, on a loopback port the OS picks."""
    return TapioSettings(remote=RemoteSettings(bind_port=0))


def gossiping() -> ClusterSettings:
    """Gossip often enough that the example finishes while you watch it."""
    return ClusterSettings(
        gossip_interval=timedelta(milliseconds=50),
        join_retry_interval=timedelta(milliseconds=50),
        seed_form_after=timedelta(milliseconds=200),
    )


async def until_one_on(registry: Coordinators, *, not_on: str | None = None) -> str:
    """Wait until the coordinator runs on exactly one node, and say which.

    Args:
        registry: Where the coordinators report where they run.
        not_on: A node the coordinator must have moved off, when waiting for a
            handoff rather than the first placement.

    Returns:
        The node the coordinator now runs on.

    Raises:
        TimeoutError: If it has not settled within five seconds.
    """
    async with asyncio.timeout(5.0):
        while True:
            running = set(registry.running)
            if len(running) == 1 and not_on not in running:
                return next(iter(running))
            await asyncio.sleep(0.005)


async def main() -> list[str]:
    """Run the example.

    Returns:
        The lines the nodes produced, in the order they produced them.
    """
    lines: list[str] = []
    registry = Coordinators()

    async with (
        ActorSystem("node1", node()) as first,
        ActorSystem("node2", node()) as second,
        ActorSystem("node3", node()) as third,
    ):
        systems = (first, second, third)
        clusters = [Cluster(system, gossiping()) for system in systems]
        seeds = [cluster.address for cluster in clusters]

        await asyncio.gather(*(cluster.join_seed_nodes(seeds) for cluster in clusters))

        # The same manager on every node. Only the one on the oldest member
        # runs the instance; the rest wait for a handoff that may never come.
        for system in systems:
            system.spawn(
                ClusterSingleton(
                    coordinator(registry, system.name), name="coordinator"
                ),
                name="singleton",
            )

        first_host = await until_one_on(registry)
        lines.append(f"the coordinator runs on {first_host}, the oldest member")

        # The oldest member leaves. Its instance stops with it, and the next
        # oldest starts one, without anything telling it to.
        await clusters[0].leave()
        second_host = await until_one_on(registry, not_on="node1")
        lines.append(f"node1 left, so the coordinator moved to {second_host}")

    for line in lines:
        print(line)
    return lines


if __name__ == "__main__":
    asyncio.run(main())
