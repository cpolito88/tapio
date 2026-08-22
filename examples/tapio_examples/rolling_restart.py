"""Restarting cluster nodes one at a time without the cluster going away.

Concepts: a graceful `leave` followed by a fresh node rejoining through a seed
that stayed up, the leader as a function of membership so it does not move while
its node stays, and a `ClusterSingleton` pinned to a role that only the node
which never restarts carries.

This is the operational move a rolling upgrade is made of: take one node down,
bring a new one up in its place, and do it again for the next, never touching
more than one at a time. `cluster_singleton` shows the other half of the story,
what happens to a singleton when its own host leaves. Here the host stays, and
the point is that nothing happens to the service at all.

Two things keep the service where it is. The leader is the first member in
address order, which every node computes for itself, and a node's name sorts
ahead of its port, so `node1` leads however the OS numbered the ports. The
singleton is placed on the `keeper` role, which only `node1` carries, so it runs
on `node1` and can run nowhere else while `node1` is up. `node1` is the node that
never restarts, so the leader never moves and the singleton never hands off.

A restart is not a move of the same node. The fresh node binds a new port, so it
is a new address and a new member, and the one it replaced leaves first and is
gone before it arrives. The rejoining node is given the nodes that stayed up as
its seeds, so it asks a cluster that already exists to let it in rather than
forming a second one beside it.

The three systems run in this one process on loopback ports the OS picks, so the
example needs no orchestration and no second machine.

What to watch in the output: the last line. The coordinator started once, on
`node1`, and every line in between reports the cluster back at three members
with `node1` still leading, so the two restarts happened underneath a service
that never noticed them.

Run it with `uv run python -m tapio_examples.rolling_restart`.
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
from tapio.cluster import Cluster, ClusterSingleton, MemberStatus
from tapio.settings import ClusterSettings

__all__ = ["Tick", "main"]


class Tick(Message):
    """A message the coordinator does not need to act on, only to exist for."""


class Coordinators:
    """Where every coordinator instance reports which node it runs on.

    It keeps two sets on purpose. `running` is where the coordinator is now, and
    `ever` is every node it has ever run on. A rolling restart is meant to leave
    `ever` at one node, which is a claim `running` alone cannot make, since a
    handoff and no handoff at all look the same at any single moment.
    """

    def __init__(self) -> None:
        """Start with nobody running the coordinator."""
        self.running: set[str] = set()
        self.ever: set[str] = set()

    def started(self, node: str) -> None:
        """Record that the coordinator started on a node."""
        self.running.add(node)
        self.ever.add(node)

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


def gossiping(roles: frozenset[str] = frozenset()) -> ClusterSettings:
    """Gossip often enough that the example finishes while you watch it.

    Args:
        roles: What the node says it is for, fixed for the life of the member.
            Only the anchor carries the `keeper` role the singleton is placed
            on, so the singleton has one node it may run on and no other.

    Returns:
        The settings.
    """
    return ClusterSettings(
        roles=roles,
        gossip_interval=timedelta(milliseconds=50),
        join_retry_interval=timedelta(milliseconds=50),
        seed_form_after=timedelta(milliseconds=200),
    )


def name_of(address: str) -> str:
    """The node name out of an address like `tapio://node2@127.0.0.1:54321`."""
    return address.split("://", 1)[1].split("@", 1)[0]


async def until_up(cluster: Cluster, names: set[str]) -> None:
    """Wait until one node sees exactly these members and every one of them up.

    Reads are of a single node's own view, which lags the others by a gossip
    round, so this waits for the view to settle rather than assuming it has. A
    restarted node is a new member at a new address, so what is checked is the
    set of names, which a restart preserves, rather than the addresses, which it
    does not.

    Args:
        cluster: The node doing the watching.
        names: The node names it must see, each of them up.

    Raises:
        TimeoutError: If the view has not settled within five seconds. Gossip
            here runs every 50ms, so that is long enough to mean something is
            wrong rather than slow.
    """
    async with asyncio.timeout(5.0):
        while True:
            alive = cluster.members
            up = {name_of(m.address) for m in alive if m.status is MemberStatus.UP}
            if up == names and len(alive) == len(names):
                return
            await asyncio.sleep(0.005)


async def main() -> list[str]:
    """Run the example.

    Returns:
        The lines the nodes produced, in the order they produced them.
    """
    lines: list[str] = []
    registry = Coordinators()
    everyone = {"node1", "node2", "node3"}

    # The systems are managed by hand rather than with `async with`, because a
    # restart replaces one of them partway through. `node1` is the anchor: it is
    # the leader and the only keeper, and it never restarts, so the service stays
    # put while the other two are cycled underneath it.
    systems: dict[str, ActorSystem] = {}
    clusters: dict[str, Cluster] = {}

    def start(name: str) -> None:
        system = ActorSystem(name, node())
        systems[name] = system
        # Only the anchor carries the keeper role, so the singleton has one node
        # it may run on. The other two are restarted and never host it.
        roles = frozenset({"keeper"}) if name == "node1" else frozenset()
        clusters[name] = Cluster(system, gossiping(roles))
        # Every node runs the same manager. Only the one on a keeper member runs
        # the instance, so a restarted node's manager just waits.
        system.spawn(
            ClusterSingleton(
                coordinator(registry, name), name="coordinator", role="keeper"
            ),
            name="singleton",
        )

    try:
        for name in everyone:
            start(name)

        # The first seed list is the same on every node, its own address
        # included, exactly as `cluster_join` forms it.
        seeds = [clusters[name].address for name in everyone]
        await asyncio.gather(
            *(clusters[name].join_seed_nodes(seeds) for name in everyone)
        )
        await until_up(clusters["node1"], everyone)

        host = next(iter(registry.running))
        lines.append(
            f"3 up, leader {name_of(clusters['node1'].leader or '')}, "
            f"coordinator on {host}"
        )

        # Restart the two nodes that are not the anchor, one at a time. Each one
        # leaves gracefully and is gone from the anchor's view before its
        # replacement arrives, so there is only ever one node down at a time and
        # the other two are a majority the whole while.
        for name in ("node2", "node3"):
            await clusters[name].leave()
            await until_up(clusters["node1"], everyone - {name})
            await systems[name].terminate()

            # The fresh node seeds off the nodes that stayed up, so it joins the
            # cluster that is already there instead of forming a new one.
            start(name)
            live = [clusters[other].address for other in everyone if other != name]
            await clusters[name].join_seed_nodes([clusters[name].address, *live])
            await until_up(clusters["node1"], everyone)

            leader = name_of(clusters["node1"].leader or "")
            lines.append(f"restarted {name}: 3 up, leader still {leader}")

        ran_on = ", ".join(sorted(registry.ever))
        lines.append(f"coordinator ran only on {ran_on}, and only once")
    finally:
        for system in systems.values():
            await system.terminate()

    for line in lines:
        print(line)
    return lines


if __name__ == "__main__":
    asyncio.run(main())
