"""Three nodes finding each other, agreeing who is in, and one of them leaving.

Concepts: `Cluster`, seed nodes, the statuses a member moves through, the
leader as something every node works out rather than votes on, and a graceful
leave that every node ends up agreeing about.

Remoting (`two_nodes`) lets two systems that already know about each other
exchange messages. Clustering answers the question remoting does not: who is
in this group right now. Nothing here is a vote. Every node merges what it
hears into what it believes, the merge is written so that the order things
arrive in cannot change the result, and the leader is the first member in
address order, which each node computes for itself from the same converged
view.

Note the shape of `join_seed_nodes`: every node is given the same list in the
same order, its own address included. The first seed in that list, and only
the first, may form a new cluster, and only if it hears from nobody. That one
rule is what stops a restarting node from founding a second cluster beside the
one that is already running.

The three systems run in this one process on loopback ports the OS picks, so
the example needs no orchestration and no second machine.

What to watch in the output: the last two lines. The node that leaves is
`node1`, which is both the first seed and the leader, and the other two end up
agreeing it is `removed`. They report that from their own view rather than by
asking anybody, and leadership moved to `node2` on the way without a handover,
because the leader is a function of the membership rather than a post somebody
holds.

They agree in the end rather than at once. `leave` returns when the leaving
node is `removed` in its own view, and the others hear about it on their next
gossip round, so the example waits for each of them instead of reading them
straight away. That wait is the honest shape for anything that watches another
node, and leaving it out is a race that shows up about once in fifteen runs.

Run it with `uv run python -m tapio_examples.cluster_join`.
"""

import asyncio
from datetime import timedelta

from tapio import ActorSystem, RemoteSettings, TapioSettings
from tapio.cluster import Cluster, MemberStatus
from tapio.settings import ClusterSettings

__all__ = ["main"]


def node() -> TapioSettings:
    """Settings for one node: remoting on, on a loopback port the OS picks."""
    return TapioSettings(remote=RemoteSettings(bind_port=0))


def gossiping() -> ClusterSettings:
    """Gossip often enough that an example finishes while you watch it.

    A real deployment leaves these alone. The defaults gossip once a second,
    which is the right rate for a cluster that will be running for weeks.
    """
    return ClusterSettings(
        gossip_interval=timedelta(milliseconds=50),
        join_retry_interval=timedelta(milliseconds=50),
        seed_form_after=timedelta(milliseconds=200),
    )


async def until_removed(cluster: Cluster, address: str) -> MemberStatus:
    """Wait for one node to see another written off.

    `leave` returns as soon as the leaving node is `removed` in its own view.
    Every other node finds out on its next gossip round, so anybody reading
    another node's view waits for it rather than assuming. Reading straight
    after `leave` returns catches a node still at `exiting` often enough to
    matter, which is a race in the reader rather than in the cluster.

    Args:
        cluster: The node doing the watching.
        address: The member it is waiting to see written off.

    Returns:
        The status it settled on, which is `removed`.

    Raises:
        TimeoutError: If the removal has not reached this node in five
            seconds. Gossip here runs every 50ms, so that is long enough to
            mean something is wrong rather than slow.
    """
    async with asyncio.timeout(5.0):
        while True:
            member = cluster.state.member(address)
            if member is not None and member.status is MemberStatus.REMOVED:
                return member.status
            await asyncio.sleep(0.005)


async def main() -> list[str]:
    """Run the example.

    Returns:
        The lines the nodes produced, in the order they produced them.
    """
    lines: list[str] = []

    async with (
        ActorSystem("node1", node()) as first,
        ActorSystem("node2", node()) as second,
        ActorSystem("node3", node()) as third,
    ):
        systems = (first, second, third)
        clusters = [Cluster(system, gossiping()) for system in systems]
        # In a real deployment this list comes from configuration, and it is
        # the same list on every node. Here the addresses are read from the
        # systems, because all three are in this process.
        seeds = [cluster.address for cluster in clusters]

        await asyncio.gather(*(cluster.join_seed_nodes(seeds) for cluster in clusters))

        for system, cluster in zip(systems, clusters, strict=True):
            member = cluster.self_member
            assert member is not None
            lines.append(
                f"{system.name}: {member.status}, member {member.up_number} "
                f"of {len(cluster.members)}"
            )

        leader = {cluster.leader for cluster in clusters}
        lines.append(f"every node agrees the leader is {leader.pop()}")

        # Leaving is not vanishing. The member walks out through Leaving and
        # Exiting, each step waiting for a view every node has seen, which is
        # where a handoff would happen once there is something to hand over.
        leaving, *staying = clusters
        before = leaving.self_member
        assert before is not None
        lines.append(f"node1: leaving, at {before.status}")
        await leaving.leave()

        for system, cluster in zip(systems[1:], staying, strict=True):
            status = await until_removed(cluster, leaving.address)
            lines.append(f"{system.name}: node1 is {status}")

    for line in lines:
        print(line)
    return lines


if __name__ == "__main__":
    asyncio.run(main())
