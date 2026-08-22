"""A cluster cut in two, and the downing strategy that decides who lives.

Concepts: a real partition injected with the TestKit's `link_faults`, a
`DownStrategy` passed to each node, and the verdict read from both sides of the
split. `KeepMajority`, `DownAll` and `LeaseMajority` are three strategies with
three different verdicts on the same kind of split.

A partition is the case remoting cannot solve on its own, shown in `partition`:
two live nodes, each concluding the other is gone, both of them right about the
silence and wrong about the cause. Downing is the cluster's answer. Every node
runs the same strategy over its own view, and a strategy is safe only when both
sides reach the same verdict from their mirror-image views, because no message
crosses the split to reconcile a disagreement. Two clusters that each think they
are the whole is the split brain the name warns about, and avoiding it is the
whole job.

The strategies here differ in what they keep. `KeepMajority` keeps the larger
side and downs the smaller, so a node cut off from a majority downs itself while
the majority writes it off and carries on. `DownAll` keeps nobody, so the same
split stops every node: the safe choice when being wrong is worse than being
down. `LeaseMajority` handles the split a count cannot, an even one with no
majority: both sides race for one lock, the side that takes it lives, and the
side that cannot downs itself, so exactly one survives.

The partition is injected with `link_faults`, which drops every frame a system
sends or receives without telling it, exactly as a real partition would. It
covers a whole system's links, so it isolates one node at a time. That is why
the majority scenes cut one node off from four, rather than splitting five into
three and two: a group split needs per-peer faults the injector does not have,
and the decision each strategy makes over the two views is the same shape either
way.

Every cluster here runs in this one process on loopback ports the OS picks, so
the example needs no orchestration and no second machine.

What to watch in the output: the three lines are three strategies on the same
kind of split, and each names what happened on both sides. The point is that in
every one both sides agree, so the cluster never becomes two.

Run it with `uv run python -m tapio_examples.split_brain`.
"""

import asyncio
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import timedelta

from tapio import ActorSystem, RemoteSettings, TapioSettings
from tapio.cluster import (
    Cluster,
    DownAll,
    DownStrategy,
    KeepMajority,
    LeaseMajority,
    LocalLease,
    MemberStatus,
)
from tapio.settings import ClusterSettings
from tapio.testkit.remote import LinkFaults, link_faults

__all__ = ["main"]


def node() -> TapioSettings:
    """Settings for one node: remoting on, on a loopback port the OS picks."""
    return TapioSettings(remote=RemoteSettings(bind_port=0))


def decisive() -> ClusterSettings:
    """Gossip and probe fast, and down a settled split quickly.

    A real deployment is patient on purpose, since downing cannot be taken back.
    These settings hurry every part of it so the example resolves a split while
    you watch it: heartbeats every 20ms, a member called unreachable after 300ms
    of silence, and a strategy acting 80ms after the split stops changing.

    Returns:
        The settings.
    """
    return ClusterSettings(
        gossip_interval=timedelta(milliseconds=50),
        join_retry_interval=timedelta(milliseconds=50),
        seed_form_after=timedelta(milliseconds=200),
        heartbeat_interval=timedelta(milliseconds=20),
        unreachable_after=timedelta(milliseconds=300),
        down_after=timedelta(milliseconds=80),
    )


@dataclass(frozen=True, slots=True)
class Node:
    """One clustered system and the controls for breaking its links."""

    system: ActorSystem
    cluster: Cluster
    faults: LinkFaults

    @property
    def status(self) -> MemberStatus | None:
        """This node's own status, as it currently sees itself."""
        member = self.cluster.self_member
        return member.status if member is not None else None

    def sees(self, other: "Node") -> MemberStatus | None:
        """What this node believes about another, if it knows one at all."""
        member = self.cluster.state.member(other.cluster.address)
        return member.status if member is not None else None


async def until(predicate: Callable[[], bool], *, within: float = 10.0) -> None:
    """Wait until a predicate holds, or give up.

    Reads are of one node's own view, which lags the others by a gossip round,
    so a scene waits for the views to settle rather than assuming they have.

    Args:
        predicate: The condition to wait for.
        within: How many seconds to wait before giving up. Everything here
            resolves in well under a second, so the timeout means something is
            wrong rather than slow.

    Raises:
        TimeoutError: If the predicate has not held within the deadline.
    """
    async with asyncio.timeout(within):
        while True:
            if predicate():
                return
            await asyncio.sleep(0.01)


@asynccontextmanager
async def live_cluster(count: int, strategy: DownStrategy) -> AsyncIterator[list[Node]]:
    """Start `count` clustered nodes under one strategy and wait for convergence.

    Fault injection is installed on every node before it sends anything, so a
    scene can cut one off without arranging it beforehand. Every system is
    terminated on the way out, however the scene ends, so no port stays bound.

    Args:
        count: How many nodes to start, named `node1` upwards.
        strategy: The downing strategy every node resolves a split with. A
            lease-backed strategy is shared across the nodes, which is what one
            lock held outside the split looks like when they share a process.

    Yields:
        The nodes, in the order they were started.
    """
    nodes: list[Node] = []
    try:
        for index in range(1, count + 1):
            system = ActorSystem(f"node{index}", node())
            faults = link_faults(system)
            cluster = Cluster(system, decisive(), downing=strategy)
            nodes.append(Node(system, cluster, faults))
        seeds = [n.cluster.address for n in nodes]
        await asyncio.gather(*(n.cluster.join_seed_nodes(seeds) for n in nodes))
        await until(lambda: all(n.cluster.state.converged for n in nodes))
        yield nodes
    finally:
        for node_ in reversed(nodes):
            await node_.system.terminate()


async def keep_majority_scene() -> str:
    """Five nodes, one cut off, kept by the majority rule.

    Returns:
        What happened, from both sides.
    """
    async with live_cluster(5, KeepMajority()) as nodes:
        majority, cut_off = nodes[:-1], nodes[-1]
        cut_off.faults.partition()

        # The isolated node is the minority of one, so it downs itself.
        await asyncio.wait_for(cut_off.cluster.when_downed(), timeout=10.0)
        # The majority writes it off and converges as a smaller cluster.
        await until(
            lambda: all(
                n.cluster.state.converged and len(n.cluster.members) == 4
                for n in majority
            )
        )

        assert cut_off.status is MemberStatus.DOWN
        assert all(n.status is MemberStatus.UP for n in majority)
        gone = {MemberStatus.DOWN, MemberStatus.REMOVED}
        assert all(n.sees(cut_off) in gone for n in majority)

    return (
        "KeepMajority: the cut-off node downed itself, and the other four "
        "stay up and agree it is down"
    )


async def down_all_scene() -> str:
    """Five nodes, one cut off, under a strategy that keeps no side.

    Returns:
        What happened, from both sides.
    """
    async with live_cluster(5, DownAll()) as nodes:
        nodes[-1].faults.partition()

        # Neither side is kept: the isolated node downs itself, and the four
        # down themselves too, because their own view has a member they cannot
        # hear and the strategy keeps no side at all.
        await asyncio.gather(
            *(asyncio.wait_for(n.cluster.when_downed(), timeout=10.0) for n in nodes)
        )
        assert all(n.status is MemberStatus.DOWN for n in nodes)

    return (
        "DownAll: the same split downs every node on both sides, since it "
        "keeps no side at all"
    )


async def lease_scene() -> str:
    """Two nodes cut apart, an even split a count cannot settle, and a lease.

    Returns:
        What happened, from both sides.
    """
    # One lease object, shared by both nodes, stands in for a lock held outside
    # the split. An even split has no majority, so the lock is what decides it.
    strategy = LeaseMajority(lease=LocalLease())
    async with live_cluster(2, strategy) as nodes:
        nodes[0].faults.partition()

        # The lease admits one owner, so one side takes it and lives while the
        # other cannot and downs itself: exactly one survivor, which is the
        # guarantee the count could not make about an even split.
        await until(lambda: sum(n.status is MemberStatus.DOWN for n in nodes) == 1)
        survivors = [n for n in nodes if n.status is MemberStatus.UP]
        downed = [n for n in nodes if n.status is MemberStatus.DOWN]
        assert len(survivors) == 1
        assert len(downed) == 1

    return (
        "LeaseMajority on an even split: one node took the lease and stays up, "
        "the other downed itself"
    )


async def main() -> list[str]:
    """Run the example.

    Returns:
        The lines the scenes produced, in the order they ran.
    """
    lines = [
        await keep_majority_scene(),
        await down_all_scene(),
        await lease_scene(),
    ]
    for line in lines:
        print(line)
    return lines


if __name__ == "__main__":
    asyncio.run(main())
