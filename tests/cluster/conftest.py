"""A cluster of systems in one process, on loopback ports the OS picks.

Everything here runs the real thing: real sockets, real handshakes, real
gossip. What is scaled down is patience, since a test that waits a second per
gossip round is a test nobody runs.
"""

from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import timedelta

from tapio.actor import ActorSystem
from tapio.cluster import Cluster, MemberStatus
from tapio.settings import ClusterSettings, RemoteSettings, TapioSettings

QUICK = ClusterSettings(
    _env_file=None,  # type: ignore[call-arg]
    gossip_interval=timedelta(milliseconds=20),
    join_retry_interval=timedelta(milliseconds=20),
    seed_form_after=timedelta(milliseconds=100),
    join_timeout=timedelta(seconds=10),
    leave_timeout=timedelta(seconds=10),
)
"""Gossip fast enough that a whole cluster converges inside a test."""


def remoting() -> TapioSettings:
    """Settings for a system listening on a loopback port the OS picks."""
    return TapioSettings(
        _env_file=None,  # type: ignore[call-arg]
        remote=RemoteSettings(_env_file=None, bind_port=0),  # type: ignore[call-arg]
    )


@dataclass(frozen=True, slots=True)
class Node:
    """One system and its membership in the cluster under test."""

    system: ActorSystem
    cluster: Cluster

    @property
    def address(self) -> str:
        """This node's canonical address, in the form members are named by."""
        return self.cluster.address

    @property
    def status(self) -> MemberStatus | None:
        """This node's own status, as it currently sees it."""
        member = self.cluster.self_member
        return member.status if member is not None else None

    def status_of(self, address: str) -> MemberStatus | None:
        """What this node believes about another member, if it knows one."""
        member = self.cluster.state.member(address)
        return member.status if member is not None else None


def seeds_of(nodes: Sequence[Node]) -> list[str]:
    """The seed list every node in a group is given, in one order."""
    return [node.address for node in nodes]


@asynccontextmanager
async def cluster_of(
    count: int, *, settings: ClusterSettings = QUICK
) -> AsyncIterator[tuple[Node, ...]]:
    """Start `count` systems, each with a cluster daemon and nothing joined yet.

    The systems are named `node1` upwards, and every one is terminated however
    the test ends, so a failure leaves no port bound.

    Args:
        count: How many nodes to start.
        settings: How they gossip.

    Yields:
        The nodes, in the order they were started.
    """
    nodes: list[Node] = []
    try:
        for index in range(1, count + 1):
            system = ActorSystem(f"node{index}", remoting())
            nodes.append(Node(system=system, cluster=Cluster(system, settings)))
        yield tuple(nodes)
    finally:
        for node in reversed(nodes):
            await node.system.terminate()
