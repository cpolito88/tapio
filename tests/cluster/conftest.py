"""A cluster of systems in one process, on loopback ports the OS picks.

Everything here runs the real thing: real sockets, real handshakes, real
gossip. What is scaled down is patience, since a test that waits a second per
gossip round is a test nobody runs.
"""

import asyncio
import json
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from tapio.actor import ActorSystem
from tapio.actor.path import ActorPath
from tapio.cluster import Cluster, DownStrategy, Member, MemberStatus
from tapio.remote.address import Address
from tapio.settings import ClusterSettings, ManagementSettings, TapioSettings
from tapio.testkit import (
    IsolatedClusterSettings,
    IsolatedRemoteSettings,
    IsolatedTapioSettings,
)
from tapio.testkit.remote import LinkFaults, link_faults

QUICK = IsolatedClusterSettings(
    gossip_interval=timedelta(milliseconds=20),
    join_retry_interval=timedelta(milliseconds=20),
    seed_form_after=timedelta(milliseconds=100),
    heartbeat_interval=timedelta(milliseconds=200),
    unreachable_after=timedelta(seconds=5),
    join_timeout=timedelta(seconds=10),
    leave_timeout=timedelta(seconds=10),
)
"""Gossip fast enough that a whole cluster converges inside a test.

Gossip is what these settings hurry. Probing is left slow on purpose: a test
about membership has no use for it, and a probe every few milliseconds only
adds links to open and close while the test is tearing its nodes down. The
window stays long for the same reason, so a busy loop never reads as a dead
node. A test about reachability asks for `WATCHFUL` instead.
"""

WATCHFUL = QUICK.model_copy(
    update={
        "heartbeat_interval": timedelta(milliseconds=20),
        "unreachable_after": timedelta(milliseconds=300),
    }
)

WATCHFUL_PHI = WATCHFUL.model_copy(
    update={
        "phi_accrual": True,
        "phi_acceptable_pause": timedelta(milliseconds=100),
    }
)
"""Watch with a phi-accrual detector instead of a fixed window.

The same fast probing as `WATCHFUL`, so a partition is seen inside a test, but
the verdict is learned from each member's rhythm rather than a deadline. The
pause covers a handful of missed probes so a busy moment is not read as death.
"""
"""Probe often, and give up quickly, for the tests that are about giving up.

Fifteen probes fit inside the window, which is enough that a busy moment does
not read as a dead node and short enough that a test does not wait seconds to
see one.
"""


def remoting(port: int = 0) -> TapioSettings:
    """Settings for a system listening on a loopback port.

    Args:
        port: The port to bind, or zero to let the OS pick, which is what
            every test wants except one that restarts a node where the
            cluster already expects it.

    Returns:
        The settings.
    """
    return IsolatedTapioSettings(remote=IsolatedRemoteSettings(bind_port=port))


@dataclass(frozen=True, slots=True)
class Node:
    """One system and its membership in the cluster under test."""

    system: ActorSystem
    cluster: Cluster
    faults: LinkFaults

    @property
    def address(self) -> str:
        """This node's canonical address, in the form members are named by."""
        return self.cluster.address

    @property
    def status(self) -> MemberStatus | None:
        """This node's own status, as it currently sees it."""
        member = self.cluster.self_member
        return member.status if member is not None else None

    @property
    def member(self) -> Member:
        """This node's own member record, which a joined node always has.

        `Cluster.self_member` is `None` until the node has joined, and a test
        about that window reads the property directly. Every other test has
        already awaited the join, so the precondition is stated here once.
        """
        member = self.cluster.self_member
        assert member is not None, f"{self.address} has not joined yet"
        return member

    def status_of(self, address: str) -> MemberStatus | None:
        """What this node believes about another member, if it knows one."""
        member = self.cluster.state.member(address)
        return member.status if member is not None else None


def seeds_of(nodes: Sequence[Node]) -> list[str]:
    """The seed list every node in a group is given, in one order."""
    return [node.address for node in nodes]


def daemon_running(node: Node) -> bool:
    """Whether the node's daemon still holds its well-known name."""
    path = ActorPath.root(node.system.name).child("system").child("cluster")
    return node.system.refs.lookup(path) is not None


def start_node(
    name: str,
    *,
    port: int = 0,
    settings: ClusterSettings = QUICK,
    downing: DownStrategy | None = None,
    terminate_on_down: bool = False,
    management: ManagementSettings | None = None,
) -> Node:
    """Start one system with a cluster daemon, joined to nothing yet.

    Fault injection is installed before the cluster is built, so it is in
    place before the node sends anything.

    Args:
        name: The system name, which is the first half of the address members
            are named by.
        port: The port to listen on, or zero to let the OS pick.
        settings: How the node gossips.
        downing: The strategy it resolves a split with, or `None` to leave a
            split blocking convergence.
        terminate_on_down: Whether it shuts its own system down when it downs
            itself.
        management: Open a management endpoint, or `None` to leave it off.

    Returns:
        The node. Terminating its system is the caller's job.
    """
    system = ActorSystem(name, remoting(port))
    faults = link_faults(system)
    return Node(
        system=system,
        cluster=Cluster(
            system,
            settings,
            downing=downing,
            terminate_on_down=terminate_on_down,
            management=management,
        ),
        faults=faults,
    )


@asynccontextmanager
async def cluster_of(
    count: int,
    *,
    settings: ClusterSettings = QUICK,
    downing: DownStrategy | None = None,
    terminate_on_down: bool = False,
    management: ManagementSettings | None = None,
) -> AsyncIterator[tuple[Node, ...]]:
    """Start `count` systems, each with a cluster daemon and nothing joined yet.

    The systems are named `node1` upwards, and every one is terminated however
    the test ends, so a failure leaves no port bound. Fault injection is
    installed on each before it sends anything, so a test can partition a node
    from the rest without having to arrange it beforehand.

    Args:
        count: How many nodes to start.
        settings: How they gossip.
        downing: The strategy every node resolves a split with, or `None` to
            leave a split blocking convergence, which is the default.
        terminate_on_down: Whether a node that downs itself shuts its own system
            down, as opposed to leaving it running for the test to inspect.
        management: Open a management endpoint on every node, or `None` to leave
            it off. Each node binds port 0, so the endpoints do not collide.

    Yields:
        The nodes, in the order they were started.
    """
    nodes: list[Node] = []
    try:
        for index in range(1, count + 1):
            nodes.append(
                start_node(
                    f"node{index}",
                    settings=settings,
                    downing=downing,
                    terminate_on_down=terminate_on_down,
                    management=management,
                )
            )
        yield tuple(nodes)
    finally:
        for node in reversed(nodes):
            await node.system.terminate()


@asynccontextmanager
async def replacement_for(
    node: Node, *, settings: ClusterSettings = QUICK
) -> AsyncIterator[Node]:
    """Start a node at a terminated one's exact address, the way a restart does.

    A member is named by its address, so a process that comes back has to bind
    the same port under the same system name for the cluster to see the same
    address answering. The port is read back from the address rather than
    chosen, since `cluster_of` binds port zero and the OS settles it. The node
    being replaced has to be terminated first, or its port is still held.

    The replacement is a different member: its incarnation uid is fresh, which
    is what tells the cluster the old one is gone.

    Args:
        node: The node being replaced, already terminated.
        settings: How the replacement gossips.

    Yields:
        The replacement, joined to nothing yet. It is terminated however the
        block ends.

    Raises:
        ValueError: If the node names no port, which a clustered node always
            does since a cluster requires remoting.
    """
    address = Address.parse(node.address)
    if address.port is None:
        msg = f"{node.address} names no port for a replacement to bind"
        raise ValueError(msg)
    replacement = start_node(address.system, port=address.port, settings=settings)
    try:
        yield replacement
    finally:
        await replacement.system.terminate()


def management_port(node: Node) -> int:
    """The management port a node bound, read from the address it reports."""
    address = node.cluster.management_address
    assert address is not None
    return int(address.rsplit(":", 1)[1])


async def management_request(
    port: int,
    method: str,
    path: str,
    *,
    body: dict[str, object] | None = None,
    token: str | None = None,
) -> tuple[int, dict[str, Any]]:
    """Make one HTTP request to a management port and read its JSON answer.

    Done with a raw asyncio connection rather than a blocking client so the
    request runs on the loop the endpoint answers on, without a thread.
    """
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    lines = [f"{method} {path} HTTP/1.1", "Host: 127.0.0.1"]
    if token is not None:
        lines.append(f"Authorization: Bearer {token}")
    payload = b""
    if body is not None:
        payload = json.dumps(body).encode("utf-8")
        lines.append("Content-Type: application/json")
        lines.append(f"Content-Length: {len(payload)}")
    lines.append("Connection: close")
    writer.write(("\r\n".join(lines) + "\r\n\r\n").encode("latin-1") + payload)
    await writer.drain()
    raw = await reader.read()
    writer.close()
    await writer.wait_closed()
    head, _, tail = raw.partition(b"\r\n\r\n")
    code = int(head.split(b"\r\n")[0].split(b" ")[1])
    parsed = json.loads(tail) if tail else {}
    return code, parsed
