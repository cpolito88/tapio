"""An operator reading a cluster and downing a member, over the management port.

Concepts: `ManagementSettings`, the small HTTP surface a node opens for an
operator, and the `tapio-cluster` command that speaks to it. A node given
management settings answers a few HTTP requests: one reads what it believes
about the cluster, and the others ask it to let a member leave or to down one.

The point of the port is that it is out of band. Reading membership and downing
a stuck member are things an operator does from outside the application, without
a code path in the application for either. Here the requests are made with the
standard library so the example needs nothing extra, but they are the same
requests the `tapio-cluster` command makes:

```bash
tapio-cluster --port 25530 status
tapio-cluster --port 25530 down tapio://cluster@127.0.0.1:...
```

Downing is ordinarily a strategy's decision about which side of a split lives.
This is the operator's version of the same move, for a member no strategy will
reach: it goes to `down` exactly as a strategy would put it there, and the
decision travels to every node as gossip. The node the operator asked answers
the moment it has been asked, not once the member has gone, which is why the
example waits for the membership to settle rather than reading it straight away.

Three systems run in this one process on loopback ports the OS picks, so the
example needs no orchestration and no second machine.

Run it with `uv run python -m tapio_examples.cluster_management`.
"""

import asyncio
import json
from datetime import timedelta

from tapio import ActorSystem, RemoteSettings, TapioSettings
from tapio.cluster import Cluster, MemberStatus
from tapio.settings import ClusterSettings, ManagementSettings

__all__ = ["main"]


def node() -> TapioSettings:
    """Settings for one node: remoting on, on a loopback port the OS picks."""
    return TapioSettings(remote=RemoteSettings(bind_port=0))


def gossiping() -> ClusterSettings:
    """Gossip often enough that an example finishes while you watch it."""
    return ClusterSettings(
        gossip_interval=timedelta(milliseconds=50),
        join_retry_interval=timedelta(milliseconds=50),
        seed_form_after=timedelta(milliseconds=200),
    )


async def request(
    port: int, method: str, path: str, body: dict[str, str] | None = None
) -> dict[str, object]:
    """Make one HTTP request to a management port and read its JSON answer.

    The management surface is plain HTTP, so this is a raw request written by
    hand rather than a client library, to show there is nothing more to it.

    Args:
        port: The management port to reach.
        method: The HTTP method.
        path: The path to request.
        body: The JSON body to send, or `None` for a request with no body.

    Returns:
        The parsed JSON answer.
    """
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    lines = [f"{method} {path} HTTP/1.1", "Host: 127.0.0.1"]
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
    _, _, tail = raw.partition(b"\r\n\r\n")
    parsed: dict[str, object] = json.loads(tail) if tail else {}
    return parsed


async def until_gone(cluster: Cluster, address: str) -> None:
    """Wait until a member has left a node's live membership.

    A down travels as gossip, so a node the operator did not ask learns about
    it a round later. Reading straight away would catch it before the news
    arrived, which is a race in the reader rather than in the cluster.

    Args:
        cluster: The node doing the watching.
        address: The member it is waiting to see leave.

    Raises:
        TimeoutError: If the member is still listed after five seconds, which
            with gossip every 50ms means something is wrong rather than slow.
    """
    async with asyncio.timeout(5.0):
        while True:
            if all(member.address != address for member in cluster.members):
                return
            await asyncio.sleep(0.005)


async def main() -> list[str]:
    """Run the example.

    Returns:
        The lines the operator's view produced, in order.
    """
    lines: list[str] = []

    async with (
        ActorSystem("node1", node()) as first,
        ActorSystem("node2", node()) as second,
        ActorSystem("node3", node()) as third,
    ):
        systems = (first, second, third)
        # Only the first node opens a management port. An operator reaches the
        # cluster through any one node, since every node holds the whole view.
        clusters = [
            Cluster(
                system,
                gossiping(),
                management=ManagementSettings(bind_port=0) if system is first else None,
            )
            for system in systems
        ]
        seeds = [cluster.address for cluster in clusters]
        await asyncio.gather(*(cluster.join_seed_nodes(seeds) for cluster in clusters))

        address = clusters[0].management_address
        assert address is not None
        port = int(address.rsplit(":", 1)[1])

        status = await request(port, "GET", "/status")
        members = status["members"]
        assert isinstance(members, list)
        lines.append(f"operator sees {len(members)} members, leader {status['leader']}")

        # Down node3 through node1's port. The operator never touches node3.
        victim = clusters[2].address
        answer = await request(port, "POST", "/down", {"address": victim})
        lines.append(f"asked node1 to {answer['accepted']} node3")

        # The decision reaches node2, which the operator did not ask, as gossip.
        await until_gone(clusters[1], victim)
        remaining = await request(port, "GET", "/status")
        members = remaining["members"]
        assert isinstance(members, list)
        alive = [m["address"] for m in members if m["status"] != MemberStatus.DOWN]
        lines.append(f"after downing node3, {len(alive)} members remain live")

    for line in lines:
        print(line)
    return lines


if __name__ == "__main__":
    asyncio.run(main())
