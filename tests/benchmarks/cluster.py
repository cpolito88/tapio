"""How fast a cluster agrees, and what it costs to keep agreeing, as it grows.

These are the numbers behind the README's claim that membership is gossip and
its cost stays modest as nodes are added. Like the resident-actor figures, this
is not a pytest-benchmark test: it starts, converges and tears down whole
clusters, the largest of which takes long enough that it should be run on
purpose.

```
make bench-cluster
```

Everything here runs the real thing: real systems, real sockets, real
handshakes, real gossip. A cluster of `n` nodes lives in this one process on
loopback ports the OS picks, so the benchmark needs no orchestration and no
second machine.

Two things are measured at each size:

Convergence, in gossip rounds and in seconds. Rounds are the portable number,
since seconds are only rounds times the gossip interval. This runs at the
default one-second interval, so the seconds are what a real deployment would
see; only the join impatience is shortened, because waiting out the first
seed's forming delay is startup and not steady state, and it does not touch
what is measured.

Gossip bandwidth, per node, in steady state after the cluster has converged.
It is the rate at which a node sends gossip and heartbeat frames, read from the
counters the cluster already keeps, times the size of those frames encoded by
the same codec the wire uses. It is the term that grows with the cluster,
because a gossip frame carries the whole view: the point of publishing it is to
show that the growth is linear and the constant is small.

One caveat comes from the single process. All the nodes share one event loop,
which no real deployment does, so the largest cluster is bottlenecked on that
loop rather than on anything in the runtime: at fifty nodes the gossip timer
itself slips below once a second because the loop is busy, which the measured
rate shows and the reported bandwidth honestly includes. The round counts and
the frame sizes are the portable numbers; the wall-clock seconds are the
pessimistic end of what one machine hosting the whole cluster would see.
"""

import asyncio
import json
import logging
import sys
import time
from datetime import timedelta

from tests.benchmarks.machine import as_text
from tests.failures import eventually

from tapio.actor import ActorSystem
from tapio.cluster import Cluster
from tapio.cluster.daemon import DAEMON_NAME
from tapio.cluster.messages import GossipEnvelope, Heartbeat
from tapio.remote.address import Address
from tapio.remote.codec import encode, parse_target
from tapio.settings import ClusterSettings, RemoteSettings, TapioSettings

SIZES = (5, 20, 50)
"""How many nodes to bring up, roughly a factor of three apart."""

WINDOW = 5.0
"""Seconds of steady state to average the send rate over, once converged.

Long enough at the default one-second gossip that several rounds and a couple
of dozen heartbeats fall inside it, so the rate is a rate and not one sample.
"""

CONVERGE_TIMEOUT = 90.0
"""How long to wait for a size to converge before giving up on it.

Generous: fifty nodes gossiping once a second take a while, and a benchmark
that gives up early would publish a number for a cluster that had not settled.
"""


def remoting() -> TapioSettings:
    """Settings for one node: remoting on, on a loopback port the OS picks."""
    return TapioSettings(
        _env_file=None,  # type: ignore[call-arg]
        remote=RemoteSettings(_env_file=None, bind_port=0),  # type: ignore[call-arg]
    )


def gossiping() -> ClusterSettings:
    """Cluster settings that gossip at the default rate but join quickly.

    Gossip and heartbeats run at their defaults, one second each, so the
    bandwidth measured is what a real deployment sends. Only three things are
    changed, and none of them touch what is measured:

    `seed_form_after` and `join_retry_interval` are shortened, because they
    govern how long the cluster spends forming rather than how much it gossips
    once formed.

    `unreachable_after` is stretched well past a whole run. All the nodes share
    one event loop here, which no real deployment does, so under the load of
    forty-nine peers a node can be slow to answer a probe and look briefly
    unreachable when it is only busy. That false verdict would ride along in the
    gossip and inflate the frame with reachability records that say nothing
    about the membership this benchmark is sizing. Stretching the window keeps
    the single-process artifact out of the number; failure detection has its
    own tests, and this is not one of them.

    Returns:
        The settings every node in a run shares.
    """
    return ClusterSettings(
        _env_file=None,  # type: ignore[call-arg]
        seed_form_after=timedelta(milliseconds=200),
        join_retry_interval=timedelta(milliseconds=100),
        unreachable_after=timedelta(seconds=300),
    )


def gossip_frame_bytes(cluster: Cluster) -> int:
    """Size the frame one gossip round puts on the wire, for this cluster's view.

    Encoded through the same codec the transport uses, from the node's actual
    converged state, so it is the real frame and not an estimate. The view is
    what grows with the cluster, so this is the size that scales.

    Args:
        cluster: A converged node, whose whole view the frame would carry.

    Returns:
        The frame's length in bytes, length prefix included.
    """
    sender = Address.parse(cluster.address)
    envelope = GossipEnvelope(sender=cluster.address, gossip=cluster.state)
    target = parse_target(sender.system, f"/system/{DAEMON_NAME}")
    return len(encode(envelope, to=target, sender=sender))


def heartbeat_frame_bytes(cluster: Cluster) -> int:
    """Size the frame one heartbeat probe puts on the wire.

    Constant, unlike gossip: a heartbeat asks whether a peer is answering and
    carries only who is asking.

    Args:
        cluster: The node the probe would come from.

    Returns:
        The frame's length in bytes, length prefix included.
    """
    sender = Address.parse(cluster.address)
    probe = Heartbeat(sender=cluster.address)
    target = parse_target(sender.system, f"/system/{DAEMON_NAME}")
    return len(encode(probe, to=target, sender=sender))


async def measure(count: int) -> dict[str, float]:
    """Bring up `count` nodes, converge them, and measure agreement and traffic.

    Args:
        count: How many nodes to start.

    Returns:
        The measurements, as plain numbers, for the caller to tabulate.
    """
    settings = gossiping()
    systems = [ActorSystem(f"node{index}", remoting()) for index in range(1, count + 1)]
    clusters = [Cluster(system, settings) for system in systems]
    seeds = [cluster.address for cluster in clusters]
    try:
        started = time.perf_counter()
        await asyncio.gather(*(cluster.join_seed_nodes(seeds) for cluster in clusters))
        # join_seed_nodes returns when a node sees itself Up; the cluster has
        # converged only when every node holds the same complete view.
        await eventually(
            lambda: all(
                cluster.state.converged and len(cluster.members) == count
                for cluster in clusters
            ),
            within=CONVERGE_TIMEOUT,
            interval=0.05,
        )
        converge_seconds = time.perf_counter() - started
        converge_rounds = max(cluster.gossip_rounds for cluster in clusters)

        # Steady state: how fast each node sends, averaged over a window that
        # holds several rounds, so a single unlucky sample does not set it.
        gossip_before = sum(cluster.gossip_rounds for cluster in clusters)
        heartbeat_before = sum(cluster.heartbeats_sent for cluster in clusters)
        window_start = time.perf_counter()
        await asyncio.sleep(WINDOW)
        window = time.perf_counter() - window_start
        gossip_sent = sum(cluster.gossip_rounds for cluster in clusters) - gossip_before
        heartbeat_sent = (
            sum(cluster.heartbeats_sent for cluster in clusters) - heartbeat_before
        )
        gossip_rate = gossip_sent / count / window
        heartbeat_rate = heartbeat_sent / count / window

        gossip_bytes = gossip_frame_bytes(clusters[0])
        heartbeat_bytes = heartbeat_frame_bytes(clusters[0])
        bandwidth = gossip_rate * gossip_bytes + heartbeat_rate * heartbeat_bytes

        return {
            "nodes": count,
            "converge_rounds": converge_rounds,
            "converge_seconds": converge_seconds,
            "gossip_frame_bytes": gossip_bytes,
            "gossip_per_second": gossip_rate,
            "heartbeat_per_second": heartbeat_rate,
            "bandwidth_bytes_per_second": bandwidth,
        }
    finally:
        for system in reversed(systems):
            await system.terminate()


async def run_all() -> list[dict[str, float]]:
    """Measure every size in turn, tearing each cluster down before the next.

    Returns:
        One row of measurements per size.
    """
    return [await measure(count) for count in SIZES]


def main() -> None:
    """Measure every size and print the table the README carries."""
    # Tearing down a live cluster leaves in-flight frames aimed at nodes that
    # have just stopped, and each one is a dead letter the runtime logs. That
    # is correct behaviour and noise here, so the log is quieted around the run
    # and the table is what reaches the terminal.
    logging.disable(logging.CRITICAL)
    rows = asyncio.run(run_all())
    print(as_text())
    print()
    print(f"{'nodes':>6}  {'converge':>13}  {'gossip frame':>12}  {'per node':>11}")
    for row in rows:
        print(
            f"{int(row['nodes']):>6}  "
            f"{int(row['converge_rounds']):>3} rounds "
            f"{row['converge_seconds']:>4.1f}s  "
            f"{row['gossip_frame_bytes'] / 1024:>9.1f}KB  "
            f"{row['bandwidth_bytes_per_second'] / 1024:>8.1f}KB/s"
        )
    print()
    for row in rows:
        print(json.dumps(row))


if __name__ == "__main__":
    main()
    sys.exit(0)
