"""A cluster singleton: one instance, on the oldest member, moved when it goes.

The probe is the whole point of these tests. It records every start and stop of
the instance across every node in the process, so a second instance running
beside the first, the failure a singleton exists to prevent, shows up as a
concurrency the probe would have seen. These would fail if the instance ran on
more than the oldest member, if it did not reappear when its host was removed,
or if the successor started beside a host that was still running it.
"""

import asyncio

from tapio import Behavior, Behaviors, Message
from tapio.actor import ActorContext, Signal
from tapio.actor.signals import PostStop
from tapio.cluster import ClusterSingleton, KeepMajority
from tapio.testkit import assert_no_leaked_tasks
from tests.cluster.conftest import WATCHFUL, cluster_of, seeds_of
from tests.failures import eventually

# Detect a lost node quickly and down it quickly, so the handoff a test is
# about happens in a test's patience rather than a deployment's.
DECISIVE = WATCHFUL.model_copy(update={"down_after": WATCHFUL.heartbeat_interval * 4})


class Ping(Message):
    """A message the instance does not need to do anything with."""


class Probe:
    """What every instance reports its life to, across every node."""

    def __init__(self) -> None:
        """Start with nothing running and nothing seen."""
        self.running: set[str] = set()
        self.starts: list[str] = []
        self.max_seen = 0

    def started(self, address: str) -> None:
        """Record that an instance started on a node."""
        self.starts.append(address)
        self.running.add(address)
        self.max_seen = max(self.max_seen, len(self.running))

    def stopped(self, address: str) -> None:
        """Record that the instance on a node stopped."""
        self.running.discard(address)


def instance(probe: Probe, address: str) -> Behavior[Ping]:
    """The singleton instance: it reports its start and stop to the probe.

    Args:
        probe: Where the instance's life is recorded.
        address: The node it runs on, which is what the probe tracks.

    Returns:
        The behavior.
    """

    def build(ctx: ActorContext[Ping]) -> Behavior[Ping]:
        probe.started(address)

        async def on_message(message: Ping) -> Behavior[Ping]:
            return Behaviors.same()

        async def on_signal(ctx: ActorContext[Ping], signal: Signal) -> Behavior[Ping]:
            if isinstance(signal, PostStop):
                probe.stopped(address)
            return Behaviors.same()

        return Behaviors.receive_message(on_message, msg_type=Ping, on_signal=on_signal)

    return Behaviors.setup(build)


async def joined(nodes):
    """Join every node and wait for a converged view."""
    seeds = seeds_of(nodes)
    await asyncio.gather(*(n.cluster.join_seed_nodes(seeds) for n in nodes))
    await eventually(lambda: all(n.cluster.state.converged for n in nodes), within=5.0)


async def test_a_singleton_runs_on_the_oldest_member_alone():
    probe = Probe()
    with assert_no_leaked_tasks():
        async with cluster_of(3) as nodes:
            await joined(nodes)
            for node in nodes:
                node.system.spawn(
                    ClusterSingleton(instance(probe, node.address), name="coordinator"),
                    name="singleton",
                )

            await eventually(lambda: len(probe.running) == 1, within=5.0)
            oldest = min(nodes, key=lambda n: n.cluster.self_member.up_number)
            assert probe.running == {oldest.address}
            # The one that matters: never two at once, even for a moment while
            # the managers were working out which of them was oldest.
            assert probe.max_seen == 1


async def test_a_graceful_leave_does_not_overlap_two_instances():
    probe = Probe()
    with assert_no_leaked_tasks():
        async with cluster_of(3) as nodes:
            await joined(nodes)
            for node in nodes:
                node.system.spawn(
                    ClusterSingleton(instance(probe, node.address), name="coordinator"),
                    name="singleton",
                )
            await eventually(lambda: len(probe.running) == 1, within=5.0)
            host = next(iter(probe.running))
            host_node = next(node for node in nodes if node.address == host)

            # A graceful leave, not a crash. The host walks out through
            # leaving, exiting and removed, and it is the node that drives its
            # own transition, so it hears MemberLeaving first and lets its
            # instance go before a successor computed from the removal starts.
            await host_node.cluster.leave()

            await eventually(
                lambda: len(probe.running) == 1 and host not in probe.running,
                within=15.0,
            )
            # The one that matters: the successor never ran beside the host,
            # even for the gossip round between leaving and removed.
            assert probe.max_seen == 1
            assert any(address != host for address in probe.starts)


async def test_a_singleton_reappears_when_its_host_is_removed():
    probe = Probe()
    with assert_no_leaked_tasks():
        async with cluster_of(3, settings=DECISIVE, downing=KeepMajority()) as nodes:
            await joined(nodes)
            for node in nodes:
                node.system.spawn(
                    ClusterSingleton(instance(probe, node.address), name="coordinator"),
                    name="singleton",
                )
            await eventually(lambda: len(probe.running) == 1, within=5.0)
            host = next(iter(probe.running))

            # The host goes away for real, the way a crashed node does. The
            # majority sees it fall silent, downs it, and writes it off.
            host_node = next(node for node in nodes if node.address == host)
            await host_node.system.terminate()

            # It comes back on a surviving node, and only once the host was
            # removed, so the two never ran together.
            await eventually(
                lambda: len(probe.running) == 1 and host not in probe.running,
                within=15.0,
            )
            assert probe.max_seen == 1
            assert any(address != host for address in probe.starts)
