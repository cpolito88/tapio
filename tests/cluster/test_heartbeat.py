"""Nodes probing the members they watch, and what a partition does to a cluster.

These are the tests that would fail if a member stopped being watched by
anybody, if a partition stopped being noticed on both sides, or if healing one
stopped bringing the cluster back without somebody being written off.
"""

import asyncio

from tapio.cluster import MemberStatus
from tapio.cluster.daemon import daemon_uri
from tapio.cluster.messages import Heartbeat, WireMessage
from tapio.remote.address import Address
from tapio.testkit import assert_no_leaked_tasks
from tests.cluster.conftest import WATCHFUL, cluster_of, seeds_of
from tests.failures import eventually


async def joined(nodes):
    """Join every node to the cluster and wait for a converged view."""
    seeds = seeds_of(nodes)
    await asyncio.gather(*(n.cluster.join_seed_nodes(seeds) for n in nodes))
    await eventually(lambda: all(n.cluster.state.converged for n in nodes), within=5.0)


async def test_every_member_is_watched_by_somebody():
    with assert_no_leaked_tasks():
        async with cluster_of(3, settings=WATCHFUL) as nodes:
            await joined(nodes)

            await eventually(lambda: all(n.cluster.monitored for n in nodes))

            watchers = {n.address: 0 for n in nodes}
            for node in nodes:
                for peer in node.cluster.monitored:
                    watchers[peer] += 1
            # Nobody arranges this and nobody is told about it: each node
            # sorts the same addresses and takes its own share of the ring.
            assert all(count > 0 for count in watchers.values())
            assert all(node.address not in node.cluster.monitored for node in nodes)


async def test_a_partitioned_node_is_unreachable_on_both_sides():
    with assert_no_leaked_tasks():
        async with cluster_of(3, settings=WATCHFUL) as nodes:
            first, second, odd = nodes
            await joined(nodes)

            odd.faults.partition()

            # Both sides are alive and neither can hear the other, so both are
            # locally correct. The cluster records who says what rather than
            # deciding, because deciding is downing.
            for node in (first, second):
                await eventually(
                    lambda node=node: (
                        odd.address in node.cluster.state.reachability.unreachable
                    ),
                    within=5.0,
                )
            await eventually(
                lambda: (
                    {first.address, second.address}
                    <= odd.cluster.state.reachability.unreachable
                ),
                within=5.0,
            )
            # Nobody is written off. An unreachable member is still a member
            # until a downing strategy says otherwise, and there is none yet.
            for node in nodes:
                for member in node.cluster.state.members:
                    assert member.status is MemberStatus.UP


async def test_the_view_stops_converging_and_comes_back_on_a_heal():
    with assert_no_leaked_tasks():
        async with cluster_of(3, settings=WATCHFUL) as nodes:
            first, _second, odd = nodes
            await joined(nodes)

            odd.faults.partition()
            await eventually(lambda: not first.cluster.state.converged, within=5.0)

            odd.faults.heal()

            # The claim is retracted by the node that made it, on the evidence
            # of an answer arriving again, and the cluster converges with the
            # same three members it started with.
            await eventually(
                lambda: all(n.cluster.state.converged for n in nodes), within=10.0
            )
            for node in nodes:
                assert node.cluster.state.reachability.unreachable == frozenset()
                assert len(node.cluster.members) == 3


async def test_a_member_the_transport_gave_up_on_is_knocked_on_again():
    with assert_no_leaked_tasks():
        async with cluster_of(2, settings=WATCHFUL) as nodes:
            first, second = nodes
            await joined(nodes)
            peer = Address.parse(second.address)

            first.system.remote.quarantine(peer, "this system gave up alone")

            # Remoting gives up for good and waits to be told otherwise, which
            # is the right answer for a system with no membership to consult.
            # A cluster has one: nobody has decided this member is gone, so
            # the node that watches it keeps knocking until somebody does.
            await eventually(
                lambda: not first.system.remote.is_quarantined(peer), within=5.0
            )
            await eventually(lambda: first.cluster.state.converged, within=10.0)


async def test_a_member_this_node_does_not_watch_is_knocked_on_too():
    with assert_no_leaked_tasks():
        watchful = WATCHFUL.model_copy(update={"monitored_peers": 1})
        async with cluster_of(4, settings=watchful) as nodes:
            first, *rest = nodes
            await joined(nodes)
            await eventually(lambda: all(n.cluster.monitored for n in nodes))

            stranger = next(n for n in rest if n.address not in first.cluster.monitored)
            peer = Address.parse(stranger.address)
            first.system.remote.quarantine(peer, "this system gave up alone")

            # Gossip goes to any member, not only the ones this node watches,
            # so forgiving just the ring would leave these two refusing each
            # other for good. In a cluster larger than monitored_peers that is
            # most pairs, and a healed partition would never converge again.
            await eventually(
                lambda: not first.system.remote.is_quarantined(peer), within=5.0
            )
            await eventually(
                lambda: all(n.cluster.state.converged for n in nodes), within=10.0
            )


async def test_answering_a_stranger_does_not_grow_the_ref_cache():
    with assert_no_leaked_tasks():
        async with cluster_of(2, settings=WATCHFUL) as nodes:
            first, second = nodes
            await joined(nodes)
            daemon = await second.system.resolve(
                daemon_uri(first.address), expect=WireMessage
            )

            daemon.tell(Heartbeat(sender="tapio://nobody@127.0.0.1:1"))

            # A few rounds, so the answer has certainly been sent by now.
            sent = first.cluster.heartbeats_sent
            await eventually(
                lambda: first.cluster.heartbeats_sent > sent + 2, within=5.0
            )

            # The asker is answered whether or not it is a member, since a
            # node behind on membership is the one that should not also look
            # dead. Keeping its ref is the part that is not safe: the address
            # came off a socket, so a cache keyed by it is one anybody can
            # grow.
            cached = set(first.cluster._daemon._peers)
            assert cached <= {n.address for n in nodes} | set(seeds_of(nodes))


async def test_the_probing_is_bounded_by_the_ring_and_not_by_the_cluster():
    with assert_no_leaked_tasks():
        watchful = WATCHFUL.model_copy(update={"monitored_peers": 1})
        async with cluster_of(4, settings=watchful) as nodes:
            await joined(nodes)
            await eventually(lambda: all(n.cluster.monitored for n in nodes))

            before = [n.cluster.heartbeats_sent for n in nodes]
            await asyncio.sleep(0.2)
            sent = [
                n.cluster.heartbeats_sent - was
                for n, was in zip(nodes, before, strict=True)
            ]

            # One peer watched is one probe a round, whatever the cluster's
            # size. Ten rounds fit in the window, and the bound is generous
            # because what it has to catch is the failure that matters: a node
            # probing everybody, which would be four times this and quadratic
            # across the cluster.
            assert all(
                n.cluster.monitored == () or len(n.cluster.monitored) == 1
                for n in nodes
            )
            assert all(count <= 15 for count in sent)
            assert any(count > 0 for count in sent)
