"""What the transport says about a peer, arriving in cluster membership.

Remoting publishes a verdict about a link. These check that it becomes this
node's observation about a member, that it can be taken back, and that it is
ignored when it is not about a member. Whether the verdict itself is right is
`tests/remote/test_unreachable.py`.
"""

import asyncio

from tapio.cluster.monitor import monitored_by
from tapio.cluster.reachability import ReachabilityStatus
from tapio.remote.failure import PeerReachable, PeerUnreachable
from tapio.testkit import assert_no_leaked_tasks
from tests.cluster.conftest import QUICK, cluster_of, seeds_of
from tests.failures import eventually

UNREACHABLE = ReachabilityStatus.UNREACHABLE
REACHABLE = ReachabilityStatus.REACHABLE


async def joined_pair(nodes):
    """Join two nodes and wait until each knows about the other."""
    seeds = seeds_of(nodes)
    await asyncio.gather(*(n.cluster.join_seed_nodes(seeds) for n in nodes))
    await eventually(lambda: all(len(n.cluster.members) == 2 for n in nodes))


async def test_a_peer_the_transport_gave_up_on_becomes_an_observation():
    with assert_no_leaked_tasks():
        async with cluster_of(2) as nodes:
            first, second = nodes
            await joined_pair(nodes)

            first.system.events.publish(
                PeerUnreachable(
                    peer=second.address, uid=1, detail="silent", quarantined=True
                )
            )

            # This node's opinion, and nobody else's: the observer that said
            # it is the only one that can take it back.
            await eventually(
                lambda: (
                    first.cluster.state.reachability.says(first.address, second.address)
                    is UNREACHABLE
                )
            )
            assert second.address in first.cluster.state.reachability.unreachable


async def test_the_observation_stops_the_view_converging():
    with assert_no_leaked_tasks():
        async with cluster_of(2) as nodes:
            first, second = nodes
            await joined_pair(nodes)
            await eventually(lambda: first.cluster.state.converged)

            first.system.events.publish(
                PeerUnreachable(
                    peer=second.address, uid=1, detail="silent", quarantined=True
                )
            )

            # One member nobody can hear stops the leader acting, which is the
            # whole reason membership wants to know about a link at all.
            await eventually(lambda: not first.cluster.state.converged)


async def test_a_peer_that_comes_back_retracts_the_observation():
    with assert_no_leaked_tasks():
        async with cluster_of(2) as nodes:
            first, second = nodes
            await joined_pair(nodes)
            first.system.events.publish(
                PeerUnreachable(
                    peer=second.address, uid=1, detail="silent", quarantined=True
                )
            )
            await eventually(
                lambda: (
                    first.cluster.state.reachability.says(first.address, second.address)
                    is UNREACHABLE
                )
            )

            first.system.events.publish(PeerReachable(peer=second.address, uid=2))

            # Without this the cluster would never converge again after a
            # blip, because nothing else can retract what this node said.
            await eventually(
                lambda: (
                    first.cluster.state.reachability.says(first.address, second.address)
                    is REACHABLE
                )
            )
            await eventually(lambda: first.cluster.state.converged)


async def test_the_transport_is_not_recorded_for_a_member_off_this_nodes_ring():
    # Every record stays in the gossip for good, so a record for every member
    # this node ever had a link to grows with the square of the cluster.
    settings = QUICK.model_copy(update={"monitored_peers": 1})
    with assert_no_leaked_tasks():
        async with cluster_of(4, settings=settings) as nodes:
            seeds = seeds_of(nodes)
            await asyncio.gather(*(n.cluster.join_seed_nodes(seeds) for n in nodes))
            await eventually(lambda: all(len(n.cluster.members) == 4 for n in nodes))
            first = nodes[0]
            members = first.cluster.state.alive
            # With `watched` unreachable the ring reaches `next_one` and stops,
            # so `off_ring` stays off it.
            watched, next_one, off_ring = monitored_by(first.address, members, 3)

            first.system.events.publish(
                PeerUnreachable(peer=off_ring, uid=1, detail="silent", quarantined=True)
            )
            # Handled after the event above, so once this one is recorded the
            # first has been handled too.
            first.system.events.publish(
                PeerUnreachable(peer=watched, uid=1, detail="silent", quarantined=True)
            )
            await eventually(
                lambda: (
                    first.cluster.state.reachability.says(first.address, watched)
                    is UNREACHABLE
                )
            )

            assert first.cluster.monitored == tuple(sorted((watched, next_one)))
            assert not any(
                record.observed == off_ring
                for record in first.cluster.state.reachability.records
            )


async def test_the_ring_reaches_past_an_unreachable_member_and_keeps_the_verdict():
    # A member this node watches is unreachable, so the ring reaches on to the
    # next one. That one starts from what the transport already said about it,
    # which is how every far member of a split stays observed from this side.
    settings = QUICK.model_copy(update={"monitored_peers": 1})
    with assert_no_leaked_tasks():
        async with cluster_of(3, settings=settings) as nodes:
            seeds = seeds_of(nodes)
            await asyncio.gather(*(n.cluster.join_seed_nodes(seeds) for n in nodes))
            await eventually(lambda: all(len(n.cluster.members) == 3 for n in nodes))
            first = nodes[0]
            members = first.cluster.state.alive
            (watched,) = monitored_by(first.address, members, 1)
            off_ring = next(
                m.address for m in members if m.address not in (first.address, watched)
            )
            first.system.events.publish(
                PeerUnreachable(peer=off_ring, uid=1, detail="silent", quarantined=True)
            )

            first.system.events.publish(
                PeerUnreachable(peer=watched, uid=1, detail="silent", quarantined=True)
            )

            await eventually(lambda: off_ring in first.cluster.monitored)
            await eventually(
                lambda: (
                    first.cluster.state.reachability.says(first.address, off_ring)
                    is UNREACHABLE
                )
            )


async def test_a_peer_that_is_not_a_member_is_not_recorded():
    with assert_no_leaked_tasks():
        async with cluster_of(2) as nodes:
            first, _second = nodes
            await joined_pair(nodes)
            before = first.cluster.state.version

            first.system.events.publish(
                PeerUnreachable(
                    peer="tapio://stranger@127.0.0.1:1",
                    uid=1,
                    detail="silent",
                    quarantined=True,
                )
            )
            await asyncio.sleep(0.05)

            # A peer this system talks to but has not clustered with is
            # nobody's business, and its record would never be cleaned up.
            assert first.cluster.state.reachability.records == ()
            assert first.cluster.state.version == before


async def test_saying_the_same_thing_twice_gossips_nothing_new():
    with assert_no_leaked_tasks():
        async with cluster_of(2) as nodes:
            first, second = nodes
            await joined_pair(nodes)

            # Published on every link that opens, so the common case by far is
            # news this node already has.
            first.system.events.publish(PeerReachable(peer=second.address, uid=1))
            await asyncio.sleep(0.05)
            settled = first.cluster.state.version

            first.system.events.publish(PeerReachable(peer=second.address, uid=1))
            await asyncio.sleep(0.05)

            assert first.cluster.state.version == settled
            assert first.cluster.state.reachability.records == ()


async def test_a_stopped_daemon_stops_listening_to_the_links():
    async with cluster_of(1) as nodes:
        (only,) = nodes
        assert PeerUnreachable in list(only.system.events)
        assert PeerReachable in list(only.system.events)

        await only.system.terminate()

        # A subscription left behind would tell a stopped actor about every
        # link event for as long as the system ran, which is a dead letter
        # each time and a reference to an actor nobody can reach.
        assert list(only.system.events) == []
