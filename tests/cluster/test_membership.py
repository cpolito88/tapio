"""Five systems, real sockets, and the membership they agree on.

These are the tests that would fail if gossip stopped spreading, if the leader
rule stopped agreeing with itself, or if a graceful leave stopped reaching
every node.
"""

import asyncio
from datetime import timedelta

import pytest

from tapio.actor import ActorPath, ActorSystem
from tapio.cluster import Cluster, MemberStatus, WireMessage
from tapio.cluster.daemon import daemon_uri
from tapio.errors import ClusterError
from tapio.testkit import assert_no_leaked_tasks
from tests.cluster.conftest import Node, cluster_of, seeds_of
from tests.failures import eventually

NODES = 5


async def test_a_cluster_converges_from_a_cold_start():
    with assert_no_leaked_tasks():
        async with cluster_of(NODES) as nodes:
            seeds = seeds_of(nodes)

            await asyncio.gather(*(n.cluster.join_seed_nodes(seeds) for n in nodes))

            # Every node is Up, and every node knows every other node is Up.
            # The second half is the one worth asserting: a node that only
            # knew about itself would pass a weaker test.
            for node in nodes:
                assert node.status is MemberStatus.UP
                assert len(node.cluster.members) == NODES
                for other in nodes:
                    assert node.status_of(other.address) is MemberStatus.UP


async def test_every_node_computes_the_same_leader():
    with assert_no_leaked_tasks():
        async with cluster_of(NODES) as nodes:
            seeds = seeds_of(nodes)
            await asyncio.gather(*(n.cluster.join_seed_nodes(seeds) for n in nodes))

            await eventually(
                lambda: all(n.cluster.state.converged for n in nodes), within=5.0
            )

            leaders = {n.cluster.leader for n in nodes}
            assert len(leaders) == 1
            # Not an election: it is the first member in address order, which
            # every node can work out for itself from the same converged view.
            assert leaders == {min(n.address for n in nodes)}


async def test_a_cluster_converges_within_a_bounded_number_of_rounds():
    with assert_no_leaked_tasks():
        async with cluster_of(NODES) as nodes:
            seeds = seeds_of(nodes)
            await asyncio.gather(*(n.cluster.join_seed_nodes(seeds) for n in nodes))
            await eventually(
                lambda: all(n.cluster.state.converged for n in nodes), within=5.0
            )

            rounds = max(n.cluster.gossip_rounds for n in nodes)

            # Gossip spreads to one peer per round, so the bound is a
            # generous multiple of the nodes rather than a tight law. What it
            # catches is the failure that matters: a cluster that only
            # converges because something retries forever.
            assert rounds < NODES * 20


async def test_a_node_joins_a_cluster_that_is_already_running():
    with assert_no_leaked_tasks():
        async with cluster_of(3) as nodes:
            first, second, latecomer = nodes
            seeds = seeds_of([first, second])
            await asyncio.gather(
                first.cluster.join_seed_nodes(seeds),
                second.cluster.join_seed_nodes(seeds),
            )

            await latecomer.cluster.join_seed_nodes(seeds)

            assert latecomer.status is MemberStatus.UP
            await eventually(
                lambda: all(len(n.cluster.members) == 3 for n in nodes), within=5.0
            )
            # It was accepted after the other two, and the order members were
            # accepted in is what "oldest member" will mean.
            numbers = {m.address: m.up_number for m in first.cluster.members}
            assert numbers[latecomer.address] > numbers[first.address]


async def test_only_the_first_seed_may_form_a_cluster():
    with assert_no_leaked_tasks():
        async with cluster_of(2) as nodes:
            first, second = nodes
            seeds = seeds_of(nodes)

            # The second seed alone forms nothing, however long it waits. That
            # is the rule that stops a restart from founding a second cluster
            # beside the first one.
            with pytest.raises(ClusterError, match="did not reach Up"):
                await second.cluster.join_seed_nodes(
                    seeds, timeout=timedelta(milliseconds=500)
                )
            assert second.status is None

            # It was asking the whole time, so the moment the first seed forms
            # the cluster, it is let in with no further prompting.
            await first.cluster.join_seed_nodes(seeds)
            await eventually(lambda: second.status is MemberStatus.UP, within=5.0)


async def test_a_graceful_leave_reaches_removed_on_every_node():
    with assert_no_leaked_tasks():
        async with cluster_of(3) as nodes:
            seeds = seeds_of(nodes)
            await asyncio.gather(*(n.cluster.join_seed_nodes(seeds) for n in nodes))
            leaving, *staying = nodes

            await leaving.cluster.leave()

            assert leaving.status is MemberStatus.REMOVED
            for node in staying:
                await eventually(
                    lambda node=node: (
                        node.status_of(leaving.address) is MemberStatus.REMOVED
                    ),
                    within=5.0,
                )
                # The tombstone stays. Dropping it would let a peer holding an
                # older view put the member back by merging it in again.
                assert leaving.address not in [m.address for m in node.cluster.members]


async def test_leaving_something_this_node_never_joined_says_so():
    with assert_no_leaked_tasks():
        async with cluster_of(1) as nodes:
            with pytest.raises(ClusterError, match="not a member"):
                await nodes[0].cluster.leave()


async def test_joining_with_no_seeds_says_so():
    with assert_no_leaked_tasks():
        async with cluster_of(1) as nodes:
            with pytest.raises(ClusterError, match="no seed nodes"):
                await nodes[0].cluster.join_seed_nodes([])


async def test_a_system_with_remoting_off_cannot_be_clustered():
    with assert_no_leaked_tasks():
        system = ActorSystem("solo")
        try:
            with pytest.raises(ClusterError, match="remoting switched off"):
                Cluster(system)
        finally:
            await system.terminate()


async def test_a_clustered_system_still_leaves_an_empty_registry_behind():
    # The daemon publishes a well-known name, which is an entry in the same
    # registry the leak checks read. It has to go when the actor does.
    async with cluster_of(2) as nodes:
        seeds = seeds_of(nodes)
        await asyncio.gather(*(n.cluster.join_seed_nodes(seeds) for n in nodes))
        for node in nodes:
            await node.system.terminate()

        for node in nodes:
            assert node.system.refs.paths() == ()
            assert node.system.refs.lookup(_daemon_path(node)) is None


async def test_a_peer_addresses_the_daemon_without_knowing_its_incarnation():
    # This is what makes a seed list usable: an address in a configuration
    # file names the daemon over there, and nothing else could.
    async with cluster_of(2) as nodes:
        first, second = nodes

        ref = await first.system.resolve(daemon_uri(second.address), expect=WireMessage)

        assert ref.path.uid == 0
        assert str(ref.path) == f"tapio://{second.system.name}/system/cluster"


def _daemon_path(node: Node) -> ActorPath:
    """The bare path the daemon publishes itself under."""
    return ActorPath.root(node.system.name).child("system").child("cluster")
