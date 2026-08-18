"""A live cluster resolving a partition, once it is given a downing strategy.

These would fail if a strategy stopped being consulted, if the losing side of a
split stopped downing itself, if the winning side stopped writing the loser off
and carrying on, or if a passing blip started downing members that a stable
partition is meant to.
"""

import asyncio

from tapio.cluster import DownAll, KeepMajority, LeaseMajority, LocalLease, MemberStatus
from tapio.testkit import assert_no_leaked_tasks
from tests.cluster.conftest import WATCHFUL, cluster_of, seeds_of
from tests.failures import eventually

# Detect a split quickly, then down it quickly, so a test does not wait on the
# production patience. The window still holds several heartbeats, so a busy
# moment does not read as a partition.
DECISIVE = WATCHFUL.model_copy(update={"down_after": WATCHFUL.heartbeat_interval * 4})

# Long enough to down nothing while a test heals a blip, short everywhere else.
PATIENT = WATCHFUL.model_copy(update={"down_after": WATCHFUL.unreachable_after * 20})


async def joined(nodes):
    """Join every node to the cluster and wait for a converged view."""
    seeds = seeds_of(nodes)
    await asyncio.gather(*(n.cluster.join_seed_nodes(seeds) for n in nodes))
    await eventually(lambda: all(n.cluster.state.converged for n in nodes), within=5.0)


async def test_the_minority_downs_itself_and_the_majority_carries_on():
    with assert_no_leaked_tasks():
        async with cluster_of(3, settings=DECISIVE, downing=KeepMajority()) as nodes:
            first, second, odd = nodes
            await joined(nodes)

            odd.faults.partition()

            # The isolated node is the minority of one, so it downs itself and
            # says so, which is the signal to shut the process down.
            await asyncio.wait_for(odd.cluster.when_downed(), timeout=5.0)
            assert odd.cluster.self_member.status is MemberStatus.DOWN

            # The majority writes the loser off and converges as a smaller
            # cluster, rather than blocking for ever on a member it cannot hear.
            majority = (first, second)
            await eventually(
                lambda: all(
                    n.cluster.state.converged and len(n.cluster.members) == 2
                    for n in majority
                ),
                within=5.0,
            )
            for node in majority:
                assert node.cluster.self_member.status is MemberStatus.UP
                assert node.status_of(odd.address) in (
                    MemberStatus.DOWN,
                    MemberStatus.REMOVED,
                )


async def test_down_all_stops_every_node_on_a_split():
    with assert_no_leaked_tasks():
        async with cluster_of(3, settings=DECISIVE, downing=DownAll()) as nodes:
            await joined(nodes)

            nodes[-1].faults.partition()

            # DownAll keeps nothing, so both sides down themselves: the isolated
            # node alone, and the majority because their own view has a member
            # they cannot hear and the strategy keeps no side at all.
            await asyncio.gather(
                *(asyncio.wait_for(n.cluster.when_downed(), timeout=5.0) for n in nodes)
            )
            for node in nodes:
                assert node.cluster.self_member.status is MemberStatus.DOWN


async def test_a_blip_shorter_than_the_window_downs_nobody():
    with assert_no_leaked_tasks():
        async with cluster_of(3, settings=PATIENT, downing=KeepMajority()) as nodes:
            first, _second, odd = nodes
            await joined(nodes)

            odd.faults.partition()
            # Wait only until the split is seen, which stops convergence, then
            # heal it well inside the downing window.
            await eventually(lambda: not first.cluster.state.converged, within=5.0)
            odd.faults.heal()

            # A split that does not hold is ridden out: nobody is downed, and
            # the cluster comes back with the three members it started with.
            await eventually(
                lambda: all(n.cluster.state.converged for n in nodes), within=10.0
            )
            for node in nodes:
                assert not node.cluster._daemon.downed.is_set()
                assert len(node.cluster.members) == 3
                for member in node.cluster.state.members:
                    assert member.status is MemberStatus.UP


async def test_lease_majority_leaves_one_survivor_of_an_even_split():
    with assert_no_leaked_tasks():
        # A shared lease stands in for one held outside the split, which is what
        # every node holding the same object gives when the nodes share a
        # process. Two nodes cut apart is an even split with no majority.
        lease = LocalLease()
        strategy = LeaseMajority(lease=lease)
        async with cluster_of(2, settings=DECISIVE, downing=strategy) as nodes:
            await joined(nodes)

            nodes[0].faults.partition()

            # The lease admits one owner, so one side takes it and lives while
            # the other cannot and downs itself: exactly one survivor, which is
            # the guarantee the count could not make about an even split.
            await eventually(
                lambda: (
                    sum(
                        n.cluster.self_member.status is MemberStatus.DOWN for n in nodes
                    )
                    == 1
                ),
                within=5.0,
            )
            survivors = [
                n for n in nodes if n.cluster.self_member.status is MemberStatus.UP
            ]
            assert len(survivors) == 1


async def test_terminate_on_down_shuts_the_losing_node_down():
    with assert_no_leaked_tasks():
        async with cluster_of(
            3, settings=DECISIVE, downing=KeepMajority(), terminate_on_down=True
        ) as nodes:
            first, second, odd = nodes
            await joined(nodes)

            odd.faults.partition()

            # The minority downs itself, and because terminate_on_down is set it
            # shuts its own system down rather than leaving that to the caller.
            await asyncio.wait_for(odd.system.when_terminated(), timeout=5.0)
            assert odd.system.is_terminating

            # The majority is untouched and carries on as a smaller cluster.
            await eventually(
                lambda: all(
                    n.cluster.state.converged and len(n.cluster.members) == 2
                    for n in (first, second)
                ),
                within=5.0,
            )
            for node in (first, second):
                assert node.cluster.self_member.status is MemberStatus.UP
