"""A live cluster resolving a partition, once it is given a downing strategy.

These would fail if a strategy stopped being consulted, if the losing side of a
split stopped downing itself, if the winning side stopped writing the loser off
and carrying on, if a passing blip started downing members that a stable
partition is meant to, or if the shutdown a downed node starts stopped being a
named task.
"""

import asyncio

from tapio import Behavior, Behaviors
from tapio.cluster import DownAll, KeepMajority, LeaseMajority, LocalLease, MemberStatus
from tapio.cluster.events import SelfDown
from tapio.cluster.gossip import Gossip
from tapio.cluster.messages import ClusterDowned, Down
from tapio.testkit import assert_no_leaked_tasks
from tests.cluster.conftest import (
    WATCHFUL,
    cluster_of,
    daemon_running,
    seeds_of,
    start_node,
)
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
            assert odd.status is MemberStatus.DOWN

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
                assert node.status is MemberStatus.UP
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
                assert node.status is MemberStatus.DOWN


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
                lambda: sum(n.status is MemberStatus.DOWN for n in nodes) == 1,
                within=5.0,
            )
            survivors = [n for n in nodes if n.status is MemberStatus.UP]
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
                assert node.status is MemberStatus.UP


async def test_the_shutdown_a_downed_node_starts_carries_a_name():
    with assert_no_leaked_tasks():
        node = start_node("solo", downing=KeepMajority(), terminate_on_down=True)
        try:
            # Subscribers run inline, so the shutdown task exists by the time
            # publish returns. Finding it by name is the point: the leak check
            # reports what it found by name, and an unnamed shutdown would
            # answer that question with "Task-17".
            node.system.events.publish(
                ClusterDowned(address=node.address, detail="downed by a test")
            )
            named = [
                task
                for task in asyncio.all_tasks()
                if task.get_name() == "tapio-cluster-shutdown:solo"
            ]
            assert len(named) == 1
            await named[0]
        finally:
            await node.system.terminate()


def recorder(seen: list[SelfDown]) -> Behavior[SelfDown]:
    """An actor that writes down every `SelfDown` it is told about."""

    async def on_message(message: SelfDown) -> Behavior[SelfDown]:
        seen.append(message)
        return Behaviors.same()

    return Behaviors.receive_message(on_message, msg_type=SelfDown)


class FailsAtFirst:
    """A strategy that raises a few times, then keeps the majority."""

    def __init__(self, failures: int) -> None:
        """Fail the first `failures` calls."""
        self.failures = failures
        self.calls = 0

    async def decide(self, state: Gossip) -> frozenset[str]:
        self.calls += 1
        if self.calls <= self.failures:
            raise RuntimeError("the strategy's backing service is down")
        return await KeepMajority().decide(state)


class Refuses:
    """A lease whose service cannot be reached from either side."""

    async def acquire(self, owner: str) -> bool:
        raise ConnectionRefusedError(owner)


async def test_a_strategy_that_raises_is_asked_again_and_the_daemon_keeps_running():
    with assert_no_leaked_tasks():
        strategy = FailsAtFirst(failures=5)
        async with cluster_of(3, settings=DECISIVE, downing=strategy) as nodes:
            first, second, odd = nodes
            await joined(nodes)

            odd.faults.partition()

            await asyncio.wait_for(odd.cluster.when_downed(), timeout=5.0)
            assert strategy.calls > strategy.failures
            assert all(daemon_running(n) for n in nodes)
            await eventually(
                lambda: all(len(n.cluster.members) == 2 for n in (first, second)),
                within=5.0,
            )


async def test_a_lease_that_cannot_be_reached_downs_both_sides():
    with assert_no_leaked_tasks():
        strategy = LeaseMajority(lease=Refuses())
        async with cluster_of(2, settings=DECISIVE, downing=strategy) as nodes:
            await joined(nodes)

            nodes[0].faults.partition()

            # Neither side can show it holds the lease, so neither stays up.
            # Each daemon goes on running to say so.
            await asyncio.gather(
                *(asyncio.wait_for(n.cluster.when_downed(), timeout=5.0) for n in nodes)
            )
            assert all(daemon_running(n) for n in nodes)


async def test_a_node_downed_by_an_operator_is_told_so():
    with assert_no_leaked_tasks():
        async with cluster_of(2) as nodes:
            first, second = nodes
            await joined(nodes)
            seen: list[SelfDown] = []
            watcher = second.system.spawn(recorder(seen), name="watcher")
            second.cluster.subscribe(watcher, SelfDown)

            first.cluster._ref.tell(Down(address=second.address))

            # Nobody gossips to a downed member, so in a cluster of two the
            # downed node hears of it only once the leader has removed it.
            await asyncio.wait_for(second.cluster.when_downed(), timeout=5.0)
            await eventually(lambda: len(seen) == 1, within=2.0)
            assert seen[0].member.status in (MemberStatus.DOWN, MemberStatus.REMOVED)


async def test_a_node_that_leaves_gracefully_is_not_told_it_was_downed():
    with assert_no_leaked_tasks():
        async with cluster_of(2) as nodes:
            second = nodes[1]
            await joined(nodes)

            await second.cluster.leave()

            assert second.status is MemberStatus.REMOVED
            assert not second.cluster._daemon.downed.is_set()


async def test_an_operator_downed_node_with_terminate_on_down_shuts_down():
    with assert_no_leaked_tasks():
        # No strategy: an operator down is the only way this node goes down.
        async with cluster_of(3, terminate_on_down=True) as nodes:
            first, _second, third = nodes
            await joined(nodes)

            first.cluster._ref.tell(Down(address=third.address))

            await asyncio.wait_for(third.system.when_terminated(), timeout=5.0)
            assert not first.system.is_terminating
