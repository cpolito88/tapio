"""The code on the clustering docs page, as tests that run or type-check.

Each block is included from this file by name, so the page cannot show a call
that does not work. The `--8<--` markers are what the docs quote; they are not
section dividers.
"""

import asyncio

from pydantic import SecretStr

from tapio import Behavior, Behaviors, Message, Routers, register_message
from tapio.actor import ActorContext, ActorSystem
from tapio.cluster import (
    Cluster,
    ClusterEvent,
    ClusterSettings,
    ManagementSettings,
    MemberRemoved,
    MemberStatus,
    MemberUp,
    UnreachableMember,
)
from tapio.settings import TLSSettings
from tapio.testkit import assert_no_leaked_tasks
from tests.cluster.conftest import QUICK, cluster_of, remoting, seeds_of
from tests.failures import eventually

WORKERS = QUICK.model_copy(update={"roles": frozenset({"worker"})})


@register_message()
class Job(Message):
    """Work for whichever worker the router picks."""


def worker(counts: dict[str, int], address: str) -> Behavior[Job]:
    """Count each job by the node that took it."""

    async def on_job(job: Job) -> Behavior[Job]:
        counts[address] = counts.get(address, 0) + 1
        return Behaviors.same()

    return Behaviors.receive_message(on_job, msg_type=Job)


# --8<-- [start:group-router]
def front_desk() -> Behavior[Job]:
    """Pass every job to a group router over the `worker` members."""

    def start(ctx: ActorContext[Job]) -> Behavior[Job]:
        proxy = ctx.spawn(
            Routers.group(Job, role="worker", path="/user/worker"), "workers"
        )

        async def on_job(job: Job) -> Behavior[Job]:
            proxy.tell(job)
            return Behaviors.same()

        return Behaviors.receive_message(on_job, msg_type=Job)

    return Behaviors.setup(start)


# --8<-- [end:group-router]


async def test_the_front_desk_reaches_a_worker_on_every_member():
    counts: dict[str, int] = {}
    with assert_no_leaked_tasks():
        async with cluster_of(2, settings=WORKERS) as nodes:
            seeds = seeds_of(nodes)
            await asyncio.gather(*(n.cluster.join_seed_nodes(seeds) for n in nodes))
            await eventually(
                lambda: all(n.cluster.state.converged for n in nodes), within=5.0
            )
            for node in nodes:
                ref = node.system.spawn(worker(counts, node.address), name="worker")
                node.system.refs.register_well_known(ref)

            desk = nodes[0].system.spawn(front_desk(), name="desk")

            async with asyncio.timeout(10):
                while set(counts) != {n.address for n in nodes}:
                    desk.tell(Job())
                    await asyncio.sleep(0.02)


# The blocks below are the rest of the page. The ones that name hosts or
# certificate files no test machine has are in functions nothing calls, so CI
# type-checks them. The others run.


async def join_the_orders_cluster(cluster: Cluster) -> None:
    """The seed list the page shows, with addresses a deployment would use."""
    # --8<-- [start:join]
    await cluster.join_seed_nodes(
        [
            "tapio://orders@10.0.0.1:2551",
            "tapio://orders@10.0.0.2:2551",
            "tapio://orders@10.0.0.3:2551",
        ]
    )
    # --8<-- [end:join]


async def test_a_member_leaves_and_is_removed():
    with assert_no_leaked_tasks():
        async with cluster_of(2) as nodes:
            seeds = seeds_of(nodes)
            await asyncio.gather(*(n.cluster.join_seed_nodes(seeds) for n in nodes))
            cluster = nodes[1].cluster

            # --8<-- [start:leave]
            await cluster.leave()
            # --8<-- [end:leave]

            assert nodes[1].status is MemberStatus.REMOVED


async def test_a_node_is_given_its_ring_size():
    with assert_no_leaked_tasks():
        system = ActorSystem("orders", remoting())
        try:
            # --8<-- [start:monitored]
            cluster = Cluster(system, ClusterSettings(monitored_peers=5))
            watched = cluster.monitored  # the members this node watches, in order
            # --8<-- [end:monitored]
            assert watched == ()
        finally:
            await system.terminate()


async def test_a_node_is_given_its_roles():
    with assert_no_leaked_tasks():
        system = ActorSystem("orders", remoting())
        try:
            # --8<-- [start:roles]
            cluster = Cluster(system, ClusterSettings(roles=frozenset({"worker"})))
            workers = cluster.members_with_role("worker")  # oldest first
            # --8<-- [end:roles]
            assert workers == ()
        finally:
            await system.terminate()


def recorder(seen: list[ClusterEvent]) -> Behavior[ClusterEvent]:
    """An actor that keeps every cluster event it is told."""

    async def on_event(event: ClusterEvent) -> Behavior[ClusterEvent]:
        seen.append(event)
        return Behaviors.same()

    return Behaviors.receive_message(on_event, msg_type=ClusterEvent)


async def test_a_subscriber_hears_who_is_up():
    seen: list[ClusterEvent] = []
    with assert_no_leaked_tasks():
        async with cluster_of(2) as nodes:
            seeds = seeds_of(nodes)
            await asyncio.gather(*(n.cluster.join_seed_nodes(seeds) for n in nodes))
            cluster = nodes[0].cluster
            worker = nodes[0].system.spawn(recorder(seen), name="worker")

            # --8<-- [start:subscribe]
            cluster.subscribe(worker, MemberUp, MemberRemoved, UnreachableMember)
            # --8<-- [end:subscribe]

            await eventually(
                lambda: (
                    {e.member.address for e in seen if isinstance(e, MemberUp)}
                    == {n.address for n in nodes}
                ),
                within=5.0,
            )


def management_beyond_loopback(system: ActorSystem) -> None:
    """The management port the page shows bound beyond loopback with a token."""
    # --8<-- [start:management-token]
    Cluster(
        system,
        management=ManagementSettings(
            bind_host="0.0.0.0",
            token=SecretStr("..."),  # required beyond loopback, or it will not start
        ),
    )
    # --8<-- [end:management-token]


def management_over_tls(system: ActorSystem) -> None:
    """The management port the page shows speaking TLS, with certificate files."""
    # --8<-- [start:management-tls]
    Cluster(
        system,
        management=ManagementSettings(
            bind_host="0.0.0.0",
            tls=TLSSettings(
                certfile="server.pem", keyfile="server.key", cafile="ca.pem"
            ),
        ),
    )
    # --8<-- [end:management-tls]
