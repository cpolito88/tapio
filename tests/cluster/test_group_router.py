"""A group router over an actor published on every member of a role.

These would fail if the router stopped discovering routees from membership, if
it stopped spreading work over them, or if it went on routing to a member the
cluster had removed.
"""

import asyncio

from tapio import Behavior, Behaviors, DeadLetter, Message, Routers, register_message
from tapio.cluster import MemberStatus
from tapio.testkit import assert_no_leaked_tasks
from tests.cluster.conftest import QUICK, cluster_of, seeds_of
from tests.failures import eventually

WORKERS = QUICK.model_copy(update={"roles": frozenset({"worker"})})
"""Every node carries the `worker` role, so the router routes to all of them."""


@register_message()
class Job(Message):
    """A unit of work, sent to whichever worker the router picks."""

    n: int = 0


def worker(counts: dict[str, int], address: str) -> Behavior[Job]:
    """A worker that counts what it was handed, by the node it runs on.

    Args:
        counts: Shared across every node in the process, keyed by address.
        address: The node this worker runs on.

    Returns:
        The behavior.
    """

    async def on_message(message: Job) -> Behavior[Job]:
        counts[address] = counts.get(address, 0) + 1
        return Behaviors.same()

    return Behaviors.receive_message(on_message, msg_type=Job)


async def joined(nodes):
    """Join every node and wait for a converged view."""
    seeds = seeds_of(nodes)
    await asyncio.gather(*(n.cluster.join_seed_nodes(seeds) for n in nodes))
    await eventually(lambda: all(n.cluster.state.converged for n in nodes), within=5.0)


def publish_workers(nodes, counts: dict[str, int]) -> None:
    """Spawn a worker on every node and publish it at a well-known path.

    A group router reaches a routee by its bare path, so the worker has to be
    published as a well-known name for the path to resolve on another node.
    """
    for node in nodes:
        ref = node.system.spawn(worker(counts, node.address), name="worker")
        node.system.refs.register_well_known(ref)


async def test_a_group_router_spreads_work_over_every_member_of_a_role():
    counts: dict[str, int] = {}
    with assert_no_leaked_tasks():
        async with cluster_of(3, settings=WORKERS) as nodes:
            await joined(nodes)
            publish_workers(nodes, counts)
            router = nodes[0].system.spawn(
                Routers.group(Job, role="worker", path="/user/worker"),
                name="router",
            )

            # The router discovers its routees from membership, so send until
            # every node has taken a share rather than assuming it has them all
            # the instant it is spawned.
            async with asyncio.timeout(10):
                while set(counts) != {n.address for n in nodes}:
                    router.tell(Job())
                    await asyncio.sleep(0.02)

            assert set(counts) == {n.address for n in nodes}


async def test_a_group_router_drops_a_removed_member():
    counts: dict[str, int] = {}
    with assert_no_leaked_tasks():
        async with cluster_of(3, settings=WORKERS) as nodes:
            await joined(nodes)
            publish_workers(nodes, counts)
            router = nodes[0].system.spawn(
                Routers.group(Job, role="worker", path="/user/worker"),
                name="router",
            )
            async with asyncio.timeout(10):
                while set(counts) != {n.address for n in nodes}:
                    router.tell(Job())
                    await asyncio.sleep(0.02)

            gone = nodes[2].address
            await nodes[2].cluster.leave()
            await eventually(
                lambda: nodes[0].status_of(gone) is MemberStatus.REMOVED, within=10.0
            )

            async def batch_avoids(address: str) -> bool:
                """Send a batch and report whether none of it reached `address`."""
                before = counts.get(address, 0)
                total = sum(counts.values())
                for _ in range(12):
                    router.tell(Job())
                # Wait for the whole batch to land, so the check sees this batch
                # alone and not a straggler from the last one.
                await eventually(lambda: sum(counts.values()) >= total + 12, within=3.0)
                return counts.get(address, 0) == before

            # Within a convergence of the removal, a whole batch avoids the node
            # that left. It keeps running here, so the only reason it stops
            # receiving is the router dropping it.
            async with asyncio.timeout(10):
                while not await batch_avoids(gone):
                    pass


def stoppable(counts: dict[str, int], name: str) -> Behavior[Job]:
    """A worker that counts under its own name, and stops on a negative job."""

    async def on_message(message: Job) -> Behavior[Job]:
        if message.n < 0:
            return Behaviors.stopped()
        counts[name] = counts.get(name, 0) + 1
        return Behaviors.same()

    return Behaviors.receive_message(on_message, msg_type=Job)


async def test_a_group_router_finds_a_local_routee_published_after_it():
    counts: dict[str, int] = {}
    letters: list[DeadLetter] = []
    with assert_no_leaked_tasks():
        async with cluster_of(1, settings=WORKERS) as nodes:
            (node,) = nodes
            node.system.dead_letters.subscribe(letters.append)
            await joined(nodes)
            router = node.system.spawn(
                Routers.group(Job, role="worker", path="/user/worker"),
                name="router",
            )

            # Wait until the router holds this node as a routee, which shows
            # as a job dead-lettered at the worker's path rather than at its own.
            async with asyncio.timeout(5):
                while not any(d.recipient.endswith("/user/worker") for d in letters):
                    router.tell(Job())
                    await asyncio.sleep(0.02)

            # The natural order: the router first, the worker when it is ready.
            worker_ref = node.system.spawn(stoppable(counts, "first"), name="worker")
            node.system.refs.register_well_known(worker_ref)
            for n in range(5):
                router.tell(Job(n=n))

            await eventually(lambda: counts == {"first": 5})


async def test_a_group_router_follows_a_local_routee_to_its_next_incarnation():
    counts: dict[str, int] = {}
    with assert_no_leaked_tasks():
        async with cluster_of(1, settings=WORKERS) as nodes:
            (node,) = nodes
            await joined(nodes)
            first = node.system.spawn(stoppable(counts, "first"), name="worker")
            node.system.refs.register_well_known(first)
            router = node.system.spawn(
                Routers.group(Job, role="worker", path="/user/worker"),
                name="router",
            )
            async with asyncio.timeout(5):
                while "first" not in counts:
                    router.tell(Job())
                    await asyncio.sleep(0.02)

            first.tell(Job(n=-1))
            await eventually(lambda: node.system.refs.lookup(first.path) is None)
            second = node.system.spawn(stoppable(counts, "second"), name="worker")
            node.system.refs.register_well_known(second)
            router.tell(Job(n=1))

            await eventually(lambda: counts.get("second") == 1)
