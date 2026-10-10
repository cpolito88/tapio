"""The group router code on the clustering docs page, as a test that runs.

The block is included from this file by name, so the page cannot show a call
that does not work. The `--8<--` markers are what the docs quote; they are not
section dividers.
"""

import asyncio

from tapio import Behavior, Behaviors, Message, Routers, register_message
from tapio.actor import ActorContext
from tapio.testkit import assert_no_leaked_tasks
from tests.cluster.conftest import QUICK, cluster_of, seeds_of
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
