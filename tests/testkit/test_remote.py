"""The fault injection the remoting tests are built on."""

from datetime import timedelta

import pytest

from tapio.actor import ActorSystem
from tapio.errors import TapioError
from tapio.settings import TapioSettings
from tapio.testkit import assert_no_leaked_tasks, link_faults, two_nodes
from tests.failures import eventually
from tests.remote.peers import Tick, counting, uri


async def test_two_nodes_can_talk_before_anything_is_broken():
    with assert_no_leaked_tasks():
        async with two_nodes() as nodes:
            seen: list[int] = []
            worker = nodes.beta.spawn(counting(seen), "worker")
            remote = await nodes.alpha.resolve(uri(nodes.beta, worker), expect=Tick)

            remote.tell(Tick(n=1))

            await eventually(lambda: seen == [1])


async def test_dropping_frames_loses_exactly_those_frames():
    # Heartbeats are frames too, and a drop takes whichever is written next.
    # Stretching the interval past the test keeps that "next" the tick below.
    with assert_no_leaked_tasks():
        async with two_nodes(
            heartbeat_interval=timedelta(seconds=30),
            unreachable_after=timedelta(seconds=60),
        ) as nodes:
            seen: list[int] = []
            worker = nodes.beta.spawn(counting(seen), "worker")
            remote = await nodes.alpha.resolve(uri(nodes.beta, worker), expect=Tick)
            remote.tell(Tick(n=1))
            await eventually(lambda: seen == [1])

            # At-most-once means a lost frame is lost. Nothing is retried and
            # nothing is reported, here or in production.
            nodes.alpha_faults.drop(1)
            remote.tell(Tick(n=2))
            remote.tell(Tick(n=3))

            await eventually(lambda: seen == [1, 3])


async def test_delaying_frames_holds_them_without_reordering():
    with assert_no_leaked_tasks():
        async with two_nodes() as nodes:
            seen: list[int] = []
            worker = nodes.beta.spawn(counting(seen), "worker")
            remote = await nodes.alpha.resolve(uri(nodes.beta, worker), expect=Tick)
            nodes.alpha_faults.delay(0.01)

            for n in range(5):
                remote.tell(Tick(n=n))

            # One writer, one link, so a delay slows the stream down and never
            # shuffles it.
            await eventually(lambda: seen == [0, 1, 2, 3, 4])


async def test_a_partition_covers_both_nodes_and_heals_on_both():
    # What a partition then does to the systems on either side is asserted in
    # tests/remote/test_unreachable.py, where the behaviour under test lives.
    # This is only that both sides are cut off by one call, since a partition
    # one node can see through is not one.
    with assert_no_leaked_tasks():
        async with two_nodes(unreachable_after=timedelta(seconds=30)) as nodes:
            nodes.partition()

            assert nodes.alpha_faults.partitioned
            assert nodes.beta_faults.partitioned

            nodes.heal()

            assert not nodes.alpha_faults.partitioned
            assert not nodes.beta_faults.partitioned


async def test_faults_need_a_system_with_remoting_switched_on(
    settings: TapioSettings,
):
    system = ActorSystem("local", settings)
    try:
        with pytest.raises(TapioError, match="remoting"):
            link_faults(system)
    finally:
        await system.terminate()
