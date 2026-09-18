"""The subscribe retry every cluster-aware actor waits for the daemon with."""

import asyncio

from tapio import Behavior, Behaviors, Message
from tapio.actor import ActorContext, ActorRef, ActorSystem
from tapio.actor.timers import TimerScheduler
from tapio.cluster.daemon import start_subscribing, subscribe_when_ready
from tapio.cluster.events import ClusterEvent, MemberUp
from tapio.cluster.messages import ClusterMessage
from tapio.testkit import assert_no_leaked_tasks
from tests.cluster.conftest import remoting, start_node
from tests.failures import eventually

KEY = "subscribe"

EVENTS: tuple[type[ClusterEvent], ...] = (MemberUp,)


class Tick(Message):
    """One retry, standing in for a caller's own reconcile message."""


class Watcher:
    """What a cluster-aware actor does on each tick, and what came of it."""

    def __init__(self) -> None:
        """Start with nothing tried and no daemon found."""
        self.ticks = 0
        self.daemon: ActorRef[ClusterMessage] | None = None
        self.retrying = True

    def behavior(self) -> Behavior[Tick]:
        """An actor that retries subscribing until the daemon answers."""

        def with_timers(timers: TimerScheduler[Tick]) -> Behavior[Tick]:
            def build(ctx: ActorContext[Tick]) -> Behavior[Tick]:
                start_subscribing(timers, KEY, Tick())

                async def on_message(
                    ctx: ActorContext[Tick], message: Tick
                ) -> Behavior[Tick]:
                    self.ticks += 1
                    if self.daemon is None:
                        self.daemon = await subscribe_when_ready(
                            ctx, timers, KEY, EVENTS
                        )
                    self.retrying = timers.is_active(KEY)
                    return Behaviors.same()

                return Behaviors.receive(on_message, Tick)

            return Behaviors.setup(build)

        return Behaviors.with_timers(with_timers)


async def test_a_node_with_no_cluster_keeps_retrying():
    with assert_no_leaked_tasks():
        system = ActorSystem("solo", remoting())
        watcher = Watcher()
        try:
            system.spawn(watcher.behavior(), name="watcher")
            # More than one tick, so the retry is still asking rather than
            # having given up on the first look.
            await eventually(lambda: watcher.ticks > 1)
        finally:
            await system.terminate()

    assert watcher.daemon is None
    assert watcher.retrying


async def test_the_retry_stops_once_the_subscription_lands():
    with assert_no_leaked_tasks():
        node = start_node("node1")
        watcher = Watcher()
        try:
            node.system.spawn(watcher.behavior(), name="watcher")
            await eventually(lambda: watcher.daemon is not None)
            settled = watcher.ticks
            # Long enough that a live retry would have ticked again.
            await asyncio.sleep(0.2)

            assert watcher.ticks == settled
            assert not watcher.retrying
        finally:
            await node.system.terminate()
