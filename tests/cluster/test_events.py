"""Cluster events delivered to an ordinary actor mailbox.

These would fail if the daemon stopped telling subscribers what changed, if a
late subscriber stopped hearing the membership it missed, or if a subscriber
that stopped were not forgotten.
"""

import asyncio

from tapio import Behavior, Behaviors
from tapio.actor import ActorRef
from tapio.cluster import MemberRemoved, MemberStatus, MemberUp
from tapio.testkit import assert_no_leaked_tasks
from tests.cluster.conftest import cluster_of, seeds_of
from tests.failures import eventually

_Event = MemberUp | MemberRemoved


def recorder(seen: list[tuple[str, str]]) -> Behavior[_Event]:
    """An actor that writes down every cluster event it is told about.

    Args:
        seen: Where each event lands, as its kind and the member's address.

    Returns:
        The behavior.
    """

    async def on_message(message: _Event) -> Behavior[_Event]:
        seen.append((type(message).__name__, message.member.address))
        return Behaviors.same()

    return Behaviors.receive_message(on_message, msg_type=_Event)


async def joined(nodes):
    """Join every node and wait for a converged view."""
    seeds = seeds_of(nodes)
    await asyncio.gather(*(n.cluster.join_seed_nodes(seeds) for n in nodes))
    await eventually(lambda: all(n.cluster.state.converged for n in nodes), within=5.0)


async def test_a_subscriber_hears_who_is_up_then_who_leaves():
    with assert_no_leaked_tasks():
        async with cluster_of(3) as nodes:
            await joined(nodes)
            seen: list[tuple[str, str]] = []
            watcher = nodes[0].system.spawn(recorder(seen), name="watcher")

            nodes[0].cluster.subscribe(watcher, MemberUp, MemberRemoved)

            # The replay is the point: an actor that subscribes after the
            # cluster has formed still learns who is up, as the events that
            # would have carried it.
            await eventually(
                lambda: (
                    {a for kind, a in seen if kind == "MemberUp"}
                    == {n.address for n in nodes}
                ),
                within=5.0,
            )

            await nodes[2].cluster.leave()
            await eventually(
                lambda: ("MemberRemoved", nodes[2].address) in seen, within=5.0
            )


async def test_a_subscriber_that_asked_for_nothing_hears_everything():
    with assert_no_leaked_tasks():
        async with cluster_of(2) as nodes:
            await joined(nodes)
            seen: list[tuple[str, str]] = []
            watcher = nodes[0].system.spawn(recorder(seen), name="watcher")

            # No event types named means every one of them.
            nodes[0].cluster.subscribe(watcher)

            await eventually(
                lambda: (
                    {a for kind, a in seen if kind == "MemberUp"}
                    == {n.address for n in nodes}
                ),
                within=5.0,
            )


async def test_unsubscribing_stops_the_events():
    with assert_no_leaked_tasks():
        async with cluster_of(2) as nodes:
            await joined(nodes)
            seen: list[tuple[str, str]] = []
            watcher: ActorRef[_Event] = nodes[0].system.spawn(
                recorder(seen), name="watcher"
            )
            nodes[0].cluster.subscribe(watcher, MemberUp, MemberRemoved)
            await eventually(lambda: len(seen) == 2, within=5.0)

            # Told to the daemon before the leave below, and on the same
            # mailbox, so it is processed before the removal it must not report.
            nodes[0].cluster.unsubscribe(watcher)
            count = len(seen)

            await nodes[1].cluster.leave()
            await eventually(
                lambda: nodes[0].status_of(nodes[1].address) is MemberStatus.REMOVED,
                within=5.0,
            )
            # The leave reached this node, but the unsubscribed watcher heard
            # nothing more.
            assert len(seen) == count
