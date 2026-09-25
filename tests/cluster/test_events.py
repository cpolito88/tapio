"""Cluster events delivered to an ordinary actor mailbox.

These would fail if the daemon stopped telling subscribers what changed, if a
late subscriber stopped hearing the membership it missed, if a subscriber
that stopped were not forgotten, or if a subscriber the daemon cannot watch or
deliver to stopped the daemon.
"""

import asyncio

from tapio import Behavior, Behaviors, Message
from tapio.actor import ActorContext, ActorRef
from tapio.actor.mailbox import MailboxConfig, OverflowStrategy
from tapio.actor.path import ActorPath
from tapio.cluster import (
    Cluster,
    ClusterEvent,
    LeaderChanged,
    MemberRemoved,
    MemberStatus,
    MemberUp,
)
from tapio.testkit import assert_no_leaked_tasks
from tests.cluster.conftest import Node, cluster_of, seeds_of
from tests.failures import eventually


def _about(event: ClusterEvent) -> str:
    """The address an event is about: its member's, or the new leader's."""
    if isinstance(event, LeaderChanged):
        return event.leader or ""
    member = getattr(event, "member", None)
    return member.address if member is not None else ""


def recorder(seen: list[tuple[str, str]]) -> Behavior[ClusterEvent]:
    """An actor that writes down every cluster event it is told about.

    It accepts every event, so it can be subscribed with no filter.

    Args:
        seen: Where each event lands, as its kind and the address it is about.

    Returns:
        The behavior.
    """

    async def on_message(message: ClusterEvent) -> Behavior[ClusterEvent]:
        seen.append((type(message).__name__, _about(message)))
        return Behaviors.same()

    return Behaviors.receive_message(on_message, msg_type=ClusterEvent)


def daemon_running(node: Node) -> bool:
    """Whether the node's daemon still holds its well-known name."""
    path = ActorPath.root(node.system.name).child("system").child("cluster")
    return node.system.refs.lookup(path) is not None


def subscribers(node: Node) -> tuple[ActorPath, ...]:
    """The paths the node's daemon currently delivers events to."""
    return node.cluster._daemon.subscribers


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
            # Only an unfiltered subscriber hears the leader in the replay.
            await eventually(lambda: any(k == "LeaderChanged" for k, _ in seen))
            assert daemon_running(nodes[0])


async def test_unsubscribing_stops_the_events():
    with assert_no_leaked_tasks():
        async with cluster_of(2) as nodes:
            await joined(nodes)
            seen: list[tuple[str, str]] = []
            watcher: ActorRef[ClusterEvent] = nodes[0].system.spawn(
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


class SawUp(Message):
    """What the listener's adapter turns a `MemberUp` into."""

    address: str


class Stop(Message):
    """Asks the listener to stop."""


def listener(cluster: Cluster, got: list[str]) -> Behavior[SawUp | Stop]:
    """An actor that hears `MemberUp` through an adapter, not in its own type.

    Args:
        cluster: The cluster to subscribe the adapter to.
        got: Where each translated event lands, as the member's address.

    Returns:
        The behavior.
    """

    def saw(up: MemberUp) -> SawUp:
        return SawUp(address=up.member.address)

    def build(ctx: ActorContext[SawUp | Stop]) -> Behavior[SawUp | Stop]:
        cluster.subscribe(ctx.message_adapter(saw, msg_type=MemberUp), MemberUp)

        async def on_message(message: SawUp | Stop) -> Behavior[SawUp | Stop]:
            if isinstance(message, Stop):
                return Behaviors.stopped()
            got.append(message.address)
            return Behaviors.same()

        return Behaviors.receive_message(on_message, msg_type=SawUp | Stop)

    return Behaviors.setup(build)


async def test_a_subscriber_can_be_an_adapter():
    with assert_no_leaked_tasks():
        async with cluster_of(2) as nodes:
            await joined(nodes)
            got: list[str] = []

            nodes[0].system.spawn(listener(nodes[0].cluster, got), name="listener")

            await eventually(lambda: set(got) == {n.address for n in nodes}, within=5.0)
            assert daemon_running(nodes[0])


async def test_an_adapter_is_forgotten_when_the_actor_that_owns_it_stops():
    with assert_no_leaked_tasks():
        async with cluster_of(2) as nodes:
            await joined(nodes)
            got: list[str] = []
            owner = nodes[0].system.spawn(
                listener(nodes[0].cluster, got), name="listener"
            )
            await eventually(lambda: len(subscribers(nodes[0])) == 1, within=5.0)

            owner.tell(Stop())

            # The adapter cannot be watched, so the daemon watches its owner.
            await eventually(lambda: subscribers(nodes[0]) == (), within=5.0)
            assert daemon_running(nodes[0])


async def test_a_subscriber_that_cannot_be_watched_is_refused():
    with assert_no_leaked_tasks():
        async with cluster_of(2) as nodes:
            await joined(nodes)
            nobody = await nodes[0].system.resolve(
                f"{nodes[0].address}/user/nobody", expect=ClusterEvent
            )
            seen: list[tuple[str, str]] = []
            watcher = nodes[0].system.spawn(recorder(seen), name="watcher")

            nodes[0].cluster.subscribe(nobody, MemberUp)
            # Subscribed after the refusal, on the same mailbox, so once it
            # hears anything the refusal has been handled.
            nodes[0].cluster.subscribe(watcher, MemberUp)

            await eventually(lambda: len(seen) == 2, within=5.0)
            assert daemon_running(nodes[0])
            assert subscribers(nodes[0]) == (watcher.path,)


async def test_a_subscriber_that_cannot_take_an_event_is_dropped():
    with assert_no_leaked_tasks():
        async with cluster_of(2) as nodes:
            await joined(nodes)
            ups: list[str] = []

            async def on_up(message: MemberUp) -> Behavior[MemberUp]:
                ups.append(message.member.address)
                return Behaviors.same()

            narrow = nodes[0].system.spawn(
                Behaviors.receive_message(on_up, msg_type=MemberUp), name="narrow"
            )
            seen: list[tuple[str, str]] = []
            watcher = nodes[0].system.spawn(recorder(seen), name="watcher")

            # Asking for everything with a type that only takes `MemberUp`:
            # the replay's `LeaderChanged` is the first event it cannot take.
            nodes[0].cluster.subscribe(narrow)
            nodes[0].cluster.subscribe(watcher, MemberUp, MemberRemoved)
            await eventually(lambda: len(seen) == 2, within=5.0)

            assert daemon_running(nodes[0])
            assert subscribers(nodes[0]) == (watcher.path,)

            # The daemon still delivers to the subscribers it kept.
            await nodes[1].cluster.leave()
            await eventually(
                lambda: ("MemberRemoved", nodes[1].address) in seen, within=5.0
            )
            assert daemon_running(nodes[0])


async def test_an_actor_and_its_adapter_share_one_watch():
    with assert_no_leaked_tasks():
        async with cluster_of(2) as nodes:
            await joined(nodes)
            cluster = nodes[0].cluster
            adapters: list[ActorRef[MemberUp]] = []

            def saw(up: MemberUp) -> SawUp:
                return SawUp(address=up.member.address)

            def build(
                ctx: ActorContext[MemberUp | SawUp | Stop],
            ) -> Behavior[MemberUp | SawUp | Stop]:
                adapter = ctx.message_adapter(saw, msg_type=MemberUp)
                adapters.append(adapter)
                cluster.subscribe(ctx.self_ref, MemberUp)
                cluster.subscribe(adapter, MemberUp)

                async def on_message(
                    message: MemberUp | SawUp | Stop,
                ) -> Behavior[MemberUp | SawUp | Stop]:
                    if isinstance(message, Stop):
                        return Behaviors.stopped()
                    return Behaviors.same()

                return Behaviors.receive_message(
                    on_message, msg_type=MemberUp | SawUp | Stop
                )

            owner = nodes[0].system.spawn(Behaviors.setup(build), name="owner")
            await eventually(lambda: len(subscribers(nodes[0])) == 2, within=5.0)

            # The owner is still subscribed, so dropping the adapter must not
            # drop the watch they share.
            cluster.unsubscribe(adapters[0])
            await eventually(lambda: subscribers(nodes[0]) == (owner.path,))

            owner.tell(Stop())
            await eventually(lambda: subscribers(nodes[0]) == (), within=5.0)


async def test_a_subscriber_with_a_full_mailbox_misses_the_event_and_stays():
    with assert_no_leaked_tasks():
        async with cluster_of(2) as nodes:
            await joined(nodes)
            seen: list[tuple[str, str]] = []

            # Room for one message: the replay's second `MemberUp` finds the
            # mailbox full, since the whole replay is sent in one turn.
            watcher = nodes[0].system.spawn(
                recorder(seen),
                name="watcher",
                mailbox=MailboxConfig(capacity=1, on_overflow=OverflowStrategy.FAIL),
            )
            nodes[0].cluster.subscribe(watcher, MemberUp, MemberRemoved)
            await eventually(lambda: len(seen) == 1, within=5.0)
            assert daemon_running(nodes[0])
            assert subscribers(nodes[0]) == (watcher.path,)

            await nodes[1].cluster.leave()
            await eventually(
                lambda: ("MemberRemoved", nodes[1].address) in seen, within=5.0
            )
