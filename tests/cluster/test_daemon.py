"""The daemon's subscribe retry, its downing decisions, and whose claims it keeps."""

import asyncio

from tapio import Behavior, Behaviors, Message
from tapio.actor import ActorContext, ActorRef, ActorSystem
from tapio.actor.events import EventStream
from tapio.actor.timers import TimerScheduler
from tapio.cluster.clock import VectorClock
from tapio.cluster.daemon import ClusterDaemon, start_subscribing, subscribe_when_ready
from tapio.cluster.downing import LeaseMajority, LocalLease
from tapio.cluster.events import ClusterEvent, MemberUp
from tapio.cluster.gossip import Gossip
from tapio.cluster.member import Member, MemberStatus
from tapio.cluster.messages import ClusterMessage, GossipEnvelope
from tapio.cluster.reachability import (
    Reachability,
    ReachabilityRecord,
    ReachabilityStatus,
)
from tapio.errors import RefResolutionError
from tapio.remote.registry import RefRegistry
from tapio.testkit import assert_no_leaked_tasks
from tests.cluster.conftest import QUICK, remoting, start_node
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


A = "tapio://n@127.0.0.1:2551"
B = "tapio://n@127.0.0.1:2552"
C = "tapio://n@127.0.0.1:2553"
D = "tapio://n@127.0.0.1:2554"


def deciding_daemon(
    state: Gossip, lease: LocalLease
) -> tuple[ClusterDaemon, list[float]]:
    """D's daemon holding a given view, with a clock the test moves by hand."""
    clock = [0.0]
    daemon = ClusterDaemon(
        address=D,
        uid=1,
        refs=RefRegistry(),
        events=EventStream(),
        settings=QUICK,
        relent=lambda address: None,
        linked=lambda address: False,
        now=lambda: clock[0],
        strategy=LeaseMajority(lease=lease),
    )
    daemon._state = state
    return daemon, clock


def split(unreachable: list[str], *, seen: set[str]) -> Gossip:
    """Four members, as D sees them after a split that C has observed."""
    members = tuple(
        Member(address=address, uid=1, status=MemberStatus.UP, up_number=number)
        for number, address in enumerate([A, B, C, D], start=1)
    )
    records = tuple(
        ReachabilityRecord(
            observer=C, observed=address, status=ReachabilityStatus.UNREACHABLE
        )
        for address in unreachable
    )
    return Gossip(
        members=members,
        reachability=Reachability(records=records),
        seen=frozenset(seen),
    )


def status_of(daemon: ClusterDaemon, address: str) -> MemberStatus | None:
    """What a daemon's view holds a member at, if it holds it at all."""
    member = daemon.state.member(address)
    return member.status if member is not None else None


async def decide_after_the_window(daemon: ClusterDaemon, clock: list[float]) -> None:
    """Let D see the split, then hold it still for longer than `down_after`."""
    await daemon._down()
    clock[0] += QUICK.down_after.total_seconds() + 1
    await daemon._down()


async def test_a_view_its_side_has_not_seen_decides_nothing() -> None:
    # The split is {A, B} against {C, D}, and {A, B} took the lease under A's
    # name. D's view has only B unreachable, because nobody on its side
    # observes A. D would ask for the lease as A, be granted it, and down B.
    lease = LocalLease()
    await lease.acquire(A)
    daemon, clock = deciding_daemon(split([B], seen={C, D}), lease)

    await decide_after_the_window(daemon, clock)

    assert status_of(daemon, B) is MemberStatus.UP


async def test_a_view_its_side_has_seen_is_decided() -> None:
    lease = LocalLease()
    await lease.acquire(A)
    daemon, clock = deciding_daemon(split([A, B], seen={C, D}), lease)

    await decide_after_the_window(daemon, clock)

    assert status_of(daemon, C) is MemberStatus.DOWN
    assert status_of(daemon, D) is MemberStatus.DOWN


class _NoPeers:
    """A context that resolves nobody, so a daemon's answer goes nowhere."""

    async def resolve(self, uri: str, *, expect: object) -> object:
        raise RefResolutionError(f"{uri} is not resolved in this test")


def plain_daemon(*, uid: int) -> ClusterDaemon:
    """D's daemon with no strategy, and an empty view."""
    return ClusterDaemon(
        address=D,
        uid=uid,
        refs=RefRegistry(),
        events=EventStream(),
        settings=QUICK,
        relent=lambda address: None,
        linked=lambda address: False,
        now=lambda: 0.0,
    )


def after_a_restart_of_d(*, claimed_by_d: ReachabilityStatus) -> Gossip:
    """The view once D restarted, with a claim about B from D's old incarnation."""
    members = (
        Member(address=A, uid=1, status=MemberStatus.UP, up_number=1),
        Member(address=B, uid=1, status=MemberStatus.UP, up_number=2),
        Member(address=C, uid=1, status=MemberStatus.UP, up_number=3),
        Member(address=D, uid=1, status=MemberStatus.DOWN, up_number=4),
        Member(address=D, uid=2, status=MemberStatus.JOINING),
    )
    claim = ReachabilityRecord(observer=D, observed=B, status=claimed_by_d, version=3)
    return Gossip(
        members=members,
        reachability=Reachability(records=(claim,)),
        version=VectorClock().increment(A),
        seen=frozenset({A}),
    )


async def test_a_restarted_node_takes_back_the_claims_of_its_predecessor() -> None:
    daemon = plain_daemon(uid=2)
    gossip = after_a_restart_of_d(claimed_by_d=ReachabilityStatus.UNREACHABLE)

    await daemon._merge(_NoPeers(), GossipEnvelope(sender=A, gossip=gossip))  # type: ignore[arg-type]

    # The new incarnation never said B was unreachable, and nobody else can
    # take the old one's claim back.
    assert daemon.state.unreachable == frozenset()
    assert daemon.state.reachability.says(D, B) is ReachabilityStatus.REACHABLE
    # At a higher version, so it wins every merge.
    assert daemon.state.merge(gossip).unreachable == frozenset()


async def test_a_claim_this_incarnation_made_survives_a_merge() -> None:
    daemon = plain_daemon(uid=2)
    gossip = after_a_restart_of_d(claimed_by_d=ReachabilityStatus.UNREACHABLE)
    daemon._state = gossip
    daemon._observe(B, ReachabilityStatus.UNREACHABLE)

    await daemon._merge(_NoPeers(), GossipEnvelope(sender=A, gossip=gossip))  # type: ignore[arg-type]

    assert daemon.state.unreachable == frozenset({B})
