"""The actor that gossips: one per node, at `/system/cluster`.

Everything that depends on time lives here, and everything that depends on
nothing but its arguments lives in [gossip][tapio.cluster.gossip]. The split
is deliberate. What this actor does is pick a peer, send it what this node
believes, merge what comes back, and, when it is the leader of a view every
member has seen, move members along. The merge it calls is a pure function
with three laws behind it; the choosing and the timing are here, where being
wrong costs a round rather than a corrupt cluster.

It publishes itself as a well-known name, because a seed node is named by an
address in a configuration file and by nothing else. A joining node has no way
to know any incarnation uid over there, so the bare path `/system/cluster` has
to be addressable. That is the only weakening of the incarnation rule in the
library, it is opt-in, and it is why this actor and not another.
"""

import asyncio
import random
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, cast, final

from tapio.actor.behavior import Behavior, Behaviors
from tapio.actor.cell import LocalActorRef
from tapio.actor.context import ActorContext
from tapio.actor.events import EventStream, Subscription
from tapio.actor.path import ActorPath
from tapio.actor.ref import ActorRef
from tapio.actor.signals import PostStop, Signal, Terminated
from tapio.actor.timers import TimerScheduler
from tapio.cluster.clock import Ordering
from tapio.cluster.downing import DownStrategy
from tapio.cluster.events import (
    ClusterEvent,
    LeaderChanged,
    MemberRemoved,
    MemberUp,
    ReachableMember,
    SelfDown,
    UnreachableMember,
)
from tapio.cluster.gossip import Gossip, leader_actions
from tapio.cluster.member import Member, MemberStatus
from tapio.cluster.messages import (
    ClusterDowned,
    ClusterMessage,
    Down,
    FormTick,
    GossipEnvelope,
    Heartbeat,
    HeartbeatReply,
    HeartbeatTick,
    Join,
    JoinTick,
    Leave,
    LinkChanged,
    Seeds,
    Subscribe,
    Tick,
    Unsubscribe,
    WireMessage,
)
from tapio.cluster.monitor import RingMonitor, deadline_detectors, phi_detectors
from tapio.cluster.reachability import ReachabilityStatus
from tapio.errors import RefResolutionError
from tapio.logging import runtime_logger
from tapio.remote.failure import PeerReachable, PeerUnreachable
from tapio.remote.registry import RefRegistry
from tapio.settings import ClusterSettings

__all__ = ["DAEMON_NAME", "ClusterDaemon", "daemon_uri", "local_daemon"]

DAEMON_NAME = "cluster"
"""What the daemon is called under `/system`, and half of its well-known name."""

_log = runtime_logger("cluster")

_GOSSIP_TIMER = "gossip"
_JOIN_TIMER = "join"
_FORM_TIMER = "form"
_HEARTBEAT_TIMER = "heartbeat"


async def local_daemon(
    ctx: ActorContext[Any],
) -> ActorRef[ClusterMessage] | None:
    """Resolve this node's own cluster daemon, if it has started yet.

    For an actor that clusters with its own system: a
    [ClusterSingleton][tapio.cluster.singleton.ClusterSingleton] manager or a
    group router, both of which subscribe to the daemon and run in the same
    system as it. The daemon publishes itself as a well-known name, so resolving
    it needs no incarnation uid.

    Args:
        ctx: The calling actor's context. Its own address names the daemon.

    Returns:
        The live local ref to the daemon, or `None` when the daemon is not
        registered yet. `None` rather than a dead-letter ref, so a caller can
        tell "not ready, ask again" from "the address named a peer": the daemon
        is a system actor that starts moments after the system does, and an
        actor spawned in the same breath as the cluster may look before it is
        there.
    """
    address = str(ctx.self_ref.address)
    ref = await ctx.resolve(daemon_uri(address), expect=Subscribe)
    if isinstance(ref, LocalActorRef):
        return cast("ActorRef[ClusterMessage]", ref)
    return None


@dataclass(frozen=True, slots=True)
class _Digest:
    """A snapshot of what this node's view says, taken to be diffed for events.

    Events are the difference between two of these, one from before a message
    was handled and one from after, so that every path that moves membership
    produces the right events without each one having to remember to.
    """

    members: dict[str, Member]
    """The primary member at each address, by address."""

    unreachable: frozenset[str]
    """The live members at least one node currently cannot hear."""

    leader: str | None
    """Who this node computes as the leader."""

    self_status: MemberStatus | None
    """This node's own status, or `None` before it has joined."""


def daemon_uri(address: str) -> str:
    """Write down the address of the cluster daemon on a node.

    Args:
        address: The node's canonical address, in its string form.

    Returns:
        The ref string for its daemon, with no uid, since a node that has not
        joined yet cannot know which incarnation is answering over there.
    """
    return f"{address}/system/{DAEMON_NAME}"


@final
class ClusterDaemon:
    """One node's membership state, and the actor that keeps it moving."""

    def __init__(
        self,
        *,
        address: str,
        uid: int,
        refs: RefRegistry,
        events: EventStream,
        settings: ClusterSettings,
        relent: Callable[[str], None],
        linked: Callable[[str], bool],
        strategy: DownStrategy | None = None,
        choose: Callable[[Sequence[str]], str] = random.choice,
    ) -> None:
        """Describe a node's cluster daemon, before its actor exists.

        Args:
            address: This node's canonical address, in its string form.
            uid: This system's incarnation uid, which is half of what
                identifies the member.
            refs: This system's ref registry, where the daemon publishes its
                well-known name.
            events: This system's event stream, where remoting says that a
                peer went out of reach or came back, and where this daemon says
                that it has downed itself.
            settings: How often to gossip, and how patient to be.
            relent: Tells remoting to stop refusing a peer it gave up on. A
                member that has not been downed is still a member, so this
                node keeps knocking rather than waiting to be told the
                quarantine is over.
            linked: Says whether remoting already holds an association with a
                peer. It is asked before answering a heartbeat from an address
                membership does not know, so that answering reuses a link that
                exists rather than opening one to wherever the message said.
            strategy: What to do about an unreachable member. `None` leaves an
                unreachable member blocking convergence for ever, which is the
                safe default for a node that has not been told how its cluster
                would rather resolve a split.
            choose: Picks the peer to gossip to this round. Injected so a test
                can make a round deterministic; the default is uniformly at
                random, which is what keeps gossip traffic linear in the
                number of nodes.
        """
        self._address = address
        self._settings = settings
        self._refs = refs
        self._events = events
        self._choose = choose
        self._relent = relent
        self._linked = linked
        self._strategy = strategy
        self._down_after = settings.down_after.total_seconds()
        self._subscription: Subscription | None = None
        self._self = Member(address=address, uid=uid, roles=settings.roles)
        self._state = Gossip()
        self._seeds: tuple[str, ...] = ()
        self._peers: dict[str, ActorRef[WireMessage]] = {}
        self._heard_from_anyone = False
        self._rounds = 0
        self._heartbeats = 0
        # The split this node last saw, and when it first saw it. Downing waits
        # for the pair to hold still, so a passing blip is ridden out.
        self._split: frozenset[str] | None = None
        self._split_since = 0.0
        self._downed = asyncio.Event()
        self._downed_announced = False
        # Actors that asked to hear about membership changes, by their path so
        # that subscribing twice replaces rather than duplicates. Each is
        # watched, so one that stops is forgotten without an Unsubscribe.
        self._subscribers: dict[
            ActorPath, tuple[ActorRef[Any], frozenset[type[ClusterEvent]]]
        ] = {}
        self._monitor = RingMonitor(
            address=address,
            size=settings.monitored_peers,
            detector=(
                phi_detectors(
                    threshold=settings.phi_threshold,
                    acceptable_pause=settings.phi_acceptable_pause.total_seconds(),
                    first_interval_estimate=settings.heartbeat_interval.total_seconds(),
                )
                if settings.phi_accrual
                else deadline_detectors(settings.unreachable_after.total_seconds())
            ),
        )

    @property
    def state(self) -> Gossip:
        """What this node currently believes about the cluster."""
        return self._state

    @property
    def address(self) -> str:
        """This node's canonical address, in its string form."""
        return self._address

    @property
    def self_member(self) -> Member | None:
        """This node as the cluster sees it, or `None` before it has joined."""
        for member in self._state.members:
            if member.key == self._self.key:
                return member
        return None

    @property
    def joined(self) -> bool:
        """Whether this node appears in the membership it holds."""
        return self.self_member is not None

    @property
    def rounds(self) -> int:
        """How many gossip rounds this node has sent, which a test counts."""
        return self._rounds

    @property
    def heartbeats(self) -> int:
        """How many probes this node has sent, which a test counts.

        One per watched member per round, so this is what shows the traffic is
        bounded by the ring rather than by the size of the cluster.
        """
        return self._heartbeats

    @property
    def monitored(self) -> tuple[str, ...]:
        """The members this node watches, in address order."""
        return self._monitor.peers

    @property
    def downed(self) -> asyncio.Event:
        """Set once this node has downed itself, for the application to wait on."""
        return self._downed

    def behavior(self) -> Behavior[ClusterMessage]:
        """Build the daemon actor."""

        def with_timers(
            timers: TimerScheduler[ClusterMessage],
        ) -> Behavior[ClusterMessage]:
            def build(ctx: ActorContext[ClusterMessage]) -> Behavior[ClusterMessage]:
                self._refs.register_well_known(ctx.self_ref)
                timers.start_fixed_delay(
                    _GOSSIP_TIMER, Tick(), self._settings.gossip_interval
                )
                timers.start_fixed_delay(
                    _HEARTBEAT_TIMER,
                    HeartbeatTick(),
                    self._settings.heartbeat_interval,
                )
                self._watch_the_links(ctx.self_ref)

                async def on_message(
                    ctx: ActorContext[ClusterMessage], message: ClusterMessage
                ) -> Behavior[ClusterMessage]:
                    return await self._receive(ctx, timers, message)

                async def on_signal(
                    ctx: ActorContext[ClusterMessage], signal: Signal
                ) -> Behavior[ClusterMessage]:
                    if isinstance(signal, PostStop):
                        self._stop_watching_the_links()
                    elif isinstance(signal, Terminated):
                        # A subscriber stopped, so forget it. Nothing else is
                        # watched, so this is the only reason one arrives.
                        self._subscribers.pop(signal.ref.path, None)
                    return Behaviors.same()

                return Behaviors.receive(
                    on_message, ClusterMessage, on_signal=on_signal
                )

            return Behaviors.setup(build)

        return Behaviors.with_timers(with_timers)

    async def _receive(
        self,
        ctx: ActorContext[ClusterMessage],
        timers: TimerScheduler[ClusterMessage],
        message: ClusterMessage,
    ) -> Behavior[ClusterMessage]:
        """Handle one message, then act if this node leads a converged view."""
        # Only snapshotted when somebody is listening, so a daemon with no
        # subscribers does none of the work of producing events.
        before = self._digest() if self._subscribers else None
        match message:
            case Seeds():
                self._start_joining(timers, message.addresses)
            case Subscribe():
                self._subscribe(ctx, message)
            case Unsubscribe():
                self._unsubscribe(ctx, message.subscriber)
            case Tick():
                await self._gossip_once(ctx)
            case JoinTick():
                await self._ask_the_seeds(ctx, timers)
            case FormTick():
                self._form_a_cluster()
            case HeartbeatTick():
                await self._probe_the_ring(ctx)
            case Heartbeat():
                await self._answer(ctx, message.sender)
            case HeartbeatReply():
                self._monitor.heard(message.sender, _now())
            case Join():
                await self._admit(ctx, message.member)
            case GossipEnvelope():
                await self._merge(ctx, message)
            case Leave():
                self._start_leaving(message.address)
            case Down():
                self._down_member(message.address)
            case LinkChanged():
                self._link_changed(message)

        self._follow_the_ring()
        self._lead()
        await self._down()
        self._announce_if_downed()
        if before is not None:
            self._emit(before)
        if (
            self.self_member is not None
            and self.self_member.status is MemberStatus.REMOVED
        ):
            # Removed is the end of this node's membership, and a removed
            # member that kept gossiping would only be telling the cluster
            # what it already decided. Stopping here also takes the
            # well-known name down with the actor.
            _log.info("%s has been removed from the cluster", self._address)
            timers.cancel_all()
            return Behaviors.stopped()
        if self.joined:
            timers.cancel(_JOIN_TIMER)
            timers.cancel(_FORM_TIMER)
        return Behaviors.same()

    def _watch_the_links(self, me: ActorRef[ClusterMessage]) -> None:
        """Take what remoting says about peers into this actor's mailbox.

        The handler runs wherever the event was published, which is inside an
        association, so it does the least it can: it turns the event into a
        message. Every change to the state is then made by the daemon in its
        own turn, like every other one.

        Args:
            me: This daemon's own ref, which the handlers send to.
        """

        def lost(event: PeerUnreachable) -> None:
            me.tell(LinkChanged(peer=event.peer, reachable=False))

        def found(event: PeerReachable) -> None:
            me.tell(LinkChanged(peer=event.peer, reachable=True))

        unreachable = self._events.subscribe(PeerUnreachable, lost)
        reachable = self._events.subscribe(PeerReachable, found)

        def cancel_both() -> None:
            unreachable.unsubscribe()
            reachable.unsubscribe()

        self._subscription = Subscription(cancel_both)

    def _stop_watching_the_links(self) -> None:
        """Stop listening, because this daemon is going away.

        A subscription left behind would send into a stopped actor's mailbox
        for as long as the system ran, which is a dead letter per link event
        and a reference to an actor nobody can reach.
        """
        if self._subscription is not None:
            self._subscription.unsubscribe()
            self._subscription = None

    def _link_changed(self, message: LinkChanged) -> None:
        """Take what the transport saw as evidence about a member this node watches.

        It is narrow on purpose. Only a member on this node's ring is
        recorded: a peer this system talks to but has not clustered with is
        nobody's business, and a member somebody else watches is judged by the
        node whose job it is rather than by whichever node happened to send it
        something.

        The transport's verdict is worth having next to this node's own probe
        because it arrives sooner. A link that failed is known now, while the
        probe is still inside a window that has not run out. It is retracted
        by the link coming back and by nothing else, since an answer to a
        probe says nothing about what the transport is refusing to carry.
        """
        now = _now()
        watched = (
            self._monitor.link_open(message.peer)
            if message.reachable
            else self._monitor.link_lost(message.peer)
        )
        if not watched:
            return
        self._observe(message.peer, self._monitor.verdicts(now)[message.peer])

    def _follow_the_ring(self) -> None:
        """Watch what the current membership says this node should watch.

        Run at the end of every turn, because membership only changes in one,
        so the ring this node holds is never a round out of date.
        """
        for peer in self._monitor.follow(self._state.alive, _now()):
            # No longer this node's to judge, so whatever it said is taken
            # back. A claim left behind by a node that has stopped watching
            # would block convergence with nothing able to retract it.
            self._observe(peer, ReachabilityStatus.REACHABLE)

    async def _probe_the_ring(self, ctx: ActorContext[ClusterMessage]) -> None:
        """Ask every member this node watches whether it is still answering.

        The judging happens first, on what arrived since the last round, and
        the asking after it. Traffic is one message per watched member, so it
        is bounded by the ring rather than by the size of the cluster.
        """
        now = _now()
        self._keep_knocking()
        for peer, status in self._monitor.verdicts(now).items():
            self._observe(peer, status)
            ref = await self._peer(ctx, peer)
            if ref is None:
                # Counted only when one is sent, so the tally stays the answer
                # to "how much traffic did the ring cost".
                continue
            self._heartbeats += 1
            ref.tell(Heartbeat(sender=self._address))
        self._forget_strangers()

    def _keep_knocking(self) -> None:
        """Stop remoting refusing any member of this cluster.

        An unreachable member is still a member, so this node keeps knocking.
        Remoting gives up for good and waits to be told otherwise, which is
        the honest answer for a system with no membership to consult and the
        wrong one here: nobody has decided this member is gone, and until
        somebody does, the cluster's job is to keep trying to reach it.

        Every alive member is forgiven, not only the ones this node watches.
        Gossip goes to any member, so a member this node does not watch is
        still one it has to be able to talk to, and a quarantine that only the
        watching node clears would leave the two of them refusing each other's
        dial for good. In a cluster larger than `monitored_peers` that is most
        pairs, so scoping this to the ring would stop a healed partition ever
        converging again.

        Clearing a quarantine dials nothing. It says only that this node is
        willing to be associated again, so doing it for a member that is
        perfectly reachable costs a set lookup and changes nothing.
        """
        for member in self._state.alive:
            if member.address != self._address:
                self._relent(member.address)

    def _forget_strangers(self) -> None:
        """Drop cached refs to nodes that are neither members nor seeds.

        A ref is cached so that talking to a member does not resolve twice.
        Answering a heartbeat caches nothing, because the asker need not be a
        member here and an unbounded cache keyed by whatever arrived on a
        socket is a cache anybody can grow. This prunes what membership has
        since moved on from, so the cache stays the size of the cluster.

        Seeds are kept because a node that has not joined yet has no members
        to speak of and still has to reach them.
        """
        keep = {member.address for member in self._state.alive}
        keep.update(self._seeds)
        for address in tuple(self._peers):
            if address not in keep:
                del self._peers[address]

    async def _answer(self, ctx: ActorContext[ClusterMessage], watcher: str) -> None:
        """Tell a node that is watching this one that it is still here.

        Answered whether or not the asker is a member here. What the answer
        means is the watcher's business, and a node that is behind on
        membership is exactly the one that should not also look dead.

        What is never done is open a new connection to an address that only
        the message vouches for. The asker names where to answer, so answering
        by dialling would let any peer that has completed a handshake make
        this node connect to any host and port it likes, as many times as it
        cares to ask. Two things make an address answerable: membership, which
        is a peer this node would gossip to anyway, and a link that already
        exists, which is how a node this one has not heard of yet is answered.
        A genuine watcher always has the second, since its heartbeat arrived
        over it. An invented address has neither and is dropped.

        The ref is kept only for a node membership already vouches for. The
        asker's address arrived in a message, so caching whatever turns up
        would let a peer grow this node's cache by asking under a new name
        each time.

        Args:
            ctx: This actor's context, for resolving the watcher.
            watcher: The node that asked.
        """
        vouched = any(member.address == watcher for member in self._state.alive)
        if not vouched and not self._linked(watcher):
            _log.debug(
                "%s asked %s for an answer and is neither a member nor linked",
                watcher,
                self._address,
            )
            return
        peer = await self._peer(ctx, watcher, remember=vouched)
        if peer is None:
            return
        peer.tell(HeartbeatReply(sender=self._address))

    def _observe(self, peer: str, status: ReachabilityStatus) -> None:
        """Record what this node believes about a member, when it is news.

        Only a change is written. Every round reaches the same conclusion
        about a member that is fine, and writing it again would bump the
        version and cost a gossip round to say nothing.

        The observation is this node's alone. It says this node cannot get
        through, which is exactly what one node can honestly claim, and the
        observer that said unreachable is the only one that can take it back.

        Args:
            peer: The member being judged.
            status: What this node now believes about it.
        """
        if self._state.reachability.says(self._address, peer) is status:
            return
        _log.info(
            "%s is %s from here",
            peer,
            "reachable again"
            if status is ReachabilityStatus.REACHABLE
            else "unreachable",
        )
        self._state = self._state.observing(self._address, peer, status).bumped_by(
            self._address
        )

    def _start_joining(
        self, timers: TimerScheduler[ClusterMessage], seeds: Sequence[str]
    ) -> None:
        """Take the seed list, and start asking to be let in.

        The first ask goes out at once rather than after the retry interval,
        because the common case is a cluster that is already running and can
        answer immediately.

        Args:
            timers: This actor's timers.
            seeds: The seed addresses, in the order every node lists them.
        """
        self._seeds = tuple(seeds)
        if self.joined:
            return
        timers.start_fixed_delay(
            _JOIN_TIMER,
            JoinTick(),
            self._settings.join_retry_interval,
            initial_delay=timedelta(0),
        )
        if self._seeds[0] == self._address:
            timers.start_single(_FORM_TIMER, FormTick(), self._settings.seed_form_after)

    async def _gossip_once(self, ctx: ActorContext[ClusterMessage]) -> None:
        """Send this node's view to one other member, chosen at random."""
        targets = [
            member.address
            for member in self._state.alive
            if member.address != self._address
        ]
        if not targets:
            return
        self._rounds += 1
        await self._send(ctx, self._choose(targets))

    async def _ask_the_seeds(
        self, ctx: ActorContext[ClusterMessage], timers: TimerScheduler[ClusterMessage]
    ) -> None:
        """Ask every seed to let this node in, until one of them does."""
        if self.joined:
            timers.cancel(_JOIN_TIMER)
            return
        for seed in self._seeds:
            if seed == self._address:
                continue
            peer = await self._peer(ctx, seed)
            if peer is None:
                continue
            peer.tell(Join(member=self._self))

    def _form_a_cluster(self) -> None:
        """Start a cluster alone, which only the first seed may ever do.

        It is allowed only after a wait in which nothing was heard from
        anybody. Any gossip at all means a cluster is already running and this
        node's job is to join it, which is the rule that stops a restarted
        seed from founding a second cluster beside the first.
        """
        if self.joined or self._heard_from_anyone:
            return
        _log.info("%s heard from no other seed, so it forms the cluster", self._address)
        self._state = Gossip.founding(
            self._self.with_status(MemberStatus.UP, up_number=1)
        )

    async def _admit(self, ctx: ActorContext[ClusterMessage], joiner: Member) -> None:
        """Record a node that asked to join, and tell it what this node knows.

        A node that has not joined anything itself ignores the request. Two
        nodes starting together would otherwise admit each other and form two
        clusters that never meet.
        """
        if not self.joined:
            _log.debug(
                "ignoring a join from %s: this node is not a member", joiner.address
            )
            return
        known = [m for m in self._state.members if m.address == joiner.address]
        if not any(m.key == joiner.key for m in known):
            state = self._state
            for stale in known:
                if stale.status not in (MemberStatus.DOWN, MemberStatus.REMOVED):
                    # The address is answering as a new incarnation, so the
                    # old one is gone. That is known rather than guessed at,
                    # which is why it is decided here and not by a failure
                    # detector: leaving it Up would block convergence on a
                    # member that no longer exists.
                    _log.info(
                        "%s restarted as incarnation %d, so %d is down",
                        joiner.address,
                        joiner.uid,
                        stale.uid,
                    )
                    state = state.with_member(stale.with_status(MemberStatus.DOWN))
            # Built rather than moved. The joiner arrived on a socket, so its
            # status is a claim, and `with_status` refuses to move one back
            # down the lattice: a Join carrying anything above `joining` would
            # raise here and stop this daemon for good. What a node may assert
            # about itself is who it is, and joining is the only status a join
            # can mean.
            admitted = Member(
                address=joiner.address,
                uid=joiner.uid,
                roles=joiner.roles,
                status=MemberStatus.JOINING,
            )
            _log.info("%s joins as %s", admitted.address, admitted.status)
            self._state = state.with_member(admitted).bumped_by(self._address)
        # Answered whether or not anything changed, because what the joiner is
        # waiting for is to see itself in a gossip, and a retry that arrives
        # after it was already admitted still has to be told.
        await self._send(ctx, joiner.address)

    async def _merge(
        self, ctx: ActorContext[ClusterMessage], envelope: GossipEnvelope
    ) -> None:
        """Merge a peer's view into this node's, and answer if this one is newer."""
        self._heard_from_anyone = True
        merged = self._state.merge(envelope.gossip)
        self._state = (
            merged.seen_by(self._address) if self._is_member_of(merged) else merged
        )
        if self._state.version.compare(envelope.gossip.version) is Ordering.AFTER:
            # The sender is behind, and telling it so now rather than waiting
            # for its turn in the rotation is most of what makes convergence
            # take rounds instead of seconds.
            await self._send(ctx, envelope.sender)

    def _start_leaving(self, address: str) -> None:
        """Mark a member as leaving, so the leader can walk it out."""
        member = self._state.member(address)
        if member is None:
            _log.warning("cannot let %s leave: it is not a member here", address)
            return
        if member.status not in (MemberStatus.JOINING, MemberStatus.UP):
            # Already on its way out, or downed. Either way the lattice says
            # this would be a step backwards, and a merge would undo it.
            return
        _log.info("%s is leaving", address)
        self._state = self._state.with_member(
            member.with_status(MemberStatus.LEAVING)
        ).bumped_by(self._address)

    def _down_member(self, address: str) -> None:
        """Down a member because an operator asked, not because a strategy did.

        The same lattice move a strategy makes, reached by a person instead:
        the member goes to `Down`, the version is bumped, and gossip carries the
        decision to everyone including the member named, which then hears it and
        shuts itself down. It needs no converged view and no leader, because
        downing only ever moves up the lattice, so an operator downing a member
        on one node and gossip downing it on another is one decision made twice.

        A member already `Down` or `Removed` is left alone: it has reached the
        end an operator was asking for, and moving it again would be a version
        bump that says nothing. An address that names no member is refused in
        words, since an operator that mistyped an address should hear so rather
        than have nothing happen.

        Args:
            address: The member to down.
        """
        member = self._state.member(address)
        if member is None:
            _log.warning("cannot down %s: it is not a member here", address)
            return
        if member.status in (MemberStatus.DOWN, MemberStatus.REMOVED):
            return
        _log.warning("%s is downed by an operator", address)
        self._state = self._state.with_member(
            member.with_status(MemberStatus.DOWN)
        ).bumped_by(self._address)

    def _lead(self) -> None:
        """Move members along, if this node leads a view every member has seen."""
        if not self.joined or self._state.leader != self._address:
            return
        if not self._state.converged:
            return
        moved = leader_actions(self._state)
        if moved != self._state:
            for member in moved.members:
                was = self._state.member(member.address)
                if was is not None and was.status is not member.status:
                    _log.info(
                        "%s moves from %s to %s",
                        member.address,
                        was.status,
                        member.status,
                    )
            self._state = moved.bumped_by(self._address)

    async def _down(self) -> None:
        """Down the losing side of a split, once a strategy says which loses.

        This is the path a node takes when the view is blocked by an
        unreachable member rather than converged, which is the one case
        [_lead][tapio.cluster.daemon.ClusterDaemon._lead] cannot make progress
        on. It is guarded twice over, because downing cannot be taken back. A
        strategy has to be configured to make the decision at all, and the
        split has to have held still for `down_after`, so a passing blip is
        ridden out rather than resolved.

        Every node runs it, not only the leader, and each downs the verdict for
        its own view. That is safe and needs no coordination because the two
        sides agree on the loser: a strategy that reads the shape of the split
        has both sides compute the same answer from mirror-image views, and the
        lease-backed one has them agree because only one side can take the
        lease. So the loser downs itself while the winner downs the loser.
        Downing is a move up the lattice, so two nodes reaching the same verdict
        is one decision made twice, not two decisions in conflict. Leaving it to
        a leader would not do: a strategy that downs a whole side, its own
        included, leaves that node no reachable member to gossip the decision
        to, so a peer that did not run the decision itself would never hear it.

        Deciding is awaited because the lease-backed strategy reaches an outside
        lock. The wait is the only point in a turn this actor gives up, and only
        while a split is being resolved, so a peer's heartbeat is answered on
        the turns either side of it.
        """
        me = self.self_member
        if (
            self._strategy is None
            or me is None
            or me.status
            in (
                MemberStatus.DOWN,
                MemberStatus.REMOVED,
            )
        ):
            # A node that has already downed itself makes no more decisions:
            # it is on its way out and has nothing left to keep or sacrifice.
            return
        unreachable = self._unreachable_alive()
        now = _now()
        if unreachable != self._split:
            self._split = unreachable
            self._split_since = now
        if not unreachable:
            return
        if now - self._split_since < self._down_after:
            return
        self._apply_downing(await self._strategy.decide(self._state))

    def _unreachable_alive(self) -> frozenset[str]:
        """The live members at least one observer currently cannot hear."""
        gone = self._state.reachability.unreachable
        return frozenset(m.address for m in self._state.alive if m.address in gone)

    def _apply_downing(self, verdict: frozenset[str]) -> None:
        """Move the members a strategy named to `Down`, in one version bump.

        A member already `Down` or `Removed` is left alone: the verdict is
        about the live members of a split, and downing one twice would be a
        version bump that says nothing.

        Args:
            verdict: The addresses to down.
        """
        changed = self._state
        downed: list[str] = []
        for address in sorted(verdict):
            member = changed.member(address)
            if member is None or member.status in (
                MemberStatus.DOWN,
                MemberStatus.REMOVED,
            ):
                continue
            changed = changed.with_member(member.with_status(MemberStatus.DOWN))
            downed.append(address)
        if not downed:
            return
        _log.warning(
            "%s downs %s, resolving a split with %r",
            self._address,
            ", ".join(downed),
            self._strategy,
        )
        self._state = changed.bumped_by(self._address)

    def _announce_if_downed(self) -> None:
        """Say once, on the event stream, that this node has downed itself.

        A node reaches this by downing its own side as the side's leader, or by
        merging in the gossip a side-mate leader sent. Either way a `Down`
        member may not rejoin as itself, so the application is told to shut the
        system down. Said once, because the status does not move back and
        repeating it would only be noise.
        """
        me = self.self_member
        if me is None or me.status is not MemberStatus.DOWN or self._downed_announced:
            return
        self._downed_announced = True
        _log.warning("%s has downed itself and should shut down", self._address)
        self._events.publish(
            ClusterDowned(
                address=self._address,
                detail=(
                    f"{self._address} was on the losing side of a split and downed "
                    "itself. A downed member may not rejoin as itself, so the "
                    "system should be shut down."
                ),
            )
        )
        self._downed.set()

    def _digest(self) -> _Digest:
        """Snapshot the facts events are made from, to diff against later.

        Returns:
            The snapshot, holding the primary member at each address, who is
            unreachable, who leads, and this node's own status.
        """
        members = self._state.primaries()
        me = self.self_member
        return _Digest(
            members=members,
            unreachable=self._unreachable_alive(),
            leader=self._state.leader,
            self_status=me.status if me is not None else None,
        )

    def _emit(self, before: _Digest) -> None:
        """Tell every subscriber what changed between two views of the cluster.

        Called at the end of every turn, so a change made by any path produces
        the right events without that path having to know about subscribers.
        Nothing is computed when nobody is listening.

        Args:
            before: The snapshot taken before this turn handled its message.
        """
        if not self._subscribers:
            return
        after = self._digest()
        for address, member in after.members.items():
            was = before.members.get(address)
            was_status = was.status if was is not None else None
            if member.status is MemberStatus.UP and was_status is not MemberStatus.UP:
                self._deliver(MemberUp(member=member))
            elif (
                member.status is MemberStatus.REMOVED
                and was_status is not MemberStatus.REMOVED
            ):
                self._deliver(MemberRemoved(member=member))
        for address in sorted(after.unreachable - before.unreachable):
            gone = after.members.get(address)
            if gone is not None:
                self._deliver(UnreachableMember(member=gone))
        for address in sorted(before.unreachable - after.unreachable):
            back = after.members.get(address)
            # A member that went unreachable and was then removed is reported
            # removed, not reachable again: it is gone, and its reachability
            # was dropped with it.
            if back is not None and back.status not in (
                MemberStatus.DOWN,
                MemberStatus.REMOVED,
            ):
                self._deliver(ReachableMember(member=back))
        if before.leader != after.leader:
            self._deliver(LeaderChanged(leader=after.leader))
        if (
            after.self_status is MemberStatus.DOWN
            and before.self_status is not MemberStatus.DOWN
        ):
            me = self.self_member
            if me is not None:
                self._deliver(SelfDown(member=me))

    def _deliver(self, event: ClusterEvent) -> None:
        """Send one event to every subscriber that asked for its kind.

        Args:
            event: The event to deliver.
        """
        for ref, wanted in self._subscribers.values():
            if not wanted or type(event) in wanted:
                ref.tell(event)

    def _subscribe(self, ctx: ActorContext[ClusterMessage], message: Subscribe) -> None:
        """Record a subscriber and hand it the current membership as events.

        Watching it is what lets a subscriber that stops be forgotten without
        an Unsubscribe. The replay is why a subscriber that starts after the
        cluster has formed still learns who is up: it hears the state it missed
        as the events that would have carried it, then each change as it comes.

        Args:
            ctx: This actor's context, for the death watch.
            message: The subscription, naming the subscriber and what it wants.
        """
        ref = message.subscriber
        wanted = frozenset(message.events)
        if ref.path not in self._subscribers:
            ctx.watch(ref)
        self._subscribers[ref.path] = (ref, wanted)
        self._replay(ref, wanted)

    def _unsubscribe(
        self, ctx: ActorContext[ClusterMessage], subscriber: ActorRef[Any]
    ) -> None:
        """Forget a subscriber that wants to keep running and stop listening.

        Args:
            ctx: This actor's context, for dropping the death watch.
            subscriber: The actor to forget. Harmless if it was not subscribed.
        """
        if self._subscribers.pop(subscriber.path, None) is not None:
            ctx.unwatch(subscriber)

    def _replay(
        self, ref: ActorRef[Any], wanted: frozenset[type[ClusterEvent]]
    ) -> None:
        """Send a new subscriber the events that describe the current view.

        Args:
            ref: The subscriber.
            wanted: The events it asked for, empty meaning all of them.
        """

        def wants(event: type[ClusterEvent]) -> bool:
            return not wanted or event in wanted

        if wants(MemberUp):
            for member in self._state.alive:
                if member.status is MemberStatus.UP:
                    ref.tell(MemberUp(member=member))
        if wants(UnreachableMember):
            for address in sorted(self._unreachable_alive()):
                gone = self._state.member(address)
                if gone is not None:
                    ref.tell(UnreachableMember(member=gone))
        if wants(LeaderChanged):
            ref.tell(LeaderChanged(leader=self._state.leader))
        if wants(SelfDown):
            me = self.self_member
            if me is not None and me.status is MemberStatus.DOWN:
                ref.tell(SelfDown(member=me))

    def _is_member_of(self, state: Gossip) -> bool:
        """Whether this node appears in a view it is about to adopt."""
        return any(member.key == self._self.key for member in state.members)

    async def _send(self, ctx: ActorContext[ClusterMessage], address: str) -> None:
        """Send this node's whole view to one other node."""
        peer = await self._peer(ctx, address)
        if peer is None:
            return
        peer.tell(GossipEnvelope(sender=self._address, gossip=self._state))

    async def _peer(
        self,
        ctx: ActorContext[ClusterMessage],
        address: str,
        *,
        remember: bool = True,
    ) -> ActorRef[WireMessage] | None:
        """Return the ref to another node's daemon, resolving it once.

        The ref is kept because it stays usable: it names a node rather than a
        link, so it survives a link that failed, and it names a path rather
        than an incarnation, so it survives a peer that restarted.

        An address this node cannot resolve is dropped rather than raised
        about. Resolving is the one thing the daemon does with an address that
        raises instead of dead-lettering, and every address it resolves came
        from somewhere else: a member merged out of a gossip, the sender of a
        heartbeat, a seed out of a configuration file. A raise here reaches
        the receive loop, where the default supervision decision is to stop,
        so one address nobody can dial would end this node's membership while
        every other member still listed it as up. `AddressStr` refuses the one
        form known to be undialable, and this is the guarantee behind it: no
        address stops the daemon, whatever it turns out to be.

        Args:
            ctx: This actor's context, which does the resolving.
            address: The node whose daemon is wanted.
            remember: Whether to keep the ref. False for an address that came
                out of a message rather than out of membership, so that what
                arrives on a socket cannot grow the cache.

        Returns:
            The ref, or `None` if the address names nothing this node can
            send to. Nothing is dialled either way: the first send through
            the ref is what opens the association.
        """
        held = self._peers.get(address)
        if held is not None:
            return held
        try:
            peer: ActorRef[WireMessage] = await ctx.resolve(
                daemon_uri(address), expect=WireMessage
            )
        except RefResolutionError as error:
            _log.warning(
                "%s cannot be reached from %s: %s", address, self._address, error
            )
            return None
        if remember:
            self._peers[address] = peer
        return peer

    def __repr__(self) -> str:
        """Render this node's address and what it believes."""
        return f"ClusterDaemon({self._address}, {self._state!r})"


def _now() -> float:
    """Return the time a failure detector reads.

    Returns:
        The loop's monotonic clock, which is the one every detector in the
        library is documented against.
    """
    return asyncio.get_running_loop().time()
