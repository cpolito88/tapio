"""A group router: one address in front of an actor on every member of a role.

A pool router owns its routees, spawning them as its own children. A group
router owns none of them. It watches membership and routes to whatever actor
each member of a role publishes at an agreed path, so the pool grows and shrinks
as nodes join and leave rather than as children start and stop.

The routee on each node is reached by its bare path, the way the cluster daemon
itself is: it is published as a well-known name, so a router on any node can
address it without knowing which incarnation is answering over there. Publish
one with `system.refs.register_well_known(ref)` on each node that should take a
share of the work. The routee may be published after the router starts, and
may be replaced by a new incarnation, on this node as on any other.

Two differences from a pool follow from owning nothing. An empty group is not
the end of the router: members come and go, and a router that stopped the first
time the last one left would have to be respawned to see the next one arrive, so
instead it holds and dead-letters what it is handed until a routee appears.
And a routee that goes away is learned from membership, not from a death watch:
`MemberRemoved` drops it, and so does `UnreachableMember`, because a member no
node can hear is not one to keep routing work to.

Selection reuses the same [RoutingStrategy][tapio.actor.router.RoutingStrategy]
as the pool, so round-robin and anything written for a pool works here without a
change.
"""

import functools
import operator
from collections.abc import Sequence
from typing import Any, cast, final

from tapio.actor.behavior import Behavior, Behaviors
from tapio.actor.context import ActorContext
from tapio.actor.dead_letters import DeadLetterReason
from tapio.actor.ref import ActorRef
from tapio.actor.router import RoundRobin, RoutingStrategy
from tapio.actor.timers import TimerScheduler
from tapio.cluster.daemon import start_subscribing, subscribe_when_ready
from tapio.cluster.events import (
    ClusterEvent,
    MemberRemoved,
    MemberUp,
    ReachableMember,
    UnreachableMember,
)
from tapio.cluster.member import Member
from tapio.errors import MailboxFullError
from tapio.logging import runtime_logger
from tapio.message import Message
from tapio.validation import MessageType, normalize_msg_type

__all__ = ["group_router"]

_log = runtime_logger("cluster.router")

_SUBSCRIBE_TIMER = "group-subscribe"

_ROUTER_EVENTS: tuple[type[ClusterEvent], ...] = (
    MemberUp,
    MemberRemoved,
    UnreachableMember,
    ReachableMember,
)


@final
class _Reconcile(Message):
    """Retry subscribing to the daemon until it has started."""


def group_router(
    msg_type: MessageType,
    *,
    path: str,
    role: str | None = None,
    strategy: RoutingStrategy | None = None,
) -> Behavior[Any]:
    """Build a group router over an actor published on the members of a role.

    Args:
        msg_type: What the routees accept, and so what this router forwards. A
            group router cannot read this off a routee the way a pool reads it
            off a child it spawned, because its routees live on other nodes, so
            it is named here.
        path: The path the routee is published at on each member, such as
            `/user/worker`. It is resolved against each member's address, so the
            actor there must be reachable by that bare path, which means
            published as a well-known name.
        role: The role a member must carry to take a share. `None` routes to
            every member.
        strategy: How to choose between routees. Round-robin when omitted.

    Returns:
        The router behavior, to spawn.
    """
    chosen = strategy if strategy is not None else RoundRobin()
    resolved = normalize_msg_type(msg_type, origin="a group router")
    return _GroupRouter(resolved, path, role, chosen).behavior()


class _GroupRouter:
    """One router's view of who is routable, and how it spreads work over them."""

    def __init__(
        self,
        msg_type: MessageType,
        path: str,
        role: str | None,
        strategy: RoutingStrategy,
    ) -> None:
        """Describe the router, before its actor exists."""
        self._msg_type = msg_type
        self._path = path
        self._role = role
        self._strategy = strategy
        self._daemon: ActorRef[Any] | None = None
        self._routees: dict[str, ActorRef[Any]] = {}
        # This node's address, in the form members are named by. Set when the
        # actor is built, since that is when there is a node to ask.
        self._here: str | None = None

    def behavior(self) -> Behavior[Any]:
        """Build the router actor, accepting its own type plus cluster events."""
        accepted = functools.reduce(
            operator.or_, (self._msg_type, *_ROUTER_EVENTS, _Reconcile)
        )

        def with_timers(timers: TimerScheduler[Any]) -> Behavior[Any]:
            def build(ctx: ActorContext[Any]) -> Behavior[Any]:
                self._here = str(ctx.self_ref.address)
                start_subscribing(timers, _SUBSCRIBE_TIMER, _Reconcile())

                async def on_message(
                    ctx: ActorContext[Any], message: Any
                ) -> Behavior[Any]:
                    return await self._receive(ctx, timers, message)

                return Behaviors.receive(on_message, accepted)

            return Behaviors.setup(build)

        return Behaviors.with_timers(with_timers)

    async def _receive(
        self,
        ctx: ActorContext[Any],
        timers: TimerScheduler[Any],
        message: Any,
    ) -> Behavior[Any]:
        """Update the pool on a membership event, or forward anything else."""
        match message:
            case _Reconcile():
                if self._daemon is None:
                    self._daemon = await subscribe_when_ready(
                        ctx, timers, _SUBSCRIBE_TIMER, _ROUTER_EVENTS
                    )
            case MemberUp():
                await self._offer(ctx, message.member)
            case ReachableMember():
                await self._offer(ctx, message.member)
            case MemberRemoved():
                self._drop(ctx, message.member)
            case UnreachableMember():
                self._drop(ctx, message.member)
            case _:
                await self._forward(ctx, message)
        return Behaviors.same()

    async def _offer(self, ctx: ActorContext[Any], member: Member) -> None:
        """Add a member's routee to the pool, if it carries the role.

        For another member, resolving gives a remote ref to a bare path, and
        the peer looks the path up for each frame it receives. So the ref keeps
        working whether the actor over there is published yet or not, and
        across its restarts. Until it is published, what it is sent
        dead-letters over there.

        For this node, resolving gives whatever holds the name at that moment:
        a live ref, or a dead-letter ref if nothing is published yet. Keeping
        that answer would freeze it, so the local routee is resolved again for
        each message instead. See `_routable`.
        """
        if self._role is not None and self._role not in member.roles:
            return
        uri = f"{member.address}{self._path}"
        routee: ActorRef[Any] = await ctx.resolve(uri, expect=cast(Any, self._msg_type))
        self._routees[member.address] = routee
        _log.debug("group router routes to %s at %s", member.address, self._path)

    def _drop(self, ctx: ActorContext[Any], member: Member) -> None:
        """Take a member's routee out of the pool."""
        if self._routees.pop(member.address, None) is not None:
            _log.debug("group router drops %s", member.address)

    async def _routable(self, ctx: ActorContext[Any]) -> list[ActorRef[Any]]:
        """The routees to choose from for one message, in a stable order.

        This node's routee is looked up by its well-known name now, the same
        lookup a peer makes for each frame. That is what lets a routee
        published after the router, or a new incarnation of one, take its
        share.
        """
        routees: list[ActorRef[Any]] = []
        for address, ref in self._routees.items():
            if address == self._here:
                ref = await ctx.resolve(
                    f"{address}{self._path}", expect=cast(Any, self._msg_type)
                )
            routees.append(ref)
        return routees

    async def _forward(self, ctx: ActorContext[Any], message: Message) -> None:
        """Send one message to the routee the strategy picked.

        An empty pool dead-letters the message rather than stopping the router:
        members come and go, and the next one to arrive is what the router is
        waiting for. A routee at capacity dead-letters too, the same recipient
        error a pool treats the same way.
        """
        routees: Sequence[ActorRef[Any]] = await self._routable(ctx)
        if not routees:
            ctx.dead_letter(
                message,
                ctx.self_ref.path,
                DeadLetterReason.UNKNOWN_RECIPIENT,
                detail="the group router has no routees to route to",
            )
            return
        routee = self._strategy.select(routees, message)
        try:
            routee.tell(message)
        except MailboxFullError:
            _log.warning("%s is full; the message could not be routed", routee.path)
            ctx.dead_letter(message, routee.path, DeadLetterReason.MAILBOX_FULL)

    def __repr__(self) -> str:
        """Render the path, the role, and how many routees are in the pool."""
        return (
            f"group_router({self._path!r}, role={self._role!r}, "
            f"routees={len(self._routees)})"
        )
