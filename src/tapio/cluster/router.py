"""A group router: one address in front of an actor on every member of a role.

A pool router owns its routees, spawning them as its own children. A group
router owns none of them. It watches membership and routes to whatever actor
each member of a role publishes at an agreed path, so the pool grows and shrinks
as nodes join and leave rather than as children start and stop.

The routee on each node is reached by its bare path, the way the cluster daemon
itself is: it is published as a well-known name, so a router on any node can
address it without knowing which incarnation is answering over there. Publish
one with `system.refs.register_well_known(ref)` on each node that should take a
share of the work.

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
from datetime import timedelta
from typing import Any, cast, final

from tapio.actor.behavior import Behavior, Behaviors
from tapio.actor.cell import LocalActorRef
from tapio.actor.context import ActorContext
from tapio.actor.dead_letters import DeadLetterOffice, DeadLetterReason
from tapio.actor.ref import ActorRef
from tapio.actor.router import RoundRobin, RoutingStrategy
from tapio.actor.timers import TimerScheduler
from tapio.cluster.daemon import local_daemon
from tapio.cluster.events import (
    ClusterEvent,
    MemberRemoved,
    MemberUp,
    ReachableMember,
    UnreachableMember,
)
from tapio.cluster.member import Member
from tapio.cluster.messages import Subscribe
from tapio.errors import MailboxFullError
from tapio.logging import runtime_logger
from tapio.message import Message
from tapio.validation import MessageType, normalize_msg_type

__all__ = ["group_router"]

_log = runtime_logger("cluster.router")

_SUBSCRIBE_TIMER = "group-subscribe"
_RETRY_INTERVAL = timedelta(milliseconds=50)

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

    def behavior(self) -> Behavior[Any]:
        """Build the router actor, accepting its own type plus cluster events."""
        accepted = functools.reduce(
            operator.or_, (self._msg_type, *_ROUTER_EVENTS, _Reconcile)
        )

        def with_timers(timers: TimerScheduler[Any]) -> Behavior[Any]:
            def build(ctx: ActorContext[Any]) -> Behavior[Any]:
                timers.start_fixed_delay(
                    _SUBSCRIBE_TIMER,
                    _Reconcile(),
                    _RETRY_INTERVAL,
                    initial_delay=timedelta(0),
                )

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
                await self._ensure_subscribed(ctx, timers)
            case MemberUp():
                await self._offer(ctx, message.member)
            case ReachableMember():
                await self._offer(ctx, message.member)
            case MemberRemoved():
                self._drop(ctx, message.member)
            case UnreachableMember():
                self._drop(ctx, message.member)
            case _:
                self._forward(ctx, message)
        return Behaviors.same()

    async def _ensure_subscribed(
        self, ctx: ActorContext[Any], timers: TimerScheduler[Any]
    ) -> None:
        """Subscribe to the daemon once it exists, then stop retrying."""
        if self._daemon is not None:
            timers.cancel(_SUBSCRIBE_TIMER)
            return
        daemon = await local_daemon(ctx)
        if daemon is None:
            return
        self._daemon = daemon
        daemon.tell(Subscribe(subscriber=ctx.self_ref, events=_ROUTER_EVENTS))
        timers.cancel(_SUBSCRIBE_TIMER)

    async def _offer(self, ctx: ActorContext[Any], member: Member) -> None:
        """Add a member's routee to the pool, if it carries the role.

        Resolving names a path rather than an incarnation, so it returns a ref
        whether or not the actor over there is published yet. One that is not
        dead-letters what it is sent, which is the same as any other routee that
        cannot take a message.
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

    def _forward(self, ctx: ActorContext[Any], message: Message) -> None:
        """Send one message to the routee the strategy picked.

        An empty pool dead-letters the message rather than stopping the router:
        members come and go, and the next one to arrive is what the router is
        waiting for. A routee at capacity dead-letters too, the same recipient
        error a pool treats the same way.
        """
        routees: Sequence[ActorRef[Any]] = list(self._routees.values())
        if not routees:
            self._office(ctx).publish(
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
            self._office(ctx).publish(
                message, routee.path, DeadLetterReason.MAILBOX_FULL
            )

    def _office(self, ctx: ActorContext[Any]) -> DeadLetterOffice:
        """Find the system's dead letter office, for work that cannot be routed."""
        return cast(LocalActorRef[Any], ctx.self_ref).cell.runtime.dead_letters

    def __repr__(self) -> str:
        """Render the path, the role, and how many routees are in the pool."""
        return (
            f"group_router({self._path!r}, role={self._role!r}, "
            f"routees={len(self._routees)})"
        )
