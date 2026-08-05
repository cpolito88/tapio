"""Routers: one address in front of several identical actors.

A pool router spawns its routees as children and forwards what it is told to
one of them. It is worth having because the version people write by hand gets
three things wrong: it keeps the routee refs in a list that nothing prunes
when one dies, it spreads work with an unguarded counter, and it has no answer
for what happens when the last routee stops.

Selection sits behind a protocol, so random, broadcast or least-recently-used
strategies can be added without a rewrite. Round-robin is the only one
shipped, because it is the only one that needs no information the router does
not already have. A strategy that guesses at load without measuring it is
worse than the obvious one.

A router is a conduit, not an origin. Two decisions follow from that:

* A routee that cannot take a message is a recipient error, so the message
  becomes a dead letter rather than a failure. The router did not write the
  message, and failing would take down a whole pool because one member was
  busy.
* A routee that stops leaves the pool, and when the last one goes the router
  stops too. An empty pool is an address that silently swallows work.
"""

from collections.abc import Sequence
from typing import Any, Protocol, TypeVar, cast, runtime_checkable

from tapio.actor.behavior import Behavior, Behaviors, ReceivingBehavior
from tapio.actor.cell import LocalActorRef
from tapio.actor.context import ActorContext
from tapio.actor.dead_letters import DeadLetterOffice, DeadLetterReason
from tapio.actor.mailbox import MailboxConfig
from tapio.actor.ref import ActorRef
from tapio.actor.signals import Signal, Terminated
from tapio.errors import BehaviorTypeError, MailboxFullError
from tapio.message import Message
from tapio.validation import MessageType

__all__ = ["RoundRobin", "Routers", "RoutingStrategy"]

T = TypeVar("T", bound=Message)


@runtime_checkable
class RoutingStrategy(Protocol):
    """Chooses which routee a message goes to.

    Called on the router's own receive loop, one message at a time. An
    implementation holding state therefore needs no locking, and it must not
    block.
    """

    def select(self, routees: Sequence[ActorRef[T]], message: Message) -> ActorRef[T]:
        """Pick a routee.

        Args:
            routees: The live routees, never empty. The pool shrinks as
                routees stop, so this is the pool as it is right now.
            message: The message being routed, for a strategy that reads it.

        Returns:
            One of the routees.
        """
        ...


class RoundRobin:
    """Hand each message to the next routee in turn.

    It keeps a counter rather than a position, so removing a dead routee
    shifts the rotation instead of restarting it. An actor that has just
    received work does not receive more straight away because the pool shrank.
    """

    __slots__ = ("_sent",)

    def __init__(self) -> None:
        """Start the rotation."""
        self._sent = 0

    def select(self, routees: Sequence[ActorRef[T]], message: Message) -> ActorRef[T]:
        """Return the next routee in the rotation."""
        routee = routees[self._sent % len(routees)]
        self._sent += 1
        return routee

    def __repr__(self) -> str:
        """Render how far round the rotation has gone."""
        return f"RoundRobin(sent={self._sent})"


class _PoolBehavior(ReceivingBehavior[T]):
    """What a pool router does: forward, and keep the pool honest."""

    def __init__(
        self,
        routees: list[ActorRef[T]],
        strategy: RoutingStrategy,
        msg_type: MessageType,
        office: DeadLetterOffice,
    ) -> None:
        """Bind the pool, its strategy, and where undeliverable work goes."""
        self._routees = routees
        self._strategy = strategy
        self._office = office
        self.msg_type = msg_type

    async def receive(self, ctx: ActorContext[T], message: T) -> Behavior[T]:
        """Forward one message to the routee the strategy picked."""
        routee = self._strategy.select(self._routees, message)
        try:
            routee.tell(message)
        except MailboxFullError:
            # A `FAIL` routee at capacity. The router is a conduit, so this is
            # a recipient error like any other. Raising here would fail the
            # whole pool because one member was busy.
            ctx.log.warning("%s is full; the message could not be routed", routee.path)
            self._office.publish(message, routee.path, DeadLetterReason.MAILBOX_FULL)
        return Behaviors.same()

    async def receive_signal(self, ctx: ActorContext[T], signal: Signal) -> Behavior[T]:
        """Drop a routee that stopped, and stop when the last one has."""
        if not isinstance(signal, Terminated):
            return Behaviors.unhandled()
        if not any(r.path == signal.ref.path for r in self._routees):
            # Something else this actor was watching. A router only has an
            # opinion about its own pool.
            return Behaviors.unhandled()

        self._routees = [r for r in self._routees if r.path != signal.ref.path]
        if self._routees:
            ctx.log.info(
                "routee %s stopped; %d left", signal.ref.path, len(self._routees)
            )
            return Behaviors.same()
        # Info rather than a warning. An empty pool is how every router ends,
        # including in an ordinary shutdown where the children stop first.
        ctx.log.info("every routee has stopped; stopping the router with them")
        return Behaviors.stopped()

    def __repr__(self) -> str:
        """Render the pool size and the strategy spreading work over it."""
        return f"Routers.pool({len(self._routees)}, strategy={self._strategy!r})"


class Routers:
    """Factories for routers, as `Behaviors` is for behaviors."""

    @staticmethod
    def pool(
        size: int,
        behavior: Behavior[T],
        *,
        strategy: RoutingStrategy | None = None,
        routee_mailbox: MailboxConfig | None = None,
    ) -> Behavior[T]:
        """Spawn `size` copies of a behavior and spread work over them.

        ```python
        workers = ctx.spawn(Routers.pool(8, worker()), name="workers")
        ```

        The router accepts exactly what a routee accepts. It reads the type
        from the routees it spawned rather than being told it again, so the
        two cannot drift apart.

        Pass a stateful routee as `Behaviors.setup(...)` or another factory.
        Every routee starts from the same object, so an already-built behavior
        holding state would be shared by the whole pool.

        The router is the routees' parent, so their failures are supervised
        the ordinary way. Wrap `behavior` in `Behaviors.supervise(...)` and a
        routee that fails is restarted in place, with the pool unchanged.
        Without that it stops and leaves the pool, and when the last routee
        goes the router stops too.

        Args:
            size: How many routees to spawn. At least one.
            behavior: What each routee does.
            strategy: How to choose between them. Round-robin when omitted.
            routee_mailbox: Capacity and overflow behaviour for each routee.
                The system default when omitted. A bounded routee that fills
                up dead-letters what it cannot take, rather than failing the
                pool. Put backpressure on the router's own mailbox instead,
                where a sender can `offer` into it and wait.

        Returns:
            A behavior to spawn.

        Raises:
            ValueError: If `size` is below one.
        """
        if size < 1:
            msg = f"a router pool needs at least one routee, got {size}"
            raise ValueError(msg)
        chosen = strategy if strategy is not None else RoundRobin()

        def build(ctx: ActorContext[T]) -> Behavior[T]:
            routees = [
                ctx.spawn(behavior, f"routee-{n}", routee_mailbox)
                for n in range(1, size + 1)
            ]
            for routee in routees:
                # The router has to hear about a routee that stops, or it goes
                # on sending work to an address nobody reads.
                ctx.watch(routee)
            return _PoolBehavior(routees, chosen, _pool_msg_type(routees), _office(ctx))

        return Behaviors.setup(build)


def _pool_msg_type(routees: Sequence[ActorRef[Any]]) -> MessageType:
    """Take the router's message type from the routees it just spawned.

    It reads the type off a started routee rather than off the behavior,
    because a `Behaviors.setup` declares no type until it has run. By this
    point it has.
    """
    first = routees[0]
    msg_type = first.cell.msg_type if isinstance(first, LocalActorRef) else None
    if msg_type is None:
        msg = (
            f"cannot route to {first.path}: it declares no message type, so "
            "there is nothing for the router to accept on its behalf"
        )
        raise BehaviorTypeError(msg)
    return msg_type


def _office(ctx: ActorContext[Any]) -> DeadLetterOffice:
    """Find the system's dead letter office from inside a behavior.

    A router forwards messages it did not write, so it needs somewhere to
    account for the ones it could not pass on. Nothing else in the library
    needs this, which is why it is a function here rather than a member of
    `ActorContext`. A user's actor produces dead letters by sending to an
    actor that has stopped, not by publishing them.
    """
    return cast(LocalActorRef[Any], ctx.self_ref).cell.runtime.dead_letters
