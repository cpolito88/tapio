"""`ActorContext`: what an actor is handed to act on its surroundings."""

from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from tapio.actor.mailbox import MailboxConfig
from tapio.actor.path import ActorPath
from tapio.actor.ref import ActorRef
from tapio.logging import ActorLogAdapter
from tapio.message import Message
from tapio.validation import MessageType

if TYPE_CHECKING:  # behavior.py imports this module, so importing it back at
    from tapio.actor.behavior import Behavior  # runtime would be a cycle

__all__ = ["ActorContext"]

T = TypeVar("T", bound=Message)
U = TypeVar("U", bound=Message)
R = TypeVar("R")


class ActorContext(ABC, Generic[T]):
    """The runtime handed to a behavior for the duration of a message.

    Only the members the runtime can honour today are declared. Timers and
    stashing are deliberately not here. A behavior receives them from
    `Behaviors.with_timers` and `Behaviors.with_stash`, because both outlive
    an incarnation and belong to the cell rather than to whatever the actor
    happens to be doing.

    It is an abstract class rather than a Protocol, because the runtime hands
    out one implementation and users are not expected to write their own.
    """

    __slots__ = ()

    @property
    @abstractmethod
    def path(self) -> ActorPath:
        """Where this actor sits in the tree."""

    @property
    @abstractmethod
    def self_ref(self) -> ActorRef[T]:
        """A ref to this actor, to hand out in messages.

        Named `self_ref` rather than Pekko's `self`, because `self` is already
        the first parameter of every method that would use it.
        """

    @property
    @abstractmethod
    def log(self) -> ActorLogAdapter:
        """A logger that tags every record with this actor's path."""

    @abstractmethod
    def spawn(
        self,
        behavior: "Behavior[U]",
        name: str,
        mailbox: MailboxConfig | None = None,
    ) -> ActorRef[U]:
        """Start a child actor under this one.

        Args:
            behavior: What the child does. `Behaviors.setup(...)` is evaluated
                here, synchronously, so the child's message type is known
                before the ref is handed back.
            name: The child's name, unique among this actor's live children.
            mailbox: Capacity and overflow behaviour for the child. The
                system's default when omitted.

        Returns:
            A ref to the new child.

        Raises:
            ActorNameError: If a live child already has that name.
            ActorSystemTerminating: If this actor is already shutting down.
            BehaviorTypeError: If the behavior declares no resolvable message
                type.
        """

    @abstractmethod
    def spawn_anonymous(
        self, behavior: "Behavior[U]", mailbox: MailboxConfig | None = None
    ) -> ActorRef[U]:
        """Start a child under a generated name.

        Generated names begin with `$`, and user-chosen names may not, so a
        generated name never collides with one someone picked.

        Args:
            behavior: What the child does.
            mailbox: Capacity and overflow behaviour for the child. The
                system's default when omitted.

        Returns:
            A ref to the new child.
        """

    @abstractmethod
    def message_adapter(
        self, adapt: Callable[[U], T], msg_type: MessageType | None = None
    ) -> ActorRef[U]:
        """Hand out a ref that translates another protocol into this actor's.

        For talking to an actor whose reply type is not yours and should not
        become yours. Widening your declared message type to admit a foreign
        reply lets anyone send it, and puts someone else's vocabulary inside
        your handlers. An adapter avoids both:

        ```python
        replies = ctx.message_adapter(
            lambda price: PriceQuoted(cents=price.cents), msg_type=Price
        )
        pricing.tell(Quote(reply_to=replies))
        ```

        A translated message arrives on this actor's own user lane, so it is
        ordinary traffic. It queues where it arrived, it never re-enters a
        running handler, and it is validated against the declared type like
        anything else.

        The translation runs in this actor rather than in the sender, so a
        failure in it becomes this actor's supervision decision. A sender that
        has never heard of the adapter must not have this actor's bug raised
        into it.

        Each call makes a new adapter, and one already handed out keeps
        working across a restart. The ref addresses the actor, not the
        incarnation that created it.

        Args:
            adapt: Turns an accepted message into one of this actor's own. Its
                parameter annotation says what it accepts.
            msg_type: What the adapter accepts, when the annotation cannot say.
                Required for a lambda, which carries none.

        Returns:
            A ref to hand out in place of this actor's own.

        Raises:
            BehaviorTypeError: If neither `msg_type` nor an annotation resolves
                what the adapter accepts.
            MessageTypeError: If what it resolves to is not a `Message`
                subclass or a union of them.
        """

    @abstractmethod
    async def run_blocking(
        self, fn: Callable[..., R], /, *args: Any, **kwargs: Any
    ) -> R:
        """Run a call that blocks on a thread, instead of on the loop.

        ```python
        rows = await ctx.run_blocking(cursor.execute, "select 1")
        ```

        Every actor in a system shares one event loop, so a handler that
        blocks stops all of them. This moves the call to a bounded pool of
        threads that belongs to the system, sized by `blocking_pool_size`.

        Two things about it are worth knowing before you rely on it.

        **The actor is parked for the duration.** It is awaiting, so it is not
        reading its mailbox: messages queue up behind the call, and on a
        bounded mailbox the overflow strategy will fire while it waits. The
        loop is free, which is the point, but this actor is not.

        **The call cannot be cancelled.** Python cannot interrupt a thread
        that is inside a C call. Cancelling the actor abandons the result and
        the thread keeps going, and shutdown waits for it only until the
        deadline. Pass whatever timeout the library you are calling offers.

        Args:
            fn: The blocking callable.
            *args: Its positional arguments.
            **kwargs: Its keyword arguments.

        Returns:
            Whatever `fn` returned.

        Raises:
            ActorSystemTerminating: If the system is shutting down, so the
                pool is no longer accepting work.
            Exception: Whatever `fn` raised, re-raised here, where it becomes
                this actor's supervision decision like any other failure.
        """

    @abstractmethod
    async def resolve(self, uri: str, *, expect: type[U]) -> ActorRef[U]:
        """Turn a ref's string form into a ref, wherever the actor it names is.

        ```python
        stock = await ctx.resolve(
            "tapio://inventory@inventory.svc:25520/user/stock", expect=Reserve
        )
        ```

        The same call as
        [ActorSystem.resolve][tapio.actor.system.ActorSystem.resolve], from
        inside an actor. An address this system owns resolves to the live
        local ref, so resolving your own system never puts a socket in the
        middle of a local send. Another system's address resolves to a ref
        that reaches it through an association.

        Nothing waits for the peer here. The association is created and
        dialled behind the sends that follow, so this call does not fail
        because a node is down, and a `tell` to a peer that never answers
        dead-letters instead of hanging.

        Args:
            uri: The full string form, `tapio://sys@host:port/user/x#uid`.
            expect: What the target accepts. This is a claim about the peer,
                checked at this end. The receiving node checks it against the
                target's real message type, and that is the check that
                decides.

        Returns:
            A ref to the actor it names.

        Raises:
            RefResolutionError: If the text is not a ref, if it names a system
                with no host to dial, or if it names a reachable peer while
                this system has remoting switched off.
            MessageTypeError: If `expect` is not a `Message` subclass or a
                union of them.
        """

    @abstractmethod
    def watch(self, ref: ActorRef[Any]) -> None:
        """Ask to be sent `Terminated` when another actor stops.

        The signal arrives on the system lane, so it is not queued behind
        waiting user traffic, and it arrives exactly once however many times
        the ref was watched. A restart produces no signal, because the actor's
        identity is unchanged and only its incarnation is new.

        Watching a ref that has already stopped delivers `Terminated` at once
        rather than refusing, so the caller's code is the same either way.
        This is also why there is no "is it alive?" call: the answer would be
        out of date before the caller could read it.

        Args:
            ref: The actor to watch. Watching an actor twice is harmless.

        Raises:
            WatchError: If the ref has no live actor behind it, or if an actor
                tries to watch itself.
        """

    @abstractmethod
    def unwatch(self, ref: ActorRef[Any]) -> None:
        """Stop being told when another actor stops.

        Harmless if this actor was not watching it. It does not retract a
        `Terminated` that is already on the system lane, because by then the
        actor it reports on has stopped.

        Args:
            ref: The actor to stop watching.
        """
