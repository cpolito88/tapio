"""`ActorContext`: what an actor is handed to act on its surroundings."""

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from tapio.actor.mailbox import MailboxConfig
from tapio.actor.path import ActorPath
from tapio.actor.ref import ActorRef
from tapio.logging import ActorLogAdapter
from tapio.message import Message

if TYPE_CHECKING:  # behavior.py imports this module, so importing it back at
    from tapio.actor.behavior import Behavior  # runtime would be a cycle

__all__ = ["ActorContext"]

T = TypeVar("T", bound=Message)
U = TypeVar("U", bound=Message)


class ActorContext(ABC, Generic[T]):
    """The runtime handed to a behavior for the duration of a message.

    Only the members the runtime can honour today are declared; `run_blocking`
    arrives with the milestone that implements it. Timers and stashing are not
    here on purpose: both are handed to a behavior by
    `Behaviors.with_timers` and `Behaviors.with_stash`, because both outlive an
    incarnation and belong to the cell rather than to whatever the actor is
    doing when it reaches for one.
    An abstract class rather than a Protocol, because the runtime hands out one
    concrete implementation and users are never expected to write their own.
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
        a parameter name in every method that would use it.
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

        Generated names begin with `$`, which user-chosen names may not, so a
        generated name can never collide with one someone picked.

        Args:
            behavior: What the child does.
            mailbox: Capacity and overflow behaviour for the child. The
                system's default when omitted.

        Returns:
            A ref to the new child.
        """

    @abstractmethod
    def watch(self, ref: ActorRef[Any]) -> None:
        """Ask to be sent `Terminated` when another actor stops.

        The signal arrives on the system lane, so it is not queued behind
        whatever user traffic is waiting, and it arrives exactly once however
        many times the ref was watched. A restart produces none: the actor's
        identity is unchanged and only its incarnation is new.

        Watching a ref that has already stopped delivers `Terminated` at once
        rather than refusing, so the caller's code is the same however the race
        came out. This is why there is no "is it alive?" predicate: any answer
        one could give is stale before the caller reads it.

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
        `Terminated` already on the system lane: by the time one is queued the
        thing it reports has happened.

        Args:
            ref: The actor to stop watching.
        """
