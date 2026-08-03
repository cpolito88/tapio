"""`ActorContext`: what an actor is handed to act on its surroundings."""

from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from tapio.actor.path import ActorPath
from tapio.actor.ref import ActorRef
from tapio.message import Message

__all__ = ["ActorContext"]

T = TypeVar("T", bound=Message)


class ActorContext(ABC, Generic[T]):
    """The runtime handed to a behavior for the duration of a message.

    Only the members a behavior signature needs are declared so far. Spawning,
    watching, timers and logging arrive with the runtime that can honour them.
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
