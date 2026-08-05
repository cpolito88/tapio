"""What the runtime needs of something that can be told an actor stopped.

Death watch started as a relationship between two cells, and the map that
holds it was typed that way. Ask broke that assumption. A promise ref watches
the actor it is waiting on, so that a target which stops fails the caller at
once rather than after the full timeout, and a promise ref has no cell. This
protocol is the two things a watched cell actually uses, so both kinds of
watcher fit in the same map.
"""

from typing import Any, Protocol, runtime_checkable

from tapio.actor.path import ActorPath
from tapio.actor.ref import ActorRef

__all__ = ["Watcher"]


@runtime_checkable
class Watcher(Protocol):
    """Something that can be registered for another actor's death."""

    @property
    def path(self) -> ActorPath:
        """Where this watcher sits, which is the key it is held under.

        Watchers are keyed by path so that watching twice still delivers
        exactly one signal.
        """
        ...

    def notify_terminated(self, ref: ActorRef[Any]) -> None:
        """Take delivery of a watched actor's death.

        Called on the system's loop, from the watched actor's own termination
        sequence, so it must not block and must not raise.

        Args:
            ref: A ref to the actor that stopped.
        """
        ...
