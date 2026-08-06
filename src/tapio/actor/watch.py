"""The two ends of a death watch, as protocols rather than as cells.

Death watch started as a relationship between two cells, and the maps that
hold it were typed that way. Two features broke that assumption.

Ask broke the watcher end. A promise ref watches the actor it is waiting on,
so that a target which stops fails the caller at once rather than after the
full timeout, and a promise ref has no cell. `Watcher` is the two things a
watched actor actually uses, so both kinds of watcher fit in one map.

Remoting broke the watched end. An actor on a peer has no cell here either,
and watching it means sending a frame and waiting for one back. `WatchTarget`
is what the watching cell actually uses, so a local actor and an actor on
another node are registered the same way.
"""

from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from tapio.actor.path import ActorPath

if TYPE_CHECKING:
    from tapio.actor.ref import ActorRef

__all__ = ["WatchTarget", "Watcher"]


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

    def notify_terminated(self, ref: "ActorRef[Any]") -> None:
        """Take delivery of a watched actor's death.

        Called on the system's loop, from the watched actor's own termination
        sequence, so it must not block and must not raise.

        Args:
            ref: A ref to the actor that stopped.
        """
        ...

    def notify_unreachable(self, ref: "ActorRef[Any]", detail: str) -> None:
        """Take delivery of a watched actor becoming unreachable.

        The actor was on a peer, and the link to that peer ended or went
        silent. Whether the actor itself is alive cannot be known from here,
        which is the difference from `notify_terminated`.

        An actor watching another sees no difference: both arrive as
        `Terminated`, because a supervisor that had to tell them apart could
        do nothing useful with the answer. An ask does tell them apart, since
        the caller may want to retry somewhere else.

        Args:
            ref: A ref to the actor that can no longer be reached.
            detail: Why the peer is considered gone, for the error and the log.
        """
        ...


@runtime_checkable
class WatchTarget(Protocol):
    """Something a death watch can be registered on."""

    @property
    def path(self) -> ActorPath:
        """Where the watched actor sits, which is the key it is held under."""
        ...

    @property
    def is_alive(self) -> bool:
        """Whether a watch registered now could still produce a signal.

        `False` means the answer is already known, so the watcher is told at
        once rather than waiting for a signal that will never come. For an
        actor on a peer it means the peer is beyond reach, not that the actor
        is known to have stopped.
        """
        ...

    def add_watcher(self, watcher: Watcher) -> None:
        """Register something to be told when this actor stops."""
        ...

    def remove_watcher(self, watcher: Watcher) -> None:
        """Deregister a watcher."""
        ...
