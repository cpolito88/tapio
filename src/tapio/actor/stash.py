"""Stash: holding messages an actor is not ready for yet.

An actor that has to load something before it can work has two bad options and
one good one. Dropping what arrives in the meantime loses work. Blocking the
receive loop on the load makes the actor unable to answer anything, including
a stop. The good one is to accept the messages, put them aside, and replay them
once the state they need exists.

Replay is the part with a decision in it. `unstash_all` puts the held messages
back at the *front* of the user lane rather than handing them to the behavior
one by one: the actor stays a normal actor for the whole replay, so signals
still outrank the backlog and a stop arriving mid-replay is honoured rather
than queued behind work the actor is no longer going to do.

A stash is bounded, always. It is a buffer fed by traffic the actor is by
definition not keeping up with, so an unbounded one is a slow memory leak with
a plausible excuse. Overflow raises in the actor that stashed, which is the one
place with the context to decide whether the right answer is to drop the
message, reply with a rejection, or fail.
"""

from typing import Generic, TypeVar

from tapio.actor.behavior import Behavior
from tapio.errors import StashOverflowError
from tapio.message import Message

__all__ = ["StashBuffer", "UnstashBehavior"]

T = TypeVar("T", bound=Message)


class StashBuffer(Generic[T]):
    """The handle `Behaviors.with_stash` gives a behavior.

    One buffer serves every incarnation of its actor, and a restart empties it
    rather than replacing it: messages stashed by the state that just failed
    are not the new state's to answer, and what is discarded is published as a
    dead letter rather than dropped.
    """

    __slots__ = ("_capacity", "_held")

    def __init__(self, capacity: int) -> None:
        """Create an empty buffer.

        Args:
            capacity: How many messages it can hold.

        Raises:
            ValueError: If the capacity is not at least one.
        """
        if capacity < 1:
            msg = f"stash capacity must be at least 1, got {capacity}"
            raise ValueError(msg)
        self._capacity = capacity
        self._held: list[T] = []

    @property
    def capacity(self) -> int:
        """How many messages this buffer can hold."""
        return self._capacity

    @property
    def size(self) -> int:
        """How many it is holding now."""
        return len(self._held)

    @property
    def is_empty(self) -> bool:
        """Whether it is holding nothing."""
        return not self._held

    @property
    def is_full(self) -> bool:
        """Whether one more message would not fit."""
        return len(self._held) >= self._capacity

    def stash(self, message: T) -> None:
        """Put a message aside to be replayed later.

        Args:
            message: The message to hold. It is held as it is, so what is
                replayed is the object the sender passed.

        Raises:
            StashOverflowError: If the buffer is full. Raised in the actor that
                stashed, since only it knows whether to shed the message,
                reject it, or let the failure become a supervision decision.
        """
        if self.is_full:
            msg = (
                f"stash is full at capacity {self._capacity}; the actor is "
                "holding more than it declared it could"
            )
            raise StashOverflowError(msg)
        self._held.append(message)

    def unstash_all(self, behavior: Behavior[T]) -> Behavior[T]:
        """Replay everything held, then continue as `behavior`.

        The held messages go back to the front of the mailbox in the order they
        arrived, ahead of anything that has queued up since, and the buffer is
        left empty.

        Args:
            behavior: What the actor becomes for the replay and afterwards.
                Usually the state that is now ready, which is the whole reason
                the messages were held.

        Returns:
            A behavior to return from a handler.
        """
        return UnstashBehavior(self, behavior)

    def take_all(self) -> tuple[T, ...]:
        """Empty the buffer and return what it held, oldest first.

        For the runtime, which does the replaying. Application code returns
        `unstash_all(...)` and lets the cell call this.
        """
        held = tuple(self._held)
        self._held.clear()
        return held

    def __repr__(self) -> str:
        """Render how full it is, which is what a reader wants to see."""
        return f"StashBuffer({len(self._held)}/{self._capacity})"


class UnstashBehavior(Behavior[T]):
    """What `unstash_all` returns: a behavior with a replay attached.

    The cell unwraps it while evaluating, exactly as it unwraps `setup` and
    `supervise`, so nothing that handles messages ever sees one.
    """

    def __init__(self, buffer: StashBuffer[T], behavior: Behavior[T]) -> None:
        """Bind the buffer to replay and the behavior to continue as."""
        self.buffer = buffer
        self.behavior = behavior
        # Whatever the wrapped behavior declares, including None when it is a
        # `setup` that has not run yet.
        self.msg_type = behavior.msg_type

    def __repr__(self) -> str:
        """Render as the call that produces it."""
        return f"{self.buffer!r}.unstash_all({self.behavior!r})"
