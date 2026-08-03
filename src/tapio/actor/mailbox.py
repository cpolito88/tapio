"""The two-lane mailbox: signals outrank user messages.

One mailbox object with two deques and a *single* waiter, not two queues. The
obvious alternative, two `asyncio.Queue`s selected with
`wait(FIRST_COMPLETED)`, is the trap this design exists to avoid: the losing
`get()` task has to survive across iterations, because cancelling it discards a
message it has already dequeued. With one waiter that race cannot be expressed.

The user lane is unbounded here. Bounded capacity and the overflow strategies
land with dead letters, since every strategy except failing outright needs
somewhere to drop into.
"""

import asyncio
from collections import deque
from typing import TypeAlias

from tapio.actor.signals import Signal
from tapio.message import Message

__all__ = ["Envelope", "Mailbox"]

Envelope: TypeAlias = Message | Signal
"""What a mailbox holds: a user message, or a runtime signal.

A union rather than a wrapper object. An envelope class would buy a place to
hang a sender, which nothing reads yet, at the price of one allocation per
message on the hottest path in the library.
"""


class Mailbox:
    """A queue with a system lane and a user lane, drained in that order."""

    __slots__ = ("_consuming", "_nonempty", "_system", "_user")

    def __init__(self) -> None:
        """Create an empty mailbox."""
        self._user: deque[Message] = deque()
        self._system: deque[Signal] = deque()
        self._nonempty = asyncio.Event()
        self._consuming = False

    def put(self, message: Message) -> None:
        """Append to the user lane. Never blocks: this lane is unbounded."""
        self._user.append(message)
        self._nonempty.set()

    def put_system(self, signal: Signal) -> None:
        """Append to the system lane.

        The system lane is unbounded whatever the user lane's capacity is: a
        limit that could refuse a stop signal would make shutdown unreliable.
        """
        self._system.append(signal)
        self._nonempty.set()

    async def get(self) -> Envelope:
        """Take the next envelope, waiting if the mailbox is empty.

        Returns:
            The next signal if any is queued, otherwise the next user message.

        Raises:
            RuntimeError: If a second reader is already waiting. One waiter is
                what makes the wakeup above safe, and a mailbox has exactly one
                consumer, its own cell, so that is asserted rather than assumed.
        """
        if self._consuming:
            msg = (
                "a mailbox has one consumer at a time, its own actor cell; "
                f"a second reader tried to take from {self!r}"
            )
            raise RuntimeError(msg)
        self._consuming = True
        try:
            while True:
                if self._system:
                    return self._system.popleft()
                if self._user:
                    return self._user.popleft()
                await self._nonempty.wait()
                # Clear *after* waking and *before* re-examining the deques. In
                # this order a concurrent append either happened before the
                # clear, and the checks above see it, or it sets the event
                # again after. Clearing any later would drop that wakeup.
                self._nonempty.clear()
        finally:
            self._consuming = False

    @property
    def user_size(self) -> int:
        """How many user messages are queued."""
        return len(self._user)

    @property
    def system_size(self) -> int:
        """How many signals are queued."""
        return len(self._system)

    def __len__(self) -> int:
        """Total queued envelopes across both lanes."""
        return len(self._user) + len(self._system)

    def __repr__(self) -> str:
        """Render both lane depths, which is what a reader wants to see."""
        return f"Mailbox(system={len(self._system)}, user={len(self._user)})"
