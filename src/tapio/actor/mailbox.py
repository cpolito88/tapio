"""The two-lane mailbox: signals outrank user messages.

One mailbox object with two deques and a *single* waiter, not two queues. The
obvious alternative, two `asyncio.Queue`s selected with
`wait(FIRST_COMPLETED)`, is the trap this design exists to avoid: the losing
`get()` task has to survive across iterations, because cancelling it discards a
message it has already dequeued. With one waiter that race cannot be expressed.

The user lane can be bounded. The system lane never is: a capacity that could
refuse a stop signal would make shutdown unreliable, so backpressure applies to
application traffic only.
"""

import asyncio
import contextlib
from collections import deque
from dataclasses import dataclass
from enum import StrEnum
from typing import TypeAlias

from tapio.actor.signals import Signal
from tapio.errors import MailboxFullError
from tapio.message import Message

__all__ = [
    "Envelope",
    "Mailbox",
    "MailboxConfig",
    "OverflowStrategy",
]

Envelope: TypeAlias = Message | Signal
"""What a mailbox holds: a user message, or a runtime signal.

A union rather than a wrapper object. An envelope class would buy a place to
hang a sender, which nothing reads yet, at the price of one allocation per
message on the hottest path in the library.
"""


class OverflowStrategy(StrEnum):
    """What a bounded user lane does when a message arrives and it is full."""

    FAIL = "fail"
    """Raise `MailboxFullError` in the sender.

    Note the direction: the *receiver's* backpressure surfaces inside the
    *sender*, so it is the sender's supervisor that sees it. That is the right
    place for the decision, since only the sender knows whether to retry, shed
    or escalate, and it surprises people, so it is said here and in the docs.

    A send from another thread cannot raise anywhere useful, so it dead-letters
    instead. That asymmetry is the one wart in the design.
    """

    DROP_NEW = "drop-new"
    """Discard the arriving message, which goes to dead letters."""

    DROP_OLDEST = "drop-oldest"
    """Discard the message at the head of the queue, which goes to dead
    letters, and enqueue the arriving one. For a stream of readings where only
    the latest matters, this is the strategy that keeps the useful one."""


@dataclass(frozen=True, slots=True)
class MailboxConfig:
    """How one actor's mailbox behaves.

    The default is unbounded, so the common case needs no configuration, and
    `capacity=None` means the strategy is never consulted.
    """

    capacity: int | None = None
    """User-lane capacity, or `None` for unbounded."""

    on_overflow: OverflowStrategy = OverflowStrategy.FAIL
    """What to do when a bounded lane is full."""

    def __post_init__(self) -> None:
        """Reject a capacity that could never hold a message.

        Raises:
            ValueError: If the capacity is not at least one.
        """
        if self.capacity is not None and self.capacity < 1:
            msg = f"mailbox capacity must be at least 1, got {self.capacity}"
            raise ValueError(msg)


class Mailbox:
    """A queue with a system lane and a user lane, drained in that order."""

    __slots__ = (
        "_closed",
        "_config",
        "_consuming",
        "_nonempty",
        "_space",
        "_system",
        "_user",
    )

    def __init__(self, config: MailboxConfig | None = None) -> None:
        """Create an empty mailbox.

        Args:
            config: Capacity and overflow behaviour. Unbounded when omitted.
        """
        self._config = config if config is not None else MailboxConfig()
        self._user: deque[Message] = deque()
        self._system: deque[Signal] = deque()
        self._nonempty = asyncio.Event()
        self._space: deque[asyncio.Future[None]] = deque()
        self._consuming = False
        self._closed = False

    @property
    def config(self) -> MailboxConfig:
        """How this mailbox handles capacity."""
        return self._config

    @property
    def is_full(self) -> bool:
        """Whether the user lane is at capacity.

        A closed mailbox is never full: senders parked in `offer` have to be
        able to finish, and what they enqueue is discarded by the cell rather
        than left to strand them.
        """
        capacity = self._config.capacity
        if self._closed or capacity is None:
            return False
        return len(self._user) >= capacity

    def put(self, message: Message) -> Message | None:
        """Append to the user lane, applying the overflow strategy if full.

        Args:
            message: The message to enqueue.

        Returns:
            The message that was displaced and should go to dead letters, or
            `None` when nothing was displaced. The caller owns the sink, so the
            mailbox decides *what* is dropped and never *where* it goes.

        Raises:
            MailboxFullError: If the lane is full under `OverflowStrategy.FAIL`.
        """
        if not self.is_full:
            self._append(message)
            return None

        match self._config.on_overflow:
            case OverflowStrategy.FAIL:
                msg = (
                    f"mailbox is full at capacity {self._config.capacity}; "
                    "the recipient is not keeping up"
                )
                raise MailboxFullError(msg)
            case OverflowStrategy.DROP_NEW:
                return message
            case OverflowStrategy.DROP_OLDEST:
                displaced = self._user.popleft()
                self._append(message)
                return displaced

    def put_system(self, signal: Signal) -> None:
        """Append to the system lane.

        The system lane is unbounded whatever the user lane's capacity is: a
        limit that could refuse a stop signal would make shutdown unreliable.
        """
        self._system.append(signal)
        self._nonempty.set()

    async def offer(self, message: Message) -> None:
        """Append to the user lane, waiting for capacity instead of dropping.

        Senders park on individual futures and are woken one per freed slot, in
        arrival order. Deliberately not symmetric with `get`: there is one
        consumer but there can be many senders, so an `Event` would wake all of
        them for one slot and hand it to whoever the scheduler happened to
        favour.

        Args:
            message: The message to enqueue.

        Raises:
            asyncio.CancelledError: If the wait is cancelled, having first
                removed this sender's own future so nothing is left behind.
        """
        loop = asyncio.get_running_loop()
        # Set once this sender has been woken at least once. A `tell` can take
        # the freed slot before the woken sender runs; re-parking at the front
        # keeps the place it had already earned, where going to the back would
        # lose it to senders that arrived later.
        served = False
        while self.is_full:
            waiter = loop.create_future()
            if served:
                self._space.appendleft(waiter)
            else:
                self._space.append(waiter)
            try:
                await waiter
            except asyncio.CancelledError:
                with contextlib.suppress(ValueError):
                    self._space.remove(waiter)
                raise
            served = True
        self._append(message)

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
                    message = self._user.popleft()
                    self._release_slot()
                    return message
                await self._nonempty.wait()
                # Clear *after* waking and *before* re-examining the deques. In
                # this order a concurrent append either happened before the
                # clear, and the checks above see it, or it sets the event
                # again after. Clearing any later would drop that wakeup.
                self._nonempty.clear()
        finally:
            self._consuming = False

    def take_pending(self) -> Message | None:
        """Pop one queued user message, or `None` when the lane is empty.

        For the cell's termination sequence, which accounts for undelivered
        messages rather than discarding them with the mailbox.
        """
        return self._user.popleft() if self._user else None

    def _append(self, message: Message) -> None:
        """Put a message on the user lane and wake the consumer."""
        self._user.append(message)
        self._nonempty.set()

    def _release_slot(self) -> None:
        """Hand the slot just freed to the sender that has waited longest."""
        while self._space:
            waiter = self._space.popleft()
            if not waiter.done():
                waiter.set_result(None)
                return

    def close(self) -> None:
        """Wake every blocked sender, so a stopped actor strands nobody.

        Their `offer` resumes, enqueues into a mailbox nobody is reading, and
        the cell dead-letters what is left. Called from the termination
        sequence, so no sender is left awaiting a slot that will never come.
        """
        self._closed = True
        while self._space:
            waiter = self._space.popleft()
            if not waiter.done():
                waiter.set_result(None)

    @property
    def user_size(self) -> int:
        """How many user messages are queued."""
        return len(self._user)

    @property
    def system_size(self) -> int:
        """How many signals are queued."""
        return len(self._system)

    @property
    def waiting_senders(self) -> int:
        """How many senders are parked in `offer` waiting for capacity."""
        return len(self._space)

    def __len__(self) -> int:
        """Total queued envelopes across both lanes."""
        return len(self._user) + len(self._system)

    def __repr__(self) -> str:
        """Render both lane depths, which is what a reader wants to see."""
        return f"Mailbox(system={len(self._system)}, user={len(self._user)})"
