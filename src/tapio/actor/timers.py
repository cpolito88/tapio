"""Timers: an actor sending itself a message later, or repeatedly.

The message goes onto the actor's own user lane, so a timer is not a callback
running beside the receive loop. It is ordinary traffic that happens to have
been scheduled, and everything an actor relies on still holds: one message is
handled at a time, a tick queues behind whatever is in front of it, and a tick
that fires while the actor is busy waits its turn instead of re-entering it.

Each timer is one task, owned by the cell that scheduled it. A cell cancels
its timers when it stops and when it restarts, so a tick from an incarnation
that no longer exists can never reach the one that replaced it.

Keys are how a timer is referred to afterwards. Starting a timer under a key
that is already running replaces it, which makes "restart the idle timeout" a
single call rather than a cancel and a start.
"""

import asyncio
from collections.abc import Coroutine
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from tapio.message import Message

if TYPE_CHECKING:
    from tapio.actor.cell import ActorCell

__all__ = ["TimerScheduler"]

T = TypeVar("T", bound=Message)


class TimerScheduler(Generic[T]):
    """The handle `Behaviors.with_timers` gives a behavior.

    One scheduler serves every incarnation of its actor. A restart cancels the
    timers it holds rather than replacing the scheduler, so the behavior built
    by the factory schedules against the same object and nothing from the
    previous incarnation survives.
    """

    __slots__ = ("_cell", "_tasks")

    def __init__(self, cell: "ActorCell[T]") -> None:
        """Bind the scheduler to the cell whose timers it owns."""
        self._cell = cell
        self._tasks: dict[str, asyncio.Task[None]] = {}

    @property
    def keys(self) -> tuple[str, ...]:
        """The keys of every timer currently running."""
        return tuple(self._tasks)

    def is_active(self, key: str) -> bool:
        """Whether a timer is running under this key.

        Args:
            key: The timer to ask about.

        Returns:
            Whether it is running. A single timer that has already fired is
            not, and neither is one that was cancelled.
        """
        return key in self._tasks

    def start_single(self, key: str, message: T, delay: timedelta) -> None:
        """Send one message to this actor after a delay.

        Args:
            key: What to call this timer, for cancelling or replacing it.
            message: What to send. Checked against this actor's declared type
                now rather than when it fires, so a mistake surfaces in the
                handler that scheduled it.
            delay: How long to wait.

        Raises:
            MessageTypeError: If the message does not match this actor's
                declared message type.
            ValueError: If the delay is negative.
            pydantic.ValidationError: If content validation is on and the
                message does not satisfy its own model.
        """
        seconds = _seconds(delay, "delay")
        self._cell.validate(message)
        self._start(key, self._single(key, message, seconds))

    def start_fixed_delay(
        self,
        key: str,
        message: T,
        interval: timedelta,
        *,
        initial_delay: timedelta | None = None,
    ) -> None:
        """Send a message repeatedly, waiting `interval` between sends.

        The gap is measured from one send to the next, so an actor that falls
        behind does not build up a backlog of ticks. The timer just sends less
        often. Use this one by default. It is the only one that is safe on a
        bounded mailbox under load.

        Args:
            key: What to call this timer.
            message: What to send, checked now as for `start_single`.
            interval: How long to wait between sends. Greater than zero: a
                repeating timer with no gap is a busy loop, not a schedule.
            initial_delay: How long to wait before the first send. The interval
                when omitted. Zero is fine here, and means send at once.

        Raises:
            MessageTypeError: If the message does not match this actor's
                declared message type.
            ValueError: If the interval is not positive, or the initial delay
                is negative.
            pydantic.ValidationError: If content validation is on and the
                message does not satisfy its own model.
        """
        every = _interval_seconds(interval)
        first = every if initial_delay is None else _seconds(initial_delay, "delay")
        self._cell.validate(message)
        self._start(key, self._fixed_delay(message, every, first))

    def start_fixed_rate(
        self,
        key: str,
        message: T,
        interval: timedelta,
        *,
        initial_delay: timedelta | None = None,
    ) -> None:
        """Send a message on a schedule, keeping the long-run average rate.

        Ticks are counted off a fixed schedule rather than from the last send,
        so time lost to a slow handler is made up. After a stall the missed
        ticks are sent one after another. That is the point of it, and also
        the risk: the burst arrives at an actor that has just shown it is not
        keeping up. Prefer `start_fixed_delay` unless something downstream is
        really counting the ticks.

        Args:
            key: What to call this timer.
            message: What to send, checked now as for `start_single`.
            interval: The scheduled gap between sends. Greater than zero: with
                no gap every tick is already due, so the timer would never
                yield and no other actor would run again.
            initial_delay: How long to wait before the first send. The interval
                when omitted. Zero is fine here, and means send at once.

        Raises:
            MessageTypeError: If the message does not match this actor's
                declared message type.
            ValueError: If the interval is not positive, or the initial delay
                is negative.
            pydantic.ValidationError: If content validation is on and the
                message does not satisfy its own model.
        """
        every = _interval_seconds(interval)
        first = every if initial_delay is None else _seconds(initial_delay, "delay")
        self._cell.validate(message)
        self._start(key, self._fixed_rate(message, every, first))

    def cancel(self, key: str) -> None:
        """Stop a timer. Cancelling one that is not running is harmless.

        A tick already on the mailbox is not retracted. By then it is a
        message like any other, and pulling one back out of a queue the actor
        is reading would be a different guarantee.

        Args:
            key: The timer to stop.
        """
        task = self._tasks.pop(key, None)
        if task is not None and not task.done():
            task.cancel()

    def cancel_all(self) -> None:
        """Stop every timer this actor has running.

        Called by the cell on restart and on termination. This is what keeps a
        tick from an old incarnation from reaching the one that replaced it.
        """
        for task in list(self._tasks.values()):
            if not task.done():
                task.cancel()
        self._tasks.clear()

    def _start(self, key: str, work: Coroutine[Any, Any, None]) -> None:
        """Replace whatever ran under this key, and start the new timer.

        The message is validated by the caller before the coroutine above is
        built, so a rejected message never leaves one un-awaited behind.
        """
        self.cancel(key)
        task = self._cell.runtime.dispatcher.spawn_task(
            work, name=f"tapio-timer:{self._cell.path}:{key}"
        )
        self._tasks[key] = task

    async def _single(self, key: str, message: T, delay: float) -> None:
        """Wait once, send once, and leave no entry behind."""
        try:
            await asyncio.sleep(delay)
            self._fire(message)
        finally:
            # The timer is over whether it fired or was cancelled, so the key
            # must stop counting as active. Guarded, because a replacement may
            # already hold the key by the time this runs.
            if self._tasks.get(key) is asyncio.current_task():
                del self._tasks[key]

    async def _fixed_delay(self, message: T, interval: float, initial: float) -> None:
        """Send, wait the interval, send again: the gap never compounds."""
        await asyncio.sleep(initial)
        while True:
            self._fire(message)
            await asyncio.sleep(interval)

    async def _fixed_rate(self, message: T, interval: float, initial: float) -> None:
        """Send against a schedule, catching up on ticks a stall cost."""
        clock = self._cell.runtime.dispatcher
        next_at = clock.now() + initial
        while True:
            delay = next_at - clock.now()
            if delay > 0:
                await asyncio.sleep(delay)
            self._fire(message)
            # Advancing the schedule rather than the clock is what makes this
            # a rate. After a stall the next delay is negative, and the missed
            # ticks go out one after another.
            next_at += interval

    def _fire(self, message: T) -> None:
        """Put one tick on the actor's own user lane.

        Validation happened when the timer was scheduled, so only the enqueue
        is left. It uses the off-loop path for the same reason a send from
        another thread does: there is no sender to raise into. A tick that
        meets a full mailbox or a stopped actor becomes a dead letter, not an
        exception in a task nobody is watching.
        """
        self._cell.deliver_offloop(message)

    def __repr__(self) -> str:
        """Render the actor and the timers it currently has running."""
        return f"TimerScheduler({str(self._cell.path)!r}, keys={sorted(self._tasks)})"


def _seconds(duration: timedelta, what: str) -> float:
    """Convert a duration to seconds, refusing one that runs backwards.

    Args:
        duration: The duration to convert.
        what: What it is, named in the error.

    Returns:
        The duration in seconds.

    Raises:
        ValueError: If it is negative.
    """
    seconds = duration.total_seconds()
    if seconds < 0:
        msg = f"a timer {what} cannot be negative, got {duration}"
        raise ValueError(msg)
    return seconds


def _interval_seconds(duration: timedelta) -> float:
    """Convert a repeating timer's interval to seconds, refusing zero.

    Zero is refused rather than approximated. A repeating timer with no gap
    between sends is a busy loop whatever the schedule means, and under
    `start_fixed_rate` it is worse than that: the next tick is always already
    due, so the coroutine never reaches its own `await` and the event loop
    never runs anything else again. An actor that wants to run as fast as it
    can should send itself a message from its handler, where the mailbox
    applies and a stop can still be read.

    Args:
        duration: The interval to convert.

    Returns:
        The interval in seconds.

    Raises:
        ValueError: If it is zero or negative.
    """
    seconds = _seconds(duration, "interval")
    if seconds == 0:
        msg = (
            "a repeating timer's interval must be greater than zero, got "
            f"{duration}. Use start_single for a one-off, or send from the "
            "handler to run without a gap"
        )
        raise ValueError(msg)
    return seconds
