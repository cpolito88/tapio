"""Counting an actor's restarts, one budget per supervisor.

A cell can carry several `supervise(...).on_failure(...)` layers, and each
brings its own `max_restarts`, `window` and `backoff`. Those are separate
budgets. A failure governed by one layer must not spend another's allowance,
prune another's window, or move another's backoff along, because the layers
are usually there precisely to treat different failures differently.

So the timestamps and the running count are kept per layer rather than per
actor. The layer is the key, and it is a value: the same
`supervise(...).on_failure(...)` written in a behavior produces an equal key
every time that behavior is evaluated, which is what lets a budget survive the
restart it paid for.
"""

from collections import deque
from collections.abc import Hashable
from typing import final

from tapio.actor.supervision import SupervisorStrategy

__all__ = ["RestartLog"]


@final
class RestartLog:
    """One actor's restarts, counted separately for each supervisor."""

    __slots__ = ("_counts", "_times")

    def __init__(self) -> None:
        """Start with nothing recorded."""
        # Kept apart because they answer different questions. The deque is
        # what is still inside the window, which the limit is checked against.
        # The count is what the backoff exponent grows with, and it tracks the
        # same window: a failure old enough to stop counting against the limit
        # is old enough to stop lengthening the delay.
        self._times: dict[Hashable, deque[float]] = {}
        self._counts: dict[Hashable, int] = {}

    def record(self, key: Hashable, strategy: SupervisorStrategy, now: float) -> bool:
        """Record one restart and say whether it stayed inside the limit.

        Args:
            key: The supervisor this restart belongs to.
            strategy: What that supervisor decided, for its limit and window.
            now: The monotonic time the restart happened at.

        Returns:
            `True` while the layer is within `max_restarts` for its window,
            `False` once this restart takes it over.
        """
        if strategy.max_restarts is None:
            # No limit to count against, so nothing is worth keeping. An actor
            # restarting for a month under an unlimited strategy would
            # otherwise accumulate a month of timestamps. There is no window to
            # age the count against either, so it grows with every restart.
            self._counts[key] = self._counts.get(key, 0) + 1
            return True
        times = self._times.setdefault(key, deque())
        if strategy.window is not None:
            horizon = now - strategy.window.total_seconds()
            while times and times[0] < horizon:
                times.popleft()
        times.append(now)
        # The exponent grows with the restarts still inside the window, so a
        # failure the limit has already forgotten does not keep lengthening the
        # wait. Without this the backoff climbs to its ceiling for an actor that
        # is never anywhere near its restart limit.
        self._counts[key] = len(times)
        return len(times) <= strategy.max_restarts

    def count(self, key: Hashable) -> int:
        """How many restarts one supervisor has made inside its window.

        For a limited strategy this is the restarts still inside the window, so
        it falls back as failures age out; for an unlimited one, which has no
        window, it is the count over the actor's life.

        Args:
            key: The supervisor to report on.

        Returns:
            The count, which is what a backoff delay grows with.
        """
        return self._counts.get(key, 0)
