"""The dispatcher: the one place that knows which loop a system runs on.

Every actor in a system runs on a single event loop, and several parts of the
runtime need to agree on which one: cells create their receive-loop task on it,
shutdown reads its clock for the deadline every cell races, and sending from
another thread has to hop back onto it. Keeping that in one object means those
answers cannot drift apart.
"""

import asyncio
from collections.abc import Coroutine
from typing import Any

__all__ = ["Dispatcher"]


class Dispatcher:
    """Owns the event loop an actor system's tasks run on."""

    __slots__ = ("_loop",)

    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        """Bind the dispatcher to a running loop."""
        self._loop = loop

    @classmethod
    def from_running_loop(cls) -> "Dispatcher":
        """Bind to the loop of the caller.

        Returns:
            A dispatcher for the running loop.

        Raises:
            RuntimeError: If there is no running loop, since an actor system
                is built out of tasks and has nowhere to put them.
        """
        return cls(asyncio.get_running_loop())

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        """The loop every actor in this system runs on."""
        return self._loop

    def spawn_task(
        self, coro: Coroutine[Any, Any, None], *, name: str
    ) -> asyncio.Task[None]:
        """Start a named task on this system's loop."""
        return self._loop.create_task(coro, name=name)

    def now(self) -> float:
        """The loop's monotonic clock, which deadlines are measured against."""
        return self._loop.time()

    def is_current(self) -> bool:
        """Whether the caller is running on this system's loop."""
        try:
            return asyncio.get_running_loop() is self._loop
        except RuntimeError:
            return False

    def __repr__(self) -> str:
        """Render the loop this dispatcher owns."""
        return f"Dispatcher({self._loop!r})"
