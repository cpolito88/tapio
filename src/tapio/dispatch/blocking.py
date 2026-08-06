"""The pool that blocking calls run on, so they do not run on the loop.

One blocking call stalls every actor in the system. They share a loop, and a
thread that is inside `requests.get` or a database driver is not running any
of them. So a call that blocks goes to a thread, and the actor awaits the
result like anything else.

The pool is per system and bounded. It is deliberately not
`asyncio.to_thread`, which submits to the loop's default executor: that one is
shared with every other library in the process and its size is not tapio's to
choose, so `blocking_pool_size` could not be honoured. It is created on the
first call, so a system that never blocks starts no threads at all.

Threads are the one piece of the runtime that is not a task, which makes them
the one piece the leak invariant does not cover for free. The system shuts the
pool down against the same deadline as the actor tree, and
`assert_no_leaked_threads()` is the companion check.

**A blocking call cannot be cancelled.** Python has no way to interrupt a
thread that is inside a C call or a sleep. Shutdown drops work that has not
started and waits for what has, and past the deadline it says what is still
running and gives up on it. That is the honest limit of running other people's
blocking code, and it is why a call with no timeout of its own is a call that
can outlive the system that made it.
"""

import asyncio
import functools
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any, ParamSpec, TypeVar

from tapio.logging import runtime_logger

__all__ = ["BlockingPool"]

P = ParamSpec("P")
R = TypeVar("R")

_log = runtime_logger("blocking")

_POLL_INTERVAL = 0.01
"""How often shutdown looks to see whether the last thread has finished."""


class BlockingPool:
    """The threads one system runs blocking calls on."""

    __slots__ = ("_closed", "_executor", "_prefix", "_size")

    def __init__(self, *, size: int, system: str) -> None:
        """Describe a pool without starting anything.

        Args:
            size: How many threads it may use.
            system: The system it belongs to, which names its threads.
        """
        self._size = size
        self._prefix = f"tapio-blocking-{system}"
        self._executor: ThreadPoolExecutor | None = None
        self._closed = False

    @property
    def size(self) -> int:
        """How many threads this pool may use."""
        return self._size

    @property
    def is_accepting(self) -> bool:
        """Whether the pool still takes work.

        `False` once shutdown has begun, which is how a caller tells "the
        system is going away" from an error the call itself raised.
        """
        return not self._closed

    @property
    def is_started(self) -> bool:
        """Whether anything has been submitted yet.

        A system that never blocks starts no threads, which is what keeps the
        thread-leak check meaningful for every other test in the suite.
        """
        return self._executor is not None

    @property
    def threads(self) -> tuple[threading.Thread, ...]:
        """The live threads this pool owns, by name.

        Exposed for the same reason a cell exposes its watchers: a test has to
        be able to assert that shutdown left nothing running.
        """
        return tuple(
            thread
            for thread in threading.enumerate()
            if thread.name.startswith(self._prefix) and thread.is_alive()
        )

    def submit(
        self,
        loop: asyncio.AbstractEventLoop,
        fn: Callable[P, R],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> "asyncio.Future[R]":
        """Run a callable on a pool thread and return what to await.

        Args:
            loop: The system's loop, which the result is delivered on.
            fn: The blocking callable.
            *args: Its positional arguments.
            **kwargs: Its keyword arguments.

        Returns:
            A future for the call's result.

        Raises:
            RuntimeError: If the pool has been shut down. The caller turns
                this into an error about the system, since a blocking call
                during shutdown is an ordering bug in the same way a spawn is.
        """
        if self._closed:
            msg = "the blocking pool is shut down"
            raise RuntimeError(msg)
        if self._executor is None:
            self._executor = ThreadPoolExecutor(
                max_workers=self._size, thread_name_prefix=self._prefix
            )
        call = functools.partial(fn, *args, **kwargs)
        return loop.run_in_executor(self._executor, call)

    async def shutdown(self, deadline: float, *, now: Callable[[], float]) -> None:
        """Drop queued work, then wait for what is running until the deadline.

        Calling this twice is safe: the second call finds nothing to do.

        Args:
            deadline: The same clock reading the actor tree is racing.
            now: Reads that clock.
        """
        self._closed = True
        executor = self._executor
        self._executor = None
        if executor is None:
            return
        # Nothing queued gets to start. What has already started cannot be
        # interrupted, so wait=False and then wait here, where the loop is
        # free to run the rest of the shutdown while the threads finish.
        executor.shutdown(wait=False, cancel_futures=True)
        # Polled rather than awaited on an event: a thread finishing is not
        # something the loop is told about, and the executor offers no
        # awaitable for it. This is the one place the runtime watches a clock.
        while self.threads and now() < deadline:  # noqa: ASYNC110 - see below
            await asyncio.sleep(_POLL_INTERVAL)
        running = self.threads
        if running:
            _log.warning(
                "%d blocking call(s) were still running at the shutdown "
                "deadline and cannot be interrupted: %s",
                len(running),
                ", ".join(sorted(thread.name for thread in running)),
            )

    def __repr__(self) -> str:
        """Render the bound and whether any threads exist yet."""
        state = "started" if self._executor is not None else "idle"
        return f"BlockingPool(size={self._size}, {state})"


def describe_blocking(fn: Callable[..., Any]) -> str:
    """Name a callable for a log line or an error message."""
    return getattr(fn, "__qualname__", None) or repr(fn)
