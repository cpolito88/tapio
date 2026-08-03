"""Assertions that a block of work left nothing running behind it.

Dropping structured concurrency in the runtime bought supervision semantics
that a task group cannot express, and the price is that "no orphaned tasks" is
an invariant the library holds rather than one the language enforces. An
invariant nobody checks is a wish, so these helpers make it a test, and every
lifecycle test wraps itself in one.
"""

import asyncio
import contextlib
import threading
from collections.abc import Iterator

__all__ = ["assert_no_leaked_tasks", "assert_no_leaked_threads"]


@contextlib.contextmanager
def assert_no_leaked_tasks() -> Iterator[None]:
    """Fail if the block leaves an unfinished task behind.

    Tasks already running when the block opens are ignored, so this nests
    inside a test runner that has tasks of its own.

    Yields:
        Nothing. The check runs when the block exits.

    Raises:
        AssertionError: If a task created inside the block is still pending.
    """
    before = asyncio.all_tasks()
    try:
        yield
    finally:
        current = asyncio.current_task()
        leaked = [
            task
            for task in asyncio.all_tasks()
            if task not in before and task is not current and not task.done()
        ]
        if leaked:
            names = ", ".join(sorted(task.get_name() for task in leaked))
            msg = f"{len(leaked)} task(s) still running: {names}"
            raise AssertionError(msg)


@contextlib.contextmanager
def assert_no_leaked_threads() -> Iterator[None]:
    """Fail if the block leaves a thread behind.

    The companion to the task check, for the blocking-call pool: a system that
    has terminated must leave no live threads either, and a pool is the one
    piece of the runtime that is not a task.

    Yields:
        Nothing. The check runs when the block exits.

    Raises:
        AssertionError: If a thread started inside the block is still alive.
    """
    before = set(threading.enumerate())
    try:
        yield
    finally:
        leaked = [t for t in threading.enumerate() if t not in before and t.is_alive()]
        if leaked:
            names = ", ".join(sorted(t.name for t in leaked))
            msg = f"{len(leaked)} thread(s) still alive: {names}"
            raise AssertionError(msg)
