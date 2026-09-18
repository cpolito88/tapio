"""Cancelling a task without swallowing the caller's own cancellation."""

import asyncio
import contextlib

import pytest

from tapio.dispatch.tasks import cancel_and_wait


async def test_cancel_and_wait_reraises_the_callers_own_cancellation():
    # The caller is cancelled while waiting, so it must stop rather than run on.
    reached = False

    async def caller() -> None:
        nonlocal reached
        await cancel_and_wait(asyncio.ensure_future(_pending()))
        reached = True

    task: asyncio.Task[None] = asyncio.ensure_future(caller())
    await asyncio.sleep(0)  # park inside `cancel_and_wait` at `await task`
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert reached is False


async def test_cancel_and_wait_swallows_the_awaited_tasks_cancellation():
    # The awaited task's own cancellation is not the caller's, so the caller
    # carries on: this is the ordinary retire path.
    reached = await _returns_after_waiting(asyncio.ensure_future(_pending()))
    assert reached is True


async def test_cancel_and_wait_swallows_the_awaited_tasks_exception():
    async def boom() -> None:
        raise RuntimeError("the reader failed")

    task: asyncio.Task[None] = asyncio.ensure_future(boom())
    with contextlib.suppress(RuntimeError):
        await task  # let it finish and retrieve the exception

    reached = await _returns_after_waiting(task)
    assert reached is True


async def _pending() -> None:
    """A task that never finishes on its own."""
    await asyncio.Event().wait()


async def _returns_after_waiting(task: "asyncio.Task[None]") -> bool:
    """Run `cancel_and_wait` from an uncancelled caller and say it returned."""
    await cancel_and_wait(task)
    return True
