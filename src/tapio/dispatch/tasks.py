"""Cancelling a task and waiting for it, without losing the caller's own stop.

One helper, kept here because it is about the task lifecycle rather than about
actors or links, and because both sides of the runtime need it: the cell
cancelling an actor that ran past the shutdown deadline, and an association
retiring the reader of a link that lost a dial.
"""

import asyncio

__all__ = ["cancel_and_wait"]


async def cancel_and_wait(
    task: "asyncio.Task[None]",
    *,
    timeout: float | None = None,  # noqa: ASYNC109 - bounds the wait, not the caller
) -> bool:
    """Cancel a task and wait for it, keeping the caller's own cancellation.

    `contextlib.suppress(CancelledError)` around `await task` cannot tell the
    awaited task's cancellation from the caller's own, so it swallows both.
    Where the caller goes on to do more work after the wait, that difference
    matters: swallowing its own cancellation makes it carry on after being told
    to stop. This waits the cancelled task out and ignores however it ended,
    but a cancellation aimed at the caller still reaches the caller.

    The wait is `asyncio.wait`, not `await task`. `asyncio.wait` never raises
    the task's own outcome, so a `CancelledError` here can only be the
    caller's. A caller that caught an earlier cancellation and went on into
    cleanup is therefore not stopped again by the task it retires. Counting
    `cancelling()` cannot tell those two apart, because that count stays
    raised until somebody calls `uncancel()`.

    It suits a site that resumes work after the wait, like the reader retiring
    the link that lost a dial, or the cell that logs and returns into the rest
    of a shutdown sweep. A site that only finishes cleanup after the wait wants
    the opposite, to complete regardless, and keeps its own suppression.

    Args:
        task: The task to cancel and wait for.
        timeout: Seconds to wait for it, or `None` to wait as long as it takes.
            A task that catches its cancellation, or awaits slow cleanup after
            it, can otherwise hold the caller for good. One still running when
            the time is up is abandoned: it has been cancelled and still
            belongs to whoever started it, but nobody waits for it any longer.

    Returns:
        Whether the task finished inside `timeout`.
    """
    # Added before the cancel, so an outcome nobody awaits is still retrieved.
    # Without it, a task abandoned at the timeout that later raises would be
    # logged by asyncio as an exception that was never retrieved.
    task.add_done_callback(_retrieve)
    task.cancel()
    done, _ = await asyncio.wait({task}, timeout=timeout)
    return task in done


def _retrieve(task: "asyncio.Task[None]") -> None:
    """Mark however a task ended as seen, since its owner has accounted for it."""
    if not task.cancelled():
        task.exception()
