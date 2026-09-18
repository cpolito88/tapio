"""Cancelling a task and waiting for it, without losing the caller's own stop.

One helper, kept here because it is about the task lifecycle rather than about
actors or links, and because both sides of the runtime need it: the cell
cancelling an actor that ran past the shutdown deadline, and an association
retiring the reader of a link that lost a dial.
"""

import asyncio

__all__ = ["cancel_and_wait"]


async def cancel_and_wait(task: "asyncio.Task[None]") -> None:
    """Cancel a task and wait for it, keeping the caller's own cancellation.

    `contextlib.suppress(CancelledError)` around `await task` cannot tell the
    awaited task's cancellation from the caller's own, so it swallows both.
    Where the caller goes on to do more work after the wait, that difference
    matters: swallowing its own cancellation makes it carry on after being told
    to stop. This waits the cancelled task out, and swallows however it ended,
    but re-raises a cancellation aimed at the caller so it still stops.

    It suits a site that resumes work after the wait, like the reader retiring
    the link that lost a dial, or the cell that logs and returns into the rest
    of a shutdown sweep. A site that only finishes cleanup after the wait wants
    the opposite, to complete regardless, and keeps its own suppression.

    Args:
        task: The task to cancel and wait for.
    """
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        current = asyncio.current_task()
        if current is not None and current.cancelling():
            raise
    except Exception:
        # However the awaited task itself ended is not the caller's to react to
        # here: whatever owns that task has already accounted for it.
        pass
