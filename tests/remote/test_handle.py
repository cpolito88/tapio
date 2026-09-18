"""One socket, its reader, and the single close that ends both."""

import asyncio

import pytest

from tapio.remote.handle import LinkHandle
from tapio.testkit import assert_no_leaked_tasks
from tests.remote.peers import RecordingLink


def _handle(link: RecordingLink) -> LinkHandle:
    """A handle on the running loop, which is where every close happens."""
    return LinkHandle(link, loop=asyncio.get_running_loop())


async def _forever() -> None:
    """A reader that ends only when it is cancelled."""
    await asyncio.Event().wait()


async def test_closing_ends_the_reader_before_the_socket():
    # A read in flight has to end first, or the reader wakes to a closed
    # transport and raises where nobody is waiting for it.
    with assert_no_leaked_tasks():
        link = RecordingLink()
        handle = _handle(link)
        order: list[str] = []

        async def reader() -> None:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                order.append("reader")
                raise

        task: asyncio.Task[None] = asyncio.ensure_future(reader())
        handle.reads_with(task)
        await asyncio.sleep(0)

        await handle.close()
        order.append("socket" if link.closed else "still open")

        assert order == ["reader", "socket"]


async def test_a_handle_with_no_reader_just_closes_its_socket():
    # A link accepted and not yet handshaken has nothing reading it.
    with assert_no_leaked_tasks():
        link = RecordingLink()
        handle = _handle(link)

        await handle.close()

        assert link.closed
        assert handle.is_closed


async def test_closing_twice_costs_nothing():
    # Ownership can be owed by two paths at once, which is the situation the
    # four fields it replaces existed to sort out. Owing it twice has to be
    # cheap rather than wrong.
    with assert_no_leaked_tasks():
        link = RecordingLink()
        handle = _handle(link)

        await handle.close()
        await handle.close()

        assert link.closed


async def test_a_second_caller_waits_for_the_close_the_first_one_is_doing():
    # What the endpoint's drain does: it holds a handle another task is
    # already closing, and has to wait that close out rather than return with
    # the socket still open.
    with assert_no_leaked_tasks():
        link = RecordingLink()
        handle = _handle(link)
        release = asyncio.Event()

        async def slow() -> None:
            await release.wait()

        task: asyncio.Task[None] = asyncio.ensure_future(slow())
        handle.reads_with(task)
        await asyncio.sleep(0)

        first: asyncio.Task[None] = asyncio.ensure_future(handle.close())
        await asyncio.sleep(0)
        second: asyncio.Task[None] = asyncio.ensure_future(handle.close())
        await asyncio.sleep(0)

        assert not second.done()
        release.set()
        await asyncio.wait_for(asyncio.gather(first, second), 1.0)

        assert link.closed
        assert handle.is_closed


async def test_the_socket_is_released_even_when_the_caller_is_cancelled():
    # A shutdown cancels whoever is closing. The caller still stops, because
    # one that resumes work afterwards must, but the socket is not left for
    # the garbage collector on the way out.
    with assert_no_leaked_tasks():
        link = RecordingLink()
        handle = _handle(link)
        task: asyncio.Task[None] = asyncio.ensure_future(_forever())
        handle.reads_with(task)
        await asyncio.sleep(0)

        closing: asyncio.Task[None] = asyncio.ensure_future(handle.close())
        await asyncio.sleep(0)
        closing.cancel()

        with pytest.raises(asyncio.CancelledError):
            await closing

        assert link.closed
        assert handle.is_closed


async def test_a_reader_that_raised_does_not_stop_the_close():
    # However the reader ended is not the close's business: whatever owns that
    # task has already accounted for it. Letting it through left the socket
    # open and the association stuck in the endpoint's table.
    with assert_no_leaked_tasks():
        link = RecordingLink()
        handle = _handle(link)

        async def angry() -> None:
            msg = "the reader blew up"
            raise RuntimeError(msg)

        task: asyncio.Task[None] = asyncio.ensure_future(angry())
        handle.reads_with(task)
        await asyncio.sleep(0)

        await handle.close()

        assert link.closed


async def test_a_handle_says_nothing_is_released_until_it_is():
    # Reading the state must not be what releases the socket, or a caller that
    # only wanted to know would be deciding.
    with assert_no_leaked_tasks():
        link = RecordingLink()
        handle = _handle(link)

        assert not handle.is_closed
        assert not link.closed

        await handle.close()

        assert handle.is_closed
