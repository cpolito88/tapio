"""The leak assertions the lifecycle tests lean on.

They are only worth having if they fail when they should, so both directions
are checked here.
"""

import asyncio
import threading

import pytest

from tapio.testkit import assert_no_leaked_tasks, assert_no_leaked_threads


async def test_a_clean_block_passes():
    with assert_no_leaked_tasks():
        await asyncio.sleep(0)


async def test_an_orphaned_task_is_caught():
    orphan: asyncio.Task[None] | None = None

    async def leak() -> None:
        nonlocal orphan
        with assert_no_leaked_tasks():
            orphan = asyncio.create_task(asyncio.sleep(30), name="orphan")
            await asyncio.sleep(0)

    with pytest.raises(AssertionError, match="still running"):
        await leak()

    assert orphan is not None
    orphan.cancel()


async def test_a_task_that_finishes_inside_the_block_is_not_a_leak():
    with assert_no_leaked_tasks():
        await asyncio.create_task(asyncio.sleep(0))


def test_a_lingering_thread_is_caught():
    stop = threading.Event()
    threads: list[threading.Thread] = []

    def leak() -> None:
        with assert_no_leaked_threads():
            thread = threading.Thread(target=stop.wait, name="lingerer")
            threads.append(thread)
            thread.start()

    with pytest.raises(AssertionError, match="still alive"):
        leak()

    stop.set()
    threads[0].join()


def test_a_joined_thread_is_not_a_leak():
    with assert_no_leaked_threads():
        thread = threading.Thread(target=lambda: None)
        thread.start()
        thread.join()
