"""The suite's own fixtures keep the invariants they promise."""

import asyncio

import pytest

from tapio.testkit import IsolatedTapioSettings
from tests.conftest import running_system


async def test_a_task_left_running_fails_the_system_fixture():
    orphan: asyncio.Task[None] | None = None
    with pytest.raises(AssertionError, match="still running: orphan"):
        async with running_system(IsolatedTapioSettings()):
            orphan = asyncio.create_task(asyncio.sleep(30), name="orphan")
    assert orphan is not None
    orphan.cancel()
    with pytest.raises(asyncio.CancelledError):
        await orphan


async def test_a_test_that_leaves_nothing_behind_passes_it():
    async with running_system(IsolatedTapioSettings()) as system:
        assert system.name == "test"
