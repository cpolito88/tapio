"""The two systems every remoting test starts with.

Fixtures rather than helpers because each one has to be terminated however the
test ends, and a system left listening outlives the test that forgot it.
"""

from collections.abc import AsyncIterator

import pytest

from tapio.actor import ActorSystem
from tests.remote.peers import remoting


@pytest.fixture
async def alpha() -> AsyncIterator[ActorSystem]:
    """One system, listening on a loopback port the OS picked."""
    running = ActorSystem("alpha", remoting())
    try:
        yield running
    finally:
        await running.terminate()


@pytest.fixture
async def beta() -> AsyncIterator[ActorSystem]:
    """The other system, listening on its own port."""
    running = ActorSystem("beta", remoting())
    try:
        yield running
    finally:
        await running.terminate()
