"""The two systems the remoting tests start.

Fixtures rather than helpers so that each is terminated however the test ends.
A system left listening would outlive the test that forgot it.
"""

from collections.abc import AsyncIterator

import pytest

from tapio.actor import ActorSystem
from tests.remote.peers import remoting


@pytest.fixture
async def alpha() -> AsyncIterator[ActorSystem]:
    """One system, listening on a loopback port the OS picks."""
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
