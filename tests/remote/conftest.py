"""The two systems the remoting tests start.

Fixtures rather than helpers so that each is terminated however the test ends.
A system left listening would outlive the test that forgot it.
"""

import itertools
from collections.abc import AsyncIterator, Iterator

import pytest

from tapio.actor import ActorSystem
from tapio.actor.cell import ActorRuntime
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


@pytest.fixture
def counted_uids(monkeypatch: pytest.MonkeyPatch) -> None:
    """Hand out actor uids from a counter per system, as tapio once did.

    Uids are random, so two nodes of one deployment almost never share a path
    uid included. This makes them share one, for the tests that check two such
    paths on two nodes are still told apart.
    """
    counters: dict[int, Iterator[int]] = {}

    def next_uid(runtime: ActorRuntime) -> int:
        return next(counters.setdefault(id(runtime), itertools.count(1)))

    monkeypatch.setattr(ActorRuntime, "next_uid", next_uid)
