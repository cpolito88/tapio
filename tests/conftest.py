"""Shared fixtures and fakes.

`FakeContext` exists for the tests that exercise behaviors without a runtime.
Anything that needs a live tree uses the `system` fixture instead, which
terminates whatever the test left running.
"""

from collections.abc import AsyncIterator

import pytest

from tapio import Message
from tapio.actor import (
    ActorContext,
    ActorPath,
    ActorRef,
    ActorSystem,
    Behavior,
    MailboxConfig,
)
from tapio.logging import ActorLogAdapter, actor_logger
from tapio.settings import TapioSettings
from tests.messages import Greeted


class FakeContext(ActorContext[Message]):
    """A context with no cell behind it, for behaviors under test."""

    def __init__(self, path: ActorPath) -> None:
        """Bind the fake to a path."""
        self._path = path

    @property
    def path(self) -> ActorPath:
        return self._path

    @property
    def self_ref(self) -> ActorRef[Message]:
        return ActorRef(self._path)

    @property
    def log(self) -> ActorLogAdapter:
        return actor_logger(self._path)

    def spawn(
        self,
        behavior: Behavior[Message],
        name: str,
        mailbox: MailboxConfig | None = None,
    ) -> ActorRef[Message]:
        raise NotImplementedError

    def spawn_anonymous(
        self, behavior: Behavior[Message], mailbox: MailboxConfig | None = None
    ) -> ActorRef[Message]:
        raise NotImplementedError


@pytest.fixture
def path() -> ActorPath:
    return ActorPath.root("sys").child("user").child("greeter", uid=42)


@pytest.fixture
def ref(path: ActorPath) -> ActorRef[Greeted]:
    return ActorRef(path)


@pytest.fixture
def ctx(path: ActorPath) -> FakeContext:
    return FakeContext(path)


@pytest.fixture
def settings() -> TapioSettings:
    """Settings that do not depend on the developer's environment."""
    return TapioSettings(_env_file=None)


@pytest.fixture
async def system(settings: TapioSettings) -> AsyncIterator[ActorSystem]:
    """A running system, terminated however the test ends."""
    running = ActorSystem("test", settings)
    try:
        yield running
    finally:
        await running.terminate()
