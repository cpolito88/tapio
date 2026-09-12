"""Fixtures and fakes shared by every test package.

`FakeContext` is for tests that exercise behaviors with no runtime. A test
that needs a live tree uses the `system` fixture, which terminates whatever
the test leaves running.
"""

import os
from collections.abc import AsyncIterator, Callable, Iterator
from typing import Any

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
from tapio.testkit import IsolatedTapioSettings
from tapio.validation import MessageType
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

    def message_adapter(
        self,
        adapt: Callable[[Message], Message],
        msg_type: MessageType | None = None,
    ) -> ActorRef[Message]:
        raise NotImplementedError

    async def run_blocking(
        self, fn: Callable[..., Any], /, *args: Any, **kwargs: Any
    ) -> Any:
        raise NotImplementedError

    async def resolve(self, uri: str, *, expect: type[Any]) -> ActorRef[Any]:
        raise NotImplementedError

    def watch(self, ref: ActorRef[Any]) -> None:
        raise NotImplementedError

    def unwatch(self, ref: ActorRef[Any]) -> None:
        raise NotImplementedError


@pytest.fixture(autouse=True, scope="session")
def _no_tapio_environment() -> Iterator[None]:
    """Keep `TAPIO_*` out of the whole suite, not just out of the fixtures.

    The fixtures and the testkit helpers build isolated settings, so they are
    covered on their own. What is not is a test that writes
    `ActorSystem("name")` with no settings: that takes `TapioSettings()`, which
    reads the environment on purpose, because that is how a real system is
    configured. There are dozens of those, and asking each one to remember
    would be a rule nobody can enforce.

    So the suite drops the variables once, here. A developer with
    `TAPIO_DEFAULT_MAILBOX_CAPACITY=2` exported then runs the same tests as
    everyone else, rather than a bounded-mailbox variant of them that fails in
    places that have nothing to do with what they were changing.

    Yields:
        Nothing. The variables are restored when the session ends.
    """
    with pytest.MonkeyPatch.context() as env:
        for name in [name for name in os.environ if name.startswith("TAPIO_")]:
            env.delenv(name)
        yield


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
    """Settings that do not depend on the developer's environment.

    Isolated rather than merely dotenv-free: `_env_file=None` reads as if it
    keeps `TAPIO_*` out and does not, since the environment arrives through a
    source of its own.
    """
    return IsolatedTapioSettings()


@pytest.fixture
async def system(settings: TapioSettings) -> AsyncIterator[ActorSystem]:
    """A running system, terminated however the test ends."""
    running = ActorSystem("test", settings)
    try:
        yield running
    finally:
        await running.terminate()
