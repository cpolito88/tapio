"""Shared fixtures and fakes.

Nothing here starts an actor system: every test in this suite runs without a
runtime.
"""

import pytest

from tapio import Message
from tapio.actor import ActorContext, ActorPath, ActorRef
from tests.messages import Greeted


class FakeContext(ActorContext[Message]):
    """The smallest thing that satisfies the context contract."""

    def __init__(self, path: ActorPath) -> None:
        """Bind the fake to a path."""
        self._path = path

    @property
    def path(self) -> ActorPath:
        return self._path

    @property
    def self_ref(self) -> ActorRef[Message]:
        return ActorRef(self._path)


@pytest.fixture
def path() -> ActorPath:
    return ActorPath.root("sys").child("user").child("greeter", uid=42)


@pytest.fixture
def ref(path: ActorPath) -> ActorRef[Greeted]:
    return ActorRef(path)


@pytest.fixture
def ctx(path: ActorPath) -> FakeContext:
    return FakeContext(path)
