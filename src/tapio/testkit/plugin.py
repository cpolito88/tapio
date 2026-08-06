"""The pytest plugin: fixtures for tests that need a running system.

Installed with tapio and registered through the `pytest11` entry point, so
there is nothing to add to `conftest.py` and nothing to import. Ask for a
fixture by name and it is there.

```python
async def test_the_greeter(actor_system, make_probe):
    probe = make_probe(Greeted)
    greeter = actor_system.spawn(greeter_behavior(), "greeter")

    greeter.tell(Greet(whom="world", reply_to=probe.ref))

    await probe.expect_message(Greeted(whom="world"))
```

Every fixture here terminates what it started, and `actor_system` asserts on
the way out that the test left no task and no thread behind. That check is the
reason the fixtures exist at all: a system a test forgot to terminate keeps a
port and a thread pool, and the failure lands in whichever test runs next.

The fixtures are async, so the plugin needs an asyncio test runner.
`pytest-asyncio` in auto mode is what tapio's own suite uses.
"""

from collections.abc import AsyncIterator, Callable
from typing import Any

import pytest

from tapio.actor.system import ActorSystem
from tapio.message import Message
from tapio.settings import TapioSettings
from tapio.testkit.leaks import assert_no_leaked_tasks, assert_no_leaked_threads
from tapio.testkit.probe import TestProbe
from tapio.validation import MessageType

__all__ = ["actor_system", "make_probe", "tapio_settings"]

ProbeFactory = Callable[..., TestProbe[Any]]
"""Makes a probe in the test's system, given what it should accept."""


@pytest.fixture
def tapio_settings() -> TapioSettings:
    """Settings for the test's system, with the environment left out.

    Override this fixture to change them. Reading `TAPIO_` variables is
    deliberately switched off: a developer's environment should not be able to
    change what a test is asserting.

    Returns:
        The settings the `actor_system` fixture uses.
    """
    return TapioSettings(_env_file=None)  # type: ignore[call-arg]


@pytest.fixture
async def actor_system(tapio_settings: TapioSettings) -> AsyncIterator[ActorSystem]:
    """A running system, terminated however the test ends.

    Args:
        tapio_settings: What to build it with. Override `tapio_settings` to
            change them, including switching remoting on.

    Yields:
        The system.

    Raises:
        AssertionError: If the test leaves a task or a thread behind, which
            means something outlived the system it belonged to.
    """
    with assert_no_leaked_threads(), assert_no_leaked_tasks():
        system = ActorSystem("test", tapio_settings)
        try:
            yield system
        finally:
            await system.terminate()


@pytest.fixture
def make_probe(actor_system: ActorSystem) -> ProbeFactory:
    """Make probes in the test's system.

    ```python
    replies = make_probe(Greeted)
    named = make_probe(Greeted, name="replies")
    ```

    A factory rather than a probe, because a probe declares what it accepts
    and only the test knows that.

    Args:
        actor_system: The system the probes are spawned in.

    Returns:
        A callable taking the message type, and optionally a name and a
        mailbox configuration.
    """

    def make(msg_type: MessageType, **kwargs: Any) -> TestProbe[Message]:
        return TestProbe(actor_system, msg_type, **kwargs)

    return make
