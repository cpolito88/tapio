"""The fixtures the plugin ships, used the way somebody else's project would.

Nothing here imports them. They arrive through the `pytest11` entry point, so
this file is also the test that the entry point is wired up.
"""

import pytest

from tapio import Behavior, Behaviors, Message
from tapio.actor import ActorRef, ActorSystem
from tapio.settings import TapioSettings


class Greeted(Message):
    """What the greeter answers."""

    whom: str


class Greet(Message):
    """A request, carrying where the answer goes."""

    whom: str
    reply_to: ActorRef[Greeted]


def greeter() -> Behavior[Greet]:
    """An actor that answers on the ref it was given."""

    async def on_greet(message: Greet) -> Behavior[Greet]:
        message.reply_to.tell(Greeted(whom=message.whom))
        return Behaviors.same()

    return Behaviors.receive_message(on_greet, msg_type=Greet)


async def test_the_fixtures_arrive_with_no_conftest(actor_system, make_probe):
    probe = make_probe(Greeted)
    hello = actor_system.spawn(greeter(), "greeter")

    hello.tell(Greet(whom="world", reply_to=probe.ref))

    await probe.expect_message(Greeted(whom="world"))


async def test_the_system_fixture_is_running_and_named(actor_system: ActorSystem):
    assert actor_system.name == "test"
    assert not actor_system.is_terminating


def test_the_settings_fixture_ignores_the_environment(
    tapio_settings: TapioSettings, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv("TAPIO_VALIDATE_ON_TELL", "false")

    # Read before the patch, which is the point: a developer's environment
    # cannot change what a test asserts.
    assert tapio_settings.validate_on_tell


async def test_a_named_probe_takes_the_name(actor_system, make_probe):
    probe = make_probe(Greeted, name="replies")

    assert probe.path.name == "replies"


def test_the_plugin_is_registered(pytestconfig: pytest.Config):
    assert pytestconfig.pluginmanager.hasplugin("tapio")
