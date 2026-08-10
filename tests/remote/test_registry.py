"""Tests for the two registries: message types by wire key, live refs by path.

The type registry keeps a name off the wire from becoming an import. The ref
registry makes a stale incarnation resolve to nothing rather than to whoever
holds that path now. Neither may leave anything behind.
"""

from datetime import timedelta

import pytest

from tapio import Message
from tapio.actor import (
    ActorContext,
    ActorPath,
    ActorRef,
    ActorSystem,
    Behavior,
    Behaviors,
)
from tapio.errors import AskTimeoutError, MessageRegistrationError
from tapio.remote.registry import (
    RefRegistry,
    key_for_type,
    register_message,
    registered_key,
    type_for_key,
)
from tapio.testkit import assert_no_leaked_tasks
from tests.failures import eventually
from tests.messages import Greeted, Ping


@register_message()
class Registered(Message):
    """Registered under the default key, `module.qualname`."""

    n: int


@register_message("tests.explicit-key")
class Renamed(Message):
    """Registered under an explicit key, so renaming the class is safe."""

    n: int


async def counts(ping: Ping) -> Behavior[Ping]:
    """Stops on a negative count, and otherwise does nothing."""
    return Behaviors.stopped() if ping.n < 0 else Behaviors.same()


def test_the_default_key_is_module_and_qualname():
    assert key_for_type(Registered) == f"{__name__}.Registered"


def test_a_type_is_found_by_its_key():
    assert type_for_key(f"{__name__}.Registered") is Registered


def test_an_explicit_key_is_used_verbatim():
    # With an explicit key, the class can be renamed or moved without
    # breaking a peer running the previous version.
    assert key_for_type(Renamed) == "tests.explicit-key"
    assert type_for_key("tests.explicit-key") is Renamed


def test_an_unregistered_key_resolves_to_nothing():
    # Not an import. A dotted name off a socket is just a string, and looking
    # it up is a dict lookup that can only miss.
    assert type_for_key("os.system") is None


def test_a_duplicate_key_raises_at_import_time():
    # It fails at import time, not decode time. Otherwise whichever class
    # imported last would silently win.
    with pytest.raises(MessageRegistrationError, match="already has that key"):

        @register_message("tests.explicit-key")
        class Clashing(Message):
            n: int


def test_registering_the_same_class_twice_under_one_key_is_harmless():
    # Re-importing a module must not be a failure.
    assert register_message("tests.explicit-key")(Renamed) is Renamed


def test_only_messages_can_be_registered():
    with pytest.raises(MessageRegistrationError, match=r"only tapio\.Message"):

        @register_message("tests.not-a-message")
        class Plain:
            pass


def test_encoding_an_unregistered_type_says_how_to_register_it():
    class Unregistered(Message):
        n: int

    with pytest.raises(MessageRegistrationError, match="@register_message"):
        registered_key(Unregistered)


def test_a_ref_is_found_by_path_and_uid():
    registry = RefRegistry()
    path = ActorPath.root("sys").child("user").child("worker", uid=1)
    ref: ActorRef[Greeted] = ActorRef(path)
    registry.register(ref)
    assert registry.lookup(path) is ref


def test_a_different_incarnation_of_the_same_path_is_not_found():
    # Paths are reusable and uids are not, so a ref written down before a stop
    # cannot address whoever comes next.
    registry = RefRegistry()
    path = ActorPath.root("sys").child("user").child("worker", uid=1)
    registry.register(ActorRef(path))
    assert registry.lookup(path.with_uid(2)) is None


def test_a_bare_path_addresses_nothing_by_default():
    # The incarnation rule, unchanged: a ref written down carries its uid, and
    # a path on its own reaches nobody.
    registry = RefRegistry()
    path = ActorPath.root("sys").child("user").child("worker", uid=1)
    registry.register(ActorRef(path))

    assert registry.lookup(path.with_uid(0)) is None


def test_an_actor_that_asks_for_a_well_known_name_is_reachable_without_a_uid():
    # For the one case that needs it: a peer named by an address in a
    # configuration file, which cannot know any uid over there.
    registry = RefRegistry()
    path = ActorPath.root("sys").child("system").child("cluster", uid=1)
    ref: ActorRef[Greeted] = ActorRef(path)
    registry.register(ref)
    registry.register_well_known(ref)

    assert registry.lookup(path.with_uid(0)) is ref
    assert registry.lookup(path) is ref


def test_a_stale_uid_still_reaches_nobody_even_with_a_well_known_name():
    registry = RefRegistry()
    path = ActorPath.root("sys").child("system").child("cluster", uid=1)
    ref: ActorRef[Greeted] = ActorRef(path)
    registry.register(ref)
    registry.register_well_known(ref)

    assert registry.lookup(path.with_uid(2)) is None


def test_a_well_known_name_goes_when_its_actor_does():
    registry = RefRegistry()
    path = ActorPath.root("sys").child("system").child("cluster", uid=1)
    ref: ActorRef[Greeted] = ActorRef(path)
    registry.register(ref)
    registry.register_well_known(ref)

    registry.deregister(path)

    assert registry.lookup(path.with_uid(0)) is None


def test_a_new_incarnation_keeps_the_name_when_the_old_one_deregisters_late():
    # The alias belongs to whoever holds it now. A stop that lands after the
    # next incarnation has published itself must not take the name away.
    registry = RefRegistry()
    path = ActorPath.root("sys").child("system").child("cluster", uid=1)
    first: ActorRef[Greeted] = ActorRef(path)
    second: ActorRef[Greeted] = ActorRef(path.with_uid(2))
    registry.register(first)
    registry.register_well_known(first)
    registry.register(second)
    registry.register_well_known(second)

    registry.deregister(path)

    assert registry.lookup(path.with_uid(0)) is second


def test_deregistering_a_path_that_was_never_there_is_harmless():
    registry = RefRegistry()
    registry.deregister(ActorPath.root("sys").child("user"))
    assert len(registry) == 0


async def test_an_actor_is_registered_while_it_runs(system: ActorSystem):
    worker = system.spawn(Behaviors.receive_message(counts, msg_type=Ping), "worker")
    assert system.refs.lookup(worker.path) is worker


async def test_a_stopped_actor_leaves_nothing_registered(system: ActorSystem):
    worker = system.spawn(Behaviors.receive_message(counts, msg_type=Ping), "worker")
    worker.tell(Ping(n=-1))
    await eventually(lambda: system.refs.lookup(worker.path) is None)


async def test_a_respawned_name_gets_a_uid_of_its_own(system: ActorSystem):
    # Stop and respawn under the same name: same path, different uid.
    behavior = Behaviors.receive_message(counts, msg_type=Ping)
    first = system.spawn(behavior, "worker")
    first.tell(Ping(n=-1))
    await eventually(lambda: system.refs.lookup(first.path) is None)

    second = system.spawn(behavior, "worker")
    assert first.path.uid != second.path.uid
    assert system.refs.lookup(first.path) is None
    assert system.refs.lookup(second.path) is second


async def test_an_adapter_is_registered_and_released_with_its_owner(
    system: ActorSystem,
):
    # An adapter is addressable like any actor, so a ref handed to a peer
    # resolves on the way back. It is released with its owner.
    adapters: list[ActorRef[Greeted]] = []

    def make(ctx: ActorContext[Ping]) -> Behavior[Ping]:
        adapters.append(
            ctx.message_adapter(lambda greeted: Ping(n=1), msg_type=Greeted)
        )
        return Behaviors.receive_message(counts, msg_type=Ping)

    owner = system.spawn(Behaviors.setup(make), "owner")
    adapter = adapters[0]
    assert system.refs.lookup(adapter.path) is adapter

    owner.tell(Ping(n=-1))
    await eventually(lambda: system.refs.lookup(adapter.path) is None)


async def test_an_ask_registers_its_promise_and_takes_it_out_again(
    system: ActorSystem,
):
    # A promise is addressable while the ask runs, so a reply off a link finds
    # the future being awaited. It must not still be there afterwards.
    seen: list[ActorPath] = []

    async def note_the_registry(message: Ping) -> Behavior[Ping]:
        seen.extend(system.refs.paths())
        return Behaviors.same()

    target = system.spawn(
        Behaviors.receive_message(note_the_registry, msg_type=Ping), "target"
    )
    with pytest.raises(AskTimeoutError):
        await target.ask(
            lambda _: Ping(n=1), expect=Greeted, timeout=timedelta(milliseconds=10)
        )

    promises = [path for path in seen if "promises" in str(path)]
    assert promises, "the promise was not registered while the ask was running"
    assert all(system.refs.lookup(path) is None for path in promises)


async def test_the_registry_is_empty_once_the_system_has_terminated(settings):
    with assert_no_leaked_tasks():
        running = ActorSystem("registry", settings)
        running.spawn(Behaviors.receive_message(counts, msg_type=Ping), "worker")
        await running.terminate()

    # What the runtime registered, the runtime releases.
    assert running.refs.paths() == ()
