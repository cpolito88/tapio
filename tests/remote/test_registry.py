"""The two registries: message types by wire key, and live refs by path.

The message-type registry is what keeps a type name off the wire from becoming
an import, and the ref registry is what makes a stale incarnation resolve to
nothing rather than to whoever occupies that path now. Both are asserted to
leave nothing behind: a registry that outlives what it names is a leak.
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
    """Registered under a key that does not follow the class around."""

    n: int


async def counts(ping: Ping) -> Behavior[Ping]:
    """Stop on a negative count, and otherwise do nothing at all."""
    return Behaviors.stopped() if ping.n < 0 else Behaviors.same()


def test_the_default_key_is_module_and_qualname():
    assert key_for_type(Registered) == f"{__name__}.Registered"


def test_a_type_is_found_by_its_key():
    assert type_for_key(f"{__name__}.Registered") is Registered


def test_an_explicit_key_is_used_verbatim():
    # The reason the explicit form exists: a class can be renamed or moved
    # without breaking a peer still running the previous version.
    assert key_for_type(Renamed) == "tests.explicit-key"
    assert type_for_key("tests.explicit-key") is Renamed


def test_an_unregistered_key_resolves_to_nothing():
    # Emphatically not an import: a dotted name that arrived on a socket is a
    # string, and looking it up is a dict lookup that can only miss.
    assert type_for_key("os.system") is None


def test_a_duplicate_key_raises_at_import_time():
    # Import time rather than decode time, because the alternative is that the
    # class which happened to import last silently wins.
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
    # The incarnation rule, at its smallest: paths are reusable and uids are
    # not, so a ref written down before a stop cannot address the newcomer.
    registry = RefRegistry()
    path = ActorPath.root("sys").child("user").child("worker", uid=1)
    registry.register(ActorRef(path))
    assert registry.lookup(path.with_uid(2)) is None


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
    # Stop and respawn under the same name: the path is reusable and the uid
    # is not, which is the whole of the incarnation rule.
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
    # An adapter is addressable like the actor behind it, so a ref handed to a
    # peer resolves on the way back, and it dies with its owner.
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
    # The promise has to be addressable while the ask is running, so a reply
    # that crossed a link finds the future somebody is awaiting, and it must
    # not still be there afterwards.
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

    # The same invariant as leaked tasks, one layer down: what the runtime
    # registered, the runtime released.
    assert running.refs.paths() == ()
