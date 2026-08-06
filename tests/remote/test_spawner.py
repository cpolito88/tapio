"""Starting an actor on another node, and everything a spawner refuses."""

from datetime import timedelta

import pytest

from tapio import (
    Behavior,
    Behaviors,
    Message,
    NoArgs,
    Spawn,
    Spawned,
    SpawnFailed,
    SpawnFailure,
    SpawnReply,
    register_message,
    remote_behavior,
    spawner,
)
from tapio.actor import (
    ActorContext,
    ActorRef,
    ActorSystem,
    Signal,
    SupervisorStrategy,
    Terminated,
)
from tapio.errors import BehaviorRegistrationError
from tapio.remote.spawner import factory_for_key, offered_keys
from tapio.testkit import LinkFaults, assert_no_leaked_tasks, two_nodes
from tests.failures import eventually
from tests.remote.peers import remoting, uri

STARTS: list[str] = []
"""One entry per incarnation of a spawned worker, in the order they started.

Module state, because a factory is called with its arguments and nothing else.
It runs in the same process as the test even when it runs on the other node,
which is what makes a restart over there observable here.
"""


class WorkerArgs(Message):
    """What the spawnable worker is built with."""

    tag: str = "w"
    factor: int = 2


@register_message()
class Result(Message):
    """What the worker answers with."""

    n: int
    incarnation: int


@register_message()
class Work(Message):
    """A number to multiply, and where the answer goes."""

    n: int
    reply_to: ActorRef[Result]


@register_message()
class Crash(Message):
    """Tells the worker to raise, so its supervisor has to decide something."""


class BoomError(RuntimeError):
    """What a crashing worker raises."""


@remote_behavior("test-worker")
def spawnable_worker(args: WorkerArgs) -> Behavior[Work | Crash]:
    """A worker that answers, and fails when told to.

    It is supervised here, in the factory, because that is the only place
    supervision can be declared: it happens entirely on the node that runs the
    actor, and the requester never hears about it.
    """

    def build(ctx: ActorContext[Work | Crash]) -> Behavior[Work | Crash]:
        STARTS.append(args.tag)
        incarnation = STARTS.count(args.tag)

        async def on_message(message: Work | Crash) -> Behavior[Work | Crash]:
            if isinstance(message, Crash):
                raise BoomError("boom")
            message.reply_to.tell(
                Result(n=message.n * args.factor, incarnation=incarnation)
            )
            return Behaviors.same()

        return Behaviors.receive_message(on_message, msg_type=Work | Crash)

    return Behaviors.supervise(Behaviors.setup(build)).on_failure(
        SupervisorStrategy.restart(max_restarts=10, window=timedelta(seconds=10))
    )


@remote_behavior("test-idler")
def spawnable_idler(args: NoArgs) -> Behavior[Crash]:
    """A worker that needs no arguments, and stops the first time it is asked."""

    async def on_message(message: Crash) -> Behavior[Crash]:
        return Behaviors.stopped()

    return Behaviors.receive_message(on_message, msg_type=Crash)


@remote_behavior("test-broken")
def spawnable_broken(args: NoArgs) -> Behavior[Crash]:
    """A factory that raises instead of returning a behavior."""
    raise BoomError("this factory is broken")


@remote_behavior("test-unoffered")
def spawnable_unoffered(args: NoArgs) -> Behavior[Crash]:
    """A registered factory that no spawner in these tests offers."""
    return Behaviors.ignore()


class CountingFaults(LinkFaults):
    """Link faults that also count what the system wrote.

    Nothing is broken. The count is the point: a supervision decision must not
    put a single frame on the wire.
    """

    def __init__(self) -> None:
        """Start with links that behave, and nothing written."""
        super().__init__()
        self.written = 0

    async def allow_write(self) -> bool:
        allowed = await super().allow_write()
        self.written += int(allowed)
        return allowed


def offering(*keys: str) -> Behavior[Spawn]:
    """A spawner offering the named factories."""
    return spawner(offers=keys)


async def ask_to_spawn(
    target: ActorRef[Spawn],
    factory: str,
    *,
    args: Message | dict[str, object] | None = None,
    name: str | None = None,
) -> SpawnReply:
    """Ask a spawner to start something, and return whichever answer came.

    `expect=SpawnReply` is the point of the shared base: a refusal is news the
    requester can act on, so it arrives as a reply rather than failing the ask.
    """
    return await target.ask(
        lambda reply_to: Spawn(
            factory=factory,
            args={} if args is None else args,
            name=name,
            reply_to=reply_to,
        ),
        expect=SpawnReply,
        timeout=timedelta(seconds=2),
    )


def test_a_factory_key_defaults_to_the_function_name():
    @remote_behavior()
    def defaulted_key(args: NoArgs) -> Behavior[Crash]:
        return Behaviors.ignore()

    assert factory_for_key("defaulted_key") is not None
    assert "defaulted_key" in offered_keys()


def test_registering_two_factories_under_one_key_raises():
    @remote_behavior("test-taken")
    def first(args: NoArgs) -> Behavior[Crash]:
        return Behaviors.ignore()

    with pytest.raises(BehaviorRegistrationError, match="already has that key"):

        @remote_behavior("test-taken")
        def second(args: NoArgs) -> Behavior[Crash]:
            return Behaviors.ignore()


def test_a_factory_with_no_annotation_is_refused_where_it_is_written():
    with pytest.raises(BehaviorRegistrationError, match="has no annotation"):

        @remote_behavior("test-unannotated")
        def unannotated(args) -> Behavior[Crash]:  # type: ignore[no-untyped-def]
            return Behaviors.ignore()


def test_a_factory_whose_arguments_are_not_a_message_is_refused():
    with pytest.raises(BehaviorRegistrationError, match=r"not a tapio\.Message"):

        @remote_behavior("test-bad-args")
        def bad_args(args: int) -> Behavior[Crash]:
            return Behaviors.ignore()


def test_a_factory_that_takes_no_arguments_is_refused():
    with pytest.raises(BehaviorRegistrationError, match="exactly one arguments model"):

        @remote_behavior("test-no-params")
        def no_params() -> Behavior[Crash]:
            return Behaviors.ignore()


def test_arguments_can_be_declared_when_the_annotation_cannot_say():
    @remote_behavior("test-explicit-args", args=WorkerArgs)
    def explicit(args: "WorkerArgs") -> Behavior[Crash]:
        return Behaviors.ignore()

    factory = factory_for_key("test-explicit-args")
    assert factory is not None
    assert factory.args_type is WorkerArgs


def test_a_spawner_cannot_offer_a_key_nobody_registered():
    with pytest.raises(BehaviorRegistrationError, match="no @remote_behavior"):
        offering("test-worker", "nothing-registers-this")


async def test_an_unknown_factory_key_is_refused_and_starts_nothing(
    system: ActorSystem,
):
    ref = system.spawn(offering("test-worker"), "spawner")

    reply = await ask_to_spawn(ref, "nobody-has-this")

    assert isinstance(reply, SpawnFailed)
    assert reply.reason == SpawnFailure.UNKNOWN_FACTORY
    assert "same code" in reply.detail
    assert _children_of(system, "spawner") == ()


async def test_a_key_that_is_registered_but_not_offered_is_refused(
    system: ActorSystem,
):
    ref = system.spawn(offering("test-worker"), "spawner")

    reply = await ask_to_spawn(ref, "test-unoffered")

    assert isinstance(reply, SpawnFailed)
    assert reply.reason == SpawnFailure.NOT_ALLOWED
    # The factory exists in this very process. Being registered is not being
    # offered: a spawner starts what it was told to start and nothing else.
    assert factory_for_key("test-unoffered") is not None
    assert _children_of(system, "spawner") == ()


async def test_arguments_that_do_not_validate_are_refused(system: ActorSystem):
    ref = system.spawn(offering("test-worker"), "spawner")

    reply = await ask_to_spawn(ref, "test-worker", args={"factor": "two"})

    assert isinstance(reply, SpawnFailed)
    assert reply.reason == SpawnFailure.INVALID_ARGS
    assert "WorkerArgs" in reply.detail
    assert _children_of(system, "spawner") == ()


async def test_a_name_a_live_child_already_holds_is_refused(system: ActorSystem):
    ref = system.spawn(offering("test-worker"), "spawner")
    first = await ask_to_spawn(ref, "test-worker", name="only-one")
    assert isinstance(first, Spawned)

    second = await ask_to_spawn(ref, "test-worker", name="only-one")

    assert isinstance(second, SpawnFailed)
    assert second.reason == SpawnFailure.NAME_REFUSED


async def test_a_generated_name_cannot_be_asked_for(system: ActorSystem):
    ref = system.spawn(offering("test-worker"), "spawner")

    reply = await ask_to_spawn(ref, "test-worker", name="$7")

    assert isinstance(reply, SpawnFailed)
    assert reply.reason == SpawnFailure.NAME_REFUSED
    assert "reserved" in reply.detail


async def test_a_name_no_actor_could_have_is_refused(system: ActorSystem):
    ref = system.spawn(offering("test-worker"), "spawner")

    reply = await ask_to_spawn(ref, "test-worker", name="not a name")

    assert isinstance(reply, SpawnFailed)
    assert reply.reason == SpawnFailure.NAME_REFUSED


async def test_a_factory_that_raises_is_refused_and_the_spawner_survives(
    system: ActorSystem,
):
    ref = system.spawn(offering("test-broken", "test-worker"), "spawner")

    reply = await ask_to_spawn(ref, "test-broken")

    assert isinstance(reply, SpawnFailed)
    assert reply.reason == SpawnFailure.FACTORY_FAILED
    assert "BoomError" in reply.detail
    # The spawner is the parent of everything it has started, so one bad
    # request must not take the rest of them down with it.
    after = await ask_to_spawn(ref, "test-worker")
    assert isinstance(after, Spawned)


async def test_a_factory_with_no_arguments_takes_the_empty_model(system: ActorSystem):
    ref = system.spawn(offering("test-idler"), "spawner")

    reply = await ask_to_spawn(ref, "test-idler")

    assert isinstance(reply, Spawned)
    assert reply.name.startswith("$")


async def test_a_named_actor_is_reachable_by_the_path_it_was_given(
    system: ActorSystem,
):
    ref = system.spawn(offering("test-worker"), "spawner")

    reply = await ask_to_spawn(ref, "test-worker", name="picked")

    assert isinstance(reply, Spawned)
    assert reply.name == "picked"
    assert reply.ref.path.parent == ref.path.parent.child("spawner")


async def test_an_actor_spawned_on_a_peer_answers_on_the_ref_it_came_back_as():
    with assert_no_leaked_tasks():
        async with two_nodes(alpha="asker", beta="compute") as nodes:
            here, there = nodes.alpha, nodes.beta
            local = there.spawn(offering("test-worker"), "spawner")
            remote = await here.resolve(uri(there, local), expect=Spawn)

            reply = await ask_to_spawn(
                remote, "test-worker", args=WorkerArgs(tag="crossing", factor=3)
            )

            assert isinstance(reply, Spawned)
            assert reply.factory == "test-worker"
            # The ref came off the wire and is an ordinary ref: it is asked
            # like any other, and the answer comes back to a promise on this
            # node.
            answer = await reply.ref.ask(
                lambda reply_to: Work(n=7, reply_to=reply_to), expect=Result
            )
            assert answer.n == 21
            assert answer.incarnation == 1
            # The actor really is a child of the spawner, over there.
            assert reply.ref.path.parent == local.path.parent.child("spawner")


async def test_the_spawner_supervises_its_child_with_nothing_crossing_the_link():
    # Long timings, so no heartbeat can fire while the frames are being
    # counted. What is being asserted is that a restart writes nothing, and a
    # heartbeat would write something for reasons of its own.
    settings = remoting(
        heartbeat_interval=timedelta(seconds=30),
        unreachable_after=timedelta(seconds=60),
    )
    with assert_no_leaked_tasks():
        async with (
            ActorSystem("asker", settings) as here,
            ActorSystem("compute", settings) as there,
        ):
            counted = CountingFaults()
            there.remote.set_link_filter(counted.wrap)
            local = there.spawn(offering("test-worker"), "spawner")
            remote = await here.resolve(uri(there, local), expect=Spawn)
            reply = await ask_to_spawn(
                remote, "test-worker", args=WorkerArgs(tag="supervised")
            )
            assert isinstance(reply, Spawned)
            started = STARTS.count("supervised")
            wrote = counted.written
            # The counter is live: the spawn reply itself crossed the link.
            assert wrote > 0

            for _ in range(3):
                reply.ref.tell(Crash())
            await eventually(lambda: STARTS.count("supervised") == started + 3)

            # Three failures, three restarts, and not one frame back. The
            # decision was taken one process boundary from the actor, at
            # in-process latency, by the parent that knows how to rebuild it.
            assert counted.written == wrote
            # The same ref still reaches it, because a restart keeps the path
            # and the uid and replaces only the incarnation.
            answer = await reply.ref.ask(
                lambda reply_to: Work(n=5, reply_to=reply_to), expect=Result
            )
            assert answer.n == 10
            assert answer.incarnation == started + 3


async def test_watching_a_remotely_spawned_actor_fires_when_it_stops():
    with assert_no_leaked_tasks():
        async with two_nodes(alpha="asker", beta="compute") as nodes:
            here, there = nodes.alpha, nodes.beta
            local = there.spawn(offering("test-idler"), "spawner")
            remote = await here.resolve(uri(there, local), expect=Spawn)
            reply = await ask_to_spawn(remote, "test-idler")
            assert isinstance(reply, Spawned)

            seen: list[str] = []
            here.spawn(_watching(reply.ref, seen), "watcher")
            # Death watch is what replaces the parent-child link the wire does
            # not carry, so it is the whole of what the requester is promised.
            reply.ref.tell(Crash())

            await eventually(lambda: seen == [f"terminated {reply.ref.path}"])


async def test_watching_a_remotely_spawned_actor_fires_when_its_node_stops():
    with assert_no_leaked_tasks():
        async with two_nodes(alpha="asker", beta="compute") as nodes:
            here, there = nodes.alpha, nodes.beta
            local = there.spawn(offering("test-worker"), "spawner")
            remote = await here.resolve(uri(there, local), expect=Spawn)
            reply = await ask_to_spawn(remote, "test-worker")
            assert isinstance(reply, Spawned)
            seen: list[str] = []
            here.spawn(_watching(reply.ref, seen), "watcher")

            await there.terminate()

            await eventually(lambda: seen == [f"terminated {reply.ref.path}"])


async def test_watching_a_remotely_spawned_actor_fires_when_the_peer_is_quarantined():
    with assert_no_leaked_tasks():
        async with two_nodes(alpha="asker", beta="compute") as nodes:
            here, there = nodes.alpha, nodes.beta
            local = there.spawn(offering("test-worker"), "spawner")
            remote = await here.resolve(uri(there, local), expect=Spawn)
            reply = await ask_to_spawn(
                remote, "test-worker", args=WorkerArgs(tag="partitioned")
            )
            assert isinstance(reply, Spawned)
            seen: list[str] = []
            here.spawn(_watching(reply.ref, seen), "watcher")
            started = STARTS.count("partitioned")

            nodes.partition()

            await eventually(
                lambda: seen == [f"terminated {reply.ref.path}"], within=5.0
            )
            # The third case is indistinguishable from the other two here, and
            # it is the one that can be wrong: the worker is running on the
            # other node the whole time, and nothing restarted it.
            assert there.refs.lookup(reply.ref.path) is not None
            assert STARTS.count("partitioned") == started


def _watching(target: ActorRef[Message], seen: list[str]) -> Behavior[Crash]:
    """An actor that watches one ref and writes down what it is told."""

    def build(ctx: ActorContext[Crash]) -> Behavior[Crash]:
        ctx.watch(target)

        async def on_message(message: Crash) -> Behavior[Crash]:
            return Behaviors.same()

        async def on_signal(
            ctx: ActorContext[Crash], signal: Signal
        ) -> Behavior[Crash]:
            if isinstance(signal, Terminated):
                seen.append(f"terminated {signal.ref.path}")
            return Behaviors.same()

        return Behaviors.receive_message(
            on_message, msg_type=Crash, on_signal=on_signal
        )

    return Behaviors.setup(build)


def _children_of(system: ActorSystem, name: str) -> tuple[str, ...]:
    """The names of whatever the registry holds under a top-level actor.

    A spawner that refused a request must have started nothing, and this is
    how a test sees that rather than inferring it from silence.
    """
    prefix = ("user", name)
    return tuple(
        path.name
        for path in system.refs.paths()
        if path.elements[: len(prefix)] == prefix and len(path.elements) > len(prefix)
    )
