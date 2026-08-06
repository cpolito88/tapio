"""The code on the testing docs page, as tests that actually run.

Every block on `docs/testing.md` is included from this file by name, so the
page cannot show code that does not work. The `--8<--` markers are what the
docs quote; they are not section dividers.
"""

from datetime import timedelta

from tapio import (
    ActorSystem,
    Behavior,
    Behaviors,
    DeadLetter,
    DeadLetterReason,
    Message,
)
from tapio.actor import ActorContext, ActorRef
from tapio.testkit import (
    BehaviorTestKit,
    TestProbe,
    Watched,
    assert_no_leaked_tasks,
    assert_no_leaked_threads,
    two_nodes,
)


class Greeted(Message):
    """What the greeter answers."""

    whom: str


class Greet(Message):
    """A request, carrying where the answer goes."""

    whom: str
    reply_to: ActorRef[Greeted]


class Count(Message):
    """The counter's answer."""

    value: int


class Increment(Message):
    """Adds one."""


class GetCount(Message):
    """Asks for the total."""

    reply_to: ActorRef[Count]


class Job(Message):
    """Work for a child."""

    item: int


class Retire(Message):
    """Tells an actor to stop through its own behavior."""


def greeter_behavior() -> Behavior[Greet]:
    """The actor under test on this page."""

    async def on_greet(message: Greet) -> Behavior[Greet]:
        message.reply_to.tell(Greeted(whom=message.whom))
        return Behaviors.same()

    return Behaviors.receive_message(on_greet, msg_type=Greet)


def retiring() -> Behavior[Retire]:
    """An actor that stops when asked."""

    async def on_message(message: Retire) -> Behavior[Retire]:
        return Behaviors.stopped()

    return Behaviors.receive_message(on_message, msg_type=Retire)


def sink() -> Behavior[Job]:
    """A child for the spawn effects to point at."""

    async def on_message(message: Job) -> Behavior[Job]:
        return Behaviors.same()

    return Behaviors.receive_message(on_message, msg_type=Job)


def counter() -> Behavior[Increment | GetCount]:
    """A counter whose state is a closure over `setup`."""

    def build(
        ctx: ActorContext[Increment | GetCount],
    ) -> Behavior[Increment | GetCount]:
        total = 0

        async def on_message(
            message: Increment | GetCount,
        ) -> Behavior[Increment | GetCount]:
            nonlocal total
            if isinstance(message, Increment):
                total += 1
                return Behaviors.same()
            message.reply_to.tell(Count(value=total))
            return Behaviors.same()

        return Behaviors.receive_message(on_message, msg_type=Increment | GetCount)

    return Behaviors.setup(build)


def supervisor(dependency: ActorRef[Job]) -> Behavior[Increment]:
    """A behavior that spawns a child and watches something, for the effects."""

    def build(ctx: ActorContext[Increment]) -> Behavior[Increment]:
        worker = ctx.spawn(sink(), "worker")
        ctx.watch(dependency)

        async def on_message(message: Increment) -> Behavior[Increment]:
            worker.tell(Job(item=1))
            return Behaviors.same()

        return Behaviors.receive_message(on_message, msg_type=Increment)

    return Behaviors.setup(build)


# --8<-- [start:probe]
async def test_the_greeter(actor_system):
    probe: TestProbe[Greeted] = TestProbe(actor_system, Greeted)
    greeter = actor_system.spawn(greeter_behavior(), "greeter")

    greeter.tell(Greet(whom="world", reply_to=probe.ref))

    await probe.expect_message(Greeted(whom="world"))
# --8<-- [end:probe]


# --8<-- [start:watch]
async def test_a_worker_that_retires(actor_system, make_probe):
    probe = make_probe(Greeted)
    worker = actor_system.spawn(retiring(), "worker")

    probe.watch(worker)
    worker.tell(Retire())

    await probe.expect_terminated(worker)
# --8<-- [end:watch]


# --8<-- [start:dead_letters]
async def test_a_message_to_a_stopped_actor_is_accounted_for(
    actor_system, make_probe
):
    letters: TestProbe[DeadLetter] = TestProbe(actor_system, DeadLetter)
    actor_system.dead_letters.subscribe(letters.tell)
    watcher = make_probe(Greeted)
    worker = actor_system.spawn(retiring(), "worker")
    watcher.watch(worker)
    worker.tell(Retire())
    await watcher.expect_terminated(worker)

    worker.tell(Retire())

    letter = await letters.expect_message_of(DeadLetter)
    assert letter.reason == DeadLetterReason.RECIPIENT_TERMINATED
# --8<-- [end:dead_letters]


# --8<-- [start:kit]
async def test_counting():
    kit: BehaviorTestKit[Increment | GetCount] = BehaviorTestKit(counter())

    await kit.run(Increment())
    await kit.run(GetCount(reply_to=kit.self_ref))

    assert kit.self_inbox == [Count(value=1)]
# --8<-- [end:kit]


# --8<-- [start:effects]
async def test_what_the_supervisor_started():
    dependency: ActorRef[Job] = ActorRef(BehaviorTestKit(counter()).ctx.path)
    kit: BehaviorTestKit[Increment] = BehaviorTestKit(supervisor(dependency))

    await kit.run(Increment())

    assert kit.effects == ("worker", Watched(dependency, watching=True))
    assert kit.child("worker").inbox == [Job(item=1)]
# --8<-- [end:effects]


# --8<-- [start:leaks]
async def test_shutdown_leaves_nothing_behind():
    with assert_no_leaked_threads(), assert_no_leaked_tasks():
        system = ActorSystem("test")
        system.spawn(greeter_behavior(), "greeter")

        await system.terminate()
# --8<-- [end:leaks]


# --8<-- [start:two_nodes]
async def test_a_partition_is_noticed():
    async with two_nodes() as nodes:
        worker = nodes.beta.spawn(retiring(), "worker")
        probe = TestProbe(nodes.alpha, Greeted)
        here = await nodes.alpha.resolve(
            f"tapio://{nodes.beta.name}@{nodes.beta.address.host}:"
            f"{nodes.beta.address.port}{_path_of(worker)}",
            expect=Retire,
        )
        probe.watch(here)

        nodes.partition()

        await probe.expect_terminated(here, timedelta(seconds=5))
# --8<-- [end:two_nodes]


def _path_of(ref: ActorRef[Retire]) -> str:
    """The path part of a ref's string form, which the URI above needs."""
    return "/" + "/".join(ref.path.elements) + f"#{ref.path.uid}"
