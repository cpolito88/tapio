"""Running one behavior with no system, and reading back what it did."""

import pytest

from tapio import Behavior, Behaviors, Message
from tapio.actor import (
    AbstractBehavior,
    ActorContext,
    ActorRef,
    MailboxConfig,
    PostStop,
    SupervisorStrategy,
)
from tapio.errors import BehaviorTypeError, MessageTypeError, TapioError
from tapio.settings import TapioSettings
from tapio.testkit import BehaviorTestKit, Spawned, Watched


class Count(Message):
    """What the counter answers with."""

    value: int


class Increment(Message):
    """Adds one."""


class GetCount(Message):
    """Asks for the total."""

    reply_to: ActorRef[Count]


class Job(Message):
    """A unit of work for a spawned child."""

    item: int


def counter() -> Behavior[Increment | GetCount]:
    """A counter in the functional style, with the count in a closure."""

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


class ClassCounter(AbstractBehavior[Increment | GetCount]):
    """The same counter in the class-based style."""

    def __init__(self, ctx: ActorContext[Increment | GetCount]) -> None:
        """Start at zero."""
        super().__init__(ctx)
        self._total = 0

    async def on_message(
        self, message: Increment | GetCount
    ) -> Behavior[Increment | GetCount]:
        """Count, or answer."""
        if isinstance(message, Increment):
            self._total += 1
            return Behaviors.same()
        message.reply_to.tell(Count(value=self._total))
        return Behaviors.same()


async def test_a_behavior_runs_with_no_system_at_all():
    kit: BehaviorTestKit[Increment | GetCount] = BehaviorTestKit(counter())

    await kit.run(Increment())
    await kit.run(Increment())
    await kit.run(GetCount(reply_to=kit.self_ref))

    # Nothing was scheduled and nothing was awaited: the handler ran to
    # completion inside `run`, so there is nothing to poll for.
    assert kit.self_inbox == [Count(value=2)]


async def test_setup_runs_when_the_kit_is_built():
    started: list[str] = []

    def build(ctx: ActorContext[Increment]) -> Behavior[Increment]:
        started.append("setup")

        async def on_message(message: Increment) -> Behavior[Increment]:
            return Behaviors.same()

        return Behaviors.receive_message(on_message, msg_type=Increment)

    BehaviorTestKit(Behaviors.setup(build))

    assert started == ["setup"]


async def test_the_class_based_style_works_the_same():
    kit: BehaviorTestKit[Increment | GetCount] = BehaviorTestKit(
        Behaviors.setup(ClassCounter)
    )

    await kit.run(Increment())
    await kit.run(GetCount(reply_to=kit.self_ref))

    assert kit.self_inbox == [Count(value=1)]


async def test_a_message_of_the_wrong_type_is_refused():
    kit: BehaviorTestKit[Increment | GetCount] = BehaviorTestKit(counter())

    with pytest.raises(MessageTypeError):
        await kit.run(Job(item=1))  # type: ignore[arg-type]


async def test_spawns_are_recorded_with_the_ref_that_was_handed_back():
    def build(ctx: ActorContext[Increment]) -> Behavior[Increment]:
        worker = ctx.spawn(_sink(), "worker", MailboxConfig(capacity=10))

        async def on_message(message: Increment) -> Behavior[Increment]:
            worker.tell(Job(item=1))
            return Behaviors.same()

        return Behaviors.receive_message(on_message, msg_type=Increment)

    kit: BehaviorTestKit[Increment] = BehaviorTestKit(Behaviors.setup(build))
    await kit.run(Increment())

    assert kit.effects == (Spawned("worker", _sink(), None, kit.child("worker").ref),)
    assert kit.child("worker").inbox == [Job(item=1)]
    assert kit.child("worker").mailbox == MailboxConfig(capacity=10)


async def test_an_anonymous_spawn_gets_a_generated_name():
    def build(ctx: ActorContext[Increment]) -> Behavior[Increment]:
        ctx.spawn_anonymous(_sink())

        async def on_message(message: Increment) -> Behavior[Increment]:
            return Behaviors.same()

        return Behaviors.receive_message(on_message, msg_type=Increment)

    kit: BehaviorTestKit[Increment] = BehaviorTestKit(Behaviors.setup(build))

    assert kit.children[0].name == "$1"


async def test_asking_for_a_child_that_was_never_spawned_says_which_were():
    kit: BehaviorTestKit[Increment | GetCount] = BehaviorTestKit(counter())

    with pytest.raises(AssertionError, match="no child named 'worker'"):
        kit.child("worker")


async def test_watches_are_recorded():
    target: ActorRef[Job] = ActorRef(
        BehaviorTestKit(counter()).ctx.path.parent.child("other", uid=3)
    )

    def build(ctx: ActorContext[Increment]) -> Behavior[Increment]:
        ctx.watch(target)

        async def on_message(message: Increment) -> Behavior[Increment]:
            ctx.unwatch(target)
            return Behaviors.same()

        return Behaviors.receive_message(on_message, msg_type=Increment)

    kit: BehaviorTestKit[Increment] = BehaviorTestKit(Behaviors.setup(build))
    await kit.run(Increment())

    assert kit.effects == (
        Watched(target, watching=True),
        Watched(target, watching=False),
    )


async def test_a_behavior_that_stops_says_so():
    async def on_message(message: Increment) -> Behavior[Increment]:
        return Behaviors.stopped()

    kit: BehaviorTestKit[Increment] = BehaviorTestKit(
        Behaviors.receive_message(on_message, msg_type=Increment)
    )

    await kit.run(Increment())

    assert kit.is_stopped
    with pytest.raises(AssertionError, match="has stopped"):
        await kit.run(Increment())


async def test_switching_behavior_is_what_the_next_message_meets():
    async def second(message: Increment) -> Behavior[Increment]:
        return Behaviors.stopped()

    async def first(message: Increment) -> Behavior[Increment]:
        return Behaviors.receive_message(second, msg_type=Increment)

    kit: BehaviorTestKit[Increment] = BehaviorTestKit(
        Behaviors.receive_message(first, msg_type=Increment)
    )

    await kit.run(Increment())
    await kit.run(Increment())

    assert kit.is_stopped


async def test_signals_are_delivered_by_hand():
    seen: list[str] = []

    async def on_message(message: Increment) -> Behavior[Increment]:
        return Behaviors.same()

    async def on_signal(
        ctx: ActorContext[Increment], signal: object
    ) -> Behavior[Increment]:
        seen.append(type(signal).__name__)
        return Behaviors.same()

    kit: BehaviorTestKit[Increment] = BehaviorTestKit(
        Behaviors.receive_message(on_message, msg_type=Increment, on_signal=on_signal)  # type: ignore[arg-type]
    )

    await kit.signal(PostStop())

    assert seen == ["PostStop"]


async def test_a_failing_handler_raises_into_the_test():
    async def on_message(message: Increment) -> Behavior[Increment]:
        msg = "boom"
        raise RuntimeError(msg)

    kit: BehaviorTestKit[Increment] = BehaviorTestKit(
        Behaviors.supervise(
            Behaviors.receive_message(on_message, msg_type=Increment)
        ).on_failure(SupervisorStrategy.restart())
    )

    # The strategy is visible, and it is not applied. What a cell does with a
    # failure is a test that needs a cell.
    assert kit.supervision == (SupervisorStrategy.restart(),)
    with pytest.raises(RuntimeError, match="boom"):
        await kit.run(Increment())


async def test_a_behavior_with_no_message_type_is_refused():
    with pytest.raises(BehaviorTypeError, match="carries no message type"):
        BehaviorTestKit(Behaviors.same())


async def test_timers_and_stash_need_a_real_system():
    with pytest.raises(TapioError, match="timers"):
        BehaviorTestKit(Behaviors.with_timers(lambda timers: _sink()))

    with pytest.raises(TapioError, match="stash"):
        BehaviorTestKit(Behaviors.with_stash(10, lambda stash: _sink()))


async def test_resolving_a_ref_needs_a_real_system():
    kit: BehaviorTestKit[Increment | GetCount] = BehaviorTestKit(counter())

    with pytest.raises(TapioError, match="live runtime"):
        await kit.ctx.resolve("tapio://other@127.0.0.1:1/user/x#1", expect=Increment)


async def test_a_blocking_call_runs_inline():
    seen: list[int] = []

    async def on_message(
        ctx: ActorContext[Increment], message: Increment
    ) -> Behavior[Increment]:
        seen.append(await ctx.run_blocking(lambda: 42))
        return Behaviors.same()

    kit: BehaviorTestKit[Increment] = BehaviorTestKit(Behaviors.receive(on_message))
    await kit.run(Increment())

    assert seen == [42]


async def test_validation_follows_the_settings_it_was_given():
    off = TapioSettings(_env_file=None, validate_on_tell=False)
    kit: BehaviorTestKit[Increment | GetCount] = BehaviorTestKit(
        counter(), settings=off
    )

    # Step 2 is unconditional, so the type check survives the setting.
    with pytest.raises(MessageTypeError):
        await kit.run(Job(item=1))  # type: ignore[arg-type]


def _sink() -> Behavior[Job]:
    """A child behavior for the spawn tests to hand around."""

    async def on_message(message: Job) -> Behavior[Job]:
        return Behaviors.same()

    return Behaviors.receive_message(on_message, msg_type=Job)
