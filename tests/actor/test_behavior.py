"""Behaviors and message-type resolution.

The combinators are exercised without an actor system, and the resolution
tests cover the resolution rules: explicit wins, introspection is the
fallback, and an unresolvable handler raises rather than spawning with the
delivery-time type check quietly disabled.
"""

import pytest

from tapio.actor import (
    AbstractBehavior,
    ActorContext,
    Behavior,
    Behaviors,
    SetupBehavior,
)
from tapio.actor.behavior import resolve_handler_msg_type
from tapio.errors import BehaviorTypeError, MessageTypeError
from tests.messages import GetCount, Greet, Greeted, Increment, NotAMessage

# --- sentinels ---------------------------------------------------------------


def test_same_is_a_singleton():
    # The runtime interprets these by identity, so a new object each call would
    # be a bug waiting for an `is` comparison.
    assert Behaviors.same() is Behaviors.same()


@pytest.mark.parametrize(
    "factory",
    [
        Behaviors.same,
        Behaviors.stopped,
        Behaviors.empty,
        Behaviors.ignore,
        Behaviors.unhandled,
    ],
)
def test_sentinels_carry_no_message_type(factory):
    # They resolve against the type the actor already has, not independently.
    assert factory().msg_type is None


def test_sentinels_are_distinct():
    sentinels = [
        Behaviors.same(),
        Behaviors.stopped(),
        Behaviors.empty(),
        Behaviors.ignore(),
        Behaviors.unhandled(),
    ]
    assert len({id(s) for s in sentinels}) == len(sentinels)


def test_a_sentinel_reprs_as_its_factory_call():
    assert repr(Behaviors.stopped()) == "Behaviors.stopped()"


# --- receive -----------------------------------------------------------------


async def handle(ctx: ActorContext[Greet], msg: Greet) -> Behavior[Greet]:
    return Behaviors.same()


async def handle_untyped(ctx, msg):
    return Behaviors.same()


async def handle_message_only(msg: Greet) -> Behavior[Greet]:
    return Behaviors.same()


def test_receive_resolves_the_type_from_the_annotation():
    assert Behaviors.receive(handle).msg_type is Greet


def test_explicit_msg_type_wins():
    assert Behaviors.receive(handle_untyped, msg_type=Greet).msg_type is Greet


def test_explicit_msg_type_overrides_an_annotation():
    # Not a conflict to resolve: the documented rule is that explicit wins.
    assert Behaviors.receive(handle, msg_type=Greeted).msg_type is Greeted


def test_receive_message_resolves_from_its_only_parameter():
    assert Behaviors.receive_message(handle_message_only).msg_type is Greet


def test_a_union_annotation_resolves():
    async def on_message(
        ctx: ActorContext[Increment | GetCount], msg: Increment | GetCount
    ) -> Behavior[Increment | GetCount]:
        return Behaviors.same()

    behavior = Behaviors.receive(on_message)
    assert behavior.msg_type == Increment | GetCount


async def test_receive_dispatches_to_the_handler(ctx, ref):
    seen = []

    async def on_message(c: ActorContext[Greet], msg: Greet) -> Behavior[Greet]:
        seen.append((c, msg))
        return Behaviors.stopped()

    behavior = Behaviors.receive(on_message)
    message = Greet(whom="world", count=1, reply_to=ref)
    result = await behavior.receive(ctx, message)

    assert seen == [(ctx, message)]
    assert result is Behaviors.stopped()


async def test_receive_message_drops_the_context(ctx, ref):
    seen = []

    async def on_message(msg: Greet) -> Behavior[Greet]:
        seen.append(msg)
        return Behaviors.same()

    message = Greet(whom="world", count=1, reply_to=ref)
    await Behaviors.receive_message(on_message).receive(ctx, message)
    assert seen == [message]


def test_receive_reprs_with_the_handler_name():
    assert "handle" in repr(Behaviors.receive(handle))


# --- unresolvable types fail loudly ------------------------------------------


def test_an_unannotated_handler_raises():
    with pytest.raises(BehaviorTypeError, match="has no annotation"):
        Behaviors.receive(handle_untyped)


def test_the_error_says_how_to_fix_it():
    with pytest.raises(BehaviorTypeError, match=r"pass\s*msg_type="):
        Behaviors.receive(handle_untyped)


def test_an_unresolvable_forward_reference_raises():
    async def on_message(
        ctx: "ActorContext[NeverDefined]",  # noqa: F821
        msg: "NeverDefined",  # noqa: F821
    ) -> "Behavior[NeverDefined]":  # noqa: F821
        return Behaviors.same()

    with pytest.raises(BehaviorTypeError, match="cannot read the annotations"):
        Behaviors.receive(on_message)


def test_a_handler_with_too_few_parameters_raises():
    async def on_message(msg: Greet) -> Behavior[Greet]:
        return Behaviors.same()

    # receive expects (ctx, message); this is receive_message's shape.
    with pytest.raises(BehaviorTypeError, match="no message parameter at position 1"):
        Behaviors.receive(on_message)  # type: ignore[arg-type]


def test_a_plain_basemodel_annotation_is_refused():
    async def on_message(ctx: ActorContext, msg: NotAMessage) -> Behavior:
        return Behaviors.same()

    with pytest.raises(MessageTypeError, match="subclasses BaseModel"):
        Behaviors.receive(on_message)  # type: ignore[arg-type]


def test_resolution_names_the_offending_function():
    with pytest.raises(BehaviorTypeError, match="handle_untyped"):
        resolve_handler_msg_type(handle_untyped, explicit=None, message_param_index=1)


# --- setup -------------------------------------------------------------------


def test_setup_defers_construction(ctx):
    calls = []

    def factory(c: ActorContext[Greet]) -> Behavior[Greet]:
        calls.append(c)
        return Behaviors.receive(handle)

    behavior = Behaviors.setup(factory)
    assert isinstance(behavior, SetupBehavior)
    assert calls == []  # nothing ran yet

    produced = behavior.setup(ctx)
    assert calls == [ctx]
    assert produced.msg_type is Greet


def test_setup_itself_carries_no_type(ctx):
    # Its type is whatever the behavior it produces declares, which is not
    # known until it runs.
    assert Behaviors.setup(lambda c: Behaviors.receive(handle)).msg_type is None


def test_setup_can_run_more_than_once(ctx):
    # A restart re-evaluates the original behavior, so this has to be re-usable.
    calls = []
    behavior = Behaviors.setup(lambda c: calls.append(c) or Behaviors.same())
    behavior.setup(ctx)
    behavior.setup(ctx)
    assert len(calls) == 2


# --- the class-based style ---------------------------------------------------


def test_a_subclass_resolves_its_type_from_the_parameter():
    class Counter(AbstractBehavior[Increment]):
        async def on_message(self, message: Increment) -> Behavior[Increment]:
            return Behaviors.same()

    assert Counter.msg_type is Increment


def test_a_subclass_resolves_a_union_parameter():
    class Counter(AbstractBehavior[Increment | GetCount]):
        async def on_message(
            self, message: Increment | GetCount
        ) -> Behavior[Increment | GetCount]:
            return Behaviors.same()

    assert Counter.msg_type == Increment | GetCount


def test_a_class_attribute_overrides_the_parameter():
    class Counter(AbstractBehavior[Increment]):
        msg_type = Greet

        async def on_message(self, message: Increment) -> Behavior[Increment]:
            return Behaviors.same()

    assert Counter.msg_type is Greet


def test_an_unparameterized_concrete_subclass_raises():
    with pytest.raises(BehaviorTypeError, match="cannot resolve the message type"):

        class Counter(AbstractBehavior):  # type: ignore[type-arg]
            async def on_message(self, message):  # noqa: ANN202
                return Behaviors.same()


def test_an_abstract_intermediate_needs_no_type():
    # Bases that leave on_message abstract are exempt, so a hierarchy does not
    # have to name a type at every level.
    class Base(AbstractBehavior[Increment]):
        pass

    class Concrete(Base):
        async def on_message(self, message: Increment) -> Behavior[Increment]:
            return Behaviors.same()

    assert Concrete.msg_type is Increment


def test_a_subclass_inherits_a_resolved_type():
    class Base(AbstractBehavior[Increment]):
        async def on_message(self, message: Increment) -> Behavior[Increment]:
            return Behaviors.same()

    class Derived(Base):
        pass

    assert Derived.msg_type is Increment


def test_a_plain_basemodel_parameter_is_refused():
    with pytest.raises(MessageTypeError, match="subclasses BaseModel"):

        class Bad(AbstractBehavior[NotAMessage]):  # type: ignore[type-var]
            async def on_message(self, message: NotAMessage) -> Behavior:
                return Behaviors.same()


async def test_a_class_based_behavior_receives(ctx, ref):
    class Counter(AbstractBehavior[Increment]):
        def __init__(self, context: ActorContext[Increment]) -> None:
            super().__init__(context)
            self.count = 0

        async def on_message(self, message: Increment) -> Behavior[Increment]:
            self.count += message.by
            return Behaviors.same()

    behavior = Counter(ctx)
    await behavior.receive(ctx, Increment(by=2))
    await behavior.receive(ctx, Increment(by=3))
    assert behavior.count == 5


def test_a_class_based_behavior_keeps_its_context(ctx):
    class Counter(AbstractBehavior[Increment]):
        async def on_message(self, message: Increment) -> Behavior[Increment]:
            return Behaviors.same()

    assert Counter(ctx).ctx is ctx
