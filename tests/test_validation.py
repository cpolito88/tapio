"""Tests that delivery-time validation is not a no-op.

The claim is that a message is re-validated on delivery. That claim does not
fail with an error if it is wrong. It fails by checking nothing, costing
nothing, and passing any test written carelessly against it. These tests are
written to fail if the guarantee is hollow.

They call the resolved validator directly rather than going through `tell`.
`tests/actor/test_delivery.py` asserts the same thing end to end.
"""

import re

import pytest
from pydantic import BaseModel, ValidationError

from tapio import Message, TapioSettings
from tapio.actor import ActorPath, ActorRef
from tapio.errors import MessageTypeError
from tapio.validation import normalize_msg_type, resolve_validator
from tests.messages import GetCount, Greet, Greeted, Increment, NotAMessage


@pytest.fixture
def on() -> TapioSettings:
    return TapioSettings(validate_on_tell=True)


@pytest.fixture
def off() -> TapioSettings:
    return TapioSettings(validate_on_tell=False)


@pytest.fixture
def good(ref) -> Greet:
    return Greet(whom="world", count=1, reply_to=ref)


@pytest.fixture
def tampered(ref) -> Greet:
    """A message whose contents Pydantic never saw.

    `model_construct` skips validation entirely, which is the easiest way to
    build the thing a delivery-time check exists to catch.
    """
    return Greet.model_construct(whom="world", count="not-an-int", reply_to=ref)


def test_a_tampered_message_is_rejected_when_validation_is_on(on, tampered):
    validate = resolve_validator(msg_type=Greet, settings=on)
    with pytest.raises(ValidationError):
        validate(tampered)


def test_the_same_message_passes_when_validation_is_off(off, tampered):
    # The other half of the proof. If this raised too, the flag would control
    # nothing. If the test above passed silently, the guarantee would be
    # hollow. Both directions are needed to show the switch is real.
    validate = resolve_validator(msg_type=Greet, settings=off)
    validate(tampered)


def test_mutation_after_construction_is_caught(on, good):
    # Messages are frozen, so this bypasses that on purpose. It stands in for
    # the real case: an object changed through another alias before it is
    # sent.
    object.__setattr__(good, "count", "not-an-int")
    validate = resolve_validator(msg_type=Greet, settings=on)
    with pytest.raises(ValidationError):
        validate(good)


def test_the_message_base_class_is_what_makes_revalidation_real():
    # Pydantic defaults revalidate_instances to "never", so re-validating a
    # plain BaseModel instance returns it untouched. Without
    # revalidate_instances="always" on tapio.Message, every test above would
    # pass for the wrong reason. This asserts the default really is that lax.
    plain = NotAMessage.model_construct(n="not-an-int")
    assert NotAMessage.model_validate(plain, strict=True) is plain

    tampered_message = Greeted.model_construct(whom=17)
    with pytest.raises(ValidationError):
        Greeted.model_validate(tampered_message, strict=True)


def test_revalidation_returns_a_copy_that_is_discarded(good):
    # This is why the validator returns None. The check builds a new instance,
    # and the mailbox must receive the original one. Identity cannot depend on
    # a setting.
    assert Greet.model_validate(good) is not good


@pytest.mark.parametrize("validate_on_tell", [True, False])
def test_the_recipient_always_gets_the_object_the_sender_passed(validate_on_tell, good):
    settings = TapioSettings(validate_on_tell=validate_on_tell)
    validate = resolve_validator(msg_type=Greet, settings=settings)
    assert validate(good) is None


@pytest.mark.parametrize(
    ("sent", "stored"),
    [
        (True, 1),  # the canonical worry: a bool into an int field
        ("3", 3),
        (1, 1),
    ],
)
def test_lax_construction_leaves_nothing_for_strict_revalidation_to_reject(
    on, ref, sent, stored
):
    """Lax construction and strict re-validation cannot disagree.

    Construction is lax while the delivery-time check is strict, so it looks
    as though a value construction accepted could be rejected on send. It
    cannot. Lax construction coerces the value and stores the canonical type,
    so by the time the message exists its fields already hold what strict mode
    wants. `Message` therefore keeps lax construction.

    What strict mode still catches is a message that skipped construction
    altogether, which is the tampering tested above.
    """
    message = Greet(whom="w", count=sent, reply_to=ref)  # type: ignore[arg-type]
    assert message.count == stored
    assert type(message.count) is int

    resolve_validator(msg_type=Greet, settings=on)(message)


def test_a_message_of_the_wrong_type_is_rejected(on, ref):
    validate = resolve_validator(msg_type=Greet, settings=on)
    with pytest.raises(MessageTypeError, match="Greeted"):
        validate(Greeted(whom="world"))


def test_the_type_check_runs_even_with_validation_off(off, ref):
    # Always on by design. It is what keeps the mailbox's type contract honest
    # when the expensive half is switched off.
    validate = resolve_validator(msg_type=Greet, settings=off)
    with pytest.raises(MessageTypeError):
        validate(Greeted(whom="world"))


def test_the_type_error_names_the_target_path(on, path):
    validate = resolve_validator(msg_type=Greet, settings=on, target=path)
    with pytest.raises(
        MessageTypeError, match=re.escape("tapio://sys/user/greeter#42")
    ):
        validate(Greeted(whom="world"))


def test_the_type_error_names_both_types(on):
    validate = resolve_validator(msg_type=Greet, settings=on)
    with pytest.raises(MessageTypeError, match=r"Greeted.*does not match.*Greet"):
        validate(Greeted(whom="world"))


def test_a_union_accepts_either_member(on, ref):
    validate = resolve_validator(msg_type=Increment | GetCount, settings=on)
    validate(Increment(by=2))
    validate(GetCount(reply_to=ref))


def test_a_union_still_rejects_an_outsider(on):
    validate = resolve_validator(msg_type=Increment | GetCount, settings=on)
    with pytest.raises(MessageTypeError, match=re.escape("Increment | GetCount")):
        validate(Greeted(whom="world"))


def test_a_union_still_revalidates_contents(on):
    validate = resolve_validator(msg_type=Increment | GetCount, settings=on)
    with pytest.raises(ValidationError):
        validate(Increment.model_construct(by="not-an-int"))


def test_typing_union_is_normalized_so_isinstance_works(on, ref):
    # typing.Union[A, B] and A | B mean the same type, but only the second
    # works as an argument to isinstance.
    import typing

    msg_type = normalize_msg_type(
        typing.Union[Increment, GetCount],  # noqa: UP007 - that is the point
        origin="test",
    )
    resolve_validator(msg_type=msg_type, settings=on)(Increment(by=1))


def test_a_plain_basemodel_message_type_is_refused():
    with pytest.raises(MessageTypeError, match="subclasses BaseModel"):
        normalize_msg_type(NotAMessage, origin="test")


def test_the_refusal_says_how_to_fix_it():
    with pytest.raises(
        MessageTypeError, match=re.escape("Subclass tapio.Message instead")
    ):
        normalize_msg_type(NotAMessage, origin="test")


def test_a_non_model_message_type_is_refused():
    with pytest.raises(
        MessageTypeError, match=re.escape("not a tapio.Message subclass")
    ):
        normalize_msg_type(str, origin="test")


def test_something_that_is_not_a_type_at_all_is_refused():
    with pytest.raises(MessageTypeError, match="neither a class nor a union"):
        normalize_msg_type(42, origin="test")


def test_a_union_containing_a_plain_basemodel_is_refused():
    with pytest.raises(MessageTypeError, match="subclasses BaseModel"):
        normalize_msg_type(Increment | NotAMessage, origin="test")


def test_validation_is_on_by_default():
    assert TapioSettings().validate_on_tell is True


def test_settings_read_the_environment(monkeypatch):
    monkeypatch.setenv("TAPIO_VALIDATE_ON_TELL", "0")
    monkeypatch.setenv("TAPIO_BLOCKING_POOL_SIZE", "4")
    settings = TapioSettings()
    assert settings.validate_on_tell is False
    assert settings.blocking_pool_size == 4


def test_the_default_mailbox_is_unbounded():
    # Unbounded is the default, so the common case needs no configuration.
    assert TapioSettings().default_mailbox_capacity is None


def test_messages_are_frozen():
    with pytest.raises(ValidationError):
        Greeted(whom="a").whom = "b"


def test_a_message_subclass_carries_the_config():
    class Custom(Message):
        n: int

    assert Custom.model_config["revalidate_instances"] == "always"
    assert Custom.model_config["frozen"] is True


def test_a_ref_field_does_not_need_arbitrary_types_allowed():
    # The core-schema hook is what makes this legal. Without it, declaring an
    # ActorRef field raises at class-definition time.
    class WithRef(Message):
        reply_to: ActorRef[Greeted]

    assert issubclass(WithRef, BaseModel)
    assert WithRef(reply_to=ActorRef(ActorPath.root("s").child("a"))) is not None
