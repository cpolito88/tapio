"""Delivery-time validation: the proof that it is not a no-op.

This is the guarantee that matters most. The headline claim is that a
message is re-validated on delivery, and the way that claim fails is not with
an error: it fails by quietly checking nothing, costing nothing, and passing
every test written against it. So the tests here are written to fail if the
guarantee is hollow.

They target the resolved validator directly rather than `tell`, because there
is no cell or mailbox yet. Once there is, the same thing gets re-asserted end
to end through a real `ref.tell`, which makes that an integration check rather
than a copy of this one.
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

    `model_construct` skips validation entirely, which is the cheapest way to
    build the thing a delivery-time check exists to catch.
    """
    return Greet.model_construct(whom="world", count="not-an-int", reply_to=ref)


# --- the proof ---------------------------------------------------------------


def test_a_tampered_message_is_rejected_when_validation_is_on(on, tampered):
    validate = resolve_validator(msg_type=Greet, settings=on)
    with pytest.raises(ValidationError):
        validate(tampered)


def test_the_same_message_passes_when_validation_is_off(off, tampered):
    # The other half of the proof. If this also raised, the flag would not be
    # controlling anything; if the test above passed silently, the guarantee
    # would be hollow. Both directions are needed to show the switch is real.
    validate = resolve_validator(msg_type=Greet, settings=off)
    validate(tampered)


def test_mutation_after_construction_is_caught(on, good):
    # Messages are frozen, so this is a deliberate bypass. It stands in for the
    # real case: an object mutated through some other alias before being sent.
    object.__setattr__(good, "count", "not-an-int")
    validate = resolve_validator(msg_type=Greet, settings=on)
    with pytest.raises(ValidationError):
        validate(good)


def test_the_message_base_class_is_what_makes_revalidation_real():
    # Pydantic defaults revalidate_instances to "never", so re-validating a
    # plain BaseModel instance returns it untouched. If tapio.Message did not
    # set revalidate_instances="always", every test above would pass for the
    # wrong reason. This asserts the default really is that permissive.
    plain = NotAMessage.model_construct(n="not-an-int")
    assert NotAMessage.model_validate(plain, strict=True) is plain

    tampered_message = Greeted.model_construct(whom=17)
    with pytest.raises(ValidationError):
        Greeted.model_validate(tampered_message, strict=True)


def test_revalidation_returns_a_copy_that_is_discarded(good):
    # Why the validator returns None: the check allocates a new instance, and
    # the mailbox must receive the original. Identity cannot depend on a flag.
    assert Greet.model_validate(good) is not good


@pytest.mark.parametrize("validate_on_tell", [True, False])
def test_the_recipient_always_gets_the_object_the_sender_passed(validate_on_tell, good):
    settings = TapioSettings(validate_on_tell=validate_on_tell)
    validate = resolve_validator(msg_type=Greet, settings=settings)
    assert validate(good) is None


# --- strict-vs-lax construction -----------------------------------------------


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
    """There is no strict-vs-lax asymmetry to document.

    The concern was that construction is lax while step 3 is strict, so a value
    lax construction accepted could be rejected on send. It cannot: lax
    construction *coerces*, and stores the canonical type. By the time the
    message exists its fields already hold what strict mode wants, so the two
    cannot disagree. `Message` therefore keeps lax construction, and no
    asymmetry needs documenting.

    What strict mode still catches is the case that skipped construction
    entirely, which is exactly the tampering tested above.
    """
    message = Greet(whom="w", count=sent, reply_to=ref)  # type: ignore[arg-type]
    assert message.count == stored
    assert type(message.count) is int

    resolve_validator(msg_type=Greet, settings=on)(message)


# --- step 2: the type check --------------------------------------------------


def test_a_message_of_the_wrong_type_is_rejected(on, ref):
    validate = resolve_validator(msg_type=Greet, settings=on)
    with pytest.raises(MessageTypeError, match="Greeted"):
        validate(Greeted(whom="world"))


def test_the_type_check_runs_even_with_validation_off(off, ref):
    # Unconditional by design: it is what keeps the mailbox's type contract
    # honest when the expensive half is switched off.
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


# --- unions ------------------------------------------------------------------


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
    # typing.Union[A, B] and A | B spell the same type, but only the latter
    # works as the second argument to isinstance.
    import typing

    msg_type = normalize_msg_type(
        typing.Union[Increment, GetCount],  # noqa: UP007 - that is the point
        origin="test",
    )
    resolve_validator(msg_type=msg_type, settings=on)(Increment(by=1))


# --- declaring a type that cannot carry the guarantee ------------------------


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


# --- settings ----------------------------------------------------------------


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
    # The core-schema hook is what makes this legal; without it, declaring an
    # ActorRef field raises at class-definition time.
    class WithRef(Message):
        reply_to: ActorRef[Greeted]

    assert issubclass(WithRef, BaseModel)
    assert WithRef(reply_to=ActorRef(ActorPath.root("s").child("a"))) is not None
