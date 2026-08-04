"""`ActorRef` as a Pydantic field.

A model with an `ActorRef` field must validate from a live ref and dump to the
ref's full string form. The dump-then-validate direction needs a system to
resolve against, and says so when there is none.
"""

import pytest
from pydantic import ValidationError

from tapio import Message
from tapio.actor import ActorPath, ActorRef
from tapio.errors import RefResolutionError
from tests.messages import Greet, Greeted


def test_a_live_ref_validates_as_a_field(ref):
    msg = Greet(whom="world", count=1, reply_to=ref)
    assert msg.reply_to is ref


def test_the_ref_survives_construction_unchanged(ref):
    # Validation is an is-instance check, so the object is passed through
    # rather than rebuilt. Sending the message must hand over this same ref.
    assert Greet(whom="w", count=1, reply_to=ref).reply_to is ref


def test_a_ref_serializes_to_its_path_string(ref):
    dumped = Greet(whom="world", count=1, reply_to=ref).model_dump()
    assert dumped["reply_to"] == "tapio://sys/user/greeter#42"


def test_a_ref_serializes_to_a_path_string_in_json_mode(ref):
    dumped = Greet(whom="world", count=1, reply_to=ref).model_dump(mode="json")
    assert dumped["reply_to"] == "tapio://sys/user/greeter#42"


def test_a_model_holding_a_ref_does_not_round_trip_outside_a_system(ref):
    # The documented asymmetry: a dump succeeds anywhere, and validating it
    # back needs the system that would resolve the ref. Worth an explicit test
    # because it reads as a bug otherwise.
    dumped = Greet(whom="world", count=1, reply_to=ref).model_dump()
    with pytest.raises(RefResolutionError, match="without a system"):
        Greet.model_validate(dumped)


def test_the_resolution_error_says_how_to_get_a_system_in_scope(ref):
    dumped = Greet(whom="world", count=1, reply_to=ref).model_dump()
    with pytest.raises(RefResolutionError, match="as_deserialization_context"):
        Greet.model_validate(dumped)


def test_a_non_ref_non_string_is_an_ordinary_validation_error():
    with pytest.raises(ValidationError):
        Greet.model_validate({"whom": "w", "count": 1, "reply_to": 17})


def test_validation_does_not_check_liveness(ref):
    # Deliberate: whether the target is alive is a race, and a dead target is
    # not a schema error. There is nothing to assert but the absence of a
    # check, so this pins the intent.
    assert Greet(whom="w", count=1, reply_to=ref).reply_to is ref


def test_the_type_parameter_is_not_checked_at_runtime():
    # The documented consequence of erasure: ActorRef[Greeted] and
    # ActorRef[Increment] validate identically. A type checker catches the
    # mismatch statically; the runtime check lives on the receiving actor.
    wrong: ActorRef[Message] = ActorRef(ActorPath.root("sys").child("elsewhere"))
    msg = Greet(whom="w", count=1, reply_to=wrong)  # type: ignore[arg-type]
    assert msg.reply_to is wrong


def test_a_ref_subclass_validates_too():
    # The ask pattern's promise ref is an ActorRef subclass with no cell, and
    # it has to pass the same validation a real ref does.
    class PromiseLikeRef(ActorRef[Greeted]):
        pass

    promise = PromiseLikeRef(ActorPath.root("sys").child("$ask-1"))
    assert Greet(whom="w", count=1, reply_to=promise).reply_to is promise


def test_refs_compare_and_hash_by_path(path):
    assert ActorRef(path) == ActorRef(path)
    assert len({ActorRef(path), ActorRef(path)}) == 1


def test_refs_to_different_incarnations_differ(path):
    assert ActorRef(path) != ActorRef(path.with_uid(path.uid + 1))


def test_a_ref_is_not_equal_to_a_non_ref(ref):
    assert ref != "tapio://sys/user/greeter#42"


def test_repr_shows_the_path(ref):
    assert repr(ref) == "ActorRef('tapio://sys/user/greeter#42')"


def test_the_base_ref_cannot_deliver(ref):
    # Delivery belongs to the refs a running actor system hands out.
    with pytest.raises(NotImplementedError, match="cannot deliver messages"):
        ref.tell(Greeted(whom="world"))
