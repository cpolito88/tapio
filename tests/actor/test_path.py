"""Actor paths: structure, string form, and name validation."""

import pytest

from tapio.actor import ActorPath


def test_root_renders_with_a_trailing_slash():
    assert str(ActorPath.root("sys")) == "tapio://sys/"


def test_string_form_carries_elements_and_uid():
    path = ActorPath.root("sys").child("user").child("greeter", uid=42)
    assert str(path) == "tapio://sys/user/greeter#42"


def test_uid_is_omitted_when_unset():
    assert str(ActorPath.root("sys").child("user")) == "tapio://sys/user"


def test_name_is_the_last_element():
    assert ActorPath.root("sys").child("user").child("greeter").name == "greeter"


def test_root_name_is_a_slash():
    assert ActorPath.root("sys").name == "/"


def test_parent_drops_the_last_element():
    path = ActorPath.root("sys").child("user").child("greeter", uid=7)
    assert path.parent == ActorPath.root("sys").child("user")


def test_parent_drops_the_uid_because_it_describes_this_incarnation():
    assert ActorPath.root("sys").child("user", uid=7).parent.uid == 0


def test_the_root_is_its_own_parent():
    root = ActorPath.root("sys")
    assert root.parent is root
    assert root.is_root


def test_paths_are_hashable_and_compare_by_value():
    a = ActorPath.root("sys").child("user", uid=1)
    b = ActorPath.root("sys").child("user", uid=1)
    assert a == b
    assert len({a, b}) == 1


def test_a_different_uid_is_a_different_path():
    # This is what the uid is for. A stale ref must not address an actor
    # spawned later under the same name.
    a = ActorPath.root("sys").child("user", uid=1)
    assert a != ActorPath.root("sys").child("user", uid=2)


def test_paths_are_immutable():
    path = ActorPath.root("sys")
    with pytest.raises((AttributeError, TypeError)):
        path.system = "other"  # type: ignore[misc]


@pytest.mark.parametrize(
    "name",
    ["with/slash", "with#hash", "with space", "", "-leading-dash", "with?query"],
)
def test_structural_characters_are_rejected_in_names(name):
    with pytest.raises(ValueError, match="invalid actor name"):
        ActorPath.root("sys").child(name)


@pytest.mark.parametrize("name", ["worker", "worker-1", "worker_1", "a.b", "$anon-3"])
def test_ordinary_and_generated_names_are_accepted(name):
    assert ActorPath.root("sys").child(name).name == name


def test_a_negative_uid_is_rejected():
    with pytest.raises(ValueError, match="invalid incarnation uid"):
        ActorPath.root("sys").child("user", uid=-1)


def test_with_uid_stamps_an_incarnation():
    assert ActorPath.root("sys").child("user").with_uid(9).uid == 9


def test_repr_shows_the_string_form():
    assert repr(ActorPath.root("sys")) == "ActorPath('tapio://sys/')"
