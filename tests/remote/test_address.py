"""Tests for addresses and the string form of a ref.

A ref's string form is its wire form, so what one system writes down another
has to be able to parse back.
"""

import pytest

from tapio.actor import ActorPath
from tapio.remote.address import Address, format_ref, parse_ref


def test_an_address_without_a_host_renders_as_the_system_alone():
    assert str(Address(system="orders")) == "tapio://orders"


def test_an_address_with_a_host_renders_both():
    address = Address(system="orders", host="10.0.0.4", port=25520)
    assert str(address) == "tapio://orders@10.0.0.4:25520"


def test_an_address_round_trips_through_its_string_form():
    address = Address(system="orders", host="orders.svc", port=25520)
    assert Address.parse(str(address)) == address


def test_an_unaddressable_address_round_trips_too():
    assert Address.parse("tapio://orders") == Address(system="orders")


def test_an_ipv6_literal_survives_the_round_trip():
    address = Address(system="orders", host="[::1]", port=25520)
    assert Address.parse(str(address)) == address


def test_a_host_without_a_port_is_refused():
    # Half an address would render as something a peer can parse but not dial.
    with pytest.raises(ValueError, match="host and a port together"):
        Address(system="orders", host="10.0.0.4")


def test_a_port_without_a_host_is_refused():
    with pytest.raises(ValueError, match="host and a port together"):
        Address(system="orders", port=25520)


def test_a_port_outside_the_range_is_refused():
    with pytest.raises(ValueError, match="invalid port"):
        Address(system="orders", host="10.0.0.4", port=99999)


def test_a_system_name_no_path_could_hold_is_refused():
    # A system name is checked with the path rules, not a second set that
    # could drift away from them.
    with pytest.raises(ValueError, match="invalid actor system name"):
        Address(system="not a name")


def test_parsing_something_that_is_not_an_address_names_the_input():
    with pytest.raises(ValueError, match="not an actor system address"):
        Address.parse("http://orders")


def test_a_ref_renders_as_address_path_and_uid():
    address = Address(system="orders", host="10.0.0.4", port=25520)
    path = ActorPath.root("orders").child("user").child("checkout", uid=7)
    assert format_ref(address, path) == "tapio://orders@10.0.0.4:25520/user/checkout#7"


def test_a_ref_from_a_system_with_remoting_off_carries_no_host():
    path = ActorPath.root("orders").child("user").child("checkout", uid=7)
    assert (
        format_ref(Address(system="orders"), path) == "tapio://orders/user/checkout#7"
    )


def test_a_ref_round_trips_through_its_string_form():
    address = Address(system="orders", host="10.0.0.4", port=25520)
    path = ActorPath.root("orders").child("user").child("checkout", uid=7)
    assert parse_ref(format_ref(address, path)) == (address, path)


def test_an_unaddressable_ref_round_trips_too():
    path = ActorPath.root("orders").child("user").child("checkout", uid=7)
    address = Address(system="orders")
    assert parse_ref(format_ref(address, path)) == (address, path)


def test_a_root_path_round_trips():
    address = Address(system="orders")
    assert parse_ref(format_ref(address, ActorPath.root("orders"))) == (
        address,
        ActorPath.root("orders"),
    )


def test_uid_zero_is_left_out_and_comes_back_as_zero():
    # Uid 0 means "no incarnation", so it is absent rather than written down.
    address = Address(system="orders")
    path = ActorPath.root("orders").child("user")
    assert format_ref(address, path) == "tapio://orders/user"
    assert parse_ref("tapio://orders/user")[1].uid == 0


def test_an_address_and_a_path_from_different_systems_are_refused():
    # Rendering this would name one system in the address and another in the
    # path.
    address = Address(system="orders")
    with pytest.raises(ValueError, match="cannot address"):
        format_ref(address, ActorPath.root("inventory").child("user"))


def test_parsing_something_that_is_not_a_ref_says_what_the_form_is():
    with pytest.raises(ValueError, match=r"tapio://system\[@host:port\]"):
        parse_ref("/user/checkout")


def test_parsing_a_ref_whose_path_holds_an_illegal_name_is_refused():
    # A name off the wire is checked like a name passed to `spawn`.
    with pytest.raises(ValueError, match="invalid actor name"):
        parse_ref("tapio://orders/user/bad!name")
