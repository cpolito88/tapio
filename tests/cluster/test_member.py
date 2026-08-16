"""Members, and the lattice their statuses are merged by."""

import pytest
from hypothesis import given
from pydantic import ValidationError

from tapio.cluster.member import Member, MemberStatus
from tests.cluster.strategies import ADDRESSES, members

ALPHA, BETA, _ = ADDRESSES


def test_a_member_is_identified_by_its_address_and_its_incarnation():
    # A restart at the same host and port is a different member, which is the
    # whole reason the uid travels in the handshake.
    assert Member(address=ALPHA, uid=1).key != Member(address=ALPHA, uid=2).key


def test_a_member_starts_out_joining_and_unnumbered():
    member = Member(address=ALPHA, uid=1)

    assert member.status is MemberStatus.JOINING
    assert member.up_number == 0


def test_an_address_that_is_not_an_address_is_rejected_where_it_is_written():
    with pytest.raises(ValidationError, match="not an actor system address"):
        Member(address="alpha", uid=1)


def test_an_address_with_no_host_to_dial_is_rejected_too():
    # It parses, since that is how a system with remoting switched off writes
    # its own refs down, and resolving one raises rather than dead-lettering.
    # A cluster reaches its members by dialling them, so a member that names
    # nowhere to send to is refused at the edge instead.
    with pytest.raises(ValidationError, match="no host to dial"):
        Member(address="tapio://ghost", uid=1)


def test_the_statuses_are_ordered_the_way_the_merge_needs():
    ranks = [
        Member(address=ALPHA, uid=1, status=status).rank for status in MemberStatus
    ]

    assert ranks == sorted(ranks)
    assert len(set(ranks)) == len(ranks)


def test_a_status_may_move_up_the_lattice():
    member = Member(address=ALPHA, uid=1).with_status(MemberStatus.UP, up_number=3)

    assert member.status is MemberStatus.UP
    assert member.up_number == 3


def test_a_status_may_not_move_back_down_it():
    member = Member(address=ALPHA, uid=1, status=MemberStatus.DOWN)

    with pytest.raises(ValueError, match="only ever move up"):
        member.with_status(MemberStatus.UP)


def test_an_accepted_member_keeps_its_number_through_later_transitions():
    member = Member(address=ALPHA, uid=1).with_status(MemberStatus.UP, up_number=7)

    assert member.with_status(MemberStatus.LEAVING).up_number == 7


def test_merging_two_views_takes_the_higher_status():
    joining = Member(address=ALPHA, uid=1)
    leaving = joining.with_status(MemberStatus.UP).with_status(MemberStatus.LEAVING)

    assert joining.merge(leaving).status is MemberStatus.LEAVING
    assert leaving.merge(joining).status is MemberStatus.LEAVING


def test_merging_unions_the_roles_and_keeps_the_number_that_was_assigned():
    left = Member(address=ALPHA, uid=1, roles=frozenset({"web"}))
    right = Member(address=ALPHA, uid=1, roles=frozenset({"batch"}), up_number=2)

    merged = left.merge(right)

    assert merged.roles == frozenset({"web", "batch"})
    assert merged.up_number == 2


def test_two_different_members_cannot_be_merged():
    with pytest.raises(ValueError, match="different members"):
        Member(address=ALPHA, uid=1).merge(Member(address=BETA, uid=1))


@given(members, members)
def test_merging_is_commutative(left: Member, right: Member):
    same = right.model_copy(update={"address": left.address, "uid": left.uid})

    assert left.merge(same) == same.merge(left)


@given(members, members, members)
def test_merging_is_associative(a: Member, b: Member, c: Member):
    b = b.model_copy(update={"address": a.address, "uid": a.uid})
    c = c.model_copy(update={"address": a.address, "uid": a.uid})

    assert a.merge(b).merge(c) == a.merge(b.merge(c))


@given(members)
def test_merging_is_idempotent(member: Member):
    assert member.merge(member) == member
