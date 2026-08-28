"""The merge, the leader and convergence.

The three merge laws are asserted with Hypothesis rather than with examples,
because a merge that is wrong in one order out of six is exactly the bug that
example tests miss and that production finds as two live singletons.
"""

import random

from hypothesis import given, settings

from tapio.cluster.clock import VectorClock
from tapio.cluster.gossip import Gossip, leader_actions
from tapio.cluster.member import Member, MemberStatus
from tapio.cluster.reachability import Reachability, ReachabilityStatus
from tests.cluster.strategies import ADDRESSES, gossips

ALPHA, BETA, GAMMA = ADDRESSES


def up(address: str, *, uid: int = 1, up_number: int = 1) -> Member:
    """An `Up` member, which is what most of these tests are made of."""
    return Member(address=address, uid=uid, status=MemberStatus.UP, up_number=up_number)


@given(gossips(), gossips())
def test_merging_is_commutative(left: Gossip, right: Gossip):
    assert left.merge(right) == right.merge(left)


@given(gossips(), gossips(), gossips())
def test_merging_is_associative(a: Gossip, b: Gossip, c: Gossip):
    assert a.merge(b).merge(c) == a.merge(b.merge(c))


@given(gossips(), gossips())
def test_merging_is_idempotent(left: Gossip, right: Gossip):
    once = left.merge(right)

    assert once.merge(right) == once
    assert once.merge(once) == once


@settings(max_examples=200)
@given(gossips(), gossips(), gossips())
def test_the_order_gossip_arrives_in_does_not_change_where_it_ends_up(
    a: Gossip, b: Gossip, c: Gossip
):
    # The property the whole design rests on: every node sees the same
    # gossip in a different order, and every node has to end up believing the
    # same thing. Duplicates are in the sequence because a node really does
    # receive the same state twice.
    sequence = [a, b, c, a, c]
    expected = sequence[0]
    for state in sequence[1:]:
        expected = expected.merge(state)

    for seed in range(5):
        shuffled = list(sequence)
        random.Random(seed).shuffle(shuffled)
        merged = shuffled[0]
        for state in shuffled[1:]:
            merged = merged.merge(state)
        assert merged == expected


def test_a_status_only_ever_moves_up_the_lattice():
    left = Gossip(members=(up(ALPHA),))
    right = Gossip(members=(up(ALPHA).with_status(MemberStatus.DOWN),))

    merged = left.merge(right)

    # A node that learns of a Down can never un-learn it, whichever side the
    # older view arrives from.
    assert merged.member(ALPHA).status is MemberStatus.DOWN
    assert right.merge(left).member(ALPHA).status is MemberStatus.DOWN


def test_two_incarnations_of_one_address_are_two_members():
    old = up(ALPHA, uid=1)
    new = Member(address=ALPHA, uid=2, status=MemberStatus.JOINING)

    merged = Gossip(members=(old,)).merge(Gossip(members=(new,)))

    # The uid is what tells a restart from a slow node, so the two records
    # stand side by side until something decides about the old one.
    assert len(merged.members) == 2
    assert {m.uid for m in merged.members} == {1, 2}


def test_the_member_at_an_address_is_the_one_furthest_through_its_life():
    slow = Member(address=ALPHA, uid=1, status=MemberStatus.JOINING)
    ahead = up(ALPHA, uid=2)

    state = Gossip(members=(slow, ahead))

    # Both are live, so the question is which one a caller is deciding about,
    # and that is the one that has got further.
    assert state.member(ALPHA).uid == 2
    assert state.member(ALPHA).status is MemberStatus.UP


def test_the_member_at_an_address_is_the_live_one_after_a_restart():
    dead = up(ALPHA, uid=1).with_status(MemberStatus.DOWN)
    running = Member(address=ALPHA, uid=2, status=MemberStatus.JOINING)

    state = Gossip(members=(dead, running))

    # Ranking on status alone would answer with the downed incarnation, which
    # is the record nobody is deciding about any more.
    assert state.member(ALPHA).uid == 2
    assert state.member(ALPHA).status is MemberStatus.JOINING


def test_a_tombstone_never_hides_the_member_that_replaced_it():
    # The tombstone is kept forever, so ranking on status alone would answer
    # with it from the restart onwards. What reads this is the daemon deciding
    # whether a member may start leaving: with the tombstone winning, a node
    # that has ever restarted is refused for ever and `leave` only times out.
    tombstone = up(ALPHA, uid=1).with_status(MemberStatus.REMOVED)
    running = up(ALPHA, uid=2, up_number=3)

    state = Gossip(members=(tombstone, running))

    assert state.member(ALPHA).uid == 2
    assert state.member(ALPHA).status in (MemberStatus.JOINING, MemberStatus.UP)


def test_the_member_at_an_address_falls_back_to_a_tombstone():
    # Nothing is running there, so the tombstone is the honest answer rather
    # than None: the address was a member, and it was removed.
    tombstone = up(ALPHA, uid=1).with_status(MemberStatus.REMOVED)

    state = Gossip(members=(tombstone,))

    assert state.member(ALPHA).status is MemberStatus.REMOVED


def test_primaries_agrees_with_member_at_every_address():
    # primaries builds in one pass what member picks one address at a time, so
    # the two must never disagree: a restart (live wins over the tombstone) and
    # a plain tombstone (the only record there is) both have to come out the
    # same whichever way they are read.
    restarted_dead = up(ALPHA, uid=1).with_status(MemberStatus.DOWN)
    restarted_live = Member(address=ALPHA, uid=2, status=MemberStatus.JOINING)
    removed = up(BETA, uid=1).with_status(MemberStatus.REMOVED)
    plain = up(GAMMA, uid=1)

    state = Gossip(members=(restarted_dead, restarted_live, removed, plain))

    primaries = state.primaries()

    assert set(primaries) == {ALPHA, BETA, GAMMA}
    for address in (ALPHA, BETA, GAMMA):
        assert primaries[address] == state.member(address)


def test_primaries_is_empty_without_members():
    assert Gossip().primaries() == {}


def test_the_leader_is_the_first_up_member_in_address_order():
    state = Gossip(members=(up(GAMMA), up(BETA), up(ALPHA)))

    assert state.leader == ALPHA


def test_a_leaving_member_may_still_lead():
    state = Gossip(members=(up(ALPHA).with_status(MemberStatus.LEAVING), up(BETA)))

    assert state.leader == ALPHA


def test_a_down_member_never_leads():
    state = Gossip(members=(up(ALPHA).with_status(MemberStatus.DOWN), up(BETA)))

    assert state.leader == BETA


def test_an_unreachable_member_never_leads():
    state = Gossip(
        members=(up(ALPHA), up(BETA)),
        reachability=Reachability().observing(
            BETA, ALPHA, ReachabilityStatus.UNREACHABLE
        ),
    )

    assert state.leader == BETA


def test_a_cluster_of_joining_members_still_has_a_leader():
    # The first moment of every cluster. Somebody has to be able to accept the
    # first join, and every node has to pick the same somebody.
    state = Gossip(
        members=(
            Member(address=BETA, uid=1),
            Member(address=ALPHA, uid=1),
        )
    )

    assert state.leader == ALPHA


def test_nobody_leads_an_empty_cluster():
    assert Gossip().leader is None


def test_convergence_needs_every_member_to_have_seen_the_version():
    state = Gossip(members=(up(ALPHA), up(BETA)), seen=frozenset({ALPHA}))

    assert not state.converged
    assert state.seen_by(BETA).converged


def test_an_unreachable_member_blocks_convergence():
    state = Gossip(
        members=(up(ALPHA), up(BETA)),
        seen=frozenset({ALPHA, BETA}),
        reachability=Reachability().observing(
            ALPHA, BETA, ReachabilityStatus.UNREACHABLE
        ),
    )

    # This is what makes downing a decision somebody has to make: until then,
    # the leader is not allowed to act at all.
    assert not state.converged


def test_a_downed_member_neither_blocks_convergence_nor_takes_part_in_it():
    state = Gossip(
        members=(up(ALPHA), up(BETA).with_status(MemberStatus.DOWN)),
        seen=frozenset({ALPHA}),
        reachability=Reachability().observing(
            ALPHA, BETA, ReachabilityStatus.UNREACHABLE
        ),
    )

    assert state.converged


def test_a_downed_members_observation_stops_counting():
    # The mirror of the test above, with the roles swapped: the observer is
    # downed, not the observed. ALPHA said BETA was unreachable and was then
    # downed. Only ALPHA could ever retract that, and it is gone, so left
    # counting it would pin the healthy BETA unreachable for good: the view
    # would never converge, and a downing strategy would be steered to down a
    # member that is answering. Judging reachability on live observers only is
    # what stops that.
    state = Gossip(
        members=(up(ALPHA).with_status(MemberStatus.DOWN), up(BETA)),
        seen=frozenset({BETA}),
        reachability=Reachability().observing(
            ALPHA, BETA, ReachabilityStatus.UNREACHABLE
        ),
    )

    assert state.converged
    assert BETA not in state.unreachable
    assert state.leader == BETA


def test_a_new_version_is_seen_by_nobody_but_its_author():
    state = Gossip(members=(up(ALPHA), up(BETA)), seen=frozenset({ALPHA, BETA}))

    bumped = state.bumped_by(ALPHA)

    assert bumped.seen == frozenset({ALPHA})
    assert not bumped.converged
    assert bumped.version.counters[ALPHA] == 1


def test_merging_the_same_version_pools_who_has_seen_it():
    version = VectorClock().increment(ALPHA)
    left = Gossip(members=(up(ALPHA),), version=version, seen=frozenset({ALPHA}))
    right = Gossip(members=(up(ALPHA),), version=version, seen=frozenset({BETA}))

    assert left.merge(right).seen == frozenset({ALPHA, BETA})


def test_merging_concurrent_versions_starts_the_seen_set_again():
    left = Gossip(
        members=(up(ALPHA),),
        version=VectorClock().increment(ALPHA),
        seen=frozenset({ALPHA}),
    )
    right = Gossip(
        members=(up(BETA),),
        version=VectorClock().increment(BETA),
        seen=frozenset({BETA}),
    )

    merged = left.merge(right)

    # Neither node has seen this state: it did not exist until the merge made
    # it, so claiming otherwise would let the leader act on a view nobody
    # holds.
    assert merged.seen == frozenset()
    assert not merged.converged


def test_an_older_view_does_not_drag_the_seen_set_backwards():
    older = Gossip(members=(up(ALPHA),), version=VectorClock().increment(ALPHA))
    newer = older.bumped_by(ALPHA).seen_by(BETA)

    assert newer.merge(older).seen == frozenset({ALPHA, BETA})


def test_a_founding_member_is_up_and_has_seen_its_own_state():
    state = Gossip.founding(up(ALPHA))

    assert state.converged
    assert state.leader == ALPHA
    assert state.seen == frozenset({ALPHA})


def test_the_leader_promotes_joining_members_in_the_order_it_finds_them():
    state = Gossip(
        members=(
            Member(address=ALPHA, uid=1),
            Member(address=BETA, uid=1),
        )
    )

    moved = leader_actions(state)

    assert [m.status for m in moved.members] == [MemberStatus.UP, MemberStatus.UP]
    # The order they were accepted in is what "oldest member" will mean, so no
    # two members may share a number.
    assert sorted(m.up_number for m in moved.members) == [1, 2]


def test_a_later_joiner_is_numbered_after_the_members_already_up():
    state = Gossip(members=(up(ALPHA, up_number=4), Member(address=BETA, uid=1)))

    moved = leader_actions(state)

    assert moved.member(BETA).up_number == 5


def test_a_member_walks_out_one_step_per_converged_round():
    # Not collapsed into one step even though the leader can see all three at
    # once: each step needs a view every member has seen, which is what gives
    # a handoff somewhere to happen.
    leaving = Gossip(members=(up(ALPHA).with_status(MemberStatus.LEAVING), up(BETA)))

    exiting = leader_actions(leaving)
    assert exiting.member(ALPHA).status is MemberStatus.EXITING

    removed = leader_actions(exiting)
    assert removed.member(ALPHA).status is MemberStatus.REMOVED


def test_a_removed_member_stays_as_a_tombstone():
    state = Gossip(members=(up(ALPHA).with_status(MemberStatus.EXITING), up(BETA)))

    removed = leader_actions(state)

    # Dropping the record would let a peer holding an older view put the
    # member back, since a merge unions the members it is given.
    assert removed.member(ALPHA) is not None
    assert removed.member(ALPHA).status is MemberStatus.REMOVED
    assert [m.address for m in removed.alive] == [BETA]


def test_a_downed_member_is_removed():
    state = Gossip(members=(up(ALPHA).with_status(MemberStatus.DOWN), up(BETA)))

    assert leader_actions(state).member(ALPHA).status is MemberStatus.REMOVED


def test_removing_a_member_forgets_what_was_observed_about_it():
    state = Gossip(
        members=(up(ALPHA).with_status(MemberStatus.EXITING), up(BETA)),
        reachability=Reachability().observing(
            BETA, ALPHA, ReachabilityStatus.UNREACHABLE
        ),
    )

    removed = leader_actions(state)

    # Otherwise the cluster would go on reporting a member unreachable that it
    # has already written off, and block convergence on nobody's behalf.
    assert removed.reachability.records == ()
    assert removed.converged is False or removed.seen == state.seen


def test_the_leader_has_nothing_to_do_in_a_settled_cluster():
    state = Gossip(members=(up(ALPHA), up(BETA)), seen=frozenset({ALPHA, BETA}))

    assert leader_actions(state) == state
