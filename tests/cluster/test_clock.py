"""Vector clocks, and the partial order they put gossip states in."""

import pytest
from hypothesis import given
from pydantic import ValidationError

from tapio.cluster.clock import Ordering, VectorClock
from tests.cluster.strategies import ADDRESSES, clocks

ALPHA, BETA, GAMMA = ADDRESSES


def test_a_new_clock_has_seen_nothing():
    assert VectorClock.empty().counters == {}


def test_incrementing_moves_one_node_on_and_leaves_the_rest():
    clock = VectorClock().increment(ALPHA).increment(ALPHA).increment(BETA)

    assert clock.counters == {ALPHA: 2, BETA: 1}


def test_a_clock_that_has_seen_everything_the_other_has_is_after_it():
    older = VectorClock().increment(ALPHA)
    newer = older.increment(BETA)

    assert newer.compare(older) is Ordering.AFTER
    assert older.compare(newer) is Ordering.BEFORE


def test_two_clocks_that_each_saw_something_the_other_missed_are_concurrent():
    left = VectorClock().increment(ALPHA)
    right = VectorClock().increment(BETA)

    assert left.compare(right) is Ordering.CONCURRENT
    assert right.compare(left) is Ordering.CONCURRENT


def test_equal_clocks_are_the_same_however_they_were_built():
    left = VectorClock().increment(ALPHA).increment(BETA)
    right = VectorClock().increment(BETA).increment(ALPHA)

    assert left.compare(right) is Ordering.SAME
    assert left == right


def test_a_node_that_has_said_nothing_is_absent_rather_than_zero():
    # Otherwise the same clock would compare unequal depending on which peer
    # happened to send it, and the merge would stop being one function.
    assert VectorClock(counters={ALPHA: 0}) == VectorClock()
    assert VectorClock(counters={ALPHA: 0}).merge(VectorClock()) == VectorClock()


def test_a_negative_counter_is_a_corrupt_frame():
    with pytest.raises(ValidationError, match="cannot go negative"):
        VectorClock(counters={ALPHA: -1})


def test_merging_takes_the_higher_count_for_every_node():
    left = VectorClock(counters={ALPHA: 3, BETA: 1})
    right = VectorClock(counters={BETA: 5, GAMMA: 2})

    assert left.merge(right).counters == {ALPHA: 3, BETA: 5, GAMMA: 2}


@given(clocks, clocks)
def test_merging_is_commutative(left: VectorClock, right: VectorClock):
    assert left.merge(right) == right.merge(left)


@given(clocks, clocks, clocks)
def test_merging_is_associative(a: VectorClock, b: VectorClock, c: VectorClock):
    assert a.merge(b).merge(c) == a.merge(b.merge(c))


@given(clocks, clocks)
def test_merging_is_idempotent(left: VectorClock, right: VectorClock):
    once = left.merge(right)

    assert once.merge(right) == once


@given(clocks, clocks)
def test_a_merge_is_at_least_as_new_as_both_of_its_arguments(
    left: VectorClock, right: VectorClock
):
    merged = left.merge(right)

    assert merged.compare(left) in (Ordering.AFTER, Ordering.SAME)
    assert merged.compare(right) in (Ordering.AFTER, Ordering.SAME)


def test_a_clock_renders_its_counters_in_node_order():
    clock = VectorClock().increment(BETA).increment(ALPHA)

    assert repr(clock) == f"VectorClock({ALPHA}=1, {BETA}=1)"
