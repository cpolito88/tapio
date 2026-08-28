"""Who can hear whom, and how an observer takes back what it said."""

from hypothesis import given

from tapio.cluster.reachability import Reachability, ReachabilityStatus
from tests.cluster.strategies import ADDRESSES, reachabilities

ALPHA, BETA, GAMMA = ADDRESSES
UNREACHABLE = ReachabilityStatus.UNREACHABLE
REACHABLE = ReachabilityStatus.REACHABLE


def test_silence_is_not_evidence_of_a_problem():
    # Nothing recorded means everyone can hear everyone, which is what a
    # cluster that has never had a failure looks like.
    table = Reachability.empty()

    assert table.says(ALPHA, BETA) is REACHABLE
    assert table.is_reachable(BETA)
    assert table.unreachable == frozenset()


def test_one_observer_is_enough_to_make_a_node_unreachable():
    table = Reachability().observing(ALPHA, BETA, UNREACHABLE)

    assert not table.is_reachable(BETA)
    assert table.is_reachable(ALPHA)
    assert table.unreachable == frozenset({BETA})


def test_an_observation_by_a_stale_observer_can_be_filtered_out():
    # A member that made an observation and was then downed can never retract
    # it, so the reader filters by who still counts: pass the live observers
    # and a dead node's claim is skipped, both when asking about one node and
    # when listing them all.
    table = (
        Reachability()
        .observing(ALPHA, GAMMA, UNREACHABLE)
        .observing(BETA, GAMMA, UNREACHABLE)
    )
    live = frozenset({BETA, GAMMA})

    # ALPHA is gone, so only BETA's claim about GAMMA still counts.
    assert not table.is_reachable(GAMMA, live)
    assert table.unreachable_among(live) == frozenset({GAMMA})

    # With ALPHA the only claimant gone, GAMMA is reachable again.
    only_alpha = Reachability().observing(ALPHA, GAMMA, UNREACHABLE)
    assert only_alpha.is_reachable(GAMMA, live)
    assert only_alpha.unreachable_among(live) == frozenset()
    # Unfiltered, the record still stands: the filter is the caller's choice.
    assert not only_alpha.is_reachable(GAMMA)
    assert only_alpha.unreachable == frozenset({GAMMA})


def test_an_observer_can_take_back_what_it_said():
    table = Reachability().observing(ALPHA, BETA, UNREACHABLE)

    healed = table.observing(ALPHA, BETA, REACHABLE)

    assert healed.is_reachable(BETA)
    # The retraction has to beat the claim wherever the two meet, so it
    # carries a higher version rather than relying on arriving later.
    assert healed.records[0].version == 2


def test_a_retraction_wins_over_the_claim_it_retracts_in_either_order():
    claim = Reachability().observing(ALPHA, BETA, UNREACHABLE)
    retraction = claim.observing(ALPHA, BETA, REACHABLE)

    assert claim.merge(retraction).is_reachable(BETA)
    assert retraction.merge(claim).is_reachable(BETA)


def test_one_observer_retracting_does_not_speak_for_another():
    table = (
        Reachability()
        .observing(ALPHA, GAMMA, UNREACHABLE)
        .observing(BETA, GAMMA, UNREACHABLE)
    )

    half_healed = table.observing(ALPHA, GAMMA, REACHABLE)

    assert not half_healed.is_reachable(GAMMA)


def test_a_forgotten_observation_is_filtered_not_deleted_so_a_merge_cannot_undo_it():
    # An observation by a member that is gone is ignored, not removed. Left in
    # the table, it survives a merge with a peer that still holds it, which a
    # deletion would not: reachability is judged on who is alive, and BETA is
    # not, so its claim about GAMMA never counts however the tables meet.
    table = Reachability().observing(BETA, GAMMA, UNREACHABLE)
    live = frozenset({ALPHA, GAMMA})

    merged = table.merge(Reachability().observing(BETA, GAMMA, UNREACHABLE))

    assert merged.says(BETA, GAMMA) is UNREACHABLE
    assert merged.is_reachable(GAMMA, live)
    assert merged.unreachable_among(live) == frozenset()


def test_an_unreachable_record_wins_a_tie_on_version():
    # Two nodes cannot honestly produce different records for one pair at one
    # version, so this only decides a corrupt case. It decides it towards
    # blocking convergence rather than towards pretending all is well.
    claim = Reachability().observing(ALPHA, BETA, UNREACHABLE)
    denial = Reachability().observing(ALPHA, BETA, REACHABLE)

    assert not claim.merge(denial).is_reachable(BETA)
    assert not denial.merge(claim).is_reachable(BETA)


@given(reachabilities(), reachabilities())
def test_merging_is_commutative(left: Reachability, right: Reachability):
    assert left.merge(right) == right.merge(left)


@given(reachabilities(), reachabilities(), reachabilities())
def test_merging_is_associative(a: Reachability, b: Reachability, c: Reachability):
    assert a.merge(b).merge(c) == a.merge(b.merge(c))


@given(reachabilities(), reachabilities())
def test_merging_is_idempotent(left: Reachability, right: Reachability):
    once = left.merge(right)

    assert once.merge(right) == once
