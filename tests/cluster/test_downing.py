"""The downing strategies, as functions over a view (and, for the lease, a lock)."""

from collections.abc import Iterable, Sequence

import pytest

from tapio.cluster.downing import (
    DownAll,
    DownStrategy,
    KeepMajority,
    KeepOldest,
    LeaseMajority,
    LocalLease,
    StaticQuorum,
)
from tapio.cluster.gossip import Gossip
from tapio.cluster.member import Member, MemberStatus
from tapio.cluster.reachability import (
    Reachability,
    ReachabilityRecord,
    ReachabilityStatus,
)

# A pool of real addresses, sorted so "the lowest address" is A and the ring
# order is predictable in the tie-break tests.
A = "tapio://n@127.0.0.1:2551"
B = "tapio://n@127.0.0.1:2552"
C = "tapio://n@127.0.0.1:2553"
D = "tapio://n@127.0.0.1:2554"
E = "tapio://n@127.0.0.1:2555"


def up(address: str, *, up_number: int = 1, roles: Iterable[str] = ()) -> Member:
    """A member that has been accepted, which is what a downing decision is about."""
    return Member(
        address=address,
        uid=1,
        status=MemberStatus.UP,
        up_number=up_number,
        roles=frozenset(roles),
    )


def view(members: Sequence[Member], *, unreachable: Iterable[str] = ()) -> Gossip:
    """A view in which some members are seen unreachable by one of the others.

    The observer is the first member that is not itself unreachable, since a
    real observation is made by a node that can still report it. Which node
    observes changes nothing the strategies read: they look at who is observed.
    """
    gone = frozenset(unreachable)
    observer = next(m.address for m in members if m.address not in gone)
    records = tuple(
        ReachabilityRecord(
            observer=observer,
            observed=address,
            status=ReachabilityStatus.UNREACHABLE,
        )
        for address in gone
    )
    return Gossip(members=tuple(members), reachability=Reachability(records=records))


def test_a_strategy_is_a_down_strategy() -> None:
    # The protocol is runtime-checkable, so the export list is enforceable.
    strategies = (
        DownAll(),
        KeepMajority(),
        StaticQuorum(size=1),
        KeepOldest(),
        LeaseMajority(lease=LocalLease()),
    )
    for strategy in strategies:
        assert isinstance(strategy, DownStrategy)


@pytest.mark.parametrize(
    "strategy",
    [
        DownAll(),
        KeepMajority(),
        StaticQuorum(size=2),
        KeepOldest(),
        LeaseMajority(lease=LocalLease()),
    ],
)
async def test_nothing_is_downed_without_an_unreachable_member(
    strategy: DownStrategy,
) -> None:
    # No split means no decision: downing is only ever a response to silence.
    state = view([up(A), up(B), up(C)])
    assert await strategy.decide(state) == frozenset()


async def test_down_all_downs_every_member_on_a_split() -> None:
    state = view([up(A), up(B), up(C)], unreachable=[C])
    assert await DownAll().decide(state) == frozenset({A, B, C})


async def test_keep_majority_downs_the_smaller_side() -> None:
    # Three reachable against two unreachable: the two are downed.
    state = view([up(A), up(B), up(C), up(D), up(E)], unreachable=[D, E])
    assert await KeepMajority().decide(state) == frozenset({D, E})


async def test_keep_majority_downs_this_side_when_it_is_the_minority() -> None:
    # This node sees itself among two, against three it cannot hear. Being the
    # minority means downing its own side, which is what self-down is.
    state = view([up(A), up(B), up(C), up(D), up(E)], unreachable=[C, D, E])
    assert await KeepMajority().decide(state) == frozenset({A, B})


async def test_keep_majority_breaks_a_tie_by_the_lowest_address() -> None:
    # Two against two. A is the lowest address, so the side holding A is kept
    # and the other side is downed.
    kept_here = view([up(A), up(B), up(C), up(D)], unreachable=[C, D])
    assert await KeepMajority().decide(kept_here) == frozenset({C, D})

    # The mirror: this node is on the side without A, so its own side loses.
    kept_there = view([up(A), up(B), up(C), up(D)], unreachable=[A, B])
    assert await KeepMajority().decide(kept_there) == frozenset({C, D})


async def test_keep_majority_counts_only_the_role_but_downs_the_whole_side() -> None:
    # Reachable side has one database, unreachable side has two. Counting
    # databases the unreachable side is the majority, so the reachable side
    # loses, and it loses whole: its web node goes down with its database.
    members = [
        up(A, roles=["db"]),
        up(B, roles=["web"]),
        up(C, roles=["db"]),
        up(D, roles=["db"]),
    ]
    state = view(members, unreachable=[C, D])
    assert await KeepMajority(role="db").decide(state) == frozenset({A, B})


async def test_keep_majority_downs_all_when_a_role_names_nobody() -> None:
    # A role no member carries leaves no side to keep, so nothing survives.
    state = view([up(A), up(B), up(C)], unreachable=[C])
    assert await KeepMajority(role="db").decide(state) == frozenset({A, B, C})


async def test_static_quorum_keeps_the_side_that_alone_reaches_it() -> None:
    state = view([up(A), up(B), up(C), up(D), up(E)], unreachable=[D, E])
    assert await StaticQuorum(size=3).decide(state) == frozenset({D, E})


async def test_static_quorum_downs_this_side_when_only_the_other_reaches_it() -> None:
    # Two reachable, below a quorum of three, against three that reach it. This
    # side downs itself, which is what self-down is.
    state = view([up(A), up(B), up(C), up(D), up(E)], unreachable=[C, D, E])
    assert await StaticQuorum(size=3).decide(state) == frozenset({A, B})


async def test_static_quorum_downs_all_when_both_sides_reach_it() -> None:
    # A quorum set below half the cluster lets both sides hold it, which is the
    # misconfiguration the strategy fails loudly on: everyone goes down.
    state = view([up(A), up(B), up(C), up(D)], unreachable=[C, D])
    assert await StaticQuorum(size=2).decide(state) == frozenset({A, B, C, D})


async def test_static_quorum_downs_all_when_neither_side_reaches_it() -> None:
    state = view([up(A), up(B), up(C), up(D)], unreachable=[C, D])
    assert await StaticQuorum(size=3, role="db").decide(state) == frozenset(
        {A, B, C, D}
    )


def test_static_quorum_refuses_a_size_below_one() -> None:
    with pytest.raises(ValueError, match="at least one member"):
        StaticQuorum(size=0)


async def test_keep_oldest_keeps_the_side_with_the_oldest_member() -> None:
    # E was accepted first, so its side is kept and the other side is downed,
    # even though the other side is larger.
    members = [
        up(A, up_number=4),
        up(B, up_number=3),
        up(C, up_number=2),
        up(E, up_number=1),
    ]
    state = view(members, unreachable=[E])
    # The oldest is alone on the unreachable side, so its side is kept and the
    # three reachable members are downed.
    assert await KeepOldest().decide(state) == frozenset({A, B, C})


async def test_keep_oldest_downs_a_lone_oldest_when_told_to() -> None:
    members = [
        up(A, up_number=4),
        up(B, up_number=3),
        up(C, up_number=2),
        up(E, up_number=1),
    ]
    state = view(members, unreachable=[E])
    # With down_if_alone the isolated oldest yields, and the majority lives.
    assert await KeepOldest(down_if_alone=True).decide(state) == frozenset({E})


async def test_keep_oldest_keeps_a_lone_oldest_that_is_not_alone_on_its_side() -> None:
    # down_if_alone only fires when the oldest is truly by itself. Here the
    # oldest keeps company, so its side is kept whatever the flag says.
    members = [
        up(A, up_number=4),
        up(B, up_number=1),
        up(C, up_number=2),
        up(D, up_number=3),
    ]
    state = view(members, unreachable=[A])
    assert await KeepOldest(down_if_alone=True).decide(state) == frozenset({A})


async def test_keep_oldest_chooses_the_oldest_within_a_role() -> None:
    # B is older overall, but only databases are eligible to be the oldest, and
    # the oldest database D is on the unreachable side, so this side is downed.
    members = [
        up(A, up_number=3, roles=["db"]),
        up(B, up_number=1, roles=["web"]),
        up(C, up_number=4, roles=["web"]),
        up(D, up_number=2, roles=["db"]),
    ]
    state = view(members, unreachable=[C, D])
    assert await KeepOldest(role="db").decide(state) == frozenset({A, B})


async def test_lease_majority_keeps_the_side_that_takes_the_lease() -> None:
    # An even split has no majority, so a lease decides it. Both sides race for
    # the same lock, and it admits one owner: the side that named it wins and
    # downs the other, the side that could not downs itself. Both sides name
    # the same losers, which is the agreement the count could not reach.
    lease = LocalLease()
    strategy = LeaseMajority(lease=lease)
    from_a_side = view([up(A), up(B), up(C), up(D)], unreachable=[C, D])
    from_c_side = view([up(A), up(B), up(C), up(D)], unreachable=[A, B])

    assert await strategy.decide(from_a_side) == frozenset({C, D})
    assert await strategy.decide(from_c_side) == frozenset({C, D})


async def test_lease_majority_downs_this_side_when_another_owner_holds_it() -> None:
    lease = LocalLease()
    await lease.acquire("some other side")
    strategy = LeaseMajority(lease=lease)
    state = view([up(A), up(B), up(C), up(D)], unreachable=[C, D])

    # This side cannot take a lease somebody else holds, so it downs itself.
    assert await strategy.decide(state) == frozenset({A, B})


async def test_lease_majority_lets_a_whole_side_share_the_lease() -> None:
    # Two nodes on the winning side name it with the same owner, so the lease's
    # re-entrancy lets both hold it. A side agrees with itself, and neither node
    # of it downs the other.
    lease = LocalLease()
    strategy = LeaseMajority(lease=lease)
    one_node = view([up(A), up(B), up(C), up(D)], unreachable=[C, D])
    its_neighbour = view([up(A), up(B), up(C), up(D)], unreachable=[C, D])

    assert await strategy.decide(one_node) == frozenset({C, D})
    assert await strategy.decide(its_neighbour) == frozenset({C, D})


@pytest.mark.parametrize(
    "strategy",
    [
        KeepMajority(),
        KeepMajority(role="db"),
        StaticQuorum(size=3),
        KeepOldest(),
        KeepOldest(down_if_alone=True),
        DownAll(),
    ],
)
async def test_both_sides_of_a_partition_down_the_same_members(
    strategy: DownStrategy,
) -> None:
    # The safety property. A split into {A, B} and {C, D, E} is seen from each
    # side as a mirror of the other, and every node runs the same strategy on
    # its own view. A strategy is only safe if both sides name the same losers
    # from those mirror-image views, because no message crosses the split to
    # reconcile a disagreement.
    members = [
        up(A, up_number=1, roles=["db"]),
        up(B, up_number=2, roles=["web"]),
        up(C, up_number=3, roles=["db"]),
        up(D, up_number=4, roles=["db"]),
        up(E, up_number=5, roles=["web"]),
    ]
    seen_from_small = view(members, unreachable=[C, D, E])
    seen_from_large = view(members, unreachable=[A, B])
    assert await strategy.decide(seen_from_small) == await strategy.decide(
        seen_from_large
    )
