"""Hypothesis strategies for gossip states, shared by the property tests.

The pool of addresses is deliberately tiny. What has to be exercised is two
views of the *same* member disagreeing, and a strategy that invented a fresh
address every time would almost never produce that.
"""

from hypothesis import strategies as st

from tapio.cluster.clock import VectorClock
from tapio.cluster.gossip import Gossip
from tapio.cluster.member import Member, MemberStatus
from tapio.cluster.reachability import (
    Reachability,
    ReachabilityRecord,
    ReachabilityStatus,
)

ADDRESSES = [
    "tapio://alpha@127.0.0.1:2551",
    "tapio://beta@127.0.0.1:2552",
    "tapio://gamma@127.0.0.1:2553",
]

addresses = st.sampled_from(ADDRESSES)
uids = st.integers(min_value=1, max_value=2)
roles = st.frozensets(st.sampled_from(["worker", "web", "batch"]), max_size=2)

members = st.builds(
    Member,
    address=addresses,
    uid=uids,
    status=st.sampled_from(list(MemberStatus)),
    roles=roles,
    up_number=st.integers(min_value=0, max_value=3),
)

records = st.builds(
    ReachabilityRecord,
    observer=addresses,
    observed=addresses,
    status=st.sampled_from(list(ReachabilityStatus)),
    version=st.integers(min_value=1, max_value=3),
)

clocks = st.builds(
    VectorClock,
    counters=st.dictionaries(
        addresses, st.integers(min_value=0, max_value=3), max_size=3
    ),
)


@st.composite
def reachabilities(draw: st.DrawFn) -> Reachability:
    """Build a table with at most one record per observer and observed pair."""
    drawn = draw(st.lists(records, max_size=4))
    unique = {record.pair: record for record in drawn}
    return Reachability(records=tuple(unique.values()))


@st.composite
def gossips(draw: st.DrawFn) -> Gossip:
    """Build a gossip state with at most one record per member."""
    drawn = draw(st.lists(members, max_size=4))
    unique = {member.key: member for member in drawn}
    return Gossip(
        members=tuple(unique.values()),
        reachability=draw(reachabilities()),
        version=draw(clocks),
        seen=draw(st.frozensets(addresses, max_size=3)),
    )
