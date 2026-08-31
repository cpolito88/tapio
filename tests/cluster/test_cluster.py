"""The read-side of a node's own membership, tested without standing up sockets."""

from types import SimpleNamespace

from tapio.cluster.cluster import Cluster
from tapio.cluster.gossip import Gossip
from tapio.cluster.member import Member, MemberStatus

# Addresses chosen so address order (A before B) and seniority disagree: the
# older member sits at the higher address.
A = "tapio://n@127.0.0.1:2551"
B = "tapio://n@127.0.0.1:2552"


def worker(address: str, *, up_number: int) -> Member:
    """A worker member accepted at a known point in the join order."""
    return Member(
        address=address,
        uid=1,
        status=MemberStatus.UP,
        up_number=up_number,
        roles=frozenset({"worker"}),
    )


def _cluster_viewing(*members: Member) -> Cluster:
    """A Cluster whose only live part is the membership view it reads.

    `members_with_role` reads `self._daemon.state.alive` and nothing else, so a
    stub daemon holding one gossip value pins down its ordering without a real
    join. The daemon keeps members in address order, so the view is built that
    way too, which is the order the method has to re-sort away from.
    """
    view = Gossip(members=tuple(sorted(members, key=lambda m: m.address)))
    cluster = object.__new__(Cluster)
    cluster._daemon = SimpleNamespace(state=view)  # type: ignore[assignment]
    return cluster


def test_members_with_role_is_oldest_first() -> None:
    # The older member (up_number 1) sits at the higher address, so address
    # order would return it second. Seniority has to put it first.
    older = worker(B, up_number=1)
    newer = worker(A, up_number=2)

    cluster = _cluster_viewing(newer, older)

    assert [m.address for m in cluster.members_with_role("worker")] == [
        older.address,
        newer.address,
    ]
