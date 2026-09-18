"""The read-side of a node's own membership, tested without standing up sockets."""

import asyncio
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


def _cluster_waiting(late: Member) -> Cluster:
    """A Cluster whose state moves in the window `_until`'s ordering guards.

    The first read of `self_member` answers "not yet" and, on its way out,
    does what a daemon turn does: moves the state and sets the event. That
    puts the change exactly between the read and the wait, which is the one
    place a wakeup can be lost.
    """
    cluster = object.__new__(Cluster)
    daemon = SimpleNamespace(changed=asyncio.Event())
    cluster._daemon = daemon  # type: ignore[assignment]
    state = {"member": None, "reads": 0}

    def self_member(self: Cluster) -> Member | None:
        state["reads"] += 1
        if state["reads"] == 1:
            state["member"] = late
            daemon.changed.set()
            return None
        return state["member"]

    # Patched on a throwaway subclass, so the property stays a property and no
    # other Cluster is affected.
    cluster.__class__ = type(
        "_Waiting", (Cluster,), {"self_member": property(self_member)}
    )
    return cluster


async def test_until_sees_a_change_that_lands_between_the_check_and_the_wait():
    # The lost-wakeup case, and the reason the clear sits before the read. A
    # turn that settles after the state is read but before the wait begins
    # leaves the event set, so the wait returns at once. Clearing there
    # instead would erase that turn, and this would wait out the full timeout
    # for something that had already happened.
    member = worker(A, up_number=1)
    cluster = _cluster_waiting(member)

    # Generous, so a failure is the ordering rather than a slow machine: the
    # correct ordering returns without ever sleeping.
    assert await cluster._until(MemberStatus.UP, 30.0) == member
