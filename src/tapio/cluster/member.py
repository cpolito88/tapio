"""Members and their statuses, and the lattice the statuses are merged by.

A member is one system in the cluster, identified by its address and its
incarnation uid together. A restart at the same host and port is a different
member, which is the whole reason the uid travels in the handshake.

The status lattice is the part to read carefully. Two nodes that have seen
different things about the same member must agree on what they now believe
without asking anyone, so the statuses are ordered and a merge takes the
higher one. That makes the merge a join, and it makes progress one-way: a node
that learns of a `Down` can never un-learn it, however old the gossip that
told it.
"""

from enum import StrEnum
from typing import Annotated, Final, final

from pydantic import AfterValidator

from tapio.message import Message
from tapio.remote.address import Address

__all__ = ["AddressStr", "Member", "MemberStatus", "rank_of", "sort_key"]


def _parses_as_an_address(text: str) -> str:
    """Check that a string is an address, since it will be dialled later.

    Args:
        text: The candidate.

    Returns:
        The same string.

    Raises:
        ValueError: If it is not an address, which Pydantic reports as a
            validation error naming the field.
    """
    Address.parse(text)
    return text


AddressStr = Annotated[str, AfterValidator(_parses_as_an_address)]
"""An address in its string form, which is how one travels in a cluster message.

Addresses are strings here rather than [Address][tapio.remote.address.Address]
objects for two reasons. They sort, and "the first member in sorted address
order" is how the leader is chosen, so a total order over the exact bytes both
nodes hold is what the rule needs. And a frame stays readable, which is the
same reason [PeerUnreachable][tapio.remote.failure.PeerUnreachable] carries
one.
"""


class MemberStatus(StrEnum):
    """Where a member is in its life, from joining to gone.

    The values are ordered, low to high, in the order they are declared here,
    and a merge takes the higher one: see
    [Member.rank][tapio.cluster.member.Member.rank]. `WeaklyUp`, which Akka has between
    `Joining` and `Up`, is deliberately absent: it lets a node join while
    another member is unreachable, at the cost of a member that half the
    cluster has agreed on, and every feature that places something has to know
    about it. It is worth adding when somebody has the problem it solves.
    """

    JOINING = "joining"
    """Contacted a seed, not yet accepted by the leader."""

    UP = "up"
    """A full member."""

    LEAVING = "leaving"
    """A graceful exit was asked for, and has not finished."""

    EXITING = "exiting"
    """Leaving, and every node has seen it, so the handoff may run."""

    DOWN = "down"
    """Declared dead. A member that reaches this may not return."""

    REMOVED = "removed"
    """Gone, and kept only as a tombstone so gossip cannot resurrect it."""


_RANK: Final[dict[MemberStatus, int]] = {
    MemberStatus.JOINING: 0,
    MemberStatus.UP: 1,
    MemberStatus.LEAVING: 2,
    MemberStatus.EXITING: 3,
    MemberStatus.DOWN: 4,
    MemberStatus.REMOVED: 5,
}


def rank_of(status: MemberStatus) -> int:
    """Return where a status sits in the lattice.

    Args:
        status: The status to place.

    Returns:
        Its rank. Higher wins a merge, and a member never moves to a lower
        one.
    """
    return _RANK[status]


@final
class Member(Message):
    """One system in the cluster, as every other system sees it."""

    address: AddressStr
    """Where the system is, and what it sorts by."""

    uid: int
    """Its incarnation. A restart at the same address is a different member."""

    status: MemberStatus = MemberStatus.JOINING
    """Where it is in its life."""

    roles: frozenset[str] = frozenset()
    """What it says it is for. Every cluster-aware feature filters on these."""

    up_number: int = 0
    """The order it was accepted in, which is what "oldest member" means.

    Zero until the leader accepts it, so a member that is still `Joining` has
    no place in that order yet.
    """

    @property
    def key(self) -> tuple[str, int]:
        """What identifies this member: its address and its incarnation."""
        return (self.address, self.uid)

    @property
    def rank(self) -> int:
        """Where this member's status sits in the lattice."""
        return rank_of(self.status)

    def with_status(self, status: MemberStatus, *, up_number: int = 0) -> "Member":
        """Return this member at a new status.

        It refuses to move backwards, because the lattice is the merge rule
        and a transition that contradicted it would be undone by the next
        gossip that arrived.

        Args:
            status: Where the member is moving to.
            up_number: The order it was accepted in, when the leader is
                accepting it. Kept as it was when zero.

        Returns:
            The member at the new status.

        Raises:
            ValueError: If the new status is below the current one.
        """
        if rank_of(status) < self.rank:
            msg = (
                f"cannot move {self.address} from {self.status} back to "
                f"{status}: member statuses only ever move up the lattice, and "
                "a merge would restore the higher one anyway"
            )
            raise ValueError(msg)
        return self.model_copy(
            update={
                "status": status,
                "up_number": up_number if up_number else self.up_number,
            }
        )

    def merge(self, other: "Member") -> "Member":
        """Return what two views of the same member agree on.

        The higher status wins, roles are unioned, and the higher `up_number`
        wins because zero means "not yet accepted". Each of those is a join,
        so the whole thing is one: order does not matter, and merging twice
        changes nothing.

        Args:
            other: The other view. It must name the same member.

        Returns:
            The merged member.

        Raises:
            ValueError: If the two are not the same member.
        """
        if self.key != other.key:
            msg = (
                f"cannot merge {self.address}#{self.uid} with "
                f"{other.address}#{other.uid}: they are different members"
            )
            raise ValueError(msg)
        winner = self if self.rank >= other.rank else other
        return Member(
            address=self.address,
            uid=self.uid,
            status=winner.status,
            roles=self.roles | other.roles,
            up_number=max(self.up_number, other.up_number),
        )

    def __repr__(self) -> str:
        """Render the address, incarnation and status, in that order."""
        return f"Member({self.address}#{self.uid} {self.status})"


def sort_key(member: Member) -> tuple[str, int]:
    """Order members the way the leader rule and the wire format expect.

    Address first, since that is the order the leader is chosen in, then the
    incarnation so that two records for one address still sort deterministically.

    Args:
        member: The member to place.

    Returns:
        Its sort key.
    """
    return member.key
