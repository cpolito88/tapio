"""Who can hear whom, which is a different question from who is a member.

Reachability and membership are separate axes, and conflating them is the
classic mistake. A member is `Up` and simultaneously unreachable: the first is
a decision the cluster made, the second is an observation one node made about
another, and only the second can be wrong.

So this is a table of observations, one per pair of nodes, each carrying the
version of the observer that produced it. A merge takes the higher version per
pair, which is what makes an unreachability *retractable*: the observer that
said "unreachable" is the only one that can say "reachable again", and it says
so with a higher number.

What writes into it is [monitor][tapio.cluster.monitor]. Every node watches a
few members, picked by their place on the sorted address ring, and records
what it finds: a member that stops answering its probe, or one the transport
has given up on. So an entry here means one node cannot currently reach
another, which is a weaker claim than the cluster having decided anything:
acting on it is the leader's job, and one unreachable member stops convergence
until somebody does.

The table has no idea where an observation came from, which is what lets the
detector behind it be replaced. A phi-accrual detector changes how a watcher
reaches its conclusion and not what is recorded here.

An observation left by a member that has since been downed or removed is not
deleted, it is ignored. A caller judging reachability passes the observers that
still count, the live members, so a record whose observer is gone stops
counting. Deleting it instead would not survive a merge: the merge is a join
that unions per pair, so any peer still holding the record would put it back.
Forgetting has to be a property of who is alive, which every node computes the
same way, not a removal one node makes and another undoes.
"""

from enum import StrEnum
from typing import final

from tapio.cluster.member import AddressStr
from tapio.message import Message

__all__ = ["Reachability", "ReachabilityRecord", "ReachabilityStatus"]


class ReachabilityStatus(StrEnum):
    """What one node currently believes about another's reachability."""

    REACHABLE = "reachable"
    """Frames are arriving often enough to believe it."""

    UNREACHABLE = "unreachable"
    """They have stopped, and this observer has given up waiting."""


@final
class ReachabilityRecord(Message):
    """One node's current observation about one other node."""

    observer: AddressStr
    """Who is watching."""

    observed: AddressStr
    """Who is being watched."""

    status: ReachabilityStatus
    """What the observer currently believes."""

    version: int = 1
    """The observer's own counter for this pair, so a later view wins."""

    @property
    def pair(self) -> tuple[str, str]:
        """The two nodes this record is about, observer first."""
        return (self.observer, self.observed)

    def __repr__(self) -> str:
        """Render observer, observed and belief, which is the whole record."""
        return (
            f"ReachabilityRecord({self.observer} sees {self.observed} "
            f"{self.status} @{self.version})"
        )


@final
class Reachability(Message):
    """Every current observation, as one mergeable value."""

    records: tuple[ReachabilityRecord, ...] = ()
    """One record per observer and observed pair, in a canonical order."""

    @classmethod
    def empty(cls) -> "Reachability":
        """Return the table in which everyone can hear everyone."""
        return cls()

    def is_reachable(
        self, address: str, observers: frozenset[str] | None = None
    ) -> bool:
        """Whether every observer that has an opinion can hear this node.

        One observer is enough to make a node unreachable, because a node that
        half the cluster cannot hear is not a node the cluster can converge
        with.

        Args:
            address: The node in question.
            observers: The observers whose opinion still counts, or `None` to
                count every observer. A caller passes the live members here so
                that a record left by a member since downed no longer pins a
                healthy node unreachable, which is a claim nobody living is
                making and nobody can retract.

        Returns:
            Whether nobody who still counts reports it unreachable.
        """
        return all(
            record.status is not ReachabilityStatus.UNREACHABLE
            for record in self.records
            if record.observed == address
            and (observers is None or record.observer in observers)
        )

    @property
    def unreachable(self) -> frozenset[str]:
        """Every node that at least one observer currently cannot hear."""
        return frozenset(
            record.observed
            for record in self.records
            if record.status is ReachabilityStatus.UNREACHABLE
        )

    def unreachable_among(self, observers: frozenset[str]) -> frozenset[str]:
        """Every node an observer that still counts currently cannot hear.

        The observer-filtered counterpart of
        [unreachable][tapio.cluster.reachability.Reachability.unreachable]. A
        record left by a member since downed is skipped, so a dead node's stale
        claim neither blocks convergence nor steers a downing strategy.

        Args:
            observers: The observers whose opinion still counts, which is the
                live members.

        Returns:
            The observed nodes at least one live observer cannot hear.
        """
        return frozenset(
            record.observed
            for record in self.records
            if record.status is ReachabilityStatus.UNREACHABLE
            and record.observer in observers
        )

    def says(self, observer: str, observed: str) -> ReachabilityStatus:
        """What one node currently says about another.

        Reachable when it has never said anything, since an observation is
        only recorded once there is something to report.

        Args:
            observer: Who is watching.
            observed: Who is being watched.

        Returns:
            That observer's current opinion, and nobody else's.
        """
        for record in self.records:
            if record.pair == (observer, observed):
                return record.status
        return ReachabilityStatus.REACHABLE

    def observing(
        self, observer: str, observed: str, status: ReachabilityStatus
    ) -> "Reachability":
        """Return this table with one observation replaced.

        The version is taken from the record being replaced and moved on by
        one, so the new observation beats the old one on every node it reaches
        and the order it arrives in does not matter.

        Args:
            observer: Who is watching.
            observed: Who is being watched.
            status: What the observer now believes.

        Returns:
            The new table. This one is unchanged.
        """
        kept = tuple(r for r in self.records if r.pair != (observer, observed))
        previous = next(
            (r for r in self.records if r.pair == (observer, observed)), None
        )
        record = ReachabilityRecord(
            observer=observer,
            observed=observed,
            status=status,
            version=previous.version + 1 if previous is not None else 1,
        )
        return Reachability(records=_ordered((*kept, record)))

    def merge(self, other: "Reachability") -> "Reachability":
        """Return the table that has seen every observation both of these have.

        Per pair, the higher version wins. Versions tie only when two nodes
        made up different records for the same pair and version, which the
        protocol does not do, so the tie is broken by preferring
        `UNREACHABLE`: it keeps the merge a join, and it fails towards
        blocking convergence rather than towards pretending everything is
        fine.

        Args:
            other: The table to merge with.

        Returns:
            The merged table.
        """
        best: dict[tuple[str, str], ReachabilityRecord] = {}
        for record in (*self.records, *other.records):
            held = best.get(record.pair)
            if held is None or _wins(record, held):
                best[record.pair] = record
        return Reachability(records=_ordered(tuple(best.values())))

    def __repr__(self) -> str:
        """Render how many observations there are and who is unreachable."""
        gone = ", ".join(sorted(self.unreachable)) or "nobody"
        return f"Reachability({len(self.records)} records, unreachable: {gone})"


def _wins(candidate: ReachabilityRecord, held: ReachabilityRecord) -> bool:
    """Whether one record for a pair supersedes another."""
    if candidate.version != held.version:
        return candidate.version > held.version
    return (
        candidate.status is ReachabilityStatus.UNREACHABLE
        and held.status is ReachabilityStatus.REACHABLE
    )


def _ordered(records: tuple[ReachabilityRecord, ...]) -> tuple[ReachabilityRecord, ...]:
    """Put records in a canonical order, so equal tables compare equal."""
    return tuple(sorted(records, key=lambda r: r.pair))
