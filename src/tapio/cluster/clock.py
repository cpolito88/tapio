"""Vector clocks: which of two gossip states happened first, if either did.

Membership is merged pairwise between nodes that see different things at
different times, so "which of these two is newer" has to be answerable without
a clock anyone shares. A vector clock answers it, and it answers honestly:
sometimes neither is newer, and the two states have to be merged rather than
one of them chosen.

Nothing here reads a wall clock. That is the point. A merge is a function of
its two arguments and of nothing else, which is what makes it testable the way
pure functions are.
"""

from enum import StrEnum
from typing import Self, final

from pydantic import Field, field_validator

from tapio.message import Message

__all__ = ["Ordering", "VectorClock"]


class Ordering(StrEnum):
    """How two vector clocks stand to each other."""

    SAME = "same"
    """Neither has seen anything the other has not."""

    BEFORE = "before"
    """The first happened before the second, which knows everything it knows."""

    AFTER = "after"
    """The first happened after the second."""

    CONCURRENT = "concurrent"
    """Each has seen something the other has not, so neither one wins."""


@final
class VectorClock(Message):
    """A counter per node, and the partial order they induce.

    Nodes are named by their address string, so two systems that restart at
    the same host and port share a counter. That is deliberate at this level:
    the incarnation lives in the member record, and the clock only has to
    order the gossip states a node produced.
    """

    counters: dict[str, int] = Field(default_factory=dict)
    """How many times each node has changed the state it gossips.

    A node that has changed nothing is absent rather than present at zero.
    The two would mean the same thing and compare differently, and a merge
    whose result depended on which of them a peer happened to send would not
    be the same function run twice.
    """

    @field_validator("counters")
    @classmethod
    def _drop_the_nodes_that_have_said_nothing(
        cls, counters: dict[str, int]
    ) -> dict[str, int]:
        """Normalise zeros away, and refuse a count that cannot be one.

        Args:
            counters: What was passed or decoded.

        Returns:
            The counters with any zero entries removed.

        Raises:
            ValueError: If a counter is negative. A counter counts changes,
                so a negative one is a corrupt frame rather than a stale view.
        """
        negative = sorted(node for node, count in counters.items() if count < 0)
        if negative:
            msg = f"a vector clock counts changes, so it cannot go negative: {negative}"
            raise ValueError(msg)
        return {node: count for node, count in counters.items() if count}

    @classmethod
    def empty(cls) -> Self:
        """Return the clock of a node that has said nothing yet."""
        return cls()

    def increment(self, node: str) -> "VectorClock":
        """Return this clock with one node's counter moved on by one.

        Args:
            node: The node making the change, as its address string.

        Returns:
            The new clock. This one is unchanged.
        """
        return VectorClock(
            counters={**self.counters, node: self.counters.get(node, 0) + 1}
        )

    def merge(self, other: "VectorClock") -> "VectorClock":
        """Return the clock that has seen everything both of these have.

        The per-key maximum, which is a join: commutative, associative and
        idempotent, so gossip may arrive in any order, twice, or out of order,
        and every node still computes the same clock.

        Args:
            other: The clock to merge with.

        Returns:
            The merged clock.
        """
        merged = dict(self.counters)
        for node, count in other.counters.items():
            if count > merged.get(node, 0):
                merged[node] = count
        return VectorClock(counters=merged)

    def compare(self, other: "VectorClock") -> Ordering:
        """Say how this clock stands to another.

        Args:
            other: The clock to compare against.

        Returns:
            The ordering. `CONCURRENT` when each has seen something the other
            has not, which is the case that forces a merge instead of a
            choice.
        """
        ahead = False
        behind = False
        for node in self.counters.keys() | other.counters.keys():
            mine = self.counters.get(node, 0)
            theirs = other.counters.get(node, 0)
            if mine > theirs:
                ahead = True
            elif mine < theirs:
                behind = True
            if ahead and behind:
                return Ordering.CONCURRENT
        if ahead:
            return Ordering.AFTER
        if behind:
            return Ordering.BEFORE
        return Ordering.SAME

    def __repr__(self) -> str:
        """Render the counters in node order, which is what a reader wants."""
        body = ", ".join(
            f"{node}={count}" for node, count in sorted(self.counters.items())
        )
        return f"VectorClock({body})"
