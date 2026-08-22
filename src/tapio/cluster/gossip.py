"""The gossip state: what a node believes about the cluster, and how two beliefs merge.

This is the whole of the membership algorithm that is worth being careful
about. Everything time-dependent lives in the daemon: how often a node
gossips, who it picks, when it gives up on a peer. What lives here is a value
and a merge, and the merge is a join.

**A join, meaning three laws.** Merging is commutative, so it does not matter
which of two nodes speaks first. It is associative, so it does not matter how
gossip is batched on the way round. It is idempotent, so the same gossip
arriving twice changes nothing. Those three are what let gossip be lossy, out
of order and duplicated without being wrong, and they are what the property
test asserts over shuffled sequences. A membership bug does not raise: it
produces two singletons, or a member everyone else removed that is still
serving traffic, and it surfaces as user-domain damage weeks later. So the
merge is a pure function of two values, and it is tested the way pure
functions are.

**Convergence is not consensus.** It is the condition under which the leader
is allowed to act: every member that is not `Down` or `Removed` is reachable,
and has seen the version being gossiped. An unreachable member therefore
blocks progress, and that blocking is the point. Deciding to stop waiting for
it is downing, which is a separate decision with its own strategies.

**The leader is a pure function.** Given a converged view, every node computes
the same leader by sorting addresses, and one of them acts on it. There is no
election protocol, so there is no election protocol to be wrong.
"""

from typing import final

from tapio.cluster.clock import Ordering, VectorClock
from tapio.cluster.member import Member, MemberStatus, sort_key
from tapio.cluster.reachability import Reachability, ReachabilityStatus
from tapio.message import Message

__all__ = ["Gossip", "leader_actions"]

_LEADS: frozenset[MemberStatus] = frozenset({MemberStatus.UP, MemberStatus.LEAVING})
"""The statuses a member may lead from, when there is one."""

_GONE: frozenset[MemberStatus] = frozenset({MemberStatus.DOWN, MemberStatus.REMOVED})
"""The statuses that neither block convergence nor take part in it."""


@final
class Gossip(Message):
    """One node's view of the cluster, and the value that travels between nodes."""

    members: tuple[Member, ...] = ()
    """Every member known, in address order, tombstones included."""

    reachability: Reachability = Reachability()
    """Who currently cannot hear whom."""

    version: VectorClock = VectorClock()
    """What orders two views of the cluster against each other."""

    seen: frozenset[str] = frozenset()
    """Which nodes are known to have seen this exact version.

    It is what makes convergence observable, and it is the one field that is
    about the spread of the state rather than about the state. A version that
    changes empties it, because nobody has seen the new one yet.
    """

    @classmethod
    def founding(cls, member: Member) -> "Gossip":
        """Return the state of a node that has just formed a cluster alone.

        Args:
            member: The founding member, which is already `Up` because there
                was nobody else to agree with.

        Returns:
            The state.
        """
        return cls(
            members=(member,),
            version=VectorClock().increment(member.address),
            seen=frozenset({member.address}),
        )

    def member(self, address: str) -> Member | None:
        """Return the member at an address, or `None`.

        Args:
            address: The address in its string form.

        Returns:
            The member. When two incarnations of one address are known, which
            happens whenever a node has restarted, the live one is returned.
            Among incarnations that are all live, or all gone, the one that
            has gone furthest through its life wins, since that is the one a
            caller is deciding about.

            The live one has to win, because a restart leaves a tombstone
            behind and tombstones are kept forever. Ranking on status alone
            would answer with the dead incarnation from the restart onwards,
            and a caller asking about the member at an address is asking about
            the one that is running.
        """
        found = [m for m in self.members if m.address == address]
        if not found:
            return None
        live = [m for m in found if m.status not in _GONE]
        return max(live or found, key=lambda m: (m.rank, m.uid))

    def primaries(self) -> dict[str, Member]:
        """Return the primary member at every address, in one pass.

        The same choice [member][tapio.cluster.gossip.Gossip.member] makes for
        one address, made for all of them at once: the live incarnation wins
        over a tombstone, and among incarnations that are all live or all gone
        the one furthest through its life wins. It exists so a caller that needs
        every address does not call `member` in a loop, which is quadratic in
        the membership because each call rescans it.

        Returns:
            The primary member keyed by address. Empty when there are no
            members.
        """
        live: dict[str, Member] = {}
        best: dict[str, Member] = {}
        for member in self.members:
            rank = (member.rank, member.uid)
            held = best.get(member.address)
            if held is None or rank > (held.rank, held.uid):
                best[member.address] = member
            if member.status not in _GONE:
                held_live = live.get(member.address)
                if held_live is None or rank > (held_live.rank, held_live.uid):
                    live[member.address] = member
        return {address: live.get(address, member) for address, member in best.items()}

    @property
    def alive(self) -> tuple[Member, ...]:
        """Every member that has not been downed or removed."""
        return tuple(m for m in self.members if m.status not in _GONE)

    @property
    def leader(self) -> str | None:
        """The node allowed to act, when the view is converged.

        The first member in address order whose status is `Up` or `Leaving`.
        Before anybody is `Up`, which is every cluster's first moment, it
        falls back to the first member in address order: somebody has to be
        able to accept the first join, and picking the same somebody on every
        node is all this rule has to do.

        Returns:
            The leader's address, or `None` when there is nobody to lead.
        """
        candidates = [
            m
            for m in sorted(self.members, key=sort_key)
            if m.status not in _GONE and self.reachability.is_reachable(m.address)
        ]
        if not candidates:
            return None
        leading = [m for m in candidates if m.status in _LEADS]
        return (leading[0] if leading else candidates[0]).address

    @property
    def converged(self) -> bool:
        """Whether every member that matters has seen this exact version.

        A `Down` or `Removed` member neither blocks convergence nor takes part
        in it. Everyone else must be reachable and must have seen the version,
        so one unreachable member stops the leader from acting until somebody
        decides what to do about it.
        """
        for member in self.members:
            if member.status in _GONE:
                continue
            if not self.reachability.is_reachable(member.address):
                return False
            if member.address not in self.seen:
                return False
        return True

    def seen_by(self, address: str) -> "Gossip":
        """Return this state recorded as seen by one more node.

        Args:
            address: The node that has now seen it.

        Returns:
            The state. This one is unchanged.
        """
        if address in self.seen:
            return self
        return self.model_copy(update={"seen": self.seen | {address}})

    def bumped_by(self, address: str) -> "Gossip":
        """Return this state as a new version, produced by one node.

        Every change a node makes to the state goes through here, which is
        what keeps the vector clock a record of who changed what. The seen set
        collapses to the node that made the change, because nobody else has
        seen this version yet.

        Args:
            address: The node making the change.

        Returns:
            The new state.
        """
        return self.model_copy(
            update={
                "version": self.version.increment(address),
                "seen": frozenset({address}),
            }
        )

    def observing(
        self, observer: str, observed: str, status: ReachabilityStatus
    ) -> "Gossip":
        """Return this state with one node's opinion of another recorded.

        The version is not touched, like
        [with_member][tapio.cluster.gossip.Gossip.with_member], so a caller
        making a change calls
        [bumped_by][tapio.cluster.gossip.Gossip.bumped_by] as well.

        Args:
            observer: Who is watching, which is always the node recording it.
            observed: Who is being watched.
            status: What the observer now believes.

        Returns:
            The new state.
        """
        return self.model_copy(
            update={
                "reachability": self.reachability.observing(observer, observed, status)
            }
        )

    def with_member(self, member: Member) -> "Gossip":
        """Return this state with a member added or replaced.

        The version is not touched, so a caller that is making a change calls
        [bumped_by][tapio.cluster.gossip.Gossip.bumped_by] as well. Keeping
        the two apart lets the leader apply several transitions and bump once.

        Args:
            member: The member to record.

        Returns:
            The new state.
        """
        kept = tuple(m for m in self.members if m.key != member.key)
        return self.model_copy(update={"members": _ordered((*kept, member))})

    def merge(self, other: "Gossip") -> "Gossip":
        """Return what two views of the cluster agree on.

        Members are merged pairwise by the status lattice, reachability by
        observation version, and the clock by per-key maximum. Each of those
        is a join, so this is one: the same three laws hold for the whole
        state.

        The seen set is the one field that is not simply joined, because it
        describes who has seen a *version* rather than what the state is. It
        is carried across only for the version it belongs to: kept when both
        sides are at that version, taken from whichever side is newer, and
        emptied when the merge produces a version neither side had seen.

        Args:
            other: The other view.

        Returns:
            The merged view.
        """
        by_key: dict[tuple[str, int], Member] = {}
        for member in (*self.members, *other.members):
            held = by_key.get(member.key)
            by_key[member.key] = held.merge(member) if held is not None else member
        ordering = self.version.compare(other.version)
        return Gossip(
            members=_ordered(tuple(by_key.values())),
            reachability=self.reachability.merge(other.reachability),
            version=self.version.merge(other.version),
            seen=_merge_seen(self, other, ordering),
        )

    def __repr__(self) -> str:
        """Render the members and whether this view has converged."""
        body = ", ".join(f"{m.address}#{m.uid}:{m.status}" for m in self.members)
        state = "converged" if self.converged else "not converged"
        return f"Gossip({body or 'nobody'}, {state})"


_TRANSITIONS: tuple[tuple[MemberStatus, MemberStatus], ...] = (
    (MemberStatus.JOINING, MemberStatus.UP),
    (MemberStatus.LEAVING, MemberStatus.EXITING),
    (MemberStatus.EXITING, MemberStatus.REMOVED),
    (MemberStatus.DOWN, MemberStatus.REMOVED),
)
"""What the leader may do to a member, one step at a time."""


def leader_actions(state: Gossip) -> Gossip:
    """Return the state after the leader has moved every member it may.

    A pure function of a converged view, which is what makes it testable
    without a cluster: the caller decides *whether* the leader may act, and
    this decides what acting means. Each member moves exactly one step, and
    the caller bumps the version afterwards, so a leaving member walks
    `Leaving` to `Exiting` to `Removed` across separate converged rounds. That
    is what gives a handoff somewhere to happen, and it is why the steps are
    not collapsed even though the leader could see all three at once.

    A member that reaches `Removed` is kept as a tombstone rather than
    dropped. Dropping it would let a peer holding an older view put it back
    by merging the record in again, since a merge unions the members it is
    given.

    Args:
        state: The converged view.

    Returns:
        The state after this round of transitions, or the same value when
        there was nothing to do.
    """
    moved = state
    for member in state.members:
        for current, target in _TRANSITIONS:
            if member.status is not current:
                continue
            number = _next_up_number(moved) if target is MemberStatus.UP else 0
            moved = moved.with_member(member.with_status(target, up_number=number))
            if target is MemberStatus.REMOVED:
                # Its observations go with it. Keeping them would leave the
                # cluster reporting a member unreachable that it has already
                # written off, and blocking convergence on a node nobody is
                # waiting for.
                moved = moved.model_copy(
                    update={"reachability": moved.reachability.without(member.address)}
                )
            break
    return moved


def _next_up_number(state: Gossip) -> int:
    """Return the next place in the order members were accepted in."""
    return max((member.up_number for member in state.members), default=0) + 1


def _merge_seen(left: Gossip, right: Gossip, ordering: Ordering) -> frozenset[str]:
    """Decide which nodes have seen the version a merge produces.

    Args:
        left: One view.
        right: The other.
        ordering: How their versions stand to each other.

    Returns:
        The seen set for the merged version. Empty when the merge invents a
        version that is newer than both, since nobody has seen that one yet.
    """
    if ordering is Ordering.SAME:
        return left.seen | right.seen
    if ordering is Ordering.AFTER:
        return left.seen
    if ordering is Ordering.BEFORE:
        return right.seen
    return frozenset()


def _ordered(members: tuple[Member, ...]) -> tuple[Member, ...]:
    """Put members in address order, so equal states compare equal."""
    return tuple(sorted(members, key=sort_key))
