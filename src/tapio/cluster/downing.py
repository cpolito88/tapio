"""Deciding to stop waiting for an unreachable member, safely on both sides.

Reachability records that one node cannot hear another, and convergence blocks
on it: a member that is `Up` and unreachable stops the leader acting until
somebody decides what to do about it. That decision is downing. It is separate
from the detector that produced the observation, the same way a
[DownDecider][tapio.remote.failure.DownDecider] is split from a
[FailureDetector][tapio.remote.failure.FailureDetector]: one says a member has
gone quiet, the other says what the cluster does about it.

**The decision has to be right on both sides of a partition at once, and there
is no message that crosses the partition to make them agree.** A network split
leaves two groups that cannot hear each other, and each one runs this decision
on its own view. Safety comes from those two views being mirror images: the
group of three sees the group of two as unreachable, and the group of two sees
the group of three as unreachable. A strategy that names a winner from the
shape of the split alone, "keep the larger side" or "keep the side with the
oldest member", therefore names the *same* winner on both sides without either
side saying a word. The loser downs itself and shuts down; the winner downs the
loser and carries on. One decision, computed twice, agreeing.

Four of the strategies here are pure functions of a view: given the same
membership and the same reachability they return the same verdict, and the
whole argument for correctness is that the two sides feed them mirror-image
inputs. [LeaseMajority][tapio.cluster.downing.LeaseMajority] is the exception.
It breaks a tie the count cannot, an even split with no majority, by reaching
for a [Lease][tapio.cluster.downing.Lease] that only one side can hold. That
reaches outside the view, so the decision is asynchronous and the safety
argument is different: not that both sides compute the same answer, but that
the lease lets only one of them win.

That one exception is why deciding is asynchronous even though most of it does
no waiting. A pure strategy returns at once; the lease one waits on an outside
lock. Making them all asynchronous keeps one protocol rather than two, and an
`async def` that never awaits is still a pure function of its inputs, so the
mirror-image argument the pure strategies rest on is untouched.

The verdict is the set of addresses to down. Applied on the winning side those
are the unreachable members, which downing lifts out of the way of convergence.
Applied on the losing side they are that side's own members, which is what
self-down means. The daemon reads which case it is in from whether its own
address is in the set.
"""

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Protocol, final, runtime_checkable

from tapio.cluster.gossip import Gossip
from tapio.cluster.member import Member, seniority

__all__ = [
    "DownAll",
    "DownStrategy",
    "KeepMajority",
    "KeepOldest",
    "Lease",
    "LeaseMajority",
    "LocalLease",
    "StaticQuorum",
]


@runtime_checkable
class DownStrategy(Protocol):
    """What a cluster does when some members are unreachable.

    Most implementations are a pure function of the view, so that the two sides
    of a partition reach an agreeing verdict from mirror-image inputs without a
    message passing between them.
    [LeaseMajority][tapio.cluster.downing.LeaseMajority] is the one that reaches
    outside the view, which is why deciding is asynchronous.
    """

    async def decide(self, state: Gossip) -> frozenset[str]:
        """Return the addresses to down, given a view with unreachable members.

        Args:
            state: This node's view. Called when its reachability shows at
                least one unreachable member, and treated as stable: the daemon
                waits for the split to settle before asking, so that a passing
                blip is not downed.

        Returns:
            The addresses to move to `Down`. Empty when nothing should be
            downed. It contains this node's own address exactly when this node
            is on the side that loses, which is how self-down is told from
            downing a peer.
        """
        ...


def _sides(state: Gossip) -> tuple[tuple[Member, ...], tuple[Member, ...]]:
    """Split the live members into the side this view can hear and the side it cannot.

    Args:
        state: The view to split.

    Returns:
        The reachable members and the unreachable members, each in address
        order. The unreachable side is every live observer's opinion, not just
        this node's, so a node a peer reports unreachable lands on the
        unreachable side even though it never reports itself unreachable.
    """
    gone = state.unreachable
    reachable = tuple(m for m in state.alive if m.address not in gone)
    unreachable = tuple(m for m in state.alive if m.address in gone)
    return reachable, unreachable


def _counted(members: Iterable[Member], role: str | None) -> tuple[Member, ...]:
    """Keep the members a decision counts, which is those in a role when one is set.

    A role narrows *who votes*, not who is downed: a side that loses is downed
    whole, but the sizes compared to decide which side that is are counted over
    the members carrying the role. Counting a subset and downing the rest is
    what lets an operator say "there are three databases, keep the side with
    two of them" in a cluster that also runs stateless web nodes.

    Args:
        members: The members on one side.
        role: The role to count, or `None` to count them all.

    Returns:
        The members that count.
    """
    if role is None:
        return tuple(members)
    return tuple(m for m in members if role in m.roles)


def _addresses(members: Iterable[Member]) -> frozenset[str]:
    """The addresses of some members, which is what a verdict is made of."""
    return frozenset(m.address for m in members)


@final
@dataclass(frozen=True, slots=True)
class DownAll:
    """Down every member, so a split heals by the whole cluster restarting.

    The one strategy that is always safe, because it keeps nothing: there is no
    surviving side that another surviving side could contradict. It trades the
    most availability for it, since a partition that a smarter strategy would
    have ridden out on the majority takes the majority down as well. It is the
    honest default when the operator cannot promise the cluster its size or its
    shape, and it is what the counting strategies fall back to when their own
    rule cannot name a single winner.
    """

    async def decide(self, state: Gossip) -> frozenset[str]:
        """Down everyone, or nobody when there is no split to resolve.

        Args:
            state: This node's view.

        Returns:
            Every live address when a member is unreachable, and the empty set
            otherwise.
        """
        reachable, unreachable = _sides(state)
        if not unreachable:
            return frozenset()
        return _addresses((*reachable, *unreachable))

    def __repr__(self) -> str:
        """Render the class name; there is no state to show."""
        return "DownAll()"


@final
@dataclass(frozen=True, slots=True)
class KeepMajority:
    """Keep the larger side and down the smaller, tie broken by lowest address.

    The usual choice for a cluster whose size drifts, since it needs to be told
    no number: it counts what it can see. Its safety rests on there being one
    majority, which holds as long as a split produces two parts. A split into
    three parts can leave every part a minority, and then this downs them all,
    which is [DownAll][tapio.cluster.downing.DownAll] arrived at by counting.

    A tie, two sides of equal size, is broken by keeping the side that holds the
    lowest address. Both sides compute the same lowest address from the same
    membership, so both agree which side that is, which is the same reason the
    leader is the lowest address: a total order over the bytes both sides hold
    needs no round to settle.
    """

    role: str | None = None
    """The role to count, or `None` to count every member."""

    async def decide(self, state: Gossip) -> frozenset[str]:
        """Keep the majority side, downing the minority.

        Args:
            state: This node's view.

        Returns:
            The minority side's addresses, or this side's own when this side is
            the minority. Every live address when neither side counts anybody,
            since a role that names nobody leaves no side to keep.
        """
        reachable, unreachable = _sides(state)
        if not unreachable:
            return frozenset()
        here = _counted(reachable, self.role)
        there = _counted(unreachable, self.role)
        if not here and not there:
            return _addresses((*reachable, *unreachable))
        if len(here) != len(there):
            loser = unreachable if len(here) > len(there) else reachable
            return _addresses(loser)
        lowest = min(_addresses((*here, *there)))
        keep_here = any(m.address == lowest for m in here)
        return _addresses(unreachable if keep_here else reachable)

    def __repr__(self) -> str:
        """Render the role, which is the whole of the configuration."""
        return f"KeepMajority(role={self.role!r})"


@final
@dataclass(frozen=True, slots=True)
class StaticQuorum:
    """Keep a side only if it still holds a fixed number of members.

    The operator names the number, and a side survives when it can count that
    many and the other side cannot. It is exact where
    [KeepMajority][tapio.cluster.downing.KeepMajority] is relative, which is
    what makes it safe for a cluster that changes size only when an operator
    says so: the quorum is set to more than half the largest the cluster is
    allowed to reach, and then two sides can never both hold it.

    If the cluster outgrows that promise both sides can reach the quorum at
    once, and keeping either would be keeping a side the other contradicts, so
    this downs everything instead. The same happens when neither side reaches
    the quorum. Naming a number the cluster then exceeds is the one way to
    misconfigure this, and downing all is how it fails when that happens: loudly
    and toward stopping, not quietly toward a split brain.
    """

    size: int
    """How many members a side must hold to be kept. At least one."""

    role: str | None = None
    """The role the quorum is counted over, or `None` to count every member."""

    def __post_init__(self) -> None:
        """Refuse a quorum of nothing, which no side could fail to reach.

        Raises:
            ValueError: If the size is below one.
        """
        if self.size < 1:
            msg = (
                f"a static quorum needs at least one member, not {self.size}: a "
                "quorum of zero is one every side reaches, so it would keep both "
                "sides of every split"
            )
            raise ValueError(msg)

    async def decide(self, state: Gossip) -> frozenset[str]:
        """Keep the side that alone reaches the quorum, downing the rest.

        Args:
            state: This node's view.

        Returns:
            The side that falls short, or every live address when both sides
            reach the quorum or neither does.
        """
        reachable, unreachable = _sides(state)
        if not unreachable:
            return frozenset()
        here = len(_counted(reachable, self.role))
        there = len(_counted(unreachable, self.role))
        if here >= self.size and there < self.size:
            return _addresses(unreachable)
        if there >= self.size and here < self.size:
            return _addresses(reachable)
        return _addresses((*reachable, *unreachable))

    def __repr__(self) -> str:
        """Render the quorum size and the role it is counted over."""
        return f"StaticQuorum(size={self.size}, role={self.role!r})"


@final
@dataclass(frozen=True, slots=True)
class KeepOldest:
    """Keep the side that holds the oldest member, and down the other.

    The oldest member is the one accepted first, by its `up_number`, and there
    is exactly one, so both sides agree which side holds it and one of them is
    kept. It suits a cluster with a member that matters more than the others,
    a singleton's usual home, since keeping the oldest keeps that member's side
    running through a split.

    Its weakness is the oldest member being cut off on its own. Then keeping the
    oldest's side keeps one node and downs the rest, which is the split brain
    resolver behaving worse than doing nothing. Setting `down_if_alone` guards
    that case: when the oldest is the only member on its side, it downs itself
    instead, and the larger side lives. Both sides see the same single node
    alone against the same larger group, so both still agree.
    """

    down_if_alone: bool = False
    """Whether an oldest member cut off on its own downs itself rather than the rest."""

    role: str | None = None
    """The role the oldest is chosen among, or `None` to choose among every member."""

    async def decide(self, state: Gossip) -> frozenset[str]:
        """Keep the oldest member's side, unless it is alone and told to yield.

        Args:
            state: This node's view.

        Returns:
            The side without the oldest member. This side's own addresses when
            this side lacks the oldest, or when the oldest is alone and
            `down_if_alone` is set. Every live address when a role names nobody
            to be oldest.
        """
        reachable, unreachable = _sides(state)
        if not unreachable:
            return frozenset()
        here = _counted(reachable, self.role)
        there = _counted(unreachable, self.role)
        candidates = (*here, *there)
        if not candidates:
            return _addresses((*reachable, *unreachable))
        oldest = min(candidates, key=seniority)
        oldest_here = any(m.key == oldest.key for m in here)
        elders_side, other_side = (
            (reachable, unreachable) if oldest_here else (unreachable, reachable)
        )
        if self.down_if_alone and len(elders_side) == 1 and other_side:
            return _addresses(elders_side)
        return _addresses(other_side)

    def __repr__(self) -> str:
        """Render whether a lone oldest yields, and the role it is chosen among."""
        return f"KeepOldest(down_if_alone={self.down_if_alone}, role={self.role!r})"


@runtime_checkable
class Lease(Protocol):
    """An outside lock that at most one owner holds, for breaking a tie.

    A split with no majority, an even one, cannot be resolved from the view: it
    is symmetric, so any rule read from its shape keeps both halves or neither.
    A lease breaks the symmetry from outside. Both sides try to take the same
    lease, it lets only one owner hold it, and the side that holds it survives
    while the side that cannot down themselves.

    For this to mean anything the lease has to live somewhere both sides can
    still reach when they cannot reach each other, which is to say outside the
    partition: a row in a database, a Kubernetes lease, a key in etcd. A lease
    held inside the split, [LocalLease][tapio.cluster.downing.LocalLease] being
    the extreme case of one held in a single process, cannot arbitrate a
    partition it is on one side of, so it is for tests and for systems that
    share a process rather than for a real cluster.

    An owner is a string, and the whole cluster resolving one split uses one
    name for it, so a side either all takes the lease or all fails to. Acquiring
    is therefore re-entrant: it succeeds when the lease is free or already held
    by this owner, and fails only when another owner holds it.

    tapio holds the lease for the life of the decision and never releases it:
    the winning side keeps it so that a node arriving late reads the same
    winner. A real lease must therefore expire on its own, the way an etcd or
    Kubernetes lease does once its holder stops renewing it. A lease that never
    expires cannot arbitrate a later, independently composed split, because the
    earlier winner still holds it under a name the new sides do not use, so both
    new sides fail to acquire and down themselves.
    [LocalLease][tapio.cluster.downing.LocalLease] is that never-expiring case,
    which is one more reason it is for tests only.
    """

    async def acquire(self, owner: str) -> bool:
        """Try to hold the lease for an owner.

        Args:
            owner: Who is asking, which for downing is the identity of a whole
                side rather than one node, so that a side agrees with itself.

        Returns:
            Whether the lease is held by this owner now. True when it was free
            or already this owner's, False when another owner holds it.
        """
        ...


@final
class LocalLease:
    """A lease held in one process, for tests and for systems that share one.

    It cannot arbitrate a real partition, because a partition splits processes
    and this lease is on one side of that split. What it does arbitrate is
    several systems in a single process, which is exactly the shape a test of
    [LeaseMajority][tapio.cluster.downing.LeaseMajority] takes: every node holds
    the same object, so the lease mediates between them the way an outside one
    would mediate between machines. In production a lease reaches an outside
    service instead, and this class is not that.
    """

    __slots__ = ("_owner",)

    def __init__(self) -> None:
        """Start with the lease free."""
        self._owner: str | None = None

    async def acquire(self, owner: str) -> bool:
        """Take the lease for an owner, if it is free or already theirs.

        Args:
            owner: Who is asking.

        Returns:
            Whether this owner holds it now.
        """
        if self._owner is None or self._owner == owner:
            self._owner = owner
            return True
        return False

    def __repr__(self) -> str:
        """Render who holds the lease, if anyone."""
        return f"LocalLease(owner={self._owner!r})"


@final
@dataclass(frozen=True, slots=True)
class LeaseMajority:
    """Keep the side that takes an outside lease, so an even split still has one winner.

    The strategy for a split the count cannot decide. Two equal halves have no
    majority, and every deterministic rule keeps both or neither, so this hands
    the decision to a [Lease][tapio.cluster.downing.Lease] that only one side
    can hold. The side that takes it survives and downs the other; the side that
    cannot downs itself. Because the lease admits one owner, exactly one side
    lives, which is the guarantee no view-only rule can make about an even
    split.

    Every node names its own side to the lease, by the lowest address on it, so
    all of a side asks with one owner and the lease's re-entrancy lets them all
    hold it or all fail together. The two sides name different owners, so the
    lease keeps them apart.

    Whichever side reaches the lease first wins it, so a partition of a running
    cluster can leave the smaller side standing if it got there first. That is
    safe, since only one side ever lives, but it is not the most available
    outcome. Preferring the majority by making the minority wait before it
    reaches for the lease is a refinement this does not make yet.

    The winning side keeps the lease and never gives it back, so the lease must
    be one that expires on its own; see [Lease][tapio.cluster.downing.Lease] for
    why a never-expiring lease cannot resolve a second, later split.
    """

    lease: Lease
    """The outside lock the sides race for. It must live outside the partition."""

    async def decide(self, state: Gossip) -> frozenset[str]:
        """Keep this side if it can take the lease, and down it if it cannot.

        Args:
            state: This node's view.

        Returns:
            The unreachable side's addresses when this side takes the lease, and
            this side's own when it cannot.
        """
        reachable, unreachable = _sides(state)
        if not unreachable:
            return frozenset()
        owner = min(_addresses(reachable))
        if await self.lease.acquire(owner):
            return _addresses(unreachable)
        return _addresses(reachable)

    def __repr__(self) -> str:
        """Render the lease, which is the whole of the configuration."""
        return f"LeaseMajority(lease={self.lease!r})"
