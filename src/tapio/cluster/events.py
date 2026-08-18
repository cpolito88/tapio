"""Cluster events: what a node tells an application about membership changes.

These are ordinary messages, delivered to an ordinary actor mailbox. Reacting
to the cluster is therefore behaviour switching and supervision like everything
else: an actor subscribes with
[Cluster.subscribe][tapio.cluster.cluster.Cluster.subscribe], and the daemon
tells it an event the moment its own view of membership changes. There is no
second event bus and no callback that runs on the daemon's turn. What the
subscriber does with an event, it does in its own turn, on its own mailbox.

An event is this node's view rather than the truth. It is emitted when this
node's membership state moves, so two nodes may see the same change a gossip
round apart. That is the same guarantee the rest of clustering gives: a value
merged pairwise, eventually consistent, never voted on.

None of these cross a link. They are built from gossip that already crossed
one, so they are not registered on the wire, and a peer that sent one would be
answered with a dead letter naming a key nothing is listening for.
"""

from typing import final

from tapio.cluster.member import AddressStr, Member
from tapio.message import Message

__all__ = [
    "ClusterEvent",
    "LeaderChanged",
    "MemberRemoved",
    "MemberUp",
    "ReachableMember",
    "SelfDown",
    "UnreachableMember",
]


class ClusterEvent(Message):
    """What the cluster tells a subscriber about a change in membership.

    A base class so that a subscriber can accept every cluster event with one
    declared type, and so that
    [Cluster.subscribe][tapio.cluster.cluster.Cluster.subscribe] with no
    filter can mean "all of them". It carries no fields of its own.
    """


@final
class MemberUp(ClusterEvent):
    """A member reached `up`, so it is a full member the cluster agreed on."""

    member: Member
    """The member, with its roles and the order it was accepted in."""


@final
class MemberRemoved(ClusterEvent):
    """A member reached `removed`, so it is gone and will not return as itself.

    A member that leaves gracefully and one that is downed both end here, since
    what a subscriber does about a member that is no longer part of the cluster
    is the same either way.
    """

    member: Member
    """The member, as it was last seen before the tombstone."""


@final
class UnreachableMember(ClusterEvent):
    """A member became unreachable: at least one node cannot hear it.

    An observation rather than a decision. The member is still `up`, and it
    blocks the leader from acting until a downing strategy resolves it or it
    answers again. What a subscriber does about that is the subscriber's call.
    """

    member: Member
    """The member that went out of reach."""


@final
class ReachableMember(ClusterEvent):
    """A member that was unreachable is reachable again.

    Every node that reported it unreachable has retracted, so the cluster as a
    whole can hear it once more.
    """

    member: Member
    """The member that came back into reach."""


@final
class LeaderChanged(ClusterEvent):
    """The node this one computes as the leader changed.

    The leader is a function of a converged view, not a post somebody holds,
    so this is emitted when the address that function returns changes, the
    empty cluster's `None` included.
    """

    leader: AddressStr | None
    """The new leader's address, or `None` when there is nobody to lead."""


@final
class SelfDown(ClusterEvent):
    """This node was downed, so its membership is over.

    A downed member may not rejoin as itself. The usual response is to shut the
    system down and come back, if at all, as a new incarnation. This is the
    same fact as
    [ClusterDowned][tapio.cluster.messages.ClusterDowned] on the system event
    stream, delivered to a subscriber's mailbox instead so that reacting to it
    is ordinary message flow.
    """

    member: Member
    """This node's own member record, at `down`."""
