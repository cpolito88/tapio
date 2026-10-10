"""Clustering: membership by gossip, and a leader computed rather than elected.

Remoting lets two systems that know about each other exchange messages. This
package answers the questions remoting deliberately does not: who is in the
group right now, who joined, and who has left.

The answer is eventually consistent membership merged pairwise between nodes.
There is no consensus algorithm here and there will not be one. The leader is
`sorted(members)[0]` computed locally from a converged view, so no election
protocol exists and none can be wrong. What genuinely needs agreement, which
side of a partition survives, is not decided here and needs strategies of its
own.

Unlike [remote][tapio.remote], this package re-exports its public names,
because nothing in the runtime imports it: clustering depends on the actor
system and the actor system knows nothing about clustering. The machinery
behind them, the gossip state and its merge, the reachability table, the ring
and the messages nodes send each other, stays importable from its own module
and is not part of what a release promises. An application that could send a
`Join` to the daemon would be speaking the wire protocol for it.
"""

from tapio.cluster.cluster import Cluster
from tapio.cluster.downing import (
    DownAll,
    DownStrategy,
    KeepMajority,
    KeepOldest,
    Lease,
    LeaseMajority,
    LocalLease,
    StaticQuorum,
)
from tapio.cluster.events import (
    ClusterEvent,
    LeaderChanged,
    MemberLeaving,
    MemberRemoved,
    MemberUp,
    ReachableMember,
    SelfDown,
    UnreachableMember,
)
from tapio.cluster.member import Member, MemberStatus
from tapio.cluster.messages import ClusterDowned
from tapio.cluster.router import group_router
from tapio.cluster.singleton import ClusterSingleton
from tapio.errors import ClusterError
from tapio.settings import ClusterSettings, ManagementSettings

__all__ = [
    "Cluster",
    "ClusterDowned",
    "ClusterError",
    "ClusterEvent",
    "ClusterSettings",
    "ClusterSingleton",
    "DownAll",
    "DownStrategy",
    "KeepMajority",
    "KeepOldest",
    "LeaderChanged",
    "Lease",
    "LeaseMajority",
    "LocalLease",
    "ManagementSettings",
    "Member",
    "MemberLeaving",
    "MemberRemoved",
    "MemberStatus",
    "MemberUp",
    "ReachableMember",
    "SelfDown",
    "StaticQuorum",
    "UnreachableMember",
    "group_router",
]
