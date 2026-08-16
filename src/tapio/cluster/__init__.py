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
system and the actor system knows nothing about clustering.
"""

from tapio.cluster.clock import Ordering, VectorClock
from tapio.cluster.cluster import Cluster
from tapio.cluster.gossip import Gossip
from tapio.cluster.member import Member, MemberStatus
from tapio.cluster.messages import (
    GossipEnvelope,
    Heartbeat,
    HeartbeatReply,
    Join,
    Leave,
    WireMessage,
)
from tapio.cluster.monitor import RingMonitor, monitored_by
from tapio.cluster.reachability import (
    Reachability,
    ReachabilityRecord,
    ReachabilityStatus,
)

__all__ = [
    "Cluster",
    "Gossip",
    "GossipEnvelope",
    "Heartbeat",
    "HeartbeatReply",
    "Join",
    "Leave",
    "Member",
    "MemberStatus",
    "Ordering",
    "Reachability",
    "ReachabilityRecord",
    "ReachabilityStatus",
    "RingMonitor",
    "VectorClock",
    "WireMessage",
    "monitored_by",
]
