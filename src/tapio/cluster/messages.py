"""What cluster nodes say to each other, and what a node says to itself.

The five that cross a link share a base class, so one resolved ref can carry
all of them, and they are registered so a peer can name them on the wire. The
ticks are the daemon's own: they never leave the process, they are not
registered, and a peer that invented one would be answered with a dead letter
naming a key nothing is listening for.

Nothing here is acknowledged. A `Join` that is lost is sent again on the next
retry, and a gossip round that is lost is superseded by the next one. That is
the same at-most-once delivery every other message in tapio gets, and gossip
is the one protocol shaped to need nothing more.
"""

from typing import Annotated, TypeAlias, final

from pydantic import Field

from tapio.cluster.gossip import Gossip
from tapio.cluster.member import AddressStr, Member
from tapio.message import Message
from tapio.remote.registry import register_message

__all__ = [
    "ClusterMessage",
    "FormTick",
    "GossipEnvelope",
    "Heartbeat",
    "HeartbeatReply",
    "HeartbeatTick",
    "Join",
    "JoinTick",
    "Leave",
    "Seeds",
    "Tick",
    "WireMessage",
]


class WireMessage(Message):
    """What one cluster node may send another.

    A base class rather than a union, so that a node resolves one ref per peer
    and sends every kind of cluster message through it. It carries no fields
    of its own and nothing declares a field of this type: a field annotated
    with a base class is re-validated as that base and loses everything the
    subclass added.
    """


@final
@register_message()
class Join(WireMessage):
    """Ask a member to let this node into the cluster.

    Sent to every seed until this node sees itself in the gossip that comes
    back. A node that is not itself a member ignores it, which is what stops
    two nodes that started together from admitting each other into two
    different clusters.
    """

    member: Member
    """The joining node, as it describes itself: address, incarnation, roles."""


@final
@register_message()
class GossipEnvelope(WireMessage):
    """One node's whole view of the cluster, sent to one other node."""

    sender: AddressStr
    """Who sent it, so the receiver can answer with a newer view."""

    gossip: Gossip
    """What the sender believes."""


@final
@register_message()
class Heartbeat(WireMessage):
    """Ask a member whether it is still answering.

    Sent every round to the members this node watches, and to nobody else, so
    the traffic is bounded by how many peers a node watches rather than by how
    many members there are.
    """

    sender: AddressStr
    """Who is asking, so the answer knows where to go."""


@final
@register_message()
class HeartbeatReply(WireMessage):
    """Answer a member that asked whether this node is still answering.

    Nothing is carried back but the answerer's address. What the watcher is
    measuring is the arrival, and the arrival is the whole of the evidence.
    """

    sender: AddressStr
    """Who answered, which is the member being watched."""


@final
@register_message()
class Leave(WireMessage):
    """Ask the cluster to let a member go gracefully.

    Ordinarily a node asks about itself, but the address is carried explicitly
    because an operator tool may ask about another one, and because what acts
    on it is the leader rather than the member named.
    """

    address: AddressStr
    """The member that is to leave."""


@final
class Seeds(Message):
    """Tell this node's daemon which seeds to ask, and start it asking.

    Seeding is a message rather than a setter because the timers it starts
    belong to the actor. Reaching in from outside to start them would be
    changing an actor's state from another task, which is the one thing an
    actor system exists to make unnecessary.
    """

    addresses: Annotated[tuple[AddressStr, ...], Field(min_length=1)]
    """The seeds, in the order every node lists them.

    At least one. The daemon reads `addresses[0]` to decide whether it is the
    first seed, which is the node allowed to form a cluster alone, so an empty
    list has no answer to that question and used to raise inside the receive
    loop instead of where the message was built.
    """


@final
class Tick(Message):
    """Gossip to one peer, and act if this node leads a converged view."""


@final
class JoinTick(Message):
    """Ask the seeds again, because joining is retried rather than acknowledged."""


@final
class FormTick(Message):
    """The moment the first seed may form a cluster, if it has heard nothing."""


@final
class HeartbeatTick(Message):
    """Probe the members this node watches, and judge the ones that went quiet."""


@final
class LinkChanged(Message):
    """What the transport saw about a peer, on its way into membership.

    Remoting publishes its verdicts on the system's event stream, and a
    subscriber runs wherever the publisher happens to be. This carries the
    verdict into the daemon's mailbox instead, so the state is changed by the
    actor that owns it, in its own turn, like every other change.

    It never leaves the process: what the cluster does with the observation
    travels as ordinary gossip.
    """

    peer: AddressStr
    """The peer the transport reached a verdict about."""

    reachable: bool
    """Whether a link to it is open, as the transport last saw."""


ClusterMessage: TypeAlias = (
    WireMessage | Seeds | Tick | JoinTick | FormTick | HeartbeatTick | LinkChanged
)
"""Everything the cluster daemon accepts, its own ticks included."""
