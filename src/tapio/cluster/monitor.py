"""Who watches whom, so that a member is judged whether or not it is talked to.

Until now an observation about a peer existed only where a link existed, and a
link is opened by the first message sent to that peer. A member this node had
no other reason to talk to was therefore never judged at all, and one that
this node happened to gossip with was judged by an accident of the rotation.

The ring settles it. Every node sorts the member addresses, finds itself, and
watches the few that follow it, wrapping round at the end. Every member is
then watched by exactly that many others whatever the traffic does, and the
heartbeat traffic stays linear in the number of nodes. All-to-all monitoring
is quadratic, and it is why naive implementations fall over at a few dozen
nodes.

What silence means is kept behind
[FailureDetector][tapio.remote.failure.FailureDetector]. Today that is a fixed
window. Phi-accrual, which learns the spread of a peer's timings instead of
being told a number, reads the same interface and replaces it without anything
here changing.

A watcher has two sources of evidence, and it believes the worse of them:

* Its own probe. It sends a heartbeat every round and the peer answers, so
  silence for a whole window means the peer has stopped answering *this* node.
* The transport. When remoting gives up on a link it says so, which is news
  this node's window has not waited long enough to reach yet.

They are separate because each is retracted by its own evidence: an answer
retracts the first, and a link coming back up retracts the second. Reading
either one as the retraction of the other would let a node call a peer
reachable on the strength of something that never asked it.
"""

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import final

from tapio.cluster.member import Member
from tapio.cluster.reachability import ReachabilityStatus
from tapio.remote.failure import DeadlineDetector, FailureDetector

__all__ = ["RingMonitor", "deadline_detectors", "monitored_by"]


def monitored_by(
    address: str, members: Iterable[Member], count: int
) -> tuple[str, ...]:
    """Return the members one node watches, by their place on the ring.

    A pure function of the membership, so every node works out the same ring
    from the same view and nobody has to be told who watches whom.

    Args:
        address: The watching node's address.
        members: The members to arrange, which are the live ones. A member
            with two incarnations counts once, since the ring is over
            addresses and both records name the same place on it.
        count: How many to watch. More than there are peers means all of them.

    Returns:
        The addresses to watch, in ring order starting after this node. Empty
        when this node is not a member of the view it was given, which is
        every node's first moment.
    """
    ring = sorted({member.address for member in members})
    if address not in ring:
        return ()
    start = ring.index(address)
    following = (*ring[start + 1 :], *ring[:start])
    return following[:count]


@dataclass(slots=True)
class _Watch:
    """What one node knows about one member it watches."""

    detector: FailureDetector
    """Whether answers are still arriving often enough to believe it."""

    link_lost: bool = False
    """Whether the transport has given up on the link and not taken it back."""

    def status(self, now: float) -> ReachabilityStatus:
        """Say what this watcher currently believes.

        Args:
            now: The current time, on the loop's monotonic clock.

        Returns:
            Unreachable if either source says so, since each of them is one
            node reporting that it cannot get through.
        """
        if self.link_lost or not self.detector.is_available(now):
            return ReachabilityStatus.UNREACHABLE
        return ReachabilityStatus.REACHABLE


@final
class RingMonitor:
    """The members one node watches, and what it currently believes about them.

    It holds no timers and reads no clock: the daemon passes the time in, the
    same way it passes the membership in. That keeps the whole of "who is
    watched and what does silence mean" testable without a cluster.
    """

    def __init__(
        self,
        *,
        address: str,
        size: int,
        detector: Callable[[float], FailureDetector],
    ) -> None:
        """Describe how one node watches its share of the cluster.

        Args:
            address: This node's address, which is its place on the ring.
            size: How many peers to watch.
            detector: Builds a detector for a peer that has just been picked
                up, given the time it was picked up at. Injected so that
                phi-accrual can replace the fixed window without this class
                knowing.
        """
        self._address = address
        self._size = size
        self._new_detector = detector
        self._watched: dict[str, _Watch] = {}

    @property
    def peers(self) -> tuple[str, ...]:
        """The members this node watches, in address order."""
        return tuple(sorted(self._watched))

    def follow(self, members: Iterable[Member], now: float) -> tuple[str, ...]:
        """Take up the ring the current membership implies.

        Args:
            members: The live members.
            now: The current time, which a peer picked up now is credited
                with. A member this node has never probed is not a silent one.

        Returns:
            The peers this node has just stopped watching, so the caller can
            take back whatever it said about them. A claim left behind by a
            node that no longer watches the member would block convergence
            with nothing left to retract it.
        """
        wanted = monitored_by(self._address, members, self._size)
        dropped = tuple(sorted(set(self._watched) - set(wanted)))
        for peer in dropped:
            del self._watched[peer]
        for peer in wanted:
            if peer not in self._watched:
                self._watched[peer] = _Watch(detector=self._new_detector(now))
        return dropped

    def heard(self, peer: str, at: float) -> bool:
        """Record that a peer answered this node's probe.

        Args:
            peer: Who answered.
            at: When, on the loop's monotonic clock.

        Returns:
            Whether this node watches that peer. An answer from a peer it does
            not watch is late or is somebody else's business, and either way
            there is nothing to record.
        """
        watch = self._watched.get(peer)
        if watch is None:
            return False
        watch.detector.heartbeat(at)
        return True

    def link_lost(self, peer: str) -> bool:
        """Record that the transport has given up on the link to a peer.

        Args:
            peer: The peer in question.

        Returns:
            Whether this node watches that peer.
        """
        watch = self._watched.get(peer)
        if watch is None:
            return False
        watch.link_lost = True
        return True

    def link_open(self, peer: str, at: float) -> bool:
        """Record that a link to a peer is up again.

        The link coming back is what retracts the transport's verdict, and
        nothing else can: this node's own probe never asked the transport
        anything. It counts as being heard from as well, because a handshake
        is a round trip and a peer that has just completed one is answering.

        Args:
            peer: The peer in question.
            at: When the link came up.

        Returns:
            Whether this node watches that peer.
        """
        watch = self._watched.get(peer)
        if watch is None:
            return False
        watch.link_lost = False
        watch.detector.heartbeat(at)
        return True

    def verdicts(self, now: float) -> dict[str, ReachabilityStatus]:
        """Say what this node believes about every peer it watches.

        Args:
            now: The current time, on the loop's monotonic clock.

        Returns:
            One belief per watched peer.
        """
        return {peer: watch.status(now) for peer, watch in self._watched.items()}

    def __repr__(self) -> str:
        """Render how many peers are watched and which of them look gone."""
        return f"RingMonitor({self._address}, watching {len(self._watched)})"


def deadline_detectors(
    unreachable_after: float,
) -> Callable[[float], FailureDetector]:
    """Build detectors that give up after a fixed silence.

    Args:
        unreachable_after: Seconds of silence that mean the peer has stopped
            answering.

    Returns:
        A factory that
        [RingMonitor][tapio.cluster.monitor.RingMonitor] calls for each peer it
        picks up.
    """

    def build(started_at: float) -> FailureDetector:
        return DeadlineDetector(
            unreachable_after=unreachable_after, started_at=started_at
        )

    return build
