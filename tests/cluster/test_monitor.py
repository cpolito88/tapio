"""The ring: who watches whom, and what a watcher concludes from silence."""

import pytest

from tapio.cluster.gossip import Gossip
from tapio.cluster.member import Member, MemberStatus
from tapio.cluster.monitor import RingMonitor, deadline_detectors, monitored_by
from tapio.cluster.reachability import ReachabilityStatus

UNREACHABLE = ReachabilityStatus.UNREACHABLE
REACHABLE = ReachabilityStatus.REACHABLE


def member(port: int, *, status: MemberStatus = MemberStatus.UP) -> Member:
    """A member at a loopback address, named by its port."""
    return Member(address=f"tapio://sys@127.0.0.1:{port}", uid=port, status=status)


def address(port: int) -> str:
    """The address a port stands for, in its string form."""
    return f"tapio://sys@127.0.0.1:{port}"


def ring(*ports: int) -> tuple[Member, ...]:
    """Members at these ports, in the order given."""
    return tuple(member(port) for port in ports)


def test_a_node_watches_the_members_that_follow_it():
    watched = monitored_by(address(2), ring(1, 2, 3, 4, 5), 2)

    assert watched == (address(3), address(4))


def test_the_ring_wraps_round_at_the_end():
    watched = monitored_by(address(5), ring(1, 2, 3, 4, 5), 3)

    assert watched == (address(1), address(2), address(3))


def test_a_node_never_watches_itself():
    watched = monitored_by(address(1), ring(1, 2, 3), 10)

    assert address(1) not in watched
    assert watched == (address(2), address(3))


def test_a_node_that_is_not_in_the_view_watches_nothing():
    # Every cluster's first moment: this node has gossip about others and has
    # not been admitted itself.
    assert monitored_by(address(9), ring(1, 2, 3), 2) == ()


def test_two_incarnations_of_one_address_are_one_place_on_the_ring():
    restarted = (member(1), Member(address=address(2), uid=7), member(2))

    # The ring is over addresses, since that is what a heartbeat is sent to.
    assert monitored_by(address(1), restarted, 5) == (address(2),)


def test_every_member_is_watched_by_somebody():
    members = ring(1, 2, 3, 4, 5, 6, 7)

    watchers = {m.address: 0 for m in members}
    for m in members:
        for peer in monitored_by(m.address, members, 2):
            watchers[peer] += 1

    # This is the property the whole ring exists for. Nobody arranges it and
    # nobody is told about it: each node computes the same order and takes its
    # own share, so no member is left unwatched however little traffic reaches
    # it.
    assert set(watchers.values()) == {2}


def test_traffic_is_bounded_by_the_ring_and_not_by_the_cluster():
    small = ring(*range(1, 6))
    large = ring(*range(1, 51))

    assert len(monitored_by(address(1), small, 5)) == 4
    # Ten times the nodes, the same traffic per node. All-to-all monitoring
    # would be 49 here and quadratic overall.
    assert len(monitored_by(address(1), large, 5)) == 5


def a_monitor(*, size: int = 5, window: float = 10.0) -> RingMonitor:
    """A monitor for the node at port 1, with a fixed-window detector."""
    return RingMonitor(
        address=address(1), size=size, detector=deadline_detectors(window)
    )


def test_a_peer_just_picked_up_is_not_a_silent_one():
    monitor = a_monitor(window=10.0)

    monitor.follow(ring(1, 2), now=100.0)

    # Its detector starts from the moment it was picked up. Starting from zero
    # would make every new member unreachable on the round it joined.
    assert monitor.verdicts(105.0) == {address(2): REACHABLE}


def test_a_peer_that_stops_answering_becomes_unreachable():
    monitor = a_monitor(window=10.0)
    monitor.follow(ring(1, 2), now=100.0)

    monitor.heard(address(2), at=105.0)

    assert monitor.verdicts(114.0) == {address(2): REACHABLE}
    assert monitor.verdicts(116.0) == {address(2): UNREACHABLE}


def test_an_answer_brings_a_peer_back():
    monitor = a_monitor(window=10.0)
    monitor.follow(ring(1, 2), now=100.0)
    assert monitor.verdicts(120.0) == {address(2): UNREACHABLE}

    monitor.heard(address(2), at=121.0)

    # A member that is unreachable has not been written off, so the round it
    # starts answering again is the round it is reachable again.
    assert monitor.verdicts(122.0) == {address(2): REACHABLE}


def test_a_link_the_transport_gave_up_on_is_unreachable_at_once():
    monitor = a_monitor(window=10.0)
    monitor.follow(ring(1, 2), now=100.0)

    monitor.link_lost(address(2))

    # The window has not run out. The transport knows something this node's
    # own probe has not waited long enough to find out.
    assert monitor.verdicts(101.0) == {address(2): UNREACHABLE}


def test_an_answer_does_not_retract_what_the_transport_said():
    monitor = a_monitor(window=10.0)
    monitor.follow(ring(1, 2), now=100.0)
    monitor.link_lost(address(2))

    monitor.heard(address(2), at=101.0)

    # Each source is retracted by its own evidence. A quarantined peer cannot
    # answer at all, so treating an answer as the retraction would only ever
    # fire on evidence that never asked the transport anything.
    assert monitor.verdicts(101.0) == {address(2): UNREACHABLE}

    monitor.link_open(address(2))

    assert monitor.verdicts(102.0) == {address(2): REACHABLE}


def test_a_peer_that_leaves_the_ring_is_handed_back():
    monitor = a_monitor(size=1)
    monitor.follow(ring(1, 2), now=100.0)
    assert monitor.peers == (address(2),)

    # Port 10 sorts between 1 and 2, so a member joining there takes over as
    # this node's successor.
    dropped = monitor.follow(ring(1, 10, 2), now=101.0)

    # The caller retracts whatever it said about a peer it has stopped
    # watching. A claim nobody is watching for any more would block
    # convergence with nothing able to take it back.
    assert dropped == (address(2),)
    assert monitor.peers == (address(10),)
    assert monitor.verdicts(101.0) == {address(10): REACHABLE}


@pytest.mark.parametrize(
    "tell",
    [
        lambda monitor: monitor.heard(address(9), at=1.0),
        lambda monitor: monitor.link_lost(address(9)),
        lambda monitor: monitor.link_open(address(9)),
    ],
)
def test_no_verdict_is_reached_about_a_peer_this_node_does_not_watch(tell):
    monitor = a_monitor()
    monitor.follow(ring(1, 2), now=100.0)

    assert tell(monitor) is False
    assert monitor.verdicts(100.0) == {address(2): REACHABLE}


def test_a_link_coming_up_does_not_count_as_an_answer():
    monitor = a_monitor(window=10.0)
    monitor.follow(ring(1, 2), now=100.0)

    # Nothing answers for a whole window, so the peer has gone silent.
    assert monitor.verdicts(111.0) == {address(2): UNREACHABLE}

    monitor.link_open(address(2))

    # A handshake proves a process is accepting connections, not that the
    # daemon behind it is still replying. Counting it as an answer would let a
    # peer whose links churn faster than the window never answer and never be
    # judged, which is the failure this monitor exists to catch.
    assert monitor.verdicts(111.0) == {address(2): UNREACHABLE}

    monitor.heard(address(2), at=111.0)

    assert monitor.verdicts(111.0) == {address(2): REACHABLE}


def test_a_peer_returning_to_the_ring_keeps_what_the_transport_said():
    monitor = a_monitor(size=1)
    monitor.follow(ring(1, 2), now=100.0)
    monitor.link_lost(address(2))

    # Port 10 sorts between 1 and 2, so it takes over as this node's
    # successor and hands port 2 back. Then it leaves again.
    monitor.follow(ring(1, 10, 2), now=101.0)
    monitor.follow(ring(1, 2), now=102.0)

    # Changing whose job a peer is says nothing about the peer. Remoting is
    # still refusing to carry frames to it, so a fresh window here would
    # report it reachable for a whole window on no evidence at all.
    assert monitor.verdicts(102.0) == {address(2): UNREACHABLE}

    monitor.link_open(address(2))
    monitor.heard(address(2), at=102.0)

    assert monitor.verdicts(102.0) == {address(2): REACHABLE}


def test_the_transport_is_forgotten_about_a_member_that_has_gone():
    monitor = a_monitor(size=1)
    monitor.follow(ring(1, 2), now=100.0)
    monitor.link_lost(address(2))

    # The member leaves the cluster altogether, and later an address is
    # reused. What the transport said about the old one is not evidence
    # about the new one, and remembering it for ever would grow without end.
    monitor.follow(ring(1, 3), now=101.0)
    monitor.follow(ring(1, 2), now=102.0)

    assert monitor.verdicts(102.0) == {address(2): REACHABLE}


def test_a_member_that_was_downed_is_no_longer_watched():
    monitor = a_monitor()
    monitor.follow(Gossip(members=ring(1, 2)).alive, now=100.0)

    downed = Gossip(members=(member(1), member(2, status=MemberStatus.DOWN)))
    dropped = monitor.follow(downed.alive, now=101.0)

    # What the daemon passes in is the live membership, so a member the
    # cluster has decided about stops being probed and stops being judged.
    assert dropped == (address(2),)
    assert monitor.peers == ()
