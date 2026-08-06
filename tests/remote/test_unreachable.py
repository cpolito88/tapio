"""Giving up on a peer, and what it costs to be wrong about it."""

import pytest

from tapio import DeadLetter, DeadLetterReason
from tapio.errors import ActorSystemTerminating, HandshakeError
from tapio.remote.failure import DeadlineDetector, DownAlone, PeerUnreachable
from tapio.testkit import assert_no_leaked_tasks, two_nodes
from tests.failures import eventually
from tests.remote.peers import GHOST, Tick, counting, dial, uri, watching


async def test_the_detector_gives_up_only_after_the_window():
    detector = DeadlineDetector(unreachable_after=1.0, started_at=100.0)

    assert detector.is_available(100.5)
    assert not detector.is_available(101.5)
    detector.heartbeat(101.0)
    assert detector.is_available(101.5)


async def test_one_node_decides_alone_and_says_so(alpha):
    decision = await DownAlone().decide(alpha.address)

    assert decision.down
    assert "alone" in decision.detail


async def test_a_partition_makes_both_sides_give_up_on_the_other():
    with assert_no_leaked_tasks():
        async with two_nodes() as nodes:
            seen: list[str] = []
            ticks: list[int] = []
            here: list[PeerUnreachable] = []
            there: list[PeerUnreachable] = []
            nodes.alpha.events.subscribe(PeerUnreachable, here.append)
            nodes.beta.events.subscribe(PeerUnreachable, there.append)

            worker = nodes.beta.spawn(counting(ticks), "worker")
            remote = await nodes.alpha.resolve(uri(nodes.beta, worker), expect=Tick)
            nodes.alpha.spawn(watching(remote, seen), "watcher")
            remote.tell(Tick(n=1))
            await eventually(lambda: ticks == [1])

            # Both nodes are alive and neither can hear the other. Both will
            # conclude the other is gone, and both will be locally correct.
            nodes.partition()

            await eventually(lambda: seen == [f"terminated {worker.path}"], within=5.0)
            await eventually(lambda: bool(here) and bool(there), within=5.0)
            assert here[0].quarantined
            assert nodes.alpha.remote.quarantined == (nodes.beta.address,)
            assert nodes.alpha.remote.associations == ()
            # The actor the watcher was told about is running the whole time.
            # That is the false positive, stated as the documented behaviour
            # rather than as a bug, because one node cannot do better.
            assert nodes.beta.refs.lookup(worker.path) is not None


async def test_sending_to_a_quarantined_peer_dead_letters_and_dials_nothing():
    with assert_no_leaked_tasks():
        async with two_nodes() as nodes:
            letters: list[DeadLetter] = []
            nodes.alpha.dead_letters.subscribe(letters.append)
            ticks: list[int] = []
            worker = nodes.beta.spawn(counting(ticks), "worker")
            remote = await nodes.alpha.resolve(uri(nodes.beta, worker), expect=Tick)
            remote.tell(Tick(n=1))
            await eventually(lambda: ticks == [1])

            nodes.partition()
            await eventually(lambda: nodes.alpha.remote.quarantined != (), within=5.0)
            nodes.heal()
            letters.clear()

            remote.tell(Tick(n=2))

            await eventually(lambda: bool(letters))
            assert letters[0].reason == DeadLetterReason.QUARANTINED
            assert letters[0].peer == str(nodes.beta.address)
            # Healing the network re-associates nothing. Watchers were already
            # told that actors over there are gone, so a link coming quietly
            # back would leave the two nodes believing different things.
            await eventually(lambda: nodes.alpha.remote.associations == ())
            assert ticks == [1]


async def test_reconnect_is_what_repairs_a_quarantine():
    with assert_no_leaked_tasks():
        async with two_nodes() as nodes:
            ticks: list[int] = []
            worker = nodes.beta.spawn(counting(ticks), "worker")
            remote = await nodes.alpha.resolve(uri(nodes.beta, worker), expect=Tick)
            remote.tell(Tick(n=1))
            await eventually(lambda: ticks == [1])

            nodes.partition()
            await eventually(
                lambda: (
                    nodes.alpha.remote.quarantined != ()
                    and nodes.beta.remote.quarantined != ()
                ),
                within=5.0,
            )
            nodes.heal()

            # Both sides gave up, so both have to relent. Beta says it is
            # willing to be dialled again and alpha does the dialling.
            assert nodes.beta.remote.clear_quarantine(nodes.alpha.address)
            await nodes.alpha.remote.reconnect(nodes.beta.address)

            assert nodes.alpha.remote.quarantined == ()
            # Refs held across a quarantine address a session that is over, so
            # the ref is resolved again rather than reused.
            again = await nodes.alpha.resolve(uri(nodes.beta, worker), expect=Tick)
            again.tell(Tick(n=2))
            await eventually(lambda: ticks == [1, 2])


async def test_reconnect_to_a_peer_that_is_not_there_says_so():
    with assert_no_leaked_tasks():
        async with two_nodes() as nodes:
            address = nodes.beta.address
            await nodes.beta.terminate()

            with pytest.raises(HandshakeError, match="did not come up"):
                await nodes.alpha.remote.reconnect(address)


async def test_reconnect_refuses_once_the_system_is_shutting_down():
    with assert_no_leaked_tasks():
        async with two_nodes() as nodes:
            address = nodes.beta.address
            await nodes.alpha.terminate()

            with pytest.raises(ActorSystemTerminating, match="shutting down"):
                await nodes.alpha.remote.reconnect(address)


async def test_a_peer_that_comes_back_as_a_new_incarnation_replaces_the_old(alpha):
    seen: list[str] = []
    letters: list[DeadLetter] = []
    events: list[PeerUnreachable] = []
    alpha.dead_letters.subscribe(letters.append)
    alpha.events.subscribe(PeerUnreachable, events.append)

    first = await dial(alpha, uid=1)
    try:
        await eventually(lambda: alpha.remote.associations == (GHOST,))
        # A ref on the peer, watched from here. The peer never has to answer:
        # what is being tested is what happens to the watch when the peer
        # underneath it turns out to be a different process.
        remote = await alpha.resolve(f"{GHOST}/user/worker#1", expect=Tick)
        alpha.spawn(watching(remote, seen), "watcher")

        second = await dial(alpha, uid=2)
        try:
            # The uid is the whole of the evidence. Same address, same port,
            # different process, so every ref held against the old one names
            # a stranger and its watchers are told so.
            await eventually(lambda: seen == ["terminated tapio://ghost/user/worker#1"])
            await eventually(lambda: bool(events))
            assert not events[0].quarantined
            # Known exactly, rather than guessed at from silence, so the
            # address is not frozen and the new incarnation is talked to.
            assert alpha.remote.quarantined == ()
            await eventually(lambda: alpha.remote.associations == (GHOST,))
        finally:
            await second.close()
    finally:
        await first.close()


async def test_a_peer_dialling_in_while_quarantined_is_refused():
    with assert_no_leaked_tasks():
        async with two_nodes() as nodes:
            ticks: list[int] = []
            answers: list[int] = []
            worker = nodes.beta.spawn(counting(ticks), "worker")
            listener = nodes.alpha.spawn(counting(answers), "listener")
            remote = await nodes.alpha.resolve(uri(nodes.beta, worker), expect=Tick)
            remote.tell(Tick(n=1))
            await eventually(lambda: ticks == [1])

            nodes.partition()
            await eventually(lambda: nodes.alpha.remote.quarantined != (), within=5.0)
            await eventually(lambda: nodes.beta.remote.associations == (), within=5.0)
            nodes.heal()

            # Beta gave up too, so it dials afresh. Alpha refuses the link
            # rather than letting the peer decide that the quarantine is over.
            back = await nodes.beta.resolve(uri(nodes.alpha, listener), expect=Tick)
            back.tell(Tick(n=9))

            await eventually(lambda: nodes.alpha.remote.associations == ())
            assert answers == []
