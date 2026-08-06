"""Watching an actor that lives on another system."""

import json

from tapio.actor import ActorSystem
from tapio.remote.codec import encode
from tapio.remote.failure import PeerUnreachable
from tapio.remote.transport import framed
from tapio.testkit import assert_no_leaked_tasks
from tests.failures import eventually
from tests.remote.peers import Tick, counting, dial, remoting, uri, watching


async def test_watching_a_remote_actor_that_stops_delivers_terminated(
    alpha: ActorSystem, beta: ActorSystem
):
    seen: list[str] = []
    ticks: list[int] = []
    worker = beta.spawn(counting(ticks), "worker")
    remote = await alpha.resolve(uri(beta, worker), expect=Tick)
    watcher = alpha.spawn(watching(remote, seen), "watcher")

    # A negative tick stops it through its own behavior, so the peer really
    # does report a stop rather than a link that broke.
    remote.tell(Tick(n=-1))

    await eventually(lambda: seen == [f"terminated {worker.path}"])
    # The watcher is still running, which is what makes this a signal rather
    # than a failure that happened to look like one.
    watcher.tell(Tick(n=1))
    await eventually(lambda: seen[-1] == "tick 1")


async def test_watching_a_remote_actor_that_already_stopped_answers_at_once(
    alpha: ActorSystem, beta: ActorSystem
):
    seen: list[str] = []
    ticks: list[int] = []
    worker = beta.spawn(counting(ticks), "worker")
    address = uri(beta, worker)
    remote = await alpha.resolve(address, expect=Tick)
    remote.tell(Tick(n=-1))
    await eventually(lambda: beta.refs.lookup(worker.path) is None)

    alpha.spawn(watching(remote, seen), "watcher")

    # The uid is what makes this safe to answer: that incarnation is over, and
    # whoever holds the path next is a stranger to this watcher.
    await eventually(lambda: seen == [f"terminated {worker.path}"])


async def test_unwatching_a_remote_actor_releases_it_on_both_systems(
    alpha: ActorSystem, beta: ActorSystem
):
    seen: list[str] = []
    ticks: list[int] = []
    worker = beta.spawn(counting(ticks), "worker")
    remote = await alpha.resolve(uri(beta, worker), expect=Tick)
    watcher = alpha.spawn(watching(remote, seen), "watcher")
    await eventually(lambda: beta.remote.associations != ())  # type: ignore[union-attr]
    watched_side = beta.remote.association_for(alpha.address)  # type: ignore[union-attr]
    watching_side = alpha.remote.association_for(beta.address)  # type: ignore[union-attr]
    assert watched_side is not None
    assert watching_side is not None
    await eventually(lambda: watched_side.watched == (worker.path,))
    assert watching_side.watching == (worker.path,)

    # A negative tick makes the watcher withdraw, which has to be undone at
    # both ends: the watcher's own record here, and the watcher standing on
    # the real cell over there.
    watcher.tell(Tick(n=-1))

    await eventually(lambda: seen == ["unwatched"])
    await eventually(lambda: watched_side.watched == ())
    assert watching_side.watching == ()

    # And nothing is reported afterwards.
    remote.tell(Tick(n=-1))
    await eventually(lambda: beta.refs.lookup(worker.path) is None)
    assert seen == ["unwatched"]


async def test_killing_the_peer_system_terminates_every_watcher():
    with assert_no_leaked_tasks():
        seen: list[str] = []
        ticks: list[int] = []
        events: list[PeerUnreachable] = []
        alpha = ActorSystem("alpha", remoting())
        beta = ActorSystem("beta", remoting())
        alpha.events.subscribe(PeerUnreachable, events.append)
        try:
            worker = beta.spawn(counting(ticks), "worker")
            remote = await alpha.resolve(uri(beta, worker), expect=Tick)
            alpha.spawn(watching(remote, seen), "watcher")
            remote.tell(Tick(n=1))
            await eventually(lambda: ticks == [1])

            await beta.terminate()

            await eventually(lambda: seen == [f"terminated {worker.path}"])
            # A peer that stopped cleanly and one that died look the same from
            # here, so the event says the link ended and not why.
            await eventually(lambda: len(events) == 1)
            assert events[0].peer == str(beta.address)
            assert not events[0].quarantined
        finally:
            await alpha.terminate()
            await beta.terminate()


async def test_a_watch_leaves_nothing_behind_on_either_system():
    with assert_no_leaked_tasks():
        seen: list[str] = []
        ticks: list[int] = []
        alpha = ActorSystem("alpha", remoting())
        beta = ActorSystem("beta", remoting())
        try:
            worker = beta.spawn(counting(ticks), "worker")
            remote = await alpha.resolve(uri(beta, worker), expect=Tick)
            alpha.spawn(watching(remote, seen), "watcher")
            remote.tell(Tick(n=1))
            await eventually(lambda: ticks == [1])
            association = beta.remote.association_for(alpha.address)  # type: ignore[union-attr]
            assert association is not None
            await eventually(lambda: association.watched == (worker.path,))

            # The watch was registered on the real cell over there, so a
            # stopped association that forgot it would leave a watcher on a
            # live actor and nobody to answer it.
            await alpha.terminate()

            await eventually(lambda: association.watched == ())
            cell = beta.refs.lookup(worker.path)
            assert cell is not None
        finally:
            await alpha.terminate()
            await beta.terminate()


async def test_a_malformed_watch_frame_is_ignored_without_dropping_the_link(
    beta: ActorSystem,
):
    # The peer proved who it was in the handshake, so a frame it got wrong is
    # a bug over there rather than an attack. Dropping the link over one would
    # cost every other conversation on it.
    ticks: list[int] = []
    worker = beta.spawn(counting(ticks), "worker")
    link = await dial(beta)
    try:
        await link.write_frame(framed(json.dumps({"link": "watch"}).encode()))
        await link.write_frame(
            framed(json.dumps({"link": "something-newer", "x": 1}).encode())
        )
        await link.write_frame(encode(Tick(n=1), to=worker.path))

        await eventually(lambda: ticks == [1])
    finally:
        await link.close()
