"""Tests for the endpoint: resolving, addressing and shutdown."""

import asyncio
import contextlib
import gc
import socket
import sys

import pytest

from tapio.actor import ActorContext, ActorSystem, Behavior, Behaviors
from tapio.errors import InsecureRemoteConfig, MessageTypeError, RefResolutionError
from tapio.remote.address import Address
from tapio.remote.transport import LinkFrame
from tapio.settings import RemoteSettings, TapioSettings
from tapio.testkit import assert_no_leaked_tasks
from tests.failures import eventually
from tests.messages import NotAMessage
from tests.remote.peers import Tick, counting, remoting, uri


async def test_resolving_this_system_gives_the_live_local_ref(alpha: ActorSystem):
    # Resolving your own address must not put a socket in the middle of a
    # local send.
    ticker = alpha.spawn(counting([]), "ticker")

    assert await alpha.resolve(uri(alpha, ticker), expect=Tick) is ticker


async def test_resolving_a_peer_gives_a_ref_addressed_to_it(
    alpha: ActorSystem, beta: ActorSystem
):
    ticker = beta.spawn(counting([]), "ticker")

    remote = await alpha.resolve(uri(beta, ticker), expect=Tick)

    assert remote.address == beta.address
    assert remote.path == ticker.path
    assert "beta" in repr(remote)


async def test_every_ref_for_a_peer_shares_one_association(
    alpha: ActorSystem, beta: ActorSystem
):
    # One connection per peer pair, however many refs point at it. That is
    # what makes FIFO per association mean anything.
    here: list[int] = []
    there: list[int] = []
    ticker = beta.spawn(counting(here), "ticker")
    other = beta.spawn(counting(there), "other")

    first = await alpha.resolve(uri(beta, ticker), expect=Tick)
    second = await alpha.resolve(uri(beta, other), expect=Tick)
    first.tell(Tick(n=1))
    second.tell(Tick(n=2))

    await eventually(lambda: here == [1] and there == [2])
    assert alpha.remote is not None
    assert alpha.remote.associations == (beta.address,)


async def test_a_ref_outlives_the_link_it_was_resolved_on(
    alpha: ActorSystem, beta: ActorSystem
):
    # A ref points at an actor on a node, not at the socket that was open when
    # it was resolved, so the next send dials again.
    seen: list[int] = []
    ticker = beta.spawn(counting(seen), "ticker")
    remote = await alpha.resolve(uri(beta, ticker), expect=Tick)
    remote.tell(Tick(n=1))
    await eventually(lambda: seen == [1])

    assert alpha.remote is not None
    association = alpha.remote.associations
    assert association == (beta.address,)
    alpha.remote.forget_all("the link went away")
    await eventually(lambda: alpha.remote.associations == ())  # type: ignore[union-attr]

    remote.tell(Tick(n=2))

    await eventually(lambda: seen == [1, 2])


async def test_a_send_while_the_old_link_is_still_draining_dials_a_new_one(
    alpha: ActorSystem, beta: ActorSystem
):
    # An association that has been asked to stop stays in the table until its
    # actor finishes stopping. Sending into that window used to hand the
    # message to the association that is draining, where it dead-lettered,
    # which is how `reconnect` could report success and then deliver nothing.
    seen: list[int] = []
    ticker = beta.spawn(counting(seen), "ticker")
    remote = await alpha.resolve(uri(beta, ticker), expect=Tick)
    remote.tell(Tick(n=1))
    await eventually(lambda: seen == [1])

    assert alpha.remote is not None
    alpha.remote.forget_all("the link went away")
    # No wait for the table to clear: sending now is the case under test.
    assert alpha.remote.associations == (beta.address,)
    remote.tell(Tick(n=2))

    await eventually(lambda: seen == [1, 2])


async def test_resolving_something_that_is_not_a_ref_says_so(alpha: ActorSystem):
    with pytest.raises(RefResolutionError, match="not an actor ref"):
        await alpha.resolve("not a ref at all", expect=Tick)


async def test_resolving_an_address_with_nowhere_to_dial_says_so(alpha: ActorSystem):
    # This is how a system with remoting off writes its refs: a name, and no
    # host to send to.
    with pytest.raises(RefResolutionError, match="no host to dial"):
        await alpha.resolve("tapio://other/user/x#1", expect=Tick)


async def test_resolving_a_peer_without_remoting_configured_says_so():
    async with ActorSystem("solo", TapioSettings(_env_file=None)) as solo:
        with pytest.raises(RefResolutionError, match="remoting switched off"):
            await solo.resolve("tapio://other@127.0.0.1:9/user/x#1", expect=Tick)


async def test_expecting_something_that_is_not_a_message_is_refused(
    alpha: ActorSystem, beta: ActorSystem
):
    ticker = beta.spawn(counting([]), "ticker")

    with pytest.raises(MessageTypeError, match=r"tapio\.Message"):
        await alpha.resolve(uri(beta, ticker), expect=NotAMessage)


async def test_an_actor_resolves_through_its_own_context(
    alpha: ActorSystem, beta: ActorSystem
):
    seen: list[int] = []
    ticker = beta.spawn(counting(seen), "ticker")
    address = uri(beta, ticker)

    def sender() -> Behavior[Tick]:
        async def on_message(ctx: ActorContext[Tick], message: Tick) -> Behavior[Tick]:
            remote = await ctx.resolve(address, expect=Tick)
            remote.tell(message)
            return Behaviors.same()

        return Behaviors.receive(on_message, msg_type=Tick)

    alpha.spawn(sender(), "sender").tell(Tick(n=3))

    await eventually(lambda: seen == [3])


async def test_the_advertised_port_is_the_one_the_os_handed_out(alpha: ActorSystem):
    # The port is bound during construction, so the first ref handed out
    # already names a port a peer can dial.
    assert alpha.address.port is not None
    assert alpha.address.port > 0
    assert alpha.address.host == "127.0.0.1"


async def test_a_canonical_address_overrides_what_the_socket_says():
    # Under NAT or port mapping, what peers dial is not what the socket is
    # bound to. A ref always writes down the former.
    settings = TapioSettings(
        _env_file=None,
        remote=RemoteSettings(
            _env_file=None,
            bind_port=0,
            canonical_host="orders.svc",
            canonical_port=25520,
        ),
    )
    async with ActorSystem("orders", settings) as system:
        assert system.address == Address(system="orders", host="orders.svc", port=25520)


async def test_each_incarnation_has_its_own_uid():
    # A system restarted on the same host and port is a different peer, and
    # the uid is what says so.
    async with (
        ActorSystem("alpha", remoting()) as one,
        ActorSystem("alpha", remoting()) as two,
    ):
        assert one.uid != two.uid


async def test_remoting_is_off_unless_it_is_configured():
    async with ActorSystem("solo", TapioSettings(_env_file=None)) as solo:
        assert solo.remote is None
        assert not solo.address.is_addressable


async def test_binding_beyond_loopback_without_a_secret_refuses_to_start():
    # It raises during construction, so a misconfigured deployment fails to
    # start rather than serving strangers.
    with pytest.raises(InsecureRemoteConfig, match="secret"):
        ActorSystem(
            "exposed",
            TapioSettings(
                _env_file=None,
                remote=RemoteSettings(_env_file=None, bind_host="0.0.0.0", bind_port=0),
            ),
        )


async def test_both_systems_terminate_with_the_sockets_closed():
    with assert_no_leaked_tasks():
        one = ActorSystem("alpha", remoting())
        two = ActorSystem("beta", remoting())
        seen: list[int] = []
        ticker = two.spawn(counting(seen), "ticker")
        remote = await one.resolve(uri(two, ticker), expect=Tick)
        remote.tell(Tick(n=1))
        await eventually(lambda: seen == [1])

        port = two.address.port
        assert port is not None
        await one.terminate()
        await two.terminate()

    assert one.refs.paths() == ()
    assert two.refs.paths() == ()
    assert one.remote is not None
    assert one.remote.associations == ()
    await refused(port)


async def test_a_system_that_never_associated_still_closes_its_port():
    with assert_no_leaked_tasks():
        system = ActorSystem("alpha", remoting())
        port = system.address.port
        assert port is not None
        await system.terminate()

    await refused(port)


async def test_closing_the_endpoint_stops_its_listener():
    # close() and the accept task are the only two owners of the socket, so
    # close() has to stop the task before it closes the socket under it. This
    # exercises the interleaving where close() wins: nothing has awaited since
    # the system was constructed, so the accept task has had no turn yet.
    loop = asyncio.get_running_loop()
    reported: list[str] = []
    previous = loop.get_exception_handler()
    loop.set_exception_handler(
        lambda _loop, context: reported.append(str(context.get("exception")))
    )
    try:
        with assert_no_leaked_tasks():
            system = ActorSystem("alpha", remoting())
            endpoint = system.remote
            assert endpoint is not None
            port = system.address.port
            assert port is not None

            await endpoint.close()
            # A turn for anything the close left pending to misbehave in.
            await asyncio.sleep(0)
            await system.terminate()

        gc.collect()
        await asyncio.sleep(0)
    finally:
        loop.set_exception_handler(previous)

    # A listener left running would have woken up to a closed socket and
    # raised where nobody was waiting, which asyncio reports here.
    assert reported == []
    await refused(port)


async def test_a_terminated_system_holds_no_associations():
    system = ActorSystem("alpha", remoting())
    await system.terminate()

    assert system.remote is not None
    assert system.remote.associations == ()


async def refused(port: int) -> None:
    """Assert that nothing answers on a port."""
    with pytest.raises(ConnectionRefusedError):
        await asyncio.open_connection("127.0.0.1", port)


async def test_a_refused_link_is_closed_by_a_task_the_endpoint_holds():
    # The event loop keeps only a weak reference to a task, so a close running
    # with nobody holding it can be collected before the socket is released.
    closed: list[str] = []

    with assert_no_leaked_tasks():
        system = ActorSystem("alpha", remoting())
        endpoint = system.remote
        assert endpoint is not None

        endpoint.close_link_later(_SlowClosingLink(0.05, closed), _elsewhere())
        await asyncio.sleep(0)
        # Nothing outside the endpoint refers to the task at this point.
        gc.collect()

        await system.terminate()

    assert closed == ["closed"]


async def test_closing_the_endpoint_waits_for_a_link_it_is_still_releasing():
    # The tree stops the associations, but a refused link belongs to nobody,
    # so the endpoint's own close is the last chance to finish releasing it.
    # The close here takes long enough that the turns close() would take
    # anyway are not enough: it has to actually wait.
    closed: list[str] = []

    with assert_no_leaked_tasks():
        system = ActorSystem("alpha", remoting())
        endpoint = system.remote
        assert endpoint is not None

        endpoint.close_link_later(_SlowClosingLink(0.2, closed), _elsewhere())
        await endpoint.close()

        assert closed == ["closed"]
        await system.terminate()


async def test_a_handshake_cancelled_before_it_starts_has_its_link_closed():
    # A task cancelled before its first line never runs, so a handshake that
    # recorded its own link from the inside would leave the socket open. The
    # link is recorded when the connection is made, so close() closes it even
    # though the handshake never ran a line.
    closed: list[str] = []

    with assert_no_leaked_tasks():
        system = ActorSystem("alpha", remoting())
        endpoint = system.remote
        assert endpoint is not None

        async def never() -> None:
            raise AssertionError("a handshake cancelled before it runs never runs")

        task = endpoint.dispatcher.spawn_task(never(), name="tapio-remote-handshake")
        endpoint._handshakes[task] = _SlowClosingLink(0.0, closed)
        task.cancel()

        await endpoint.close()

        assert closed == ["closed"]
        await system.terminate()


async def test_a_connection_accepted_after_close_is_closed_at_once():
    # The listener can still be readable at the moment it shuts, so the loop
    # delivers a connection after close() has drained. Nobody is left to hand
    # it to and nobody is left to run a handshake, so the endpoint closes the
    # transport on the spot rather than leaving it for the garbage collector.
    with assert_no_leaked_tasks():
        system = ActorSystem("alpha", remoting())
        endpoint = system.remote
        assert endpoint is not None
        await endpoint.close()

        here, there = socket.socketpair()
        reader, writer = await asyncio.open_connection(sock=here)
        try:
            endpoint._accept(reader, writer)
            assert writer.is_closing()
        finally:
            writer.close()
            with contextlib.suppress(OSError):
                await writer.wait_closed()
            there.close()

        await system.terminate()


def _elsewhere() -> Address:
    """An address to name in a close, which nothing dials."""
    return Address(system="beta", host="127.0.0.1", port=1)


class _SlowClosingLink:
    """A link whose close takes long enough to outlast an incidental turn."""

    def __init__(self, takes: float, closed: list[str]) -> None:
        self._takes = takes
        self._closed = closed

    @property
    def peer(self) -> str:
        return "127.0.0.1:1"

    async def read_frame(self) -> bytes:
        raise AssertionError("a refused link is never read")

    async def write_frame(self, data: bytes) -> None:
        raise AssertionError("a refused link is never written")

    async def write_link(self, message: LinkFrame) -> None:
        raise AssertionError("a refused link is never written")

    async def close(self) -> None:
        await asyncio.sleep(self._takes)
        self._closed.append("closed")


def test_a_construction_that_fails_after_the_bind_releases_the_port():
    # The socket is bound inside __init__, before the guardians exist, so
    # nothing else holds a reference that could close it afterwards. This
    # takes the raise path that is already there: Dispatcher.from_running_loop
    # runs just after the bind and needs a running loop.
    port = _free_port()
    settings = TapioSettings(
        _env_file=None, remote=RemoteSettings(_env_file=None, bind_port=port)
    )

    held = None
    try:
        ActorSystem("off-loop", settings)
    except RuntimeError:
        # Kept, so the partly built system stays reachable. Without it the
        # socket is closed by refcounting and the leak hides itself.
        held = sys.exc_info()[2]
    assert held is not None

    with socket.socket() as probe:
        probe.bind(("127.0.0.1", port))


def _free_port() -> int:
    """A port nothing is listening on, for a bind that is meant to fail later."""
    with socket.socket() as finder:
        finder.bind(("127.0.0.1", 0))
        chosen: int = finder.getsockname()[1]
    return chosen
