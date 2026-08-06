"""Tests for the endpoint: resolving, addressing and shutdown."""

import asyncio

import pytest

from tapio.actor import ActorContext, ActorSystem, Behavior, Behaviors
from tapio.errors import InsecureRemoteConfig, MessageTypeError, RefResolutionError
from tapio.remote.address import Address
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


async def test_a_terminated_system_holds_no_associations():
    system = ActorSystem("alpha", remoting())
    await system.terminate()

    assert system.remote is not None
    assert system.remote.associations == ()


async def refused(port: int) -> None:
    """Assert that nothing answers on a port."""
    with pytest.raises(ConnectionRefusedError):
        await asyncio.open_connection("127.0.0.1", port)
