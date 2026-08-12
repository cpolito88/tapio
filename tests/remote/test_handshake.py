"""Tests for the handshake, mostly from the receiving end."""

import asyncio
import contextlib
import json
from collections.abc import AsyncIterator

import pytest

from tapio.actor import ActorSystem, DeadLetter
from tapio.remote.address import Address
from tapio.remote.codec import encode
from tapio.remote.transport import FrameLink, connect, framed
from tapio.version import __version__
from tests.failures import eventually
from tests.remote.peers import GHOST, Tick, counting, dial, remoting


@pytest.fixture
async def guarded() -> AsyncIterator[ActorSystem]:
    """A system that requires a secret."""
    running = ActorSystem("beta", remoting(secret="shh"))
    try:
        yield running
    finally:
        await running.terminate()


async def closed(link: FrameLink) -> bool:
    """Whether the peer closed the link."""
    try:
        await link.read_frame()
    except (asyncio.IncompleteReadError, ConnectionError):
        return True
    return False


async def test_a_peer_that_says_who_it_is_gets_a_welcome(beta: ActorSystem):
    link = await dial(beta)
    try:
        assert beta.remote is not None
        # `dial` has read the welcome. The association is keyed by the address
        # the peer advertised, not the socket it dialled from: only the
        # advertised one can be dialled back.
        await eventually(lambda: beta.remote.associations != ())  # type: ignore[union-attr]
    finally:
        await link.close()


async def test_the_protocol_must_match(beta: ActorSystem):
    # A wire format that half works is worse than one that refuses.
    link = await dial(beta, protocol=99, welcome=False)
    try:
        assert await closed(link)
        assert beta.remote is not None
        assert beta.remote.associations == ()
    finally:
        await link.close()


async def test_a_peer_on_another_release_is_welcome(beta: ActorSystem):
    # The link is pinned to the wire protocol, not to the library version, so
    # a fleet can roll from one release to the next instead of stopping to
    # swap every node at once.
    link = await dial(beta, version="99.0.0")
    try:
        assert beta.remote is not None
        await eventually(lambda: beta.remote.associations != ())  # type: ignore[union-attr]
    finally:
        await link.close()


async def test_a_peer_with_no_address_to_dial_is_refused(beta: ActorSystem):
    # A peer advertising no address cannot be answered at.
    link = await dial(beta, address=Address(system="ghost"), welcome=False)
    try:
        assert await closed(link)
    finally:
        await link.close()


async def test_a_peer_whose_name_and_address_disagree_is_refused(beta: ActorSystem):
    link = await dial(beta, system="someone-else", welcome=False)
    try:
        assert await closed(link)
    finally:
        await link.close()


async def test_a_peer_that_writes_nonsense_instead_of_a_hello_is_refused(
    beta: ActorSystem,
):
    port = beta.address.port
    assert port is not None
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    try:
        writer.write(b"\x00\x00\x00\x05hello")
        await writer.drain()
        with pytest.raises(asyncio.IncompleteReadError):
            await reader.readexactly(4096)
    finally:
        writer.close()
        # Awaited, not just asked for: a transport still closing when the loop
        # goes away is collected unclosed, and this suite turns that warning
        # into an error at whichever test happens to run next.
        await writer.wait_closed()


async def test_a_peer_that_answers_the_challenge_is_let_in(guarded: ActorSystem):
    link = await dial(guarded, secret="shh")
    try:
        assert guarded.remote is not None
        await eventually(lambda: guarded.remote.associations != ())  # type: ignore[union-attr]
    finally:
        await link.close()


async def test_a_peer_with_the_wrong_answer_is_refused(guarded: ActorSystem):
    link = await dial(guarded, proof="not-the-answer", welcome=False)
    try:
        assert await closed(link)
        assert guarded.remote is not None
        assert guarded.remote.associations == ()
    finally:
        await link.close()


async def test_a_peer_with_no_answer_at_all_is_refused(guarded: ActorSystem):
    link = await dial(guarded, welcome=False)
    try:
        assert await closed(link)
    finally:
        await link.close()


async def test_a_refused_peer_gets_nothing_delivered(guarded: ActorSystem):
    # No frames are read after a failed handshake, so writing a message right
    # behind a bad hello is not a way in.
    seen: list[int] = []
    ticker = guarded.spawn(counting(seen), "ticker")
    link = await dial(guarded, proof="wrong", welcome=False)
    try:
        # The peer may already have hung up, which is the point of the test.
        with contextlib.suppress(OSError):
            await link.write_frame(encode(Tick(n=1), to=ticker.path))
        assert await closed(link)
        assert seen == []
    finally:
        await link.close()


async def test_dialling_something_that_is_not_tapio_fails_as_a_handshake(
    alpha: ActorSystem,
):
    # The dial fails, so what was queued for the peer is dead-lettered rather
    # than silently never landing.
    async def rude(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        writer.close()
        await writer.wait_closed()

    listener = await asyncio.start_server(rude, "127.0.0.1", 0)
    port = listener.sockets[0].getsockname()[1]
    try:
        reasons: list[str] = []
        alpha.dead_letters.subscribe(lambda letter: reasons.append(letter.reason))
        remote = await alpha.resolve(
            f"tapio://stranger@127.0.0.1:{port}/user/ticker#1", expect=Tick
        )
        remote.tell(Tick(n=1))

        await eventually(lambda: bool(reasons))
    finally:
        listener.close()
        await listener.wait_closed()


async def test_the_first_frame_names_nothing_about_the_system(beta: ActorSystem):
    # A listening port answers anything that can reach it, so whatever it says
    # first is readable for the cost of one connection. It used to volunteer
    # the system name, the canonical address, the incarnation uid and the exact
    # release, which is a deployment's identity handed to a scanner.
    port = beta.address.port
    assert port is not None
    link = await connect("127.0.0.1", port, max_frame_bytes=65536, ssl_context=None)
    try:
        hello = await link.read_link(2.0)

        assert hello["link"] == "server-hello"
        assert set(hello) == {"link", "protocol", "nonce"}
        # Said another way, so that a field added later without thought fails
        # this rather than passing it.
        said = json.dumps(hello)
        assert beta.name not in said
        assert str(beta.address) not in said
        assert str(beta.uid) not in said
        assert __version__ not in said
    finally:
        await link.close()


async def test_the_welcome_names_the_system_once_the_peer_has_answered(
    beta: ActorSystem,
):
    # The identity is not withheld, only deferred to the first point at which
    # the peer reading it has proved it holds the secret.
    port = beta.address.port
    assert port is not None
    link = await connect("127.0.0.1", port, max_frame_bytes=65536, ssl_context=None)
    try:
        hello = await link.read_link(2.0)
        await link.write_frame(
            framed(
                json.dumps(
                    {
                        "link": "client-hello",
                        "system": "ghost",
                        "address": str(GHOST),
                        "uid": 99,
                        "protocol": hello["protocol"],
                        "version": __version__,
                        "nonce": "0" * 32,
                        "proof": "",
                    }
                ).encode()
            )
        )

        welcome = await link.read_link(2.0)

        assert welcome["link"] == "welcome"
        assert welcome["system"] == beta.name
        assert welcome["address"] == str(beta.address)
        assert welcome["uid"] == beta.uid
        assert welcome["version"] == __version__
    finally:
        await link.close()


async def test_a_dialler_refuses_a_protocol_mismatch_before_naming_itself(
    alpha: ActorSystem,
):
    # The check runs against the server-hello, so a peer speaking something
    # else never learns who dialled it. The refusal has to survive not knowing
    # the peer's release, since the hello no longer carries one.
    said: list[bytes] = []

    async def wrong_protocol(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            writer.write(
                framed(
                    json.dumps(
                        {"link": "server-hello", "protocol": 99, "nonce": "0" * 32}
                    ).encode()
                )
            )
            await writer.drain()
            # Returns empty as soon as the dialler gives up, which is the
            # assertion: it closed without having written anything.
            with contextlib.suppress(asyncio.IncompleteReadError, ConnectionError):
                said.append(await reader.read(4096))
        finally:
            writer.close()
            with contextlib.suppress(OSError, asyncio.CancelledError):
                await writer.wait_closed()

    server = await asyncio.start_server(wrong_protocol, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        peer = Address(system="stranger", host="127.0.0.1", port=port)
        letters: list[DeadLetter] = []
        alpha.dead_letters.subscribe(letters.append)
        remote = await alpha.resolve(f"{peer}/user/ticker#1", expect=Tick)
        remote.tell(Tick(n=1))

        # The dial is refused and the message accounted for.
        await eventually(lambda: bool(letters))
        # And nothing this system knows about itself went out with it.
        assert alpha.name.encode() not in b"".join(said)
    finally:
        server.close()
        await server.wait_closed()
