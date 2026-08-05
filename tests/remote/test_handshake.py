"""Tests for the handshake, mostly from the receiving end."""

import asyncio
import contextlib
from collections.abc import AsyncIterator

import pytest

from tapio.actor import ActorSystem
from tapio.remote.address import Address
from tapio.remote.codec import encode
from tapio.remote.transport import FrameLink
from tests.failures import eventually
from tests.remote.peers import Tick, counting, dial, remoting


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


# --- what the handshake establishes -----------------------------------------


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


async def test_the_version_must_match(beta: ActorSystem):
    # A wire format that half works is worse than one that refuses.
    link = await dial(beta, version="99.0.0", welcome=False)
    try:
        assert await closed(link)
        assert beta.remote is not None
        assert beta.remote.associations == ()
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


# --- the secret --------------------------------------------------------------


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


# --- the other direction -----------------------------------------------------


async def test_dialling_something_that_is_not_tapio_fails_as_a_handshake(
    alpha: ActorSystem,
):
    # The dial fails, so what was queued for the peer is dead-lettered rather
    # than silently never landing.
    async def rude(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        writer.close()

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
