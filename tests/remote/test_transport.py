"""Tests for the link: framing, frame kinds, binding and TLS."""

import asyncio
import contextlib
import socket
from collections.abc import AsyncIterator
from typing import Any

import pytest
from pydantic import ValidationError

from tapio.actor.path import ActorPath
from tapio.errors import FrameTooLargeError, InsecureRemoteConfig, MessageDecodingError
from tapio.remote import transport
from tapio.remote.codec import LENGTH_PREFIX, encode
from tapio.remote.transport import (
    HANDSHAKE_FRAME_BYTES,
    FrameLink,
    Heartbeat,
    bind,
    client_ssl_context,
    close_server,
    connect,
    framed,
    is_link_frame,
    is_loopback,
    link_body,
    listen,
    server_ssl_context,
    verify_bind_security,
)
from tapio.settings import RemoteSettings
from tapio.testkit import (
    IsolatedRemoteSettings,
    IsolatedTLSSettings,
)
from tests.failures import eventually
from tests.remote.peers import Tick


def remote(**overrides: Any) -> RemoteSettings:
    """Remote settings that ignore the developer's environment."""
    return IsolatedRemoteSettings(**overrides)


async def linked() -> tuple[FrameLink, FrameLink, asyncio.Server]:
    """A pair of connected links, and the server holding one end open."""
    accepted: asyncio.Queue[FrameLink] = asyncio.Queue()

    def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        # Synchronous, like the endpoint's own accept. A connection handler
        # runs as the connection is made, which is the one moment nothing can
        # cancel it, so that is where the link is taken over. The queue is
        # unbounded, so recording it cannot block.
        accepted.put_nowait(FrameLink(reader, writer, max_frame_bytes=1024))

    listener = bind(remote(bind_port=0))
    server = await listen(handle, listener, ssl_context=None)
    port = listener.getsockname()[1]
    client = await connect("127.0.0.1", port, max_frame_bytes=1024, ssl_context=None)
    return client, await accepted.get(), server


async def test_a_frame_arrives_whole():
    client, server_side, server = await linked()
    try:
        await client.write_frame(framed(b'{"v":1}'))
        assert await server_side.read_frame() == framed(b'{"v":1}')
    finally:
        await client.close()
        await server_side.close()
        server.close()


async def test_a_frame_over_the_limit_is_refused_from_its_prefix():
    # Refused before the body is read, so a peer cannot make this end allocate
    # the memory it announced.
    client, server_side, server = await linked()
    try:
        await client.write_frame((10_000).to_bytes(LENGTH_PREFIX, "big") + b"x")
        with pytest.raises(FrameTooLargeError, match="refused without reading"):
            await server_side.read_frame()
    finally:
        await client.close()
        await server_side.close()
        server.close()


async def test_a_closed_peer_ends_the_read():
    client, server_side, server = await linked()
    await client.close()
    try:
        with pytest.raises(asyncio.IncompleteReadError):
            await server_side.read_frame()
    finally:
        await server_side.close()
        server.close()


@pytest.mark.parametrize("turns", range(4))
async def test_closing_a_server_closes_a_connection_it_already_accepted(turns: int):
    # The loop accepts a connection one turn before it builds its transport.
    # A close in that gap used to drop the socket with nothing left to close
    # it, so the client waited until the garbage collector found it. Closing
    # after each of these turn counts puts at least one close in that gap.
    def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        writer.close()

    loop = asyncio.get_running_loop()
    listener = bind(remote(bind_port=0))
    server = await listen(handle, listener, ssl_context=None)
    client = socket.create_connection(listener.getsockname())
    client.setblocking(False)
    try:
        for _ in range(turns):
            await asyncio.sleep(0)
        await close_server(server, listener)

        # An accepted connection is closed by the handler. One the server
        # never accepted is reset along with the listener.
        try:
            async with asyncio.timeout(2.0):
                assert await loop.sock_recv(client, 1) == b""
        except ConnectionResetError:
            pass
    finally:
        client.close()


@contextlib.asynccontextmanager
async def stalled() -> AsyncIterator[tuple[FrameLink, socket.socket]]:
    """A link whose write buffer is full, because the peer never reads.

    Yields:
        The link, and its socket for the test to check.
    """
    accepted: asyncio.Queue[asyncio.StreamWriter] = asyncio.Queue()

    def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        accepted.put_nowait(writer)

    listener = bind(remote(bind_port=0))
    server = await listen(handle, listener, ssl_context=None)
    reader, writer = await asyncio.open_connection(*listener.getsockname()[:2])
    peer = await accepted.get()
    try:
        link = FrameLink(reader, writer, max_frame_bytes=1024)
        chunk = framed(b"x" * (1024 * 1024))
        # The kernel buffers on both ends fill first, then the transport's. A
        # write that cannot drain is the sign that they have.
        while True:
            try:
                async with asyncio.timeout(0.2):
                    await link.write_frame(chunk)
            except TimeoutError:
                break
        yield link, writer.get_extra_info("socket")
    finally:
        # The peer first: on 3.12 and later a server's close waits for every
        # connection it accepted.
        peer.transport.abort()
        writer.transport.abort()
        server.close()


async def test_closing_a_link_whose_peer_stopped_reading_aborts_it(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(transport, "CLOSE_GRACE", 0.1)
    async with stalled() as (link, sock):
        async with asyncio.timeout(2.0):
            await link.close()
        await eventually(lambda: sock.fileno() == -1)


async def test_a_caller_cancelled_while_a_link_closes_is_cancelled_and_it_aborts():
    async with stalled() as (link, sock):
        closing = asyncio.ensure_future(link.close())
        await asyncio.sleep(0)
        closing.cancel()
        with pytest.raises(asyncio.CancelledError):
            await closing
        await eventually(lambda: sock.fileno() == -1)


def test_a_link_frame_is_recognised_without_being_parsed():
    assert is_link_frame(framed(Heartbeat().model_dump_json().encode()))


def test_a_message_frame_is_not_a_link_frame():
    path = ActorPath.root("beta").child("user").child("ticker", uid=1)
    assert not is_link_frame(encode(Tick(n=1), to=path))


def test_a_link_frame_that_is_not_an_object_is_refused():
    with pytest.raises(MessageDecodingError, match="a JSON object"):
        link_body(framed(b"[1, 2]"))


def test_a_deeply_nested_link_frame_is_refused():
    # Before the handshake, a RecursionError here escaped `accept` from a peer
    # that had proved nothing.
    depth = 200_000
    frame = framed(b'{"link":' + b"[" * depth + b"]" * depth + b"}")
    with pytest.raises(MessageDecodingError, match="not JSON"):
        link_body(frame)


def test_a_link_frame_that_is_not_json_is_refused():
    with pytest.raises(MessageDecodingError, match="not JSON"):
        link_body(framed(b"{oops"))


async def test_a_bracketed_ipv6_host_is_dialled_bare():
    # Port 9 is unbound, so the dial fails. What matters is how: a resolver
    # given `[::1]` looks for a host by that name and finds none.
    try:
        link = await connect("[::1]", 9, max_frame_bytes=1024, ssl_context=None)
    except socket.gaierror as error:
        pytest.fail(f"the brackets reached the resolver: {error}")
    except OSError:
        return
    await link.close()


async def test_a_handshake_frame_is_capped_below_the_link_limit():
    # The peer has proved nothing during a handshake, so the link's own limit,
    # 4 MiB by default, is far more than it should make this end hold.
    accepted: list[FrameLink] = []

    def announce(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        writer.write((HANDSHAKE_FRAME_BYTES + 1).to_bytes(LENGTH_PREFIX, "big"))
        accepted.append(FrameLink(reader, writer, max_frame_bytes=1024))

    listener = bind(remote(bind_port=0))
    server = await listen(announce, listener, ssl_context=None)
    port = listener.getsockname()[1]
    link = await connect(
        "127.0.0.1", port, max_frame_bytes=4 * 1024 * 1024, ssl_context=None
    )
    try:
        with pytest.raises(FrameTooLargeError):
            await link.read_link(2.0)
    finally:
        await link.close()
        for side in accepted:
            await side.close()
        server.close()


def test_binding_port_zero_gives_a_port_that_can_be_read_back():
    listener = bind(remote(bind_port=0))
    try:
        assert listener.getsockname()[1] > 0
    finally:
        listener.close()


def test_is_loopback_names_this_machine_and_nothing_else():
    # Public now, because the cluster's management port gates on it too and was
    # reaching across packages for a private name. A public promise is worth
    # asserting directly, rather than only through the two checks that call it.
    assert is_loopback("127.0.0.1")
    assert is_loopback("localhost")
    assert is_loopback("[::1]")
    assert not is_loopback("0.0.0.0")
    assert not is_loopback("")
    assert not is_loopback("db.internal")


def test_loopback_needs_no_secret():
    verify_bind_security(remote(bind_host="127.0.0.1"))
    verify_bind_security(remote(bind_host="localhost"))


def test_binding_anywhere_else_without_a_secret_is_refused():
    with pytest.raises(InsecureRemoteConfig, match="RemoteSettings\\(secret"):
        verify_bind_security(remote(bind_host="0.0.0.0"))


def test_an_empty_bind_host_is_not_loopback():
    # It reads like a setting nobody filled in, and create_server reads it as
    # INADDR_ANY. Treating it as loopback was the one way past this check.
    with pytest.raises(InsecureRemoteConfig, match="every interface"):
        verify_bind_security(remote(bind_host=""))


def test_an_empty_bind_host_binds_every_interface():
    # Which is what makes the refusal above the right answer rather than a
    # pedantic one.
    listener = bind(remote(bind_host="", bind_port=0, secret="shh"))
    try:
        assert listener.getsockname()[0] == "0.0.0.0"
    finally:
        listener.close()


def test_a_name_that_is_not_an_address_literal_is_not_assumed_to_be_loopback():
    # It might resolve to loopback, it might not. Guessing wrong leaves an
    # open port, so it does not guess.
    with pytest.raises(InsecureRemoteConfig):
        verify_bind_security(remote(bind_host="orders.svc"))


def test_an_empty_secret_is_refused_where_it_is_configured():
    # Anyone can answer the challenge with an HMAC keyed by an empty secret,
    # so it would pass the bind check and prove nothing.
    with pytest.raises(ValidationError, match="empty secret"):
        remote(bind_host="0.0.0.0", secret="")


def test_binding_anywhere_with_a_secret_is_allowed():
    verify_bind_security(remote(bind_host="0.0.0.0", secret="shh"))


def test_a_certificate_that_is_not_there_fails_where_it_is_configured(tmp_path):
    tls = IsolatedTLSSettings(certfile=str(tmp_path / "absent.pem"))
    with pytest.raises(OSError, match="No such file"):
        server_ssl_context(tls)
    with pytest.raises(OSError, match="No such file"):
        client_ssl_context(tls)


async def test_a_link_names_the_socket_on_the_other_end():
    client, server_side, server = await linked()
    try:
        assert client.peer.startswith("127.0.0.1:")
        assert repr(client).startswith("FrameLink(")
    finally:
        await client.close()
        await server_side.close()
        server.close()
