"""Tests for the link: framing, frame kinds, binding and TLS."""

import asyncio

import pytest

from tapio.actor.path import ActorPath
from tapio.errors import FrameTooLargeError, InsecureRemoteConfig, MessageDecodingError
from tapio.remote.codec import LENGTH_PREFIX, encode
from tapio.remote.transport import (
    FrameLink,
    Heartbeat,
    bind,
    client_ssl_context,
    connect,
    framed,
    is_link_frame,
    link_body,
    listen,
    server_ssl_context,
    verify_bind_security,
)
from tapio.settings import RemoteSettings, TLSSettings
from tests.remote.peers import Tick


def remote(**overrides: object) -> RemoteSettings:
    """Remote settings that ignore the developer's environment."""
    return RemoteSettings(_env_file=None, **overrides)  # type: ignore[arg-type]


async def linked() -> tuple[FrameLink, FrameLink, asyncio.Server]:
    """A pair of connected links, and the server holding one end open."""
    accepted: asyncio.Queue[FrameLink] = asyncio.Queue()

    async def handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        await accepted.put(FrameLink(reader, writer, max_frame_bytes=1024))
        # The handler task ends here; the link stays open for the test.

    listener = bind(remote(bind_port=0))
    server = await listen(handle, listener, ssl_context=None)
    port = listener.getsockname()[1]
    client = await connect("127.0.0.1", port, max_frame_bytes=1024, ssl_context=None)
    return client, await accepted.get(), server


# --- framing -----------------------------------------------------------------


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


# --- telling the two kinds of frame apart ------------------------------------


def test_a_link_frame_is_recognised_without_being_parsed():
    assert is_link_frame(framed(Heartbeat().model_dump_json().encode()))


def test_a_message_frame_is_not_a_link_frame():
    path = ActorPath.root("beta").child("user").child("ticker", uid=1)
    assert not is_link_frame(encode(Tick(n=1), to=path))


def test_a_link_frame_that_is_not_an_object_is_refused():
    with pytest.raises(MessageDecodingError, match="a JSON object"):
        link_body(framed(b"[1, 2]"))


def test_a_link_frame_that_is_not_json_is_refused():
    with pytest.raises(MessageDecodingError, match="not JSON"):
        link_body(framed(b"{oops"))


# --- binding -----------------------------------------------------------------


def test_binding_port_zero_gives_a_port_that_can_be_read_back():
    listener = bind(remote(bind_port=0))
    try:
        assert listener.getsockname()[1] > 0
    finally:
        listener.close()


def test_loopback_needs_no_secret():
    verify_bind_security(remote(bind_host="127.0.0.1"))
    verify_bind_security(remote(bind_host="localhost"))


def test_binding_anywhere_else_without_a_secret_is_refused():
    with pytest.raises(InsecureRemoteConfig, match="RemoteSettings\\(secret"):
        verify_bind_security(remote(bind_host="0.0.0.0"))


def test_a_name_that_is_not_an_address_literal_is_not_assumed_to_be_loopback():
    # It might resolve to loopback, it might not. Guessing wrong leaves an
    # open port, so it does not guess.
    with pytest.raises(InsecureRemoteConfig):
        verify_bind_security(remote(bind_host="orders.svc"))


def test_binding_anywhere_with_a_secret_is_allowed():
    verify_bind_security(remote(bind_host="0.0.0.0", secret="shh"))


# --- TLS ---------------------------------------------------------------------


def test_a_certificate_that_is_not_there_fails_where_it_is_configured(tmp_path):
    tls = TLSSettings(_env_file=None, certfile=str(tmp_path / "absent.pem"))
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
