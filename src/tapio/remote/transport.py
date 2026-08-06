"""The link: length-framed JSON over a TCP stream, with optional TLS.

A link carries two kinds of frame over the same stream, and tells them apart
without parsing either. **Message frames** are what [tapio.remote.codec][]
writes, and they open with `{"v":`. **Link frames** are the transport's own,
the handshake, the heartbeat and the death watch, and they open with
`{"link":`. The reader can therefore pass a message frame straight on, and
only parses the frames meant for itself.

Death watch travels as link frames rather than as messages because it is the
runtime talking to itself. A watch is not addressed to an actor, carries no
user payload, and must work whether or not the two systems have registered any
message types in common.

Everything here is about bytes and sockets. What a frame means is
[tapio.remote.codec][]'s job, and who it reaches is
[tapio.remote.association][]'s.
"""

import asyncio
import contextlib
import ipaddress
import json
import socket
import ssl
from collections.abc import Awaitable, Callable
from types import TracebackType
from typing import Any, Final, Protocol, Self, TypeAlias

from pydantic import BaseModel

from tapio.errors import InsecureRemoteConfig, MessageDecodingError
from tapio.remote.codec import LENGTH_PREFIX, frame_length
from tapio.settings import RemoteSettings, TLSSettings

__all__ = [
    "LINK_PREFIX",
    "FrameLink",
    "Heartbeat",
    "Link",
    "LinkFrame",
    "Unwatch",
    "Watch",
    "WatcheeTerminated",
    "bind",
    "client_ssl_context",
    "connect",
    "framed",
    "is_link_frame",
    "link_body",
    "listen",
    "server_ssl_context",
    "verify_bind_security",
]

ConnectionHandler: TypeAlias = Callable[
    [asyncio.StreamReader, asyncio.StreamWriter], Awaitable[None]
]
"""What a listening endpoint does with each accepted connection."""

LINK_PREFIX: Final = b'{"link":'
"""What a link frame opens with, and a message frame never does.

Every link frame model below declares `link` as its first field, so
`model_dump_json` emits it first and this prefix is a fact about the encoding
rather than a hope about it.
"""


class LinkFrame(BaseModel):
    """Base for the transport's own frames, which no actor ever sees."""

    model_config = {"frozen": True}


class Heartbeat(LinkFrame):
    """Proof that a silent peer is still there.

    Written on an idle association every `heartbeat_interval`. The receiving
    end records when it arrived and does nothing else with it. What to
    conclude when heartbeats stop is the failure detector's decision, not the
    reader's.
    """

    link: str = "heartbeat"
    """The frame kind, first in the encoding so `LINK_PREFIX` holds."""


class Watch(LinkFrame):
    """Ask a peer to report when one of its actors stops."""

    link: str = "watch"
    """The frame kind, first in the encoding so `LINK_PREFIX` holds."""

    watchee: str
    """The actor to watch, in the receiving system's path space, uid included.
    A uid that no longer matches is answered at once, since that incarnation is
    already over."""

    watcher: str
    """Who is watching, in the sending system's path space. The receiver never
    resolves it. It is an opaque key that comes back on the answer, which is
    what keeps a watch from being a way to address actors on the watcher."""


class Unwatch(LinkFrame):
    """Withdraw a watch. The pair of paths identifies which one."""

    link: str = "unwatch"
    """The frame kind, first in the encoding so `LINK_PREFIX` holds."""

    watchee: str
    """The actor being watched, in the receiving system's path space."""

    watcher: str
    """Who was watching, in the sending system's path space."""


class WatcheeTerminated(LinkFrame):
    """Report that a watched actor has stopped.

    Sent by the node that owns the actor, so it means the actor really did
    stop. A watcher that is told the same thing because the link went silent
    is being told a guess, and that one is decided locally and never travels.
    """

    link: str = "terminated"
    """The frame kind, first in the encoding so `LINK_PREFIX` holds."""

    watchee: str
    """The actor that stopped, in the sending system's path space."""

    watcher: str
    """Who was watching, in the receiving system's path space, as they said it."""


class Link(Protocol):
    """One open connection, as everything above the transport uses it.

    A protocol rather than the class, so that a test can put a link that
    drops, delays or swallows frames where a real one goes and exercise the
    failure detector without breaking anything real.
    """

    @property
    def peer(self) -> str:
        """The socket address on the other end, for a log line."""
        ...

    async def read_frame(self) -> bytes:
        """Read one complete frame, prefix included."""
        ...

    async def write_frame(self, data: bytes) -> None:
        """Write one complete frame and wait for the buffer to drain."""
        ...

    async def write_link(self, message: LinkFrame) -> None:
        """Write one of the transport's own frames."""
        ...

    async def close(self) -> None:
        """Close the connection, ignoring how it ends."""
        ...


def is_link_frame(frame: bytes) -> bool:
    """Whether a complete frame belongs to the transport rather than an actor.

    Args:
        frame: One complete frame, length prefix included.

    Returns:
        `True` for the handshake and heartbeat frames this module writes.
    """
    return frame[LENGTH_PREFIX : LENGTH_PREFIX + len(LINK_PREFIX)] == LINK_PREFIX


def link_body(frame: bytes) -> dict[str, Any]:
    """Read a link frame's JSON object.

    Args:
        frame: One complete link frame, length prefix included.

    Returns:
        The decoded object.

    Raises:
        MessageDecodingError: If the body is not a JSON object.
    """
    try:
        parsed = json.loads(frame[LENGTH_PREFIX:])
    except ValueError as error:
        raise MessageDecodingError(f"link frame is not JSON: {error}") from error
    if not isinstance(parsed, dict):
        msg = f"a link frame is a JSON object, got {type(parsed).__name__}"
        raise MessageDecodingError(msg)
    return parsed


def framed(body: bytes) -> bytes:
    """Put a length prefix in front of an encoded body."""
    return len(body).to_bytes(LENGTH_PREFIX, "big") + body


class FrameLink:
    """One TCP connection, read and written a whole frame at a time.

    Not thread-safe, and not meant to be. A link is read by one task and
    written by one actor, both on the system's own loop.
    """

    __slots__ = ("_max_frame_bytes", "_peer", "_reader", "_writer")

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        *,
        max_frame_bytes: int,
    ) -> None:
        """Bind a link to an open stream pair.

        Args:
            reader: The read half.
            writer: The write half.
            max_frame_bytes: Refuse an inbound frame declaring more than this,
                from its length prefix and before its body is read.
        """
        self._reader = reader
        self._writer = writer
        self._max_frame_bytes = max_frame_bytes
        self._peer = _describe_socket(writer)

    @property
    def peer(self) -> str:
        """The socket address on the other end, for a log line.

        The handshake establishes the dialable address of the system over
        there. This is only where the packets come from, which is not always
        the same thing, and is still what a reader wants in a log.
        """
        return self._peer

    async def read_frame(self) -> bytes:
        """Read one complete frame, prefix included.

        Returns:
            The frame, ready to be classified and handed on.

        Raises:
            FrameTooLargeError: If the declared length is over the limit. The
                body is not read, so a peer announcing a gigabyte costs only
                a header.
            MessageDecodingError: If the prefix is malformed.
            asyncio.IncompleteReadError: If the peer closed, either cleanly
                between frames or part-way through one. Both mean the link is
                over, and the association reports them the same way.
            OSError: If the connection failed.
        """
        prefix = await self._reader.readexactly(LENGTH_PREFIX)
        length = frame_length(prefix, max_frame_bytes=self._max_frame_bytes)
        body = await self._reader.readexactly(length)
        return prefix + body

    async def write_frame(self, data: bytes) -> None:
        """Write one complete frame and wait for the buffer to drain.

        The drain is what makes a slow peer show up as a slow actor. The
        association waits here, its mailbox fills, and the overflow strategy
        decides what happens. The write buffer never grows without a limit.

        Args:
            data: One complete frame, length prefix included.

        Raises:
            OSError: If the connection failed.
        """
        self._writer.write(data)
        await self._writer.drain()

    async def write_link(self, message: LinkFrame) -> None:
        """Write one of the transport's own frames.

        Args:
            message: The link frame to send.

        Raises:
            OSError: If the connection failed.
        """
        await self.write_frame(framed(message.model_dump_json().encode()))

    async def read_link(self, timeout: float) -> dict[str, Any]:  # noqa: ASYNC109 - the handshake deadline
        """Read one link frame, refusing anything else.

        Args:
            timeout: Seconds to wait for it.

        Returns:
            The decoded object.

        Raises:
            MessageDecodingError: If what arrived was not a link frame.
            TimeoutError: If nothing arrived in time.
            asyncio.IncompleteReadError: If the peer closed first.
            OSError: If the connection failed.
        """
        async with asyncio.timeout(timeout):
            data = await self.read_frame()
        if not is_link_frame(data):
            msg = "expected a link frame before any message frame"
            raise MessageDecodingError(msg)
        return link_body(data)

    async def close(self) -> None:
        """Close the connection, ignoring how it ends.

        A link is closed because something already went wrong or because the
        system is going away, and neither is improved by an error raised on the
        way out.
        """
        self._writer.close()
        with contextlib.suppress(OSError, asyncio.CancelledError):
            await self._writer.wait_closed()

    async def __aenter__(self) -> Self:
        """Return the open link."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Close the link however the block ended."""
        await self.close()

    def __repr__(self) -> str:
        """Render the socket on the other end."""
        return f"FrameLink({self._peer!r})"


async def connect(
    host: str, port: int, *, max_frame_bytes: int, ssl_context: ssl.SSLContext | None
) -> FrameLink:
    """Dial a peer and return the link to it.

    Args:
        host: The canonical host the peer advertises.
        port: Its port.
        max_frame_bytes: The inbound frame limit for this link.
        ssl_context: The client context, or `None` for plaintext.

    Returns:
        An open link, before any handshake.

    Raises:
        OSError: If the connection could not be made.
    """
    reader, writer = await asyncio.open_connection(host, port, ssl=ssl_context)
    return FrameLink(reader, writer, max_frame_bytes=max_frame_bytes)


def bind(settings: RemoteSettings) -> socket.socket:
    """Bind and listen, synchronously, so the port is known before anything runs.

    Binding here rather than inside the server task lets a system with
    `bind_port=0` advertise a canonical address as soon as it is constructed.
    The first ref it hands out already names a port a peer can dial.

    Args:
        settings: Where to listen.

    Returns:
        A listening socket, not yet accepting.

    Raises:
        OSError: If the address could not be bound.
    """
    # IPv6 only when the host is written as an IPv6 literal. Guessing the
    # address family from anything else is guessing about someone's network.
    family = socket.AF_INET6 if ":" in settings.bind_host else socket.AF_INET
    listener = socket.create_server(
        (settings.bind_host.strip("[]"), settings.bind_port), family=family
    )
    listener.setblocking(False)
    return listener


async def listen(
    handler: ConnectionHandler,
    listener: socket.socket,
    *,
    ssl_context: ssl.SSLContext | None,
) -> asyncio.Server:
    """Start accepting on an already-bound socket.

    Args:
        handler: Called with the reader and writer of each accepted connection.
        listener: The socket returned by `bind`.
        ssl_context: The server context, or `None` for plaintext.

    Returns:
        The running server.
    """
    return await asyncio.start_server(handler, sock=listener, ssl=ssl_context)


def verify_bind_security(settings: RemoteSettings) -> None:
    """Refuse to listen beyond loopback with nothing for a peer to prove.

    Args:
        settings: The remoting configuration about to be used.

    Raises:
        InsecureRemoteConfig: If `bind_host` is not a loopback address and no
            `secret` is set.
    """
    if settings.secret is not None or _is_loopback(settings.bind_host):
        return
    msg = (
        f"remoting is configured to bind {settings.bind_host!r} with no secret. "
        "A port that accepts frames naming actor paths and message types is a "
        "serious surface, so binding beyond loopback requires "
        "RemoteSettings(secret=...); bind 127.0.0.1 instead if this system is "
        "only meant to be reachable from its own host."
    )
    raise InsecureRemoteConfig(msg)


def _is_loopback(host: str) -> bool:
    """Whether a bind host names this machine and nothing else."""
    if host in {"localhost", ""}:
        return True
    try:
        return ipaddress.ip_address(host.strip("[]")).is_loopback
    except ValueError:
        # A name that is not an address literal. It might resolve to loopback,
        # it might not. Guessing wrong leaves an open port, so it refuses to
        # guess.
        return False


def server_ssl_context(tls: TLSSettings) -> ssl.SSLContext:
    """Build the context this system presents to peers that dial it.

    Args:
        tls: The certificate settings.

    Returns:
        A server context, requiring a client certificate when `cafile` is set.

    Raises:
        OSError: If a certificate or key file cannot be read.
    """
    context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    context.load_cert_chain(tls.certfile, tls.keyfile)
    if tls.cafile is not None:
        context.load_verify_locations(tls.cafile)
        context.verify_mode = ssl.CERT_REQUIRED
    return context


def client_ssl_context(tls: TLSSettings) -> ssl.SSLContext:
    """Build the context this system uses when it dials a peer.

    Args:
        tls: The certificate settings.

    Returns:
        A client context, presenting this system's own certificate so a peer
        configured for mutual authentication can check it.

    Raises:
        OSError: If a certificate or key file cannot be read.
    """
    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=tls.cafile)
    context.check_hostname = tls.check_hostname
    context.load_cert_chain(tls.certfile, tls.keyfile)
    return context


def _describe_socket(writer: asyncio.StreamWriter) -> str:
    """Render the far end of a stream as `host:port`, or say it is unknown."""
    peer = writer.get_extra_info("peername")
    if isinstance(peer, tuple) and len(peer) >= 2:
        return f"{peer[0]}:{peer[1]}"
    return "an unknown peer"
