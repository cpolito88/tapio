"""A small HTTP surface an operator reaches a cluster through.

A node given [ManagementSettings][tapio.settings.ManagementSettings] runs one of
these: an actor under `/system`, beside the cluster daemon and remoting, that
answers a handful of HTTP requests. It reads what the node believes about the
cluster, and it asks the node to let a member leave or to down one. The
[tapio-cluster][tapio.cluster.cli] command is the client, but the surface is
plain HTTP and JSON, so a health probe or a shell one-liner reaches it too.

It owns its socket the way remoting owns its listener. The port is bound while
the cluster is being constructed, so a test that asks for port `0` learns the
port it got before the actor has run a line, and the actor owns the accept task
and every connection under it, so shutting the system down closes them through
the ordinary stop sweep rather than leaving a listening port behind.

Reading the state needs no turn of the daemon. What a request reports is an
immutable snapshot of gossip taken between the daemon's turns, which is the same
thing [Cluster.state][tapio.cluster.cluster.Cluster.state] hands an application
and carries the same caveat: it is this node's view, true once it has converged
and this node's best guess until then. Changing the state does need a turn, so a
leave or a down is a message to the daemon and the answer is `202 Accepted`: the
node has been asked, and the decision travels as gossip like every other one.
"""

import asyncio
import contextlib
import hmac
import json
import socket
from collections.abc import Callable, Mapping
from typing import Any, Final, final

from pydantic import SecretStr, ValidationError

from tapio.actor.behavior import Behavior, Behaviors
from tapio.actor.context import ActorContext
from tapio.actor.ref import ActorRef
from tapio.actor.signals import PostStop, Signal
from tapio.cluster.messages import ClusterMessage, Down, Leave
from tapio.errors import InsecureRemoteConfig
from tapio.logging import runtime_logger
from tapio.message import Message
from tapio.remote.transport import _is_loopback, server_ssl_context
from tapio.settings import ManagementSettings, TLSSettings

__all__ = [
    "ClusterManagement",
    "open_management_listener",
    "verify_management_security",
]

_log = runtime_logger("cluster")

# The header buffer is the stream's read limit, so a request that keeps sending
# headers without ever ending them is refused once it passes this, rather than
# growing without bound. The body is bounded separately, by Content-Length,
# since an operator's leave is a few dozen bytes and nothing here has a reason to
# read more.
_MAX_HEADER_BYTES: Final = 16 * 1024
_MAX_BODY_BYTES: Final = 64 * 1024

# How long one request has to arrive and be answered. A connection that opens
# and then sends nothing, or dribbles its bytes, is closed at this deadline
# rather than parking a task for as long as it stays open. It bounds the write
# too, so a peer that stops reading cannot hold the response half-sent.
_REQUEST_TIMEOUT: Final = 30.0


class _ManagementMessage(Message):
    """A type nobody can send: the management actor is a listener, not a peer."""


def verify_management_security(settings: ManagementSettings) -> None:
    """Refuse to answer operators beyond loopback with nothing for one to prove.

    The mirror of [verify_bind_security][tapio.remote.transport.verify_bind_security]
    for the management port. Reaching this port is enough to down a member, so
    binding it where another host can reach it, with no way to tell an operator
    from a stranger, fails to start rather than serving strangers. Two things
    tell them apart: a bearer `token`, and a client certificate, which is `tls`
    with a `cafile` the port checks a client's certificate against. Either is
    enough; TLS that only proves the server is not, since it authenticates the
    wrong end.

    Args:
        settings: The management configuration about to be used.

    Raises:
        InsecureRemoteConfig: If `bind_host` is not a loopback address and
            neither a token nor mutual TLS is configured.
    """
    authenticates_the_operator = settings.token is not None or (
        settings.tls is not None and settings.tls.cafile is not None
    )
    if authenticates_the_operator or _is_loopback(settings.bind_host):
        return
    host = (
        "'' (which means every interface)"
        if not settings.bind_host
        else repr(settings.bind_host)
    )
    msg = (
        f"cluster management is configured to bind {host} with nothing to "
        "authenticate an operator. This port can down a member, so binding it "
        "beyond loopback requires ManagementSettings(token=...) or mutual TLS "
        "(tls with a cafile); bind 127.0.0.1 instead if only this host is meant "
        "to manage the cluster."
    )
    raise InsecureRemoteConfig(msg)


@final
class ClusterManagement:
    """A node's operator surface: the port, and what answers on it."""

    def __init__(
        self,
        *,
        listener: socket.socket,
        snapshot: Callable[[], Mapping[str, Any]],
        members: Callable[[], frozenset[str]],
        daemon: ActorRef[ClusterMessage],
        token: SecretStr | None,
        tls: TLSSettings | None,
        address: str,
    ) -> None:
        """Describe a node's management endpoint, before its actor exists.

        Args:
            listener: The socket bound by
                [open_management_listener][tapio.cluster.management.open_management_listener],
                already listening so the port is settled.
            snapshot: Reads what this node believes about the cluster, as the
                plain data a JSON response is built from. Called between the
                daemon's turns, so it must not await.
            members: The addresses this node currently holds a live member
                record for, read the same way and used to answer a leave or a
                down for a member the node does not know with a `404`.
            daemon: This node's cluster daemon, where a leave or a down is sent.
            token: The bearer token an operator must present, or `None` to ask
                for nothing.
            tls: Certificates for the port, or `None` for plaintext HTTP. When
                set the port speaks HTTPS, and requires a client certificate too
                when the settings carry a `cafile`.
            address: This node's canonical address, for the log.
        """
        self._listener = listener
        self._snapshot = snapshot
        self._members = members
        self._daemon = daemon
        self._token = token
        self._tls = tls
        self._address = address
        self._server: asyncio.Server | None = None
        self._serving: asyncio.Task[None] | None = None
        # Each connection being handled, held so shutdown can cancel it. The
        # event loop keeps only a weak reference to a task, so an unheld one
        # could be collected mid-response and leave the socket open.
        self._connections: set[asyncio.Task[None]] = set()
        self._closed = False

    def behavior(self) -> Behavior[_ManagementMessage]:
        """Build the `/system/cluster-management` actor."""

        def build(
            ctx: ActorContext[_ManagementMessage],
        ) -> Behavior[_ManagementMessage]:
            self._start()

            async def on_message(
                message: _ManagementMessage,
            ) -> Behavior[_ManagementMessage]:
                return Behaviors.same()

            async def on_signal(
                ctx: ActorContext[_ManagementMessage], signal: Signal
            ) -> Behavior[_ManagementMessage]:
                if isinstance(signal, PostStop):
                    await self._close()
                return Behaviors.same()

            return Behaviors.receive_message(
                on_message, _ManagementMessage, on_signal=on_signal
            )

        return Behaviors.setup(build)

    def _start(self) -> None:
        """Begin accepting on the socket that was bound at construction.

        The task is held, not discarded, for the reason the remoting listener's
        is: it and `_close` are the two owners of the socket, so `_close` has to
        be able to stop it before it touches the socket, or the loser works on a
        closed one.
        """
        loop = asyncio.get_running_loop()
        self._serving = loop.create_task(
            self._serve(), name="tapio-cluster-management-listener"
        )

    async def _serve(self) -> None:
        """Accept connections on the already-bound socket.

        The TLS context, when there is one, is applied here at the server rather
        than at the socket, the same way remoting does it: the listener is a
        plain socket bound at construction, and every connection accepted through
        it is wrapped as it arrives.
        """
        context = server_ssl_context(self._tls) if self._tls is not None else None
        self._server = await asyncio.start_server(
            self._on_connection,
            sock=self._listener,
            ssl=context,
            limit=_MAX_HEADER_BYTES,
        )
        _log.debug(
            "cluster management is answering on %s%s",
            self._address,
            " over TLS" if context is not None else "",
        )

    def _on_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """Take an accepted connection and hand it to a task this actor owns.

        This runs synchronously as the connection is made, the one moment
        nothing can cancel, so the task is recorded before it has run a line.
        A connection accepted after `_close` has run is closed here, since there
        is nobody left to answer it.
        """
        if self._closed:
            writer.close()
            return
        task = asyncio.get_running_loop().create_task(
            self._handle(reader, writer), name="tapio-cluster-management-connection"
        )
        self._connections.add(task)
        task.add_done_callback(self._connections.discard)

    async def _handle(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """Read one request, answer it, and close the connection.

        One request per connection, closed when it is answered: an operator
        command is a single call, so keep-alive would only be state to get
        wrong. Everything that can go wrong with a request, a header line too
        long or a body that is not the JSON it claimed, becomes a status code
        rather than an exception, because the peer is whoever reached the port
        and the answer to a bad request is to say so and hang up. A request that
        does not arrive within the deadline is a `408`, so a peer that opens a
        connection and then stalls cannot hold a task open for as long as it
        likes.
        """
        try:
            async with asyncio.timeout(_REQUEST_TIMEOUT):
                status, body = await self._answer(reader)
        except TimeoutError:
            status, body = (
                _HTTP_REQUEST_TIMEOUT,
                _error("the request was not sent in time"),
            )
        except (asyncio.IncompleteReadError, OSError):
            # The peer went away or never finished its request. There is nobody
            # to tell, so the connection is just closed.
            status, body = None, b""
        except asyncio.CancelledError:
            with contextlib.suppress(OSError):
                writer.close()
            raise
        try:
            if status is not None:
                writer.write(_response(status, body))
                async with asyncio.timeout(_REQUEST_TIMEOUT):
                    await writer.drain()
        except (OSError, TimeoutError):
            pass
        finally:
            with contextlib.suppress(OSError):
                writer.close()
                await writer.wait_closed()

    async def _answer(
        self, reader: asyncio.StreamReader
    ) -> tuple[tuple[int, str] | None, bytes]:
        """Work out what one request asks for, and what to answer it.

        Returns:
            The status to send and the JSON body, or a `None` status when the
            request was too malformed to answer and the connection is dropped.
        """
        try:
            head = await reader.readuntil(b"\r\n\r\n")
        except asyncio.LimitOverrunError:
            # The headers passed the read buffer without ending. That is the
            # request being too large, not the peer going away, so it is a code.
            return _HTTP_REQUEST_TOO_LARGE, _error("the request headers are too large")
        method, path, headers = _parse_head(head)
        if self._token is not None and not self._authorized(headers):
            return _HTTP_UNAUTHORIZED, _error("a valid bearer token is required")
        length = _content_length(headers)
        if length > _MAX_BODY_BYTES:
            return _HTTP_REQUEST_TOO_LARGE, _error("the request body is too large")
        body = await reader.readexactly(length) if length else b""
        return self._route(method, path, body)

    def _authorized(self, headers: Mapping[str, str]) -> bool:
        """Whether the request presented the token this node was configured with.

        Compared in constant time, so a token is not learned a character at a
        time from how long the comparison took.
        """
        token = self._token
        if token is None:  # pragma: no cover - the caller checks first
            return True
        presented = headers.get("authorization", "")
        scheme, _, value = presented.partition(" ")
        if scheme.lower() != "bearer":
            return False
        return hmac.compare_digest(value, token.get_secret_value())

    def _route(
        self, method: str, path: str, body: bytes
    ) -> tuple[tuple[int, str], bytes]:
        """Match a request to what answers it.

        The routes are few enough to read: the state is a `GET`, and a leave or
        a down is a `POST` that names the member in a JSON body. A path nobody
        serves is a `404`, and the right path with the wrong method is a `405`,
        so a client learns which half it got wrong.
        """
        if path in ("/", "/status"):
            if method != "GET":
                return _HTTP_METHOD_NOT_ALLOWED, _error("read the status with GET")
            return _HTTP_OK, _json(self._snapshot())
        if path in ("/leave", "/down"):
            if method != "POST":
                return _HTTP_METHOD_NOT_ALLOWED, _error(f"ask with POST {path}")
            return self._request_transition(path, body)
        return _HTTP_NOT_FOUND, _error(f"nothing is served at {path!r}")

    def _request_transition(
        self, path: str, body: bytes
    ) -> tuple[tuple[int, str], bytes]:
        """Ask the daemon to move a named member, for a leave or a down.

        The address is read from the JSON body and turned into the message the
        daemon accepts, which is where an address that could never be dialled is
        refused: building the message validates it, so a mistyped address is a
        `400` here rather than a warning in the node's log and nothing else. A
        member the node does not know is a `404`, so an operator that named the
        wrong one hears it. Everything past that is a `202`: the node has been
        asked, and the move travels as gossip.
        """
        address = _address_from(body)
        if address is None:
            return _HTTP_BAD_REQUEST, _error('the body must be {"address": "..."}')
        try:
            message: ClusterMessage = (
                Leave(address=address) if path == "/leave" else Down(address=address)
            )
        except ValidationError:
            return _HTTP_BAD_REQUEST, _error(f"{address!r} is not a dialable address")
        if address not in self._member_addresses():
            return _HTTP_NOT_FOUND, _error(f"{address} is not a member here")
        self._daemon.tell(message)
        action = "leave" if path == "/leave" else "down"
        _log.info("an operator asked %s to %s", address, action)
        return _HTTP_ACCEPTED, _json({"accepted": action, "address": address})

    def _member_addresses(self) -> frozenset[str]:
        """The addresses this node currently holds a live member record for."""
        return self._members()

    async def _close(self) -> None:
        """Stop listening, and let go of every connection still open.

        The mirror of the remoting endpoint's close: stop the accept task before
        anything closes the socket under it, close the server, then cancel and
        await the connections so none is left half-written when the loop goes.
        """
        if self._closed:
            return
        self._closed = True
        serving = self._serving
        self._serving = None
        if serving is not None:
            serving.cancel()
            with contextlib.suppress(asyncio.CancelledError, OSError):
                await serving
        server = self._server
        self._server = None
        if server is not None:
            server.close()
            with contextlib.suppress(OSError, asyncio.CancelledError):
                await server.wait_closed()
        else:
            self._listener.close()
        for task in list(self._connections):
            task.cancel()
        for task in list(self._connections):
            with contextlib.suppress(asyncio.CancelledError, OSError):
                await task
        self._connections.clear()

    def __repr__(self) -> str:
        """Render the address this endpoint answers for."""
        return f"ClusterManagement({self._address!r})"


def open_management_listener(settings: ManagementSettings) -> socket.socket:
    """Bind the port an operator reaches this node on, before anything runs.

    Bound here rather than inside the serving task so a node asked for port `0`
    knows the port it got before it hands anyone a way to reach it, the same
    reason remoting binds at construction.

    Args:
        settings: How this node answers operators.

    Returns:
        A listening socket, not yet accepting.

    Raises:
        InsecureRemoteConfig: If it would answer beyond loopback with no token.
        OSError: If the address could not be bound.
    """
    verify_management_security(settings)
    family = socket.AF_INET6 if ":" in settings.bind_host else socket.AF_INET
    listener = socket.create_server(
        (settings.bind_host.strip("[]"), settings.bind_port), family=family
    )
    listener.setblocking(False)
    return listener


def _parse_head(head: bytes) -> tuple[str, str, dict[str, str]]:
    """Pull the method, path and headers out of a request's head.

    Args:
        head: Everything up to and including the blank line.

    Returns:
        The method, the path with any query stripped, and the headers with
        lower-cased names.
    """
    lines = head.split(b"\r\n")
    request_line = lines[0].decode("latin-1")
    parts = request_line.split(" ")
    method = parts[0] if parts else ""
    target = parts[1] if len(parts) > 1 else "/"
    path = target.split("?", 1)[0]
    headers: dict[str, str] = {}
    for line in lines[1:]:
        if not line:
            continue
        name, _, value = line.decode("latin-1").partition(":")
        headers[name.strip().lower()] = value.strip()
    return method, path, headers


def _content_length(headers: Mapping[str, str]) -> int:
    """Read the declared body length, treating anything unparseable as none."""
    raw = headers.get("content-length")
    if raw is None:
        return 0
    try:
        return max(0, int(raw))
    except ValueError:
        return 0


def _address_from(body: bytes) -> str | None:
    """Read the `address` field out of a JSON body, or `None` if it is not there.

    Args:
        body: The request body.

    Returns:
        The address string, or `None` when the body is not an object with a
        string `address`.
    """
    try:
        parsed = json.loads(body or b"{}")
    except (ValueError, UnicodeDecodeError):
        return None
    if not isinstance(parsed, dict):
        return None
    address = parsed.get("address")
    return address if isinstance(address, str) else None


def _json(payload: Mapping[str, Any]) -> bytes:
    """Render a payload as a JSON body."""
    return json.dumps(payload).encode("utf-8")


def _error(detail: str) -> bytes:
    """Render an error body, so every failure answers with the same shape."""
    return _json({"error": detail})


def _response(status: tuple[int, str], body: bytes) -> bytes:
    """Frame a status and a JSON body as one HTTP/1.1 response.

    Args:
        status: The code and its reason phrase.
        body: The JSON body, already encoded.

    Returns:
        The bytes to write, with `Connection: close` because a management call
        is a single request and the connection is not reused.
    """
    code, reason = status
    head = (
        f"HTTP/1.1 {code} {reason}\r\n"
        "Content-Type: application/json\r\n"
        f"Content-Length: {len(body)}\r\n"
        "Connection: close\r\n"
        "\r\n"
    )
    return head.encode("latin-1") + body


_HTTP_OK: Final = (200, "OK")
_HTTP_ACCEPTED: Final = (202, "Accepted")
_HTTP_BAD_REQUEST: Final = (400, "Bad Request")
_HTTP_UNAUTHORIZED: Final = (401, "Unauthorized")
_HTTP_NOT_FOUND: Final = (404, "Not Found")
_HTTP_METHOD_NOT_ALLOWED: Final = (405, "Method Not Allowed")
_HTTP_REQUEST_TIMEOUT: Final = (408, "Request Timeout")
_HTTP_REQUEST_TOO_LARGE: Final = (413, "Content Too Large")
