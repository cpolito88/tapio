"""The endpoint: one listening port, and every association behind it.

A system with `remote` settings gets one of these. It owns the socket peers
dial, the table of associations by peer address, and the resolver that turns a
foreign address into a ref that reaches it. It is itself an actor, `/system/
remote`, whose children are the associations, so shutting down the system
closes the port and every link under it through the ordinary stop sweep.

The socket is bound **synchronously, at system construction**. That is what
makes `bind_port=0` usable: the canonical address is settled before the first
ref is handed out, rather than changing under a ref that has already been
written down. It is also what makes a misconfigured deployment fail to start
instead of failing to be reachable.
"""

import asyncio
import contextlib
import re
import socket
from collections.abc import Callable
from typing import Any, Final, TypeVar

from tapio.actor.behavior import Behavior, Behaviors
from tapio.actor.cell import ActorCell
from tapio.actor.context import ActorContext
from tapio.actor.dead_letters import DeadLetterOffice, DeadLetterReason
from tapio.actor.mailbox import MailboxConfig, OverflowStrategy
from tapio.actor.path import ActorPath
from tapio.actor.ref import ActorRef
from tapio.actor.signals import PostStop, Signal
from tapio.dispatch.dispatcher import Dispatcher
from tapio.errors import HandshakeError, TapioError
from tapio.logging import runtime_logger
from tapio.message import Message
from tapio.remote.address import Address
from tapio.remote.association import Association, AssociationMessage
from tapio.remote.handshake import accept
from tapio.remote.ref import RemoteRef
from tapio.remote.transport import (
    FrameLink,
    bind,
    listen,
    server_ssl_context,
    verify_bind_security,
)
from tapio.settings import RemoteSettings, TapioSettings
from tapio.validation import MessageType, MessageValidator, resolve_validator

__all__ = ["RemoteEndpoint"]

T = TypeVar("T", bound=Message)

_log = runtime_logger("remote")

_UNSAFE_IN_A_NAME: Final = re.compile(r"[^A-Za-z0-9._-]")


class _EndpointMessage(Message):
    """A message type nobody can send: the endpoint actor is a parent, not a peer."""


class RemoteEndpoint:
    """This system's remoting: the port, the associations, and the resolver.

    Constructed with an already-bound socket, so that the canonical address is
    known before anything can serialize a ref, and started once the system's
    guardians exist to hang its actors from.
    """

    def __init__(
        self,
        *,
        address: Address,
        uid: int,
        settings: TapioSettings,
        dispatcher: Dispatcher,
        dead_letters: DeadLetterOffice,
        deliver: Callable[[bytes, Address], None],
        listener: socket.socket,
    ) -> None:
        """Wire an endpoint to the system it belongs to.

        Args:
            address: This system's canonical address.
            uid: This system's incarnation uid.
            settings: The system's tunables, whose `remote` half is required
                here and whose `validate_on_tell` decides how much a ref
                resolved with an expected type checks before it encodes.
            dispatcher: The loop everything here runs on.
            dead_letters: Where a frame that never left is accounted for.
            deliver: Hands an inbound message frame to the system.
            listener: The socket already bound by `open_listener`.
        """
        remote = settings.remote
        if remote is None:  # pragma: no cover - the system checks before it builds one
            msg = "an endpoint needs RemoteSettings; this system has remoting off"
            raise TapioError(msg)
        self._address = address
        self._uid = uid
        self._all_settings = settings
        self._settings = remote
        self._dispatcher = dispatcher
        self._dead_letters = dead_letters
        self._deliver = deliver
        self._listener = listener
        self._server: asyncio.Server | None = None
        self._associations: dict[Address, Association] = {}
        self._handshakes: set[asyncio.Task[None]] = set()
        self._names = 0
        self._parent: ActorCell[Any] | None = None
        self._closed = False

    @property
    def address(self) -> Address:
        """This system's canonical address, which peers dial and refs write."""
        return self._address

    @property
    def uid(self) -> int:
        """This system's incarnation uid, presented in every handshake."""
        return self._uid

    @property
    def settings(self) -> RemoteSettings:
        """How this system does remoting."""
        return self._settings

    @property
    def dead_letters(self) -> DeadLetterOffice:
        """Where a frame that never left is accounted for."""
        return self._dead_letters

    @property
    def dispatcher(self) -> Dispatcher:
        """The loop this system runs on."""
        return self._dispatcher

    @property
    def associations(self) -> tuple[Address, ...]:
        """The peers this system currently holds a link, or a dial, for.

        Exposed for the same reason a cell exposes its watchers: "the link was
        released" has to be a thing a test can assert rather than infer.
        """
        return tuple(self._associations)

    def behavior(self) -> Behavior[_EndpointMessage]:
        """Build the `/system/remote` actor: parent of every association."""

        async def on_message(message: _EndpointMessage) -> Behavior[_EndpointMessage]:
            return Behaviors.same()

        async def on_signal(
            ctx: ActorContext[_EndpointMessage], signal: Signal
        ) -> Behavior[_EndpointMessage]:
            if isinstance(signal, PostStop):
                await self.close()
            return Behaviors.same()

        return Behaviors.receive_message(
            on_message, _EndpointMessage, on_signal=on_signal
        )

    def start(self, cell: ActorCell[Any]) -> None:
        """Begin accepting connections, under the endpoint's own actor.

        Args:
            cell: The endpoint's actor, which parents every association.
        """
        self._parent = cell
        self._dispatcher.spawn_task(self._serve(), name="tapio-remote-listener")

    async def _serve(self) -> None:
        """Accept on the socket that was bound at construction."""
        context = (
            server_ssl_context(self._settings.tls)
            if self._settings.tls is not None
            else None
        )
        self._server = await listen(
            self._on_connection, self._listener, ssl_context=context
        )
        _log.debug("listening for peers on %s", self._address)

    async def _on_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """Handshake an inbound connection, then hand the link to an association.

        The accepting task ends as soon as the handshake does: reading the link
        belongs to the association's own reader, which is a task a cell owns
        and cancels. This one is tracked only so that a connection caught
        mid-handshake at shutdown is cancelled rather than left talking.
        """
        task = asyncio.current_task()
        if task is not None:
            self._handshakes.add(task)
        link = FrameLink(reader, writer, max_frame_bytes=self._settings.max_frame_bytes)
        try:
            identity = await accept(
                link,
                address=self._address,
                uid=self._uid,
                secret=self._settings.secret,
                timeout=self._settings.handshake_timeout.total_seconds(),
            )
        except (OSError, TapioError, TimeoutError, EOFError) as error:
            # Refused before a single message frame was read, which is the
            # point of doing this first: a peer that cannot say who it is, or
            # runs a version this one does not speak, gets a closed connection
            # and a log line rather than a partly understood session.
            _log.warning("refused a connection from %s: %s", link.peer, error)
            await link.close()
            return
        except asyncio.CancelledError:
            await link.close()
            raise
        finally:
            if task is not None:
                self._handshakes.discard(task)
        self._adopt(identity.address, identity.uid, link)

    def _adopt(self, peer: Address, uid: int, link: FrameLink) -> None:
        """Take a handshaken inbound link, resolving a simultaneous dial.

        Both ends connecting at once is normal under load, and without a rule
        the pair keeps two connections and FIFO per association stops meaning
        anything. The rule is address order: the link opened by the
        lower-sorting system wins, and the loser is closed.
        """
        if self._closed:
            # The system is going away. Closing is the whole of the answer: the
            # peer will see the link drop, which is the same thing it would see
            # a moment later anyway.
            self._close_later(link, peer)
            return
        existing = self._associations.get(peer)
        if existing is not None:
            if _wins(existing.initiator, peer):
                _log.debug("closing a second link from %s; ours won", peer)
                self._close_later(link, peer)
                return
            _log.debug("taking the link %s dialled; theirs won", peer)
            existing.adopt(link, uid)
            return
        self._start_association(
            Association(host=self, peer=peer, initiator=peer, link=link, uid=uid)
        )

    def _close_later(self, link: FrameLink, peer: Address) -> None:
        """Close a link this endpoint is not going to use.

        Its own task because closing waits for the transport, and the caller
        here is a handshake that has finished having anything to say.
        """
        self._dispatcher.spawn_task(link.close(), name=f"tapio-link-close:{peer}")

    def outbound(self, peer: Address) -> Association | None:
        """Return the association for a peer, dialling if there is none.

        Args:
            peer: The peer's canonical address.

        Returns:
            The association, whose link may still be coming up: sends queue
            behind it rather than waiting on it, so nothing blocks on a dial.
            `None` once this system is shutting down, when there is nothing
            left to dial with.
        """
        if self._closed:
            return None
        existing = self._associations.get(peer)
        if existing is not None:
            return existing
        return self._start_association(
            Association(host=self, peer=peer, initiator=self._address)
        )

    def resolve_peer(self, address: Address, path: ActorPath) -> ActorRef[Any] | None:
        """Turn a foreign address and path into a ref that reaches it.

        This is the peer resolver the system calls when a ref names another
        system, which happens both at `resolve` and inside a decode. A ref that
        arrives in a message field therefore works without anyone arranging
        anything, which is what makes a `reply_to` from a third system an
        ordinary send.

        Args:
            address: The system the ref names.
            path: Where in that system it points.

        Returns:
            A ref, or `None` when the address names nowhere to dial and the
            system should account for the message instead.
        """
        if self._closed or not address.is_addressable:
            return None
        return self._ref_to(address, path, _ACCEPT_ANY_MESSAGE)

    def resolve_expecting(
        self, address: Address, path: ActorPath, expect: MessageType
    ) -> ActorRef[Any]:
        """Return a ref checked against what the caller says the peer accepts.

        Args:
            address: The peer's canonical address.
            path: Where in its tree the actor sits.
            expect: What the caller declares that actor accepts.

        Returns:
            The ref.
        """
        validate = resolve_validator(
            msg_type=expect, settings=self._all_settings, target=path
        )
        return self._ref_to(address, path, validate)

    def _ref_to(
        self, address: Address, path: ActorPath, validate: MessageValidator
    ) -> ActorRef[Any]:
        """Build a remote ref that finds its association at every send."""
        # Bound to the peer rather than to one association, so a ref outlives
        # a link that failed: the next send through it dials again. A ref is a
        # handle to an actor on a node, not to a socket that happened to be
        # open when it was resolved.
        return RemoteRef[Any](
            path,
            outbox=PeerOutbox(self, address),
            validate=validate,
            max_frame_bytes=self._settings.max_frame_bytes,
        )

    def deliver(self, frame: bytes, peer: Address) -> None:
        """Hand an inbound message frame to the system that owns the recipient."""
        self._deliver(frame, peer)

    def forget_all(self, detail: str) -> None:
        """Close every association, as a link failure would one at a time.

        For the tests that need a link to go away without a peer going away,
        which is the only way to show that a ref survives one.

        Args:
            detail: Why, for the log and the dead letters that follow.
        """
        for association in list(self._associations.values()):
            association.close(detail)

    def forget(self, association: Association) -> None:
        """Drop an association that has stopped.

        The next send to that peer creates a new one and dials again. Holding a
        stopped association would mean holding a link that is not there, and
        the honest answer to "is the peer back" is to find out by dialling.
        """
        if self._associations.get(association.peer) is association:
            del self._associations[association.peer]

    def _start_association(self, association: Association) -> Association:
        """Spawn the actor that owns a link and record it in the table."""
        parent = self._parent
        if parent is None:  # pragma: no cover - start() precedes every send
            msg = "the remoting endpoint has not started"
            raise TapioError(msg)
        self._associations[association.peer] = association
        self._names += 1
        name = f"{_sanitize(association.peer)}-{self._names}"
        ref: ActorRef[AssociationMessage] = parent.spawn(
            association.behavior(),
            name,
            MailboxConfig(
                capacity=self._settings.outbound_capacity,
                on_overflow=OverflowStrategy.FAIL,
            ),
        )
        association.bind(ref)
        return association

    async def close(self) -> None:
        """Stop listening, and let the tree stop the links.

        The associations are children of this endpoint's actor, so they are
        already being stopped by the sweep that got here. What is left is the
        socket and any connection still mid-handshake, which belongs to nobody
        else.
        """
        if self._closed:
            return
        self._closed = True
        server = self._server
        self._server = None
        if server is not None:
            server.close()
            with contextlib.suppress(OSError, asyncio.CancelledError):
                await server.wait_closed()
        else:
            self._listener.close()
        for task in list(self._handshakes):
            task.cancel()
        for task in list(self._handshakes):
            with contextlib.suppress(asyncio.CancelledError, HandshakeError, OSError):
                await task
        self._handshakes.clear()

    def __repr__(self) -> str:
        """Render the address and how many peers are associated."""
        return (
            f"RemoteEndpoint({str(self._address)!r}, peers={len(self._associations)})"
        )


class PeerOutbox:
    """A peer, as a place to hand frames, whatever link is up at the time.

    One of these sits behind every remote ref. It looks the association up per
    send instead of holding one, which is what makes a ref survive a link that
    failed, and it is where a send lands when there is no endpoint left to hold
    an association at all.
    """

    __slots__ = ("_endpoint", "_peer")

    def __init__(self, endpoint: RemoteEndpoint, peer: Address) -> None:
        """Bind an outbox to one peer of one endpoint."""
        self._endpoint = endpoint
        self._peer = peer

    @property
    def peer(self) -> Address:
        """The address on the other end."""
        return self._peer

    def send(self, message: Message, frame: bytes, recipient: ActorPath) -> None:
        """Queue a frame with the association for this peer, dialling if needed."""
        association = self._endpoint.outbound(self._peer)
        if association is None:
            self._endpoint.dead_letters.publish(
                message,
                recipient,
                DeadLetterReason.NO_ASSOCIATION,
                peer=self._peer,
                detail="this system is shutting down and holds no links",
            )
            return
        association.send(message, frame, recipient)

    async def offer(self, message: Message, frame: bytes, recipient: ActorPath) -> None:
        """Queue a frame, waiting for room in the outbound buffer."""
        association = self._endpoint.outbound(self._peer)
        if association is None:
            self.send(message, frame, recipient)
            return
        await association.offer(message, frame, recipient)

    def __repr__(self) -> str:
        """Render the peer, which is the whole of what this is."""
        return f"PeerOutbox({str(self._peer)!r})"


def open_listener(settings: RemoteSettings) -> socket.socket:
    """Bind the port this system will be dialled on, before anything runs.

    Args:
        settings: How this system does remoting.

    Returns:
        A listening socket, not yet accepting.

    Raises:
        InsecureRemoteConfig: If it would listen beyond loopback with no secret.
        OSError: If the address could not be bound.
    """
    verify_bind_security(settings)
    return bind(settings)


def _wins(initiator: Address, challenger: Address) -> bool:
    """Whether the link opened by `initiator` beats one opened by `challenger`.

    Sorted by address string, so both ends compute the same answer from the
    same two names and neither has to be told which one it is.
    """
    return str(initiator) < str(challenger)


def _sanitize(peer: Address) -> str:
    """Turn a peer address into something an actor name can hold."""
    return _UNSAFE_IN_A_NAME.sub("-", f"{peer.system}-{peer.host}-{peer.port}")


def _accept_any_message(message: Message) -> None:
    """Check nothing, for a ref whose peer protocol was never declared.

    A ref that arrived in a message field carries no claim about what the actor
    on the other end accepts, and inventing one here would be a check on a
    guess. The authoritative check runs on the receiving node against the
    target's real message type, where it can be trusted.
    """


_ACCEPT_ANY_MESSAGE: Final[MessageValidator] = _accept_any_message
