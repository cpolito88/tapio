"""The endpoint: one listening port, and every association behind it.

A system with `remote` settings gets one of these. It owns the socket peers
dial, the table of associations by peer address, and the resolver that turns a
foreign address into a ref that reaches it. It is itself an actor, `/system/
remote`, whose children are the associations, so shutting down the system
closes the port and every link under it through the ordinary stop sweep.

The socket is bound **synchronously, at system construction**. That is what
makes `bind_port=0` usable: the canonical address is settled before the first
ref is handed out, instead of changing under a ref already written down. It is
also what makes a misconfigured deployment fail to start rather than fail to
be secure.
"""

import asyncio
import contextlib
import re
import socket
from collections.abc import Callable
from typing import Any, Final, TypeVar

from tapio.actor.behavior import Behavior, Behaviors
from tapio.actor.cell import ActorCell, ActorRuntime
from tapio.actor.context import ActorContext
from tapio.actor.dead_letters import DeadLetterOffice, DeadLetterReason
from tapio.actor.events import EventStream
from tapio.actor.mailbox import MailboxConfig, OverflowStrategy
from tapio.actor.path import ActorPath
from tapio.actor.ref import ActorRef
from tapio.actor.signals import PostStop, Signal
from tapio.actor.watch import Watcher
from tapio.dispatch.dispatcher import Dispatcher
from tapio.errors import ActorSystemTerminating, HandshakeError, TapioError
from tapio.logging import runtime_logger
from tapio.message import Message
from tapio.remote.address import Address
from tapio.remote.association import Association, AssociationMessage
from tapio.remote.handshake import accept
from tapio.remote.peers import PeerProvider, StaticPeers
from tapio.remote.ref import RemoteRef
from tapio.remote.transport import (
    FrameLink,
    Link,
    bind,
    listen,
    server_ssl_context,
    verify_bind_security,
)
from tapio.settings import RemoteSettings
from tapio.validation import MessageType, MessageValidator, resolve_validator

__all__ = ["RemoteEndpoint"]

T = TypeVar("T", bound=Message)

_log = runtime_logger("remote")

_UNSAFE_IN_A_NAME: Final = re.compile(r"[^A-Za-z0-9._-]")


class _EndpointMessage(Message):
    """A message type nobody can send: the endpoint actor is a parent, not a peer."""


class RemoteEndpoint:
    """This system's remoting: the port, the associations, and the resolver.

    It is constructed with an already-bound socket, so the canonical address
    is known before anything can serialize a ref. It starts once the system's
    guardians exist to hang its actors from.
    """

    def __init__(
        self,
        *,
        runtime: ActorRuntime,
        uid: int,
        deliver: Callable[[bytes, Address], None],
        listener: socket.socket,
    ) -> None:
        """Wire an endpoint to the system it belongs to.

        Args:
            runtime: The system slice this endpoint works through: its
                canonical address, its loop, its dead letters, its event
                stream, and the registry a frame's recipient is looked up in.
            uid: This system's incarnation uid.
            deliver: Hands an inbound message frame to the system.
            listener: The socket already bound by `open_listener`.
        """
        remote = runtime.settings.remote
        if remote is None:  # pragma: no cover - the system checks before it builds one
            msg = "an endpoint needs RemoteSettings; this system has remoting off"
            raise TapioError(msg)
        self._runtime = runtime
        self._address = runtime.address
        self._uid = uid
        self._all_settings = runtime.settings
        self._settings = remote
        self._deliver = deliver
        self._listener = listener
        self._server: asyncio.Server | None = None
        self._associations: dict[Address, Association] = {}
        self._peers: PeerProvider = StaticPeers()
        self._listening: asyncio.Task[None] | None = None
        # Each accepted connection, keyed by the task handshaking it, held by
        # its link. The link is recorded the moment the connection is made, so
        # `close` can close it even for a task cancelled before it ever ran.
        self._handshakes: dict[asyncio.Task[None], FrameLink] = {}
        # A link this endpoint decided not to use still has to be closed, and
        # the task doing it is nobody's child. The event loop holds only a
        # weak reference to a task, so without this set one can be collected
        # mid-close and leave the socket open.
        self._closing_links: set[asyncio.Task[None]] = set()
        self._names = 0
        self._parent: ActorCell[Any] | None = None
        self._closed = False
        # `_closed` is set at the start of `close`, `_done` only once it has
        # finished draining. A connection accepted between the two is closed
        # by that drain; one accepted after it is closed on the spot, since no
        # drain is left to wait for.
        self._done = False
        self._link_filter: Callable[[FrameLink], Link] | None = None

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
        return self._runtime.dead_letters

    @property
    def events(self) -> EventStream:
        """Where this system says that a peer went out of reach."""
        return self._runtime.events

    @property
    def dispatcher(self) -> Dispatcher:
        """The loop this system runs on."""
        return self._runtime.dispatcher

    @property
    def is_closing(self) -> bool:
        """Whether this system's remoting is shutting down."""
        return self._closed

    @property
    def associations(self) -> tuple[Address, ...]:
        """The peers this system currently holds a link, or a dial, for.

        Exposed for the same reason a cell exposes its watchers: a test has to
        be able to assert that the link was released.
        """
        return tuple(self._associations)

    @property
    def peers(self) -> PeerProvider:
        """Who decides which addresses this system may associate with.

        [StaticPeers][tapio.remote.peers.StaticPeers] until something replaces
        it: every address that was written down is a peer, minus the ones a
        detector here gave up on.
        """
        return self._peers

    @property
    def quarantined(self) -> tuple[Address, ...]:
        """The peers this system has given up on and will not dial again.

        An address stays here until `reconnect` clears it, which is the whole
        point: recovery from a peer declared unreachable is a decision
        somebody made, never something that happened while nobody was looking.
        """
        return tuple(self._peers.refusals())

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
        # Held, not discarded. This task and `close` are the only two owners of
        # the socket, so `close` has to be able to stop it before it touches
        # the socket; otherwise the two race and the loser works on a closed
        # fd. It is the endpoint actor's task in the same sense the handshakes
        # below are: started here, cancelled in the termination sequence.
        self._listening = self.dispatcher.spawn_task(
            self._serve(), name="tapio-remote-listener"
        )

    async def _serve(self) -> None:
        """Accept on the socket that was bound at construction."""
        context = (
            server_ssl_context(self._settings.tls)
            if self._settings.tls is not None
            else None
        )
        self._server = await listen(self._accept, self._listener, ssl_context=context)
        _log.debug("listening for peers on %s", self._address)

    def _accept(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """Take ownership of an accepted connection, then hand it to a task.

        This runs synchronously as the connection is made, which is the one
        moment nothing can cancel. The link is recorded before the task that
        reads it has run a line, because a task cancelled before its first line
        never runs at all: a handshake that registered itself from the inside
        would leave the socket for the garbage collector on a shutdown that
        raced it. Recording the link here means `close` can close it either
        way.

        A connection accepted once `close` has begun is not handshaken. How its
        socket is closed depends on where `close` has got to. While `close` is
        still draining, the socket is handed to a close that its drain waits
        for, so `terminate` does not return with a socket half-closed that the
        loop might never finish. Once `close` has drained, there is nobody left
        to wait for a task, so the socket is closed on the spot and the loop
        runs that close as it would any other.
        """
        if self._closed:
            if self._done:
                # Accepted after close() finished. Nobody is left to drain a
                # task, so schedule the socket close and let the loop run it.
                writer.close()
                return
            # Accepted while close() is still draining, which a pending accept
            # callback can be as `await server.wait_closed()` runs. A bare
            # writer.close() only schedules the close, so if the loop is torn
            # down before it runs the transport is collected unclosed. Hand it
            # to close_link_later, whose close close()'s own drain awaits.
            self.close_link_later(
                FrameLink(
                    reader, writer, max_frame_bytes=self._settings.max_frame_bytes
                ),
                self._address,
            )
            return
        link = FrameLink(reader, writer, max_frame_bytes=self._settings.max_frame_bytes)
        task = self.dispatcher.spawn_task(
            self._handshake(link), name="tapio-remote-handshake"
        )
        self._handshakes[task] = link

    async def _handshake(self, link: FrameLink) -> None:
        """Handshake an inbound link, then hand it to an association.

        The task ends as soon as the link has been handed over. Reading it
        belongs to the association's own reader, which is a task a cell owns
        and cancels. This task stays in `_handshakes` until the handover, so a
        connection caught at shutdown is closed rather than left open, and a
        link this endpoint decides not to keep is closed by a task `close` has
        to be able to wait for.

        A connection accepted after this endpoint has closed is closed here
        rather than handshaken. The socket was accepted by the loop before the
        listener shut, so this task can be the first thing that runs after
        `close` finished, and there would be nobody left to hand it to.
        """
        task = asyncio.current_task()
        try:
            if self._closed:
                _log.debug("closing a connection from %s: shutting down", link.peer)
                await link.close()
                return
            identity = await accept(
                link,
                address=self._address,
                uid=self._uid,
                secret=self._settings.secret,
                timeout=self._settings.handshake_timeout.total_seconds(),
            )
            self._adopt(identity.address, identity.uid, self.wrap(link))
        except ActorSystemTerminating:
            # The endpoint began stopping in the window between the _closed
            # check above and the spawn that _adopt does, so the spawn is
            # refused. This must be caught here, not left to propagate: the
            # `finally` has already taken this task out of `_handshakes`, so
            # `close` would never await it and the exception would surface at
            # collection time, unattributable to any actor. The half-started
            # association `_adopt` recorded is closed by `close`'s own sweep;
            # here the job is only to close this link and end cleanly.
            _log.debug(
                "closing a connection from %s: the endpoint is stopping", link.peer
            )
            await link.close()
            return
        except (OSError, TapioError, TimeoutError, EOFError) as error:
            # Refused before any message frame was read, which is why the
            # handshake comes first. A peer that cannot say who it is, or runs
            # a version this one does not speak, gets a closed connection and
            # a log line instead of a half-understood session.
            _log.warning("refused a connection from %s: %s", link.peer, error)
            await link.close()
            return
        except asyncio.CancelledError:
            await link.close()
            raise
        finally:
            # This task took responsibility for the link: adopted it, or closed
            # it above. What stays in the map is a task that never got to run,
            # whose link `close` closes.
            if task is not None:
                self._handshakes.pop(task, None)

    def _adopt(self, peer: Address, uid: int, link: Link) -> None:
        """Take a handshaken inbound link, resolving a simultaneous dial.

        Both ends connecting at once is normal under load. Without a rule the
        pair keeps two connections, and FIFO per association stops meaning
        anything. The rule is address order: the link opened by the
        lower-sorting system wins, and the other is closed.
        """
        if self._closed:
            # The system is going away, so closing is the whole answer. The
            # peer sees the link drop, which is what it would see a moment
            # later anyway.
            self.close_link_later(link, peer)
            return
        refusal = self._peers.refusal(peer)
        if refusal is not None:
            # This system will not talk to that address, and a peer dialling
            # in is not a reason to change its mind. Recovery is a decision
            # somebody makes with `reconnect`, because watchers were already
            # told that actors over there are gone, and resuming quietly would
            # leave two nodes believing different things with no way to notice.
            _log.warning("refused a link from %s: %s", peer, refusal)
            self.close_link_later(link, peer)
            return
        existing = self._live(peer)
        if existing is not None:
            if existing.peer_uid not in (0, uid):
                # A different incarnation answers to that address, so the peer
                # this system was talking to is gone. Its watchers are told,
                # and the new link is a new association rather than the old
                # one quietly changing who is on the other end. The address is
                # not quarantined: what happened here is known exactly, which
                # is the opposite of the silence a quarantine exists for.
                _log.info(
                    "%s restarted: incarnation %d replaces %d",
                    peer,
                    uid,
                    existing.peer_uid,
                )
                existing.close(f"{peer} restarted as incarnation {uid}")
            elif _wins(existing.initiator, peer):
                _log.debug("closing a second link from %s; ours won", peer)
                self.close_link_later(link, peer)
                return
            else:
                _log.debug("taking the link %s dialled; theirs won", peer)
                existing.adopt(link, uid)
                return
        self._start_association(
            Association(host=self, peer=peer, initiator=peer, link=link, uid=uid)
        )

    def wrap(self, link: FrameLink) -> Link:
        """Put whatever sits between this system and its sockets in the way.

        Nothing, in production: the link is returned as it came. A test
        installs a filter with `set_link_filter` to drop, delay or swallow
        frames, which is how a partition is simulated with no second machine
        and nothing real broken.

        Args:
            link: The link just opened or accepted.

        Returns:
            The link the association will read and write.
        """
        if self._link_filter is None:
            return link
        return self._link_filter(link)

    def set_link_filter(self, wrap: Callable[[FrameLink], Link] | None) -> None:
        """Install what every link opened from now on passes through.

        Args:
            wrap: Called with each new link, returning what to use in its
                place. `None` removes the filter. Links already open keep
                whatever they were wrapped in.
        """
        self._link_filter = wrap

    def close_link_later(self, link: Link, peer: Address) -> None:
        """Close a link this endpoint is not going to use.

        It gets its own task because closing waits for the transport, and the
        caller is a handshake that has nothing left to say. The task is held
        until it finishes, since the loop would not hold it for us.
        """
        task = self.dispatcher.spawn_task(link.close(), name=f"tapio-link-close:{peer}")
        self._closing_links.add(task)
        task.add_done_callback(self._closing_links.discard)

    def outbound(self, peer: Address) -> Association | None:
        """Return the association for a peer, dialling if there is none.

        Args:
            peer: The peer's canonical address.

        Returns:
            The association. Its link may still be coming up, and sends queue
            behind it rather than waiting, so nothing blocks on a dial.
            `None` once this system is shutting down, and for a peer it
            refuses: neither is a thing to dial, and what was being sent is
            accounted for instead.
        """
        if self._closed or self._peers.refusal(peer) is not None:
            return None
        existing = self._live(peer)
        if existing is not None:
            return existing
        return self._start_association(
            Association(host=self, peer=peer, initiator=self._address)
        )

    def _live(self, peer: Address) -> Association | None:
        """Return the association for a peer, if it is one that can still work.

        An association that has been asked to stop is still in the table until
        its actor finishes stopping, and handing that one back would lose
        whatever was written to it. It is already draining and it will not
        take a new link, so from here it is the same as no association at
        all: the caller dials afresh, and `forget` will not disturb the
        replacement, because it drops an association only if it is the one
        still recorded.

        Args:
            peer: The peer's canonical address.

        Returns:
            The association, or `None` when there is none or the one there is
            has begun stopping.
        """
        existing = self._associations.get(peer)
        if existing is None or existing.is_closing:
            return None
        return existing

    def association_for(self, peer: Address) -> Association | None:
        """Return the association for a peer, without dialling one.

        Args:
            peer: The peer's canonical address.

        Returns:
            The association, or `None` if this system holds none.
        """
        return self._associations.get(peer)

    def resolve_peer(self, address: Address, path: ActorPath) -> ActorRef[Any] | None:
        """Turn a foreign address and path into a ref that reaches it.

        This is the peer resolver the system calls when a ref names another
        system, both at `resolve` and inside a decode. A ref that arrives in a
        message field therefore works with no setup, which is what makes a
        `reply_to` from a third system an ordinary send.

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
        # a failed link and the next send through it dials again. A ref points
        # at an actor on a node, not at the socket that was open when it was
        # resolved.
        return RemoteRef[Any](
            path,
            outbox=PeerOutbox(self, address),
            validate=validate,
            max_frame_bytes=self._settings.max_frame_bytes,
            runtime=self._runtime,
        )

    def deliver(self, frame: bytes, peer: Address) -> None:
        """Hand an inbound message frame to the system that owns the recipient."""
        self._deliver(frame, peer)

    def lookup(self, path: ActorPath) -> ActorRef[Any] | None:
        """Find a live local actor by path and incarnation uid.

        Args:
            path: The path a peer named, uid included.

        Returns:
            The live ref, or `None` when nothing is there. A uid whose
            incarnation is over answers `None`, which is what stops a watch
            from attaching to whoever holds that path now.
        """
        return self._runtime.refs.lookup(path)

    def peer_ref(self, peer: Address, path: ActorPath) -> ActorRef[Any]:
        """Build a ref to an actor on a peer, to name it in a `Terminated`.

        Args:
            peer: The peer's canonical address.
            path: Where in its tree the actor sat.

        Returns:
            A ref. It stays a valid handle after the actor it names has
            stopped, exactly as a local one does.
        """
        return self._ref_to(peer, path, _ACCEPT_ANY_MESSAGE)

    def quarantine(self, peer: Address, detail: str) -> None:
        """Freeze an address, because this system decided the peer is gone.

        Nothing is sent there and nothing is dialled, in either direction,
        until `reconnect` clears it. That is deliberate. Watchers have already
        been told that actors over there are gone, so a link coming quietly
        back would leave this system and its peer believing different things
        with no way to notice.

        Args:
            peer: The peer's canonical address.
            detail: Why, kept for the log and for `reconnect`.
        """
        self._peers.give_up(peer, detail)

    def use_peers(self, peers: PeerProvider) -> None:
        """Hand the question of who may be associated with to somebody else.

        One system decides alone, so it decides from a table of its own. A
        clustered one decides from membership, where a peer is refused because
        the cluster downed it rather than because this node stopped hearing
        from it. The consequences are the same either way, which is why this
        replaces the answer and nothing else: an association is still refused,
        watchers are still told, sends still dead-letter.

        Whatever was already refused is carried over, because those refusals
        were acted on. Watchers were told the actors over there are gone, and
        a peer that quietly became dialable again on a change of authority
        would leave two nodes believing different things.

        Args:
            peers: The new authority.
        """
        for peer, detail in self._peers.refusals().items():
            peers.give_up(peer, detail)
        self._peers = peers

    def refusal(self, peer: Address) -> str | None:
        """Why this system will not associate with a peer, if it will not.

        Args:
            peer: The peer's canonical address.

        Returns:
            The words that explain the refusal, or `None` when the peer may
            be dialled. They are what a dead letter carries, so a subscriber
            reads why the message went nowhere rather than only that it did.
        """
        return self._peers.refusal(peer)

    def is_quarantined(self, peer: Address) -> bool:
        """Whether this system has given up on an address."""
        return self._peers.refusal(peer) is not None

    def clear_quarantine(self, peer: Address) -> bool:
        """Take an address off the list this system refuses to talk to.

        Nothing is dialled. It says only that this system is willing to be
        associated with that peer again, which is what the *other* end of a
        healed partition needs before its dial can be accepted.

        Each node gives up for itself, so each node relents for itself. A pair
        that gave up on each other is repaired by relenting on one side and
        dialling from the other:

        ```python
        beta.remote.clear_quarantine(alpha.address)
        await alpha.remote.reconnect(beta.address)
        ```

        Args:
            peer: The peer's canonical address.

        Returns:
            Whether it was quarantined at all.
        """
        detail = self._peers.relent(peer)
        if detail is None:
            return False
        _log.info("clearing the quarantine on %s: %s", peer, detail)
        return True

    async def reconnect(self, peer: Address) -> None:
        """Clear a quarantine and associate again, because someone decided to.

        Recovery is never automatic. A peer declared unreachable may have been
        alive the whole time, and its watchers here were told otherwise, so
        resuming is a decision a person or a supervisor makes rather than
        something a timer does.

        This repairs one end. A peer that gave up on this system at the same
        moment, which is what both sides of a partition do, refuses the dial
        until it has relented too. See `clear_quarantine`.

        Refs held from before are not reusable: their uid belongs to a session
        that is over. Resolve again after this returns.

        Args:
            peer: The peer's canonical address.

        Raises:
            ActorSystemTerminating: If this system is shutting down.
            HandshakeError: If the peer could not be dialled, refused this
                system, or dropped the link before it carried anything.
            TimeoutError: If the peer did not answer within
                `handshake_timeout`.
        """
        if self._closed:
            msg = f"cannot reconnect to {peer}: this system's remoting is shutting down"
            raise ActorSystemTerminating(msg)
        self.clear_quarantine(peer)
        association = self.outbound(peer)
        if association is None:  # pragma: no cover - the check above covers it
            msg = f"cannot reconnect to {peer}: this system holds no links"
            raise ActorSystemTerminating(msg)
        await association.wait_connected(
            self._settings.handshake_timeout.total_seconds()
        )

    def forget_all(self, detail: str) -> None:
        """Close every association, as a link failure would one at a time.

        For the tests that need a link to go away while the peer stays, which
        is the only way to show that a ref survives a failed link.

        Args:
            detail: Why, for the log and the dead letters that follow.
        """
        for association in list(self._associations.values()):
            association.close(detail)

    def forget(self, association: Association) -> None:
        """Drop an association that has stopped.

        The next send to that peer creates a new one and dials again. Holding
        a stopped association would mean holding a link that is not there, and
        the only way to know whether the peer is back is to dial it.
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

        The associations are children of this endpoint's actor, so the sweep
        that reached here is already stopping them. What is left is the
        listener, the socket, any connection still mid-handshake, any link this
        endpoint refused and is still closing, and any association adopted so
        late that the sweep had already passed it. Nobody else owns those.
        """
        if self._closed:
            return
        self._closed = True
        await self._stop_listening()
        server = self._server
        self._server = None
        if server is not None:
            server.close()
            with contextlib.suppress(OSError, asyncio.CancelledError):
                await server.wait_closed()
        else:
            self._listener.close()
        # Both are drained until they stay empty, because draining one fills
        # the other: a handshake that finishes here hands its link to `_adopt`,
        # which has nowhere to put it now and starts closing it. A single pass
        # would return with that close still owed, and the task doing it dies
        # with the dispatcher, leaving the socket open.
        while self._handshakes or self._closing_links:
            for task in list(self._handshakes):
                task.cancel()
            for task in list(self._handshakes):
                with contextlib.suppress(
                    asyncio.CancelledError, HandshakeError, OSError
                ):
                    await task
                # A task cancelled before its first line never ran its own
                # cleanup, so its link is still open and still recorded here.
                # One that did run took its link out of the map itself.
                link = self._handshakes.pop(task, None)
                if link is not None:
                    await link.close()
            # Awaited rather than cancelled: these are already closing, and
            # cancelling one would leave the socket it was releasing open.
            for task in list(self._closing_links):
                with contextlib.suppress(asyncio.CancelledError, OSError):
                    await task
                self._closing_links.discard(task)
        # An association whose actor stopped normally took itself out of this
        # table on the way, so whatever is left was adopted after the stop
        # sweep had passed and will get no `PostStop` to close its link. Close
        # it here, or the socket is left for the garbage collector.
        for association in list(self._associations.values()):
            with contextlib.suppress(OSError, asyncio.CancelledError):
                await association.detach()
        # Everything the drain could wait for has been waited for. A connection
        # accepted from here on is closed on the spot instead.
        self._done = True

    async def _stop_listening(self) -> None:
        """Stop the accept task, before anything closes the socket under it.

        It runs first in `close` for two reasons. A task still on its way to
        `asyncio.start_server` would otherwise wake up to a closed socket and
        raise `OSError` where nobody is waiting for it, which asyncio reports
        at collection time rather than as a failure anyone can act on. And a
        task suspended *inside* `start_server` would go on to publish a
        running server into `self._server` after `close` had already read it,
        leaving a listening port behind that nothing will ever shut.

        Its result is awaited rather than dropped, so an accept loop that
        ended badly is reported here instead of surfacing as an unhandled
        exception long afterwards.
        """
        listening = self._listening
        self._listening = None
        if listening is None:
            return
        listening.cancel()
        try:
            await listening
        except asyncio.CancelledError:
            # The listener's, not this caller's: `close` runs in the endpoint
            # actor's own task, and cancelling that one does not come through
            # here.
            pass
        except OSError as error:
            _log.warning("the listener on %s ended: %s", self._address, error)

    def __repr__(self) -> str:
        """Render the address and how many peers are associated."""
        return (
            f"RemoteEndpoint({str(self._address)!r}, peers={len(self._associations)})"
        )


class PeerOutbox:
    """A peer, as a place to hand frames, whatever link is up at the time.

    One of these sits behind every remote ref. It looks the association up on
    each send instead of holding one, which is what lets a ref survive a
    failed link. It is also where a send lands when there is no endpoint left
    to hold an association.
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

    @property
    def is_quarantined(self) -> bool:
        """Whether this system has given up on the peer.

        A watch registered against a quarantined peer is answered at once
        rather than waiting for a signal that nothing is left to send.
        """
        return self._endpoint.is_quarantined(self._peer)

    def send(self, message: Message, frame: bytes, recipient: ActorPath) -> None:
        """Queue a frame with the association for this peer, dialling if needed."""
        association = self._endpoint.outbound(self._peer)
        if association is None:
            refusal = self._endpoint.refusal(self._peer)
            self._endpoint.dead_letters.publish(
                message,
                recipient,
                DeadLetterReason.QUARANTINED
                if refusal is not None
                else DeadLetterReason.NO_ASSOCIATION,
                peer=self._peer,
                detail=(
                    f"{refusal}; remote.reconnect clears it"
                    if refusal is not None
                    else "this system is shutting down and holds no links"
                ),
            )
            return
        association.send(message, frame, recipient)

    def watch(self, watchee: ActorPath, watcher: Watcher) -> None:
        """Ask the peer to report when one of its actors stops."""
        association = self._endpoint.outbound(self._peer)
        if association is None:
            watcher.notify_unreachable(
                self._endpoint.peer_ref(self._peer, watchee),
                f"this system holds no link to {self._peer}",
            )
            return
        association.watch(watchee, watcher)

    def unwatch(self, watchee: ActorPath, watcher: Watcher) -> None:
        """Withdraw a watch on an actor over there.

        It looks the association up rather than dialling, because withdrawing
        a watch nobody holds is not a reason to open a connection.
        """
        association = self._endpoint.association_for(self._peer)
        if association is not None:
            association.unwatch(watchee, watcher)

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

    It sorts the address strings, so both ends compute the same answer from
    the same two names and neither has to be told which one it is.
    """
    return str(initiator) < str(challenger)


def _sanitize(peer: Address) -> str:
    """Turn a peer address into something an actor name can hold."""
    return _UNSAFE_IN_A_NAME.sub("-", f"{peer.system}-{peer.host}-{peer.port}")


def _accept_any_message(message: Message) -> None:
    """Check nothing, for a ref whose peer protocol was never declared.

    A ref that arrived in a message field carries no claim about what the
    actor on the other end accepts, and inventing one here would check a
    guess. The check that decides runs on the receiving node, against the
    target's real message type.
    """


_ACCEPT_ANY_MESSAGE: Final[MessageValidator] = _accept_any_message
