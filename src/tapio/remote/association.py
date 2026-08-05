"""An association: one link to one peer, and the actor that owns it.

Two systems associate on demand. The first ref resolved or deserialized for an
address creates the association; every ref for that address then uses it, which
is what makes "FIFO per association" a guarantee worth having rather than a
coincidence of how many connections happened to be open.

An association is an actor, and that is not decoration. Its writer is the cell's
own receive loop, its outbound buffer is the cell's bounded mailbox, its
heartbeat is a cell timer, and its reader is one task the cell cancels when it
stops. So remoting introduces no new rule about who owns a task, and the leak
check that covers every other actor covers this one for free.

Delivery stays **at-most-once**. No acks, no retries, no resend buffer: a frame
written to a socket that then failed is lost, and it dead-letters here if the
failure is visible from this side. Acks would make delivery at-least-once,
which is not better, only different, and it would quietly oblige every
receiving actor to be idempotent. That is a decision for the user's protocol,
where they know what is safe to repeat.
"""

import asyncio
import contextlib
from collections import deque
from typing import Protocol, TypeAlias

from tapio.actor.behavior import Behavior, Behaviors
from tapio.actor.context import ActorContext
from tapio.actor.dead_letters import Carrier, DeadLetterOffice, DeadLetterReason
from tapio.actor.path import ActorPath
from tapio.actor.ref import ActorRef
from tapio.actor.signals import PostStop, Signal
from tapio.actor.timers import TimerScheduler
from tapio.dispatch.dispatcher import Dispatcher
from tapio.errors import (
    FrameTooLargeError,
    HandshakeError,
    MailboxFullError,
    MessageDecodingError,
    TapioError,
)
from tapio.logging import runtime_logger
from tapio.message import Message
from tapio.remote.address import Address
from tapio.remote.codec import UndecodableFrame
from tapio.remote.handshake import introduce
from tapio.remote.transport import (
    FrameLink,
    Heartbeat,
    client_ssl_context,
    connect,
    is_link_frame,
    link_body,
)
from tapio.settings import RemoteSettings

__all__ = ["Association", "AssociationHost", "AssociationMessage", "Beat", "Outbound"]

_log = runtime_logger("remote")

_HEARTBEAT = "heartbeat"
"""The timer key, and the link frame kind it writes."""


class Outbound(Carrier):
    """One frame queued for a peer, with the message it was made from.

    The frame is what travels; the payload rides along so that a frame which
    never leaves reports the message its sender actually sent rather than this
    wrapper. Encoding happens at the send site, on the caller's thread, because
    an error about the message belongs to whoever wrote it.
    """

    frame: bytes
    """The complete frame, length prefix included."""

    recipient: ActorPath
    """Where it was addressed, in the peer's path space."""


class Beat(Message):
    """A tick asking the association to prove the link is still there."""


class Close(Message):
    """Ask an association to stop, because its link is over."""

    detail: str
    """What happened, for the log and for the dead letters that follow."""


AssociationMessage: TypeAlias = Outbound | Beat | Close
"""Everything an association actor accepts. None of it is user traffic."""


class AssociationHost(Protocol):
    """What an association needs from the endpoint that owns it."""

    @property
    def address(self) -> Address:
        """This system's canonical address, which peers dial and refs write."""
        ...

    @property
    def uid(self) -> int:
        """This system's incarnation uid, presented in every handshake."""
        ...

    @property
    def settings(self) -> RemoteSettings:
        """How this system does remoting."""
        ...

    @property
    def dead_letters(self) -> DeadLetterOffice:
        """Where a frame that never left is accounted for."""
        ...

    @property
    def dispatcher(self) -> Dispatcher:
        """The loop this system runs on, and the reader task runs on."""
        ...

    def deliver(self, frame: bytes, peer: Address) -> None:
        """Hand an inbound message frame to the system that owns the recipient."""
        ...

    def forget(self, association: "Association") -> None:
        """Drop an association that has stopped, so the next send dials afresh."""
        ...


class Association:
    """One link to one peer: the actor's state, and the reader behind it.

    Created in one of two ways, and the difference is only in how it starts.
    Dialled, when something here resolved a ref for an address with no link to
    it; or adopted, when the peer dialled in and the handshake said who it was.
    """

    __slots__ = (
        "_accepted",
        "_closing",
        "_host",
        "_initiator",
        "_last_frame_at",
        "_link",
        "_peer",
        "_pending",
        "_reader",
        "_ref",
        "_socket",
        "_uid",
    )

    def __init__(
        self,
        *,
        host: AssociationHost,
        peer: Address,
        initiator: Address,
        link: FrameLink | None = None,
        uid: int = 0,
    ) -> None:
        """Create an association, before its actor exists.

        Args:
            host: The endpoint that owns it.
            peer: The peer's canonical address, which keys this association.
            initiator: Whose address opened the link, which is how a
                simultaneous dial is resolved.
            link: An already-handshaken link, when the peer dialled in.
            uid: The peer's incarnation uid, when the handshake established it.
        """
        self._host = host
        self._peer = peer
        self._initiator = initiator
        self._link: FrameLink | None = None
        self._pending: deque[Outbound] = deque()
        self._reader: asyncio.Task[None] | None = None
        self._ref: ActorRef[AssociationMessage] | None = None
        self._closing = False
        self._uid = uid
        self._last_frame_at = host.dispatcher.now()
        # Held until the reader task starts and takes it over.
        self._accepted: FrameLink | None = link
        # Every link this association has opened, writable or not. `_link` is
        # what the actor may write to and is set only once nothing is queued
        # ahead of it; this one exists from the moment there is a socket, so a
        # cancellation between the two still has something to close.
        self._socket: FrameLink | None = link

    @property
    def peer(self) -> Address:
        """The address on the other end, which keys this association."""
        return self._peer

    @property
    def initiator(self) -> Address:
        """Whose dial opened this link.

        Both sides connecting at once is normal under load, and without a rule
        the pair ends up with two connections and FIFO quietly stops meaning
        anything. The rule is address order, applied to this.
        """
        return self._initiator

    @property
    def peer_uid(self) -> int:
        """The peer's incarnation uid, or `0` before the handshake."""
        return self._uid

    @property
    def is_connected(self) -> bool:
        """Whether a handshaken link is currently carrying frames."""
        return self._link is not None

    @property
    def last_frame_at(self) -> float:
        """When something last arrived from the peer, on the loop's clock.

        Read by nothing yet. It is recorded here because the reader is the only
        place that knows, and a failure detector that has to be retrofitted
        into a reader is a failure detector written twice.
        """
        return self._last_frame_at

    def bind(self, ref: ActorRef[AssociationMessage]) -> None:
        """Take the ref to the actor that owns this association."""
        self._ref = ref

    def behavior(self) -> Behavior[AssociationMessage]:
        """Build the actor that writes to this link and reads from it."""

        def build(
            ctx: ActorContext[AssociationMessage],
        ) -> Behavior[AssociationMessage]:
            self._reader = self._host.dispatcher.spawn_task(
                self._run(), name=f"tapio-link:{self._peer}"
            )
            return Behaviors.with_timers(self._with_heartbeat)

        return Behaviors.setup(build)

    def send(self, message: Message, frame: bytes, recipient: ActorPath) -> None:
        """Queue a frame for the peer, or account for why it cannot be.

        Never raises about the peer, exactly as a local `tell` never raises
        about a recipient: a full outbound buffer, a link that has failed and
        an association that has stopped are all things the sender can do
        nothing about, so they become dead letters naming the peer.

        Args:
            message: The message the frame carries, for the dead letter.
            frame: The complete frame.
            recipient: Where it was addressed, in the peer's path space.
        """
        ref = self._ref
        if ref is None or self._closing:
            self._dead_letter(message, recipient, DeadLetterReason.NO_ASSOCIATION)
            return
        try:
            ref.tell(Outbound(payload=message, frame=frame, recipient=recipient))
        except MailboxFullError as error:
            self._dead_letter(
                message,
                recipient,
                DeadLetterReason.OUTBOUND_BUFFER_FULL,
                detail=str(error),
            )

    async def offer(self, message: Message, frame: bytes, recipient: ActorPath) -> None:
        """Queue a frame, waiting for room in the outbound buffer.

        Local backpressure against a socket that is not draining, and honestly
        not more than that: it is not end-to-end backpressure from the actor on
        the other side, and nothing in a fire-and-forget wire protocol could
        be. A protocol that needs the latter builds it out of messages.

        Args:
            message: The message the frame carries.
            frame: The complete frame.
            recipient: Where it was addressed.
        """
        ref = self._ref
        if ref is None or self._closing:
            self._dead_letter(message, recipient, DeadLetterReason.NO_ASSOCIATION)
            return
        await ref.offer(Outbound(payload=message, frame=frame, recipient=recipient))

    def adopt(self, link: FrameLink, uid: int) -> None:
        """Take over a link the peer opened, in place of the one in hand.

        What resolving a simultaneous dial comes to on the losing side. The
        association survives it: the queue, the mailbox and every ref pointing
        through it are unchanged, and only the socket underneath is swapped.
        Frames already written to the old link are at-most-once, like every
        other frame that was on a link when it ended.

        Args:
            link: The handshaken link to take over.
            uid: The peer's incarnation uid, as that handshake established it.
        """
        if self._closing:
            self._host.dispatcher.spawn_task(
                link.close(), name=f"tapio-link-close:{self._peer}"
            )
            return
        self._uid = uid
        # The writer stops until the new link has caught up with what is
        # queued, which is the same rule as a link coming up for the first
        # time and is what keeps order across the swap.
        self._link = None
        self._accepted = link
        previous, self._socket = self._socket, link
        reader, self._reader = self._reader, None
        self._reader = self._host.dispatcher.spawn_task(
            self._resume(previous, reader), name=f"tapio-link:{self._peer}"
        )

    async def _resume(
        self, previous: FrameLink | None, reader: "asyncio.Task[None] | None"
    ) -> None:
        """Retire the link that lost the dial, then read the one that won."""
        if reader is not None:
            reader.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await reader
        if previous is not None:
            await previous.close()
        await self._run()

    def close(self, detail: str) -> None:
        """Ask this association to stop, through its own behavior.

        Args:
            detail: Why, for the log and the dead letters that follow.
        """
        if self._closing:
            return
        self._closing = True
        self._link = None
        ref = self._ref
        if ref is not None:
            ref.tell(Close(detail=detail))

    def _with_heartbeat(
        self, timers: TimerScheduler[AssociationMessage]
    ) -> Behavior[AssociationMessage]:
        """Start the heartbeat and return the behavior that answers it."""
        timers.start_fixed_delay(
            _HEARTBEAT, Beat(), self._host.settings.heartbeat_interval
        )
        return Behaviors.receive_message(
            self._on_message, AssociationMessage, on_signal=self._on_signal
        )

    async def _on_message(
        self, message: AssociationMessage
    ) -> Behavior[AssociationMessage]:
        """Write what is queued, beat when there is nothing to write, or stop."""
        match message:
            case Outbound():
                await self._write(message)
            case Beat():
                await self._beat()
            case Close():
                _log.debug(
                    "association with %s is closing: %s", self._peer, message.detail
                )
                return Behaviors.stopped()
        return Behaviors.same()

    async def _on_signal(
        self, ctx: ActorContext[AssociationMessage], signal: Signal
    ) -> Behavior[AssociationMessage]:
        """Release the link and the reader when this actor stops."""
        if isinstance(signal, PostStop):
            await self._release()
        return Behaviors.same()

    async def _write(self, outbound: Outbound) -> None:
        """Write one frame, or hold it until there is a link to write it to."""
        link = self._link
        if link is None:
            self._hold(outbound)
            return
        try:
            await link.write_frame(outbound.frame)
        except OSError as error:
            self._dead_letter(
                outbound.payload,
                outbound.recipient,
                DeadLetterReason.LINK_FAILED,
                detail=str(error),
            )
            self.close(f"the link failed while writing: {error}")

    def _hold(self, outbound: Outbound) -> None:
        """Keep a frame until the link is up, or shed it if too many already are."""
        if len(self._pending) >= self._host.settings.outbound_capacity:
            self._dead_letter(
                outbound.payload,
                outbound.recipient,
                DeadLetterReason.OUTBOUND_BUFFER_FULL,
                detail=(
                    f"{len(self._pending)} frames are already waiting for a link "
                    f"to {self._peer}"
                ),
            )
            return
        self._pending.append(outbound)

    async def _beat(self) -> None:
        """Tell a silent peer this end is still here."""
        link = self._link
        if link is None:
            return
        try:
            await link.write_link(Heartbeat())
        except OSError as error:
            self.close(f"the link failed while heartbeating: {error}")

    async def _run(self) -> None:
        """Open the link, then read it until it ends. The association's one task."""
        try:
            link = self._accepted
            self._accepted = None
            if link is None:
                link = await self._dial()
                self._socket = link
            await self._open(link)
            await self._read(link)
        except asyncio.CancelledError:
            raise
        except (FrameTooLargeError, MessageDecodingError) as error:
            # A frame this end refused rather than a link that failed. It is
            # accounted for before the link goes, because a peer that can make
            # a system drop a connection is exactly the thing an operator needs
            # to be able to see.
            self._refused(error)
            self.close(str(error))
        except EOFError:
            # The peer closed. Deliberately or not: a system shutting down and
            # a process that died look identical from here, and deciding
            # between them is a failure detector's job rather than a reader's.
            # Not a warning for that reason, since the ordinary case is a peer
            # that went away on purpose.
            _log.info("link to %s was closed by the peer", self._peer)
            self.close(f"{self._peer} closed the link")
        except (OSError, TapioError, TimeoutError) as error:
            _log.warning("link to %s ended: %s", self._peer, error)
            self.close(str(error))

    def _refused(self, error: Exception) -> None:
        """Account for a frame that was refused before it could be read."""
        reason = (
            DeadLetterReason.FRAME_TOO_LARGE
            if isinstance(error, FrameTooLargeError)
            else DeadLetterReason.MALFORMED_FRAME
        )
        _log.warning("refused a frame from %s: %s", self._peer, error)
        self._dead_letter(
            UndecodableFrame(sender=str(self._peer)),
            ActorPath.root(self._host.address.system),
            reason,
            detail=str(error),
        )

    async def _dial(self) -> FrameLink:
        """Connect to the peer and handshake, against one deadline.

        Returns:
            The open, handshaken link.

        Raises:
            HandshakeError: If the peer refused this system or is not one.
            OSError: If it could not be reached.
            TimeoutError: If the whole opening did not finish in time.
        """
        settings = self._host.settings
        host, port = self._peer.host, self._peer.port
        if host is None or port is None:  # pragma: no cover - guarded at resolve
            msg = f"{self._peer} names a system and no host to dial"
            raise HandshakeError(msg)
        context = client_ssl_context(settings.tls) if settings.tls else None
        seconds = settings.handshake_timeout.total_seconds()
        async with asyncio.timeout(seconds):
            link = await connect(
                host,
                port,
                max_frame_bytes=settings.max_frame_bytes,
                ssl_context=context,
            )
            try:
                identity = await introduce(
                    link,
                    address=self._host.address,
                    uid=self._host.uid,
                    secret=settings.secret,
                    timeout=seconds,
                )
            except BaseException:
                await link.close()
                raise
        self._uid = identity.uid
        _log.debug("dialled %s, incarnation %d", identity.address, identity.uid)
        return link

    async def _open(self, link: FrameLink) -> None:
        """Flush what was waiting, then let the actor write for itself.

        Order is the whole of it. Frames held while the link was coming up
        arrived before anything still in the mailbox, so they go first, and the
        link only becomes the actor's to write to once nothing is left waiting.
        Nothing awaits between that check and the assignment, so no frame can
        slip past the ones in front of it.
        """
        while self._pending:
            outbound = self._pending.popleft()
            try:
                await link.write_frame(outbound.frame)
            except OSError:
                self._pending.appendleft(outbound)
                raise
        if self._closing:
            await link.close()
            return
        self._link = link

    async def _read(self, link: FrameLink) -> None:
        """Read frames until the link ends, handing on what is not ours."""
        while True:
            frame = await link.read_frame()
            self._last_frame_at = self._host.dispatcher.now()
            if is_link_frame(frame):
                self._on_link_frame(frame)
                continue
            self._host.deliver(frame, self._peer)

    def _on_link_frame(self, frame: bytes) -> None:
        """Note one of the transport's own frames.

        A heartbeat has already done its work by arriving: what is recorded is
        the timestamp above, and everything else is logged rather than acted
        on, since a frame kind this version does not know is a peer running
        something newer and not a reason to drop a working link.
        """
        kind = link_body(frame).get("link")
        if kind != _HEARTBEAT:
            _log.debug("ignoring a %r link frame from %s", kind, self._peer)

    async def _release(self) -> None:
        """Cancel the reader, close the link, and account for what never left."""
        self._closing = True
        reader = self._reader
        self._reader = None
        if reader is not None and reader is not asyncio.current_task():
            reader.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await reader
        link = self._socket or self._accepted
        self._link = None
        self._socket = None
        self._accepted = None
        if link is not None:
            await link.close()
        while self._pending:
            outbound = self._pending.popleft()
            self._dead_letter(
                outbound.payload,
                outbound.recipient,
                DeadLetterReason.LINK_FAILED,
                detail=f"the association with {self._peer} stopped before it left",
            )
        self._host.forget(self)

    def _dead_letter(
        self,
        message: Message,
        recipient: ActorPath,
        reason: str,
        *,
        detail: str | None = None,
    ) -> None:
        """Account for a frame that will never reach its peer."""
        self._host.dead_letters.publish(
            message, recipient, reason, peer=self._peer, detail=detail
        )

    def __repr__(self) -> str:
        """Render the peer and whether a link is up."""
        state = "connected" if self._link is not None else "connecting"
        if self._closing:
            state = "closing"
        return f"Association({str(self._peer)!r}, {state})"
