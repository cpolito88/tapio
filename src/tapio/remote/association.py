"""An association: one link to one peer, and the actor that owns it.

Two systems associate on demand. The first message sent to an address creates
the association, and every ref for that address then uses it. That is what
makes "FIFO per association" a guarantee rather than a coincidence of how many
connections happen to be open.

An association is an actor. Its writer is the cell's receive loop, its
outbound buffer is the cell's bounded mailbox, its heartbeat is a cell timer,
and its reader is one task the cell cancels when it stops. Remoting therefore
adds no new rule about who owns a task, and the existing leak check covers it.

Delivery is **at-most-once**. No acks, no retries, no resend buffer: a frame
written to a socket that then failed is lost, and it dead-letters here if the
failure is visible from this side. Acks would make delivery at-least-once.
That is not an improvement, only a different trade-off, and it would oblige
every receiving actor to be idempotent. That belongs in the user's protocol,
where they know what is safe to repeat.

An association also holds the death watches that cross it, in both
directions: the local watchers of actors over there, and the local actors
watched from over there. Both sets end when the association does. Watchers of
a peer that went away are told `Terminated`, which is the one signal in the
library that can be wrong, and [tapio.remote.failure][] says why there is no
better answer available to a single node.
"""

import asyncio
import contextlib
from collections import deque
from typing import Any, Protocol, TypeAlias

from pydantic import ValidationError

from tapio.actor.behavior import Behavior, Behaviors
from tapio.actor.context import ActorContext
from tapio.actor.dead_letters import Carrier, DeadLetterOffice, DeadLetterReason
from tapio.actor.events import EventStream
from tapio.actor.path import ActorPath
from tapio.actor.ref import ActorRef
from tapio.actor.signals import PostStop, Signal
from tapio.actor.timers import TimerScheduler
from tapio.actor.watch import Watcher, WatchTarget
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
from tapio.remote.codec import UndecodableFrame, format_target, parse_target
from tapio.remote.failure import (
    DeadlineDetector,
    DownAlone,
    DownDecider,
    FailureDetector,
    PeerReachable,
    PeerUnreachable,
)
from tapio.remote.handshake import introduce
from tapio.remote.transport import (
    FrameLink,
    Heartbeat,
    Link,
    LinkFrame,
    Unwatch,
    Watch,
    WatcheeTerminated,
    client_ssl_context,
    connect,
    framed,
    is_link_frame,
    link_body,
)
from tapio.settings import RemoteSettings

__all__ = [
    "Association",
    "AssociationHost",
    "AssociationMessage",
    "Beat",
    "LinkOut",
    "Outbound",
]

_log = runtime_logger("remote")

_HEARTBEAT = "heartbeat"
"""The timer key, and the link frame kind it writes."""


class Outbound(Carrier):
    """One frame queued for a peer, with the message it was made from.

    The frame is what travels. The payload comes along so that a frame which
    never leaves can report the message its sender sent, rather than this
    wrapper. Encoding happens at the send site, on the caller's thread,
    because an error about the message belongs to whoever wrote it.
    """

    frame: bytes
    """The complete frame, length prefix included."""

    recipient: ActorPath
    """Where it was addressed, in the peer's path space."""


class LinkOut(Message):
    """One of the transport's own frames, queued behind whatever is in front.

    A watch has to arrive after the messages sent before it and before the
    ones sent after, so it travels through the same mailbox as user traffic
    rather than jumping the queue. A frame that never leaves is dropped: there
    is no user message to account for, and a peer that cannot be written to is
    about to be declared unreachable anyway.
    """

    frame: bytes
    """The complete frame, length prefix included."""

    kind: str
    """Which link frame it is, for the log line if it has to be dropped."""


class Beat(Message):
    """A tick asking the association to prove the link is still there."""


class Close(Message):
    """Ask an association to stop, because its link is over."""

    detail: str
    """What happened, for the log and for the dead letters that follow."""


AssociationMessage: TypeAlias = Outbound | LinkOut | Beat | Close
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
    def events(self) -> EventStream:
        """Where this system says that a peer went out of reach."""
        ...

    @property
    def dispatcher(self) -> Dispatcher:
        """The loop this system runs on, and the reader task runs on."""
        ...

    @property
    def is_closing(self) -> bool:
        """Whether this system is shutting down.

        A link that ends because this end is going away is not news about the
        peer, so nothing is published about it.
        """
        ...

    def deliver(self, frame: bytes, peer: Address) -> None:
        """Hand an inbound message frame to the system that owns the recipient."""
        ...

    def wrap(self, link: FrameLink) -> Link:
        """Put whatever sits between this system and its sockets in the way.

        Nothing, in production. A test installs a wrapper here to drop, delay
        or swallow frames, which is how a partition is simulated without
        breaking anything real.
        """
        ...

    def lookup(self, path: ActorPath) -> ActorRef[Any] | None:
        """Find a live local actor by path and incarnation uid, for a watch."""
        ...

    def peer_ref(self, peer: Address, path: ActorPath) -> ActorRef[Any]:
        """Build a ref to an actor on a peer, to name it in a `Terminated`."""
        ...

    def quarantine(self, peer: Address, detail: str) -> None:
        """Freeze an address: nothing sent, nothing dialled, until told otherwise."""
        ...

    def forget(self, association: "Association") -> None:
        """Drop an association that has stopped, so the next send dials afresh."""
        ...

    def close_link_later(self, link: Link, peer: Address) -> None:
        """Close a link nobody is going to use, in a task the endpoint holds."""
        ...


class Association:
    """One link to one peer: the actor's state, and the reader behind it.

    It is created in one of two ways, and only the start differs. Dialled,
    when this system sent to an address it has no link to. Adopted, when the
    peer dialled in and the handshake said who it was.
    """

    __slots__ = (
        "_accepted",
        "_closing",
        "_decider",
        "_detector",
        "_host",
        "_initiator",
        "_link",
        "_peer",
        "_pending",
        "_quarantined",
        "_reader",
        "_ready",
        "_ref",
        "_retiring",
        "_socket",
        "_uid",
        "_watched_here",
        "_watching_there",
    )

    def __init__(
        self,
        *,
        host: AssociationHost,
        peer: Address,
        initiator: Address,
        link: Link | None = None,
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
        self._link: Link | None = None
        self._pending: deque[Outbound | LinkOut] = deque()
        self._reader: asyncio.Task[None] | None = None
        self._ref: ActorRef[AssociationMessage] | None = None
        self._closing = False
        self._quarantined = False
        self._uid = uid
        self._detector: FailureDetector = DeadlineDetector(
            unreachable_after=host.settings.unreachable_after.total_seconds(),
            started_at=host.dispatcher.now(),
        )
        self._decider: DownDecider = DownAlone()
        # Both directions of every death watch that crosses this link. The
        # first holds local watchers of actors on the peer, and the second
        # holds the watchers this system registered on its own actors for the
        # peer. Neither may outlive the association, or a stopped link would
        # leave watchers on live cells and watchers waiting on a signal that
        # nothing is left to send.
        self._watching_there: dict[ActorPath, dict[ActorPath, Watcher]] = {}
        self._watched_here: dict[tuple[ActorPath, str], _PeerWatcher] = {}
        self._ready: asyncio.Future[None] = host.dispatcher.loop.create_future()
        # Held until the reader task starts and takes it over.
        self._accepted: Link | None = link
        # Every link this association has opened, writable or not. `_link` is
        # what the actor may write to, and is set only once nothing is queued
        # ahead of it. This one exists as soon as there is a socket, so a
        # cancellation between the two still has something to close.
        self._socket: Link | None = link
        # The link a simultaneous dial retired, held from the moment it loses
        # the race until it is closed. `_resume` closes it, but that runs in a
        # task that a shutdown can cancel before its first line, so the field is
        # what lets `_release` and `detach` close it when `_resume` never runs.
        self._retiring: Link | None = None

    @property
    def peer(self) -> Address:
        """The address on the other end, which keys this association."""
        return self._peer

    @property
    def initiator(self) -> Address:
        """Whose dial opened this link.

        Both sides connecting at once is normal under load. Without a rule the
        pair keeps two connections, and FIFO per association stops meaning
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
    def is_closing(self) -> bool:
        """Whether this association has been asked to stop."""
        return self._closing

    @property
    def watching(self) -> tuple[ActorPath, ...]:
        """The actors on the peer that something here is watching.

        Exposed for the same reason a cell exposes its watchers: a test has to
        be able to assert that the watch was released.
        """
        return tuple(self._watching_there)

    @property
    def watched(self) -> tuple[ActorPath, ...]:
        """The local actors the peer is watching, one entry per watch."""
        return tuple(watchee for watchee, _ in self._watched_here)

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

        Never raises about the peer, just as a local `tell` never raises about
        a recipient. A full outbound buffer, a failed link and a stopped
        association are all things the sender can do nothing about, so they
        become dead letters naming the peer.

        Args:
            message: The message the frame carries, for the dead letter.
            frame: The complete frame.
            recipient: Where it was addressed, in the peer's path space.
        """
        ref = self._ref
        if ref is None or self._closing:
            reason = (
                DeadLetterReason.QUARANTINED
                if self._quarantined
                else DeadLetterReason.NO_ASSOCIATION
            )
            self._dead_letter(message, recipient, reason)
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

        This is local backpressure against a socket that is not draining, and
        nothing more. It is not end-to-end backpressure from the actor on the
        other side, which no fire-and-forget wire protocol can give. Build
        that out of messages if you need it.

        Args:
            message: The message the frame carries.
            frame: The complete frame.
            recipient: Where it was addressed.
        """
        ref = self._ref
        if ref is None or self._closing:
            self.send(message, frame, recipient)
            return
        await ref.offer(Outbound(payload=message, frame=frame, recipient=recipient))

    def watch(self, watchee: ActorPath, watcher: Watcher) -> None:
        """Ask the peer to report when one of its actors stops.

        Args:
            watchee: The actor over there, uid included.
            watcher: What to tell when it stops, or when the peer goes out of
                reach, which from here look the same.
        """
        if self._closing:
            watcher.notify_unreachable(
                self._host.peer_ref(self._peer, watchee),
                f"the association with {self._peer} had already ended",
            )
            return
        self._watching_there.setdefault(watchee, {})[watcher.path] = watcher
        if self._write_link(
            Watch(
                watchee=format_target(watchee),
                watcher=format_target(watcher.path),
            )
        ):
            return
        # The frame did not get queued, so the peer will never register this
        # watch and no `Terminated` is coming from over there. Holding the
        # entry anyway would leave the watcher waiting on a signal nothing is
        # left to send, which is the failure death watch exists to prevent, so
        # it is answered now instead.
        self._forget_watch(watchee, watcher)
        watcher.notify_unreachable(
            self._host.peer_ref(self._peer, watchee),
            f"the watch on {watchee} could not be sent to {self._peer}",
        )

    def unwatch(self, watchee: ActorPath, watcher: Watcher) -> None:
        """Withdraw a watch on an actor over there.

        Harmless if there was none. It does not retract a `Terminated` that is
        already on its way, since by then it is as true as it was going to be.

        Args:
            watchee: The actor over there.
            watcher: Who was watching.
        """
        if not self._forget_watch(watchee, watcher):
            return
        if self._closing:
            return
        self._write_link(
            Unwatch(
                watchee=format_target(watchee),
                watcher=format_target(watcher.path),
            )
        )

    def _forget_watch(self, watchee: ActorPath, watcher: Watcher) -> bool:
        """Drop one local watcher of an actor over there.

        Returns:
            Whether there was one to drop.
        """
        watchers = self._watching_there.get(watchee)
        if watchers is None or watchers.pop(watcher.path, None) is None:
            return False
        if not watchers:
            del self._watching_there[watchee]
        return True

    def adopt(self, link: Link, uid: int) -> None:
        """Take over a link the peer opened, in place of the one in hand.

        This is how the losing side of a simultaneous dial is resolved. The
        association survives. The queue, the mailbox, every ref pointing
        through it and every watch across it are unchanged, and only the
        socket underneath is swapped. Frames already written to the old link
        are at-most-once, like every other frame on a link that ended.

        Args:
            link: The handshaken link to take over.
            uid: The peer's incarnation uid, as that handshake established it.
        """
        if self._closing:
            # This association is going away and will not adopt anything, but
            # the link still has to be released. The endpoint closes it, since
            # it outlives every association and drains these in its own close.
            self._host.close_link_later(link, self._peer)
            return
        self._uid = uid
        # The writer waits until the new link has caught up with what is
        # queued. That is the same rule as a link coming up for the first
        # time, and it is what keeps the order across the swap.
        self._link = None
        self._accepted = link
        # The losing socket is put in `_retiring` before the task that closes
        # it is spawned, so a shutdown that cancels that task before it runs a
        # line still finds the link to close in `_release`.
        previous, self._socket = self._socket, link
        self._retiring = previous
        reader, self._reader = self._reader, None
        self._reader = self._host.dispatcher.spawn_task(
            self._resume(reader), name=f"tapio-link:{self._peer}"
        )

    async def _resume(self, reader: "asyncio.Task[None] | None") -> None:
        """Retire the link that lost the dial, then read the one that won."""
        try:
            if reader is not None:
                reader.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await reader
        finally:
            # Close the retired link even if this task is cancelled while the
            # old reader is ending. It is taken out of `_retiring` here so that
            # `_release` does not close it a second time, and closed with a
            # synchronous `writer.close()` first, so the socket is released even
            # under cancellation. If this task is cancelled before its first
            # line runs, this never executes and `_release` closes `_retiring`
            # instead.
            retiring, self._retiring = self._retiring, None
            if retiring is not None:
                await retiring.close()
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
        self._fail_ready(detail)
        ref = self._ref
        if ref is not None:
            ref.tell(Close(detail=detail))

    async def detach(self) -> None:
        """Close the link when this association's actor never got to stop.

        `_release` closes the socket on `PostStop`, which is the ordinary
        path. An association adopted so late that the endpoint's stop sweep had
        already passed it gets no `PostStop`, so its socket would be left for
        the garbage collector. The endpoint calls this on its own way down to
        close it, in the same reader-then-link order `_release` takes so a read
        in flight ends before the link does.
        """
        if self._closing:
            return
        self._closing = True
        self._fail_ready("the association stopped")
        reader = self._reader
        self._reader = None
        if reader is not None and reader is not asyncio.current_task():
            reader.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await reader
        link = self._socket or self._accepted
        retiring, self._retiring = self._retiring, None
        self._link = None
        self._socket = None
        self._accepted = None
        if link is not None:
            await link.close()
        # A dial race retired a link into `_retiring`, and the task that would
        # have closed it never ran because this detach cancelled it first.
        if retiring is not None and retiring is not link:
            await retiring.close()
        self._host.forget(self)

    async def wait_connected(self, timeout: float) -> None:  # noqa: ASYNC109 - the dial deadline
        """Wait until the link is up, for a caller that asked for it by hand.

        Nothing else waits for a dial. A send queues behind one and a resolve
        starts none, so this exists for `remote.reconnect`, where a person or
        a supervisor decided to re-associate and wants to know whether it
        worked.

        Args:
            timeout: Seconds to wait.

        Raises:
            HandshakeError: If the link ended before it came up.
            TimeoutError: If it had not come up in time.
        """
        await asyncio.wait_for(asyncio.shield(self._ready), timeout)

    def _write_link(self, frame: LinkFrame) -> bool:
        """Queue one of the transport's own frames behind the traffic in front.

        Returns:
            Whether it was queued. A `False` matters: a watch the peer never
            hears about is a watch this end must not go on believing in, so
            the caller has to be able to tell.
        """
        ref = self._ref
        if ref is None:  # pragma: no cover - the endpoint binds before any send
            return False
        body = framed(frame.model_dump_json().encode())
        kind = type(frame).__name__
        try:
            ref.tell(LinkOut(frame=body, kind=kind))
        except MailboxFullError:
            # Logged rather than swallowed. There is no user message to
            # dead-letter, so this line is the only trace a dropped watch
            # leaves, and it used to leave none at all.
            _log.warning(
                "dropped a %s frame for %s: the outbound buffer is full",
                kind,
                self._peer,
            )
            return False
        return True

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
            case LinkOut():
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

    def _write_budget(self) -> float:
        """How long a write has to reach a peer before the peer counts as gone.

        The silence window, reused rather than configured twice. If this
        system cannot get any bytes into a peer for as long as it would
        tolerate hearing nothing back, that peer is unreachable by the same
        standard. A second setting would only be a way for the two to
        disagree.
        """
        return self._host.settings.unreachable_after.total_seconds()

    async def _write(self, outbound: Outbound | LinkOut) -> None:
        """Write one frame, or hold it until there is a link to write it to.

        The write is bounded. `drain` waits for the peer's receive window, and
        a peer that holds a connection open while never reading would
        otherwise park this actor here for good: the mailbox fills behind it,
        the heartbeat tick that would notice never gets handled, and the
        failure detector is never consulted. The deadline is what breaks that,
        so the actor is always the one that gets its loop back.
        """
        link = self._link
        if link is None:
            self._hold(outbound)
            return
        try:
            async with asyncio.timeout(self._write_budget()):
                await link.write_frame(outbound.frame)
        except TimeoutError:
            # Before OSError, which TimeoutError subclasses. A peer that
            # accepts no bytes is a different diagnosis from a link that
            # broke, and only this one means the peer should be given up on.
            stalled = f"{self._peer} accepted no bytes for {self._write_budget():g}s"
            if isinstance(outbound, Outbound):
                self._dead_letter(
                    outbound.payload,
                    outbound.recipient,
                    DeadLetterReason.LINK_FAILED,
                    detail=stalled,
                )
            await self._declare_unreachable(stalled)
        except OSError as error:
            if isinstance(outbound, Outbound):
                self._dead_letter(
                    outbound.payload,
                    outbound.recipient,
                    DeadLetterReason.LINK_FAILED,
                    detail=str(error),
                )
            self.close(f"the link failed while writing: {error}")

    def _hold(self, outbound: Outbound | LinkOut) -> None:
        """Keep a frame until the link is up, or shed it if too many already are."""
        if len(self._pending) >= self._host.settings.outbound_capacity:
            self._shed(outbound)
            return
        self._pending.append(outbound)

    def _shed(self, outbound: Outbound | LinkOut) -> None:
        """Give up on a frame there was never going to be room for."""
        waiting = (
            f"{len(self._pending)} frames are already waiting for a link "
            f"to {self._peer}"
        )
        if isinstance(outbound, LinkOut):
            _log.warning(
                "dropped a %s frame for %s: %s", outbound.kind, self._peer, waiting
            )
            return
        self._dead_letter(
            outbound.payload,
            outbound.recipient,
            DeadLetterReason.OUTBOUND_BUFFER_FULL,
            detail=waiting,
        )

    async def _beat(self) -> None:
        """Tell a silent peer this end is still here, and judge its silence."""
        link = self._link
        if link is not None:
            try:
                async with asyncio.timeout(self._write_budget()):
                    await link.write_link(Heartbeat())
            except TimeoutError:
                await self._declare_unreachable(
                    f"{self._peer} accepted no heartbeat for {self._write_budget():g}s"
                )
                return
            except OSError as error:
                self.close(f"the link failed while heartbeating: {error}")
                return
        # Judged whether or not there was a link to write to. Having no
        # writable link is not a reason to skip the question: it is most of
        # the reason to ask it, and skipping it meant an association stuck
        # without a link was never given up on at all.
        await self._judge()

    async def _judge(self) -> None:
        """Ask whether a peer that has stopped answering should be given up on.

        The detector says how it looks and the decider says what to do, and
        neither decision is made inline here. Today the first is a fixed
        timeout and the second is always yes. Both are interfaces because
        clustering replaces them, and the association is written against the
        interfaces so that nothing here changes when it does.
        """
        if self._detector.is_available(self._host.dispatcher.now()):
            return
        await self._declare_unreachable(
            f"nothing has arrived from {self._peer} inside the silence window"
        )

    async def _declare_unreachable(self, why: str) -> None:
        """Put a peer to the decider, and act on a decision to give up on it.

        Two things reach this: silence, which the detector judges, and a write
        that never drained, which needs no detector because it is direct
        evidence. Both are the same verdict about the peer and both go through
        the decider, so a clustered deployment changes how the answer is
        reached in one place rather than two.

        Args:
            why: What prompted the question, for the log. The words that
                travel to the quarantine and the dead letters are the
                decider's, since it is the one that decided.
        """
        decision = await self._decider.decide(self._peer)
        if not decision.down or self._closing:
            return
        _log.warning("%s is unreachable (%s): %s", self._peer, why, decision.detail)
        self._quarantined = True
        self._host.quarantine(self._peer, decision.detail)
        self.close(decision.detail)

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
            # A frame this end refused, rather than a link that failed. It is
            # accounted for before the link goes, because an operator needs to
            # see a peer that can make this system drop a connection.
            self._refused(error)
            self.close(str(error))
        except EOFError:
            # The peer closed. A system shutting down and a process that died
            # look the same from here, and telling them apart is the failure
            # detector's job. Logged at info rather than warning, because the
            # ordinary case is a peer that went away on purpose.
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

    async def _dial(self) -> Link:
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
            link: FrameLink = await connect(
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
        return self._host.wrap(link)

    async def _open(self, link: Link) -> None:
        """Flush what was waiting, then let the actor write for itself.

        This is all about order. Frames held while the link was coming up
        arrived before anything still in the mailbox, so they go first. The
        link becomes the actor's to write to only once nothing is left
        waiting. Nothing awaits between that check and the assignment, so no
        frame can overtake the ones in front of it.
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
        # A peer that has just handshaken is not a silent one, whatever the
        # clock said while the dial was in flight.
        self._detector.heartbeat(self._host.dispatcher.now())
        if not self._ready.done():
            self._ready.set_result(None)
        # Said once the link can actually carry traffic, which is the point at
        # which an earlier verdict about this peer stops being true. Whoever
        # wrote the peer off is the only one that can take it back, so this is
        # the fact rather than the retraction.
        self._host.events.publish(PeerReachable(peer=str(self._peer), uid=self._uid))

    async def _read(self, link: Link) -> None:
        """Read frames until the link ends, handing on what is not ours."""
        while True:
            frame = await link.read_frame()
            self._detector.heartbeat(self._host.dispatcher.now())
            if is_link_frame(frame):
                self._on_link_frame(frame)
                continue
            self._host.deliver(frame, self._peer)

    def _on_link_frame(self, frame: bytes) -> None:
        """Act on one of the transport's own frames.

        A heartbeat has done its work by arriving, and the timestamp recorded
        by the reader is what notes it. A frame kind this version does not
        know is logged rather than acted on, because a peer running something
        newer is no reason to drop a working link.
        """
        body = link_body(frame)
        kind = body.get("link")
        try:
            match kind:
                case "heartbeat":
                    return
                case "watch":
                    self._on_watch(Watch.model_validate(body))
                case "unwatch":
                    self._on_unwatch(Unwatch.model_validate(body))
                case "terminated":
                    self._on_terminated(WatcheeTerminated.model_validate(body))
                case _:
                    _log.debug("ignoring a %r link frame from %s", kind, self._peer)
        except (ValidationError, ValueError) as error:
            # Post-handshake, so this peer proved who it was. A frame it got
            # wrong is a bug over there, not an attack, and dropping the link
            # over one would cost every other conversation on it.
            _log.warning(
                "ignoring a malformed %r frame from %s: %s", kind, self._peer, error
            )

    def _on_watch(self, request: Watch) -> None:
        """Register a peer's watch on one of this system's actors."""
        watchee = parse_target(self._host.address.system, request.watchee)
        ref = self._host.lookup(watchee)
        target = ref.watch_target() if ref is not None else None
        if target is None or not target.is_alive:
            # Nothing there, or a uid whose incarnation is over. Answering at
            # once is what makes watching a stopped actor the same call as
            # watching a live one.
            self._write_link(
                WatcheeTerminated(watchee=request.watchee, watcher=request.watcher)
            )
            return
        key = (watchee, request.watcher)
        if key in self._watched_here:
            return
        proxy = _PeerWatcher(
            association=self,
            target=target,
            watchee=watchee,
            watcher=parse_target(self._peer.system, request.watcher),
            reply_to=request.watcher,
        )
        self._watched_here[key] = proxy
        target.add_watcher(proxy)

    def _on_unwatch(self, request: Unwatch) -> None:
        """Withdraw a peer's watch on one of this system's actors."""
        watchee = parse_target(self._host.address.system, request.watchee)
        proxy = self._watched_here.pop((watchee, request.watcher), None)
        if proxy is not None:
            proxy.release()

    def _on_terminated(self, report: WatcheeTerminated) -> None:
        """Tell the local watchers that an actor on the peer has stopped."""
        watchee = parse_target(self._peer.system, report.watchee)
        watchers = self._watching_there.pop(watchee, {})
        ref = self._host.peer_ref(self._peer, watchee)
        for watcher in list(watchers.values()):
            watcher.notify_terminated(ref)

    def report_terminated(self, watchee: ActorPath, watcher: str) -> None:
        """Tell the peer that one of this system's actors has stopped.

        Args:
            watchee: The actor that stopped, in this system's path space.
            watcher: The peer's watcher path, exactly as the peer wrote it.
        """
        self._watched_here.pop((watchee, watcher), None)
        self._write_link(
            WatcheeTerminated(watchee=format_target(watchee), watcher=watcher)
        )

    async def _release(self) -> None:
        """Cancel the reader, close the link, and account for what is left.

        Everything this association was holding ends here, in one place: the
        socket, the frames that never left, the watches in both directions,
        and the entry in the endpoint's table.
        """
        self._closing = True
        self._fail_ready("the association stopped")
        reader = self._reader
        self._reader = None
        if reader is not None and reader is not asyncio.current_task():
            reader.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await reader
        link = self._socket or self._accepted
        retiring, self._retiring = self._retiring, None
        self._link = None
        self._socket = None
        self._accepted = None
        if link is not None:
            await link.close()
        # A dial race retired a link into `_retiring`, and `_resume` was
        # cancelled before it could close it, since cancelling the reader above
        # is what cancelled `_resume`. Close it here so its socket is not left
        # for the garbage collector.
        if retiring is not None and retiring is not link:
            await retiring.close()
        while self._pending:
            outbound = self._pending.popleft()
            if isinstance(outbound, Outbound):
                self._dead_letter(
                    outbound.payload,
                    outbound.recipient,
                    DeadLetterReason.LINK_FAILED,
                    detail=f"the association with {self._peer} stopped before it left",
                )
        self._end_watches()
        self._host.forget(self)

    def _end_watches(self) -> None:
        """Release both sides of every watch that crossed this link.

        Local watchers are told the actor they were watching is beyond reach.
        That may be false: the peer could be alive on the other side of a
        partition. It is still the only answer a single node can give, and
        saying nothing would leave a supervisor waiting forever on a signal
        that is not coming.
        """
        for proxy in list(self._watched_here.values()):
            proxy.release()
        self._watched_here.clear()
        watching, self._watching_there = self._watching_there, {}
        detail = f"the association with {self._peer} ended"
        for watchee, watchers in watching.items():
            ref = self._host.peer_ref(self._peer, watchee)
            for watcher in list(watchers.values()):
                watcher.notify_unreachable(ref, detail)
        if self._host.is_closing:
            # This system is going away. That the peer cannot be reached from
            # a system that no longer exists is not news about the peer.
            return
        self._host.events.publish(
            PeerUnreachable(
                peer=str(self._peer),
                uid=self._uid,
                detail=detail,
                quarantined=self._quarantined,
            )
        )

    def _fail_ready(self, detail: str) -> None:
        """Fail whoever was waiting for this link to come up.

        The exception is retrieved when nobody was waiting, so a link that
        never came up does not surface later as an unhandled error from the
        garbage collector.
        """
        if self._ready.done():
            return
        self._ready.set_exception(
            HandshakeError(f"the link to {self._peer} did not come up: {detail}")
        )
        self._ready.exception()

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
        if self._quarantined:
            state = "quarantined"
        elif self._closing:
            state = "closing"
        return f"Association({str(self._peer)!r}, {state})"


class _PeerWatcher:
    """A watch a peer holds on one of this system's actors.

    It stands in the cell's watcher map exactly as a local watcher does, and
    turns the death it is told about into a frame. Keyed by the peer's own
    watcher path, so two peers watching the same actor are two watchers and
    one peer watching twice is one.
    """

    __slots__ = ("_association", "_reply_to", "_target", "_watchee", "_watcher")

    def __init__(
        self,
        *,
        association: Association,
        target: WatchTarget,
        watchee: ActorPath,
        watcher: ActorPath,
        reply_to: str,
    ) -> None:
        """Bind a proxy to the watch it stands for.

        Args:
            association: The link to report over.
            target: The local actor being watched, so the watch can be undone.
            watchee: That actor's path, as the report names it.
            watcher: The peer's watcher path, which is this proxy's key.
            reply_to: The peer's watcher path exactly as the peer wrote it,
                since that is what it expects to see come back.
        """
        self._association = association
        self._target = target
        self._watchee = watchee
        self._watcher = watcher
        self._reply_to = reply_to

    @property
    def path(self) -> ActorPath:
        """The peer's watcher path, which is the key this is held under."""
        return self._watcher

    def notify_terminated(self, ref: ActorRef[Any]) -> None:
        """Report the death across the link."""
        self._association.report_terminated(self._watchee, self._reply_to)

    def notify_unreachable(self, ref: ActorRef[Any], detail: str) -> None:
        """Do nothing, because this watcher is on a local actor.

        A local actor never becomes unreachable to the system running it. The
        method exists so that a proxy is a `Watcher` like any other.
        """

    def release(self) -> None:
        """Stop watching, because the watch or the link is over."""
        self._target.remove_watcher(self)

    def __repr__(self) -> str:
        """Render both ends of the watch this proxy stands for."""
        return f"_PeerWatcher({str(self._watchee)!r}, for {str(self._watcher)!r})"
