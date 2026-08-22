"""`RemoteRef`: a handle to an actor on another system.

It is an `ActorRef` and it behaves like one. `tell` does not block and does
not raise about the recipient. A message that cannot be delivered becomes a
dead letter. The ref stays a valid handle whatever happens to the actor behind
it. An actor holding a ref just sends, and does not need to know which node
the target is on.

Failure is not transparent, and it splits the same way a local send does. The
message is yours, the recipient is not. Validation and encoding happen at the
send site, so a message that cannot be written raises where it was written.
Everything about the far end becomes a dead letter naming the peer: no link, a
full buffer, or a path the peer does not know.

A message that crossed a link is `==` to what was sent, and never `is` it,
because it was rebuilt from JSON on the other side. Inside a system, identity
is guaranteed. Across a link, equality is the most that can hold.

Watching and asking work through the same ref and the same calls. Both have
one failure a local one does not: the peer can go out of reach. A watcher is
told `Terminated` and cannot tell that from the actor stopping, because there
is nothing useful it could do differently. An ask is told
`AskTargetUnreachable`, because its caller can.
"""

from collections.abc import Callable
from datetime import timedelta
from typing import TYPE_CHECKING, Protocol, TypeVar

from pydantic_core import PydanticSerializationError

from tapio.actor.ask import ask_through
from tapio.actor.path import ActorPath
from tapio.actor.ref import ActorRef
from tapio.actor.watch import Watcher, WatchTarget
from tapio.errors import (
    AskTargetUnreachable,
    MessageEncodingError,
    MessageRegistrationError,
)
from tapio.message import Message
from tapio.remote.address import Address, format_ref
from tapio.remote.codec import encode
from tapio.validation import MessageValidator

if TYPE_CHECKING:
    from tapio.actor.cell import ActorRuntime

__all__ = ["Outbox", "PeerWatch", "RemoteRef"]

T = TypeVar("T", bound=Message)
R = TypeVar("R", bound=Message)


class Outbox(Protocol):
    """Where a remote ref hands the frames it has encoded."""

    @property
    def peer(self) -> Address:
        """The address on the other end."""
        ...

    @property
    def is_quarantined(self) -> bool:
        """Whether this system has given up on the peer."""
        ...

    def send(self, message: Message, frame: bytes, recipient: ActorPath) -> None:
        """Queue a frame, or account for why it cannot be queued."""
        ...

    async def offer(self, message: Message, frame: bytes, recipient: ActorPath) -> None:
        """Queue a frame, waiting for room in the outbound buffer."""
        ...

    def watch(self, watchee: ActorPath, watcher: Watcher) -> None:
        """Ask the peer to report when one of its actors stops."""
        ...

    def unwatch(self, watchee: ActorPath, watcher: Watcher) -> None:
        """Withdraw a watch on an actor over there."""
        ...


class PeerWatch:
    """A death watch on an actor that lives on another node.

    It stands where a cell stands for a local watch, so `ctx.watch` is one
    call whichever node the actor is on. What it can promise is weaker. A
    local cell knows whether its actor is alive; this knows only whether the
    peer is still being talked to, and a peer that goes silent produces the
    same `Terminated` as one whose actor really stopped.
    """

    __slots__ = ("_outbox", "_path")

    def __init__(self, outbox: Outbox, path: ActorPath) -> None:
        """Bind a watch to one actor on one peer."""
        self._outbox = outbox
        self._path = path

    @property
    def path(self) -> ActorPath:
        """Where the watched actor sits in the peer's tree."""
        return self._path

    @property
    def is_alive(self) -> bool:
        """Whether a watch registered now could still produce a signal.

        It says nothing about the actor. A quarantined peer answers `False`,
        because nothing will be sent there and nothing will come back, so the
        watcher is told at once instead of waiting forever.
        """
        return not self._outbox.is_quarantined

    def add_watcher(self, watcher: Watcher) -> None:
        """Ask the peer to report this actor's death."""
        self._outbox.watch(self._path, watcher)

    def remove_watcher(self, watcher: Watcher) -> None:
        """Tell the peer to stop reporting it."""
        self._outbox.unwatch(self._path, watcher)

    def __repr__(self) -> str:
        """Render the actor being watched, address included."""
        return f"PeerWatch({format_ref(self._outbox.peer, self._path)!r})"


class RemoteRef(ActorRef[T]):
    """A ref to an actor on a peer system, reached through one association."""

    __slots__ = ("_max_frame_bytes", "_outbox", "_runtime", "_validate")

    def __init__(
        self,
        path: ActorPath,
        *,
        outbox: Outbox,
        validate: MessageValidator,
        max_frame_bytes: int,
        runtime: "ActorRuntime",
    ) -> None:
        """Bind a ref to a path on a peer, and to the link that reaches it.

        Args:
            path: Where the actor sits in the peer's tree, uid included.
            outbox: The association to that peer.
            validate: The sender-side check, resolved from what the caller
                declared the peer accepts.
            max_frame_bytes: The size limit this system enforces on a frame.
            runtime: The sending system's slice, which an ask needs: its loop,
                and the registry a reply finds its way back through.
        """
        super().__init__(path)
        self._outbox = outbox
        self._validate = validate
        self._max_frame_bytes = max_frame_bytes
        self._runtime = runtime

    @property
    def address(self) -> Address:
        """The peer's canonical address, which is what this ref writes down."""
        return self._outbox.peer

    def tell(self, message: T) -> None:
        """Send a message to the peer, without waiting and without blocking.

        The type check here is against what the caller declared the peer
        accepts, which is a claim about the peer rather than knowledge of it.
        The check that decides runs on the receiving node, against the target
        actor's real message type, and a mismatch there dead-letters on that
        node. The sender's declaration and the receiver's protocol are
        deployed separately.

        Args:
            message: The message to deliver.

        Raises:
            MessageTypeError: If the message does not match the type this ref
                was resolved with.
            MessageEncodingError: If the message has no wire key or no JSON
                representation. Raised before any I/O, since the message is
                the sender's.
            FrameTooLargeError: If the encoded frame is over the size limit.
            pydantic.ValidationError: If content validation is on and the
                message does not satisfy its own model.
        """
        self._validate(message)
        self._outbox.send(message, self._encode(message), self.path)

    async def offer(self, message: T) -> None:
        """Send a message, waiting for room in the outbound buffer.

        This is local backpressure against a socket that is not draining. It
        is not end-to-end backpressure from the receiving actor, which a
        fire-and-forget wire protocol cannot provide.

        Args:
            message: The message to deliver.

        Raises:
            MessageTypeError: If the message does not match the type this ref
                was resolved with.
            MessageEncodingError: If the message cannot be written to a frame.
            pydantic.ValidationError: If content validation is on and the
                message does not satisfy its own model.
        """
        self._validate(message)
        await self._outbox.offer(message, self._encode(message), self.path)

    def watch_target(self) -> WatchTarget:
        """Return the peer, which is what a death watch on this ref goes through."""
        return PeerWatch(self._outbox, self.path)

    async def ask(
        self,
        make: "Callable[[ActorRef[R]], T]",
        *,
        expect: type[R],
        timeout: timedelta | None = None,  # noqa: ASYNC109 - the ask deadline
    ) -> R:
        """Send one message across the link and await one reply.

        The same call as a local ask, and the same promise behind it. The
        reply comes back addressed to `/system/promises`, which is why a
        promise is addressable at all. The target is watched for the duration,
        so an actor that stops and a peer that goes silent both fail the ask
        at once rather than after the full deadline.

        Args:
            make: Builds the request from the ref the reply should go to.
            expect: The reply type, which is required.
            timeout: How long to wait. The system's `ask_timeout` when omitted.

        Returns:
            The reply, rebuilt from JSON, so it is equal to what the responder
            sent and never the same object.

        Raises:
            AskTimeoutError: If no reply arrived in time.
            AskTargetTerminated: If the actor over there stopped without
                replying.
            AskTargetUnreachable: If the peer went out of reach, which may
                mean the actor is alive and unreachable rather than gone.
            AskTypeError: If a reply arrived that was not an `expect`.
            MessageTypeError: If the request does not match the type this ref
                was resolved with.
            MessageEncodingError: If the request cannot be written to a frame.
            RuntimeError: If called from a thread that is not running the
                system's loop.
        """
        gone = AskTargetUnreachable(
            f"{self.path} was beyond reach when an ask expecting "
            f"{expect.__name__} was made: this system has given up on "
            f"{self._outbox.peer}"
        )
        return await ask_through(
            self.watch_target(),
            self.tell,
            make,
            runtime=self._runtime,
            expect=expect,
            timeout=timeout,
            gone=gone,
        )

    def _encode(self, message: T) -> bytes:
        """Turn a message into the frame that carries it.

        Raises:
            MessageEncodingError: If the type has no wire key, or a field has
                no JSON representation.
            FrameTooLargeError: If the frame is over the size limit.
        """
        try:
            # The sending system, not a sending actor. A `tell` carries no
            # sender, so there is none to name, and this is what lets a dead
            # letter over there say which node produced the frame.
            return encode(
                message,
                to=self.path,
                sender=self._runtime.address,
                max_frame_bytes=self._max_frame_bytes,
            )
        except MessageRegistrationError as error:
            raise MessageEncodingError(str(error)) from error
        except PydanticSerializationError as error:
            msg = (
                f"{type(message).__name__} to {self.path} has no JSON "
                f"representation, so it cannot cross a link: {error}"
            )
            raise MessageEncodingError(msg) from error

    def __repr__(self) -> str:
        """Render the full string form: where this ref points is what names it."""
        return f"RemoteRef({format_ref(self.address, self.path)!r})"
