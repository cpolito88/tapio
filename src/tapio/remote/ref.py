"""`RemoteRef`: a handle to an actor on another system.

It is an `ActorRef` and it behaves like one. `tell` does not block and does not
raise about the recipient; a message that cannot be delivered becomes a dead
letter; the ref stays a valid handle whatever happens to the actor behind it.
That is what location transparency means here: an actor holding a ref sends,
and does not know or care which node the target is on.

What is not transparent is failure, and the split is the same rule that decides
a local send. **The message is yours, the recipient is not.** Validating and
encoding happen at the send site, on the caller's thread, so a message that
cannot be written raises where it was written. Everything about the far end,
no link, a buffer that is full, a path the peer does not know, is a dead letter
with the peer named.

A message that crossed a link is `==` to what was sent and never `is` it: it
was rebuilt from JSON on the other side. Within a system identity is
guaranteed; across a link, equality is the strongest thing that can be true.
"""

from collections.abc import Callable
from datetime import timedelta
from typing import Protocol, TypeVar

from pydantic_core import PydanticSerializationError

from tapio.actor.path import ActorPath
from tapio.actor.ref import ActorRef
from tapio.errors import MessageEncodingError, MessageRegistrationError
from tapio.message import Message
from tapio.remote.address import Address, format_ref
from tapio.remote.codec import encode
from tapio.validation import MessageValidator

__all__ = ["Outbox", "RemoteRef"]

T = TypeVar("T", bound=Message)
R = TypeVar("R", bound=Message)


class Outbox(Protocol):
    """Where a remote ref hands the frames it has encoded."""

    @property
    def peer(self) -> Address:
        """The address on the other end."""
        ...

    def send(self, message: Message, frame: bytes, recipient: ActorPath) -> None:
        """Queue a frame, or account for why it cannot be queued."""
        ...

    async def offer(self, message: Message, frame: bytes, recipient: ActorPath) -> None:
        """Queue a frame, waiting for room in the outbound buffer."""
        ...


class RemoteRef(ActorRef[T]):
    """A ref to an actor on a peer system, reached through one association."""

    __slots__ = ("_max_frame_bytes", "_outbox", "_validate")

    def __init__(
        self,
        path: ActorPath,
        *,
        outbox: Outbox,
        validate: MessageValidator,
        max_frame_bytes: int,
    ) -> None:
        """Bind a ref to a path on a peer, and to the link that reaches it.

        Args:
            path: Where the actor sits in the peer's tree, uid included.
            outbox: The association to that peer.
            validate: The sender-side check, resolved from what the caller
                declared the peer accepts.
            max_frame_bytes: The size limit this system enforces on a frame.
        """
        super().__init__(path)
        self._outbox = outbox
        self._validate = validate
        self._max_frame_bytes = max_frame_bytes

    @property
    def address(self) -> Address:
        """The peer's canonical address, which is what this ref writes down."""
        return self._outbox.peer

    def tell(self, message: T) -> None:
        """Send a message to the peer, without waiting and without blocking.

        The type check here is against what the *caller* declared the peer
        accepts, which is a claim about the peer rather than knowledge of it.
        The authoritative check runs on the receiving node against the target
        actor's real message type, and a mismatch there dead-letters on that
        node: the sender's declaration and the receiver's protocol are two
        independently deployed pieces of code.

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

        Local backpressure against a socket that is not draining, and not
        end-to-end backpressure from the receiving actor: a fire-and-forget
        wire protocol has nothing to offer the latter with, and pretending
        otherwise would be the kind of transparency that lies.

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

    async def ask(
        self,
        make: "Callable[[ActorRef[R]], T]",
        *,
        expect: type[R],
        timeout: timedelta | None = None,  # noqa: ASYNC109 - the ask deadline
    ) -> R:
        """Not yet: asking across a link waits on remote lifecycle.

        An ask fails fast when its target stops, which across a link means a
        remote death watch, and it has a failure mode a local ask does not: a
        peer that becomes unreachable mid-flight is a different diagnosis from
        one that answered slowly. Shipping the timeout half alone would give
        callers an ask that waits out its full deadline for an answer that
        provably is not coming.

        Args:
            make: Builds the request from the ref the reply should go to.
            expect: The reply type.
            timeout: How long to wait.

        Raises:
            NotImplementedError: Always. Send a message carrying a `reply_to`
                in the meantime, which is what an ask is sugar over.
        """
        msg = (
            f"cannot ask {self.path} across an association yet; send a message "
            "with a reply_to field, which is what ask is built on"
        )
        raise NotImplementedError(msg)

    def _encode(self, message: T) -> bytes:
        """Turn a message into the frame that carries it.

        Raises:
            MessageEncodingError: If the type has no wire key, or a field has
                no JSON representation.
            FrameTooLargeError: If the frame is over the size limit.
        """
        try:
            return encode(message, to=self.path, max_frame_bytes=self._max_frame_bytes)
        except MessageRegistrationError as error:
            raise MessageEncodingError(str(error)) from error
        except PydanticSerializationError as error:
            msg = (
                f"{type(message).__name__} to {self.path} has no JSON "
                f"representation, so it cannot cross a link: {error}"
            )
            raise MessageEncodingError(msg) from error

    def __repr__(self) -> str:
        """Render the full string form, since where this points is the point."""
        return f"RemoteRef({format_ref(self.address, self.path)!r})"
