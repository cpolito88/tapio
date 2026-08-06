"""The wire format: a length prefix, a JSON object, and no imports.

A frame is a 4-byte big-endian length followed by a JSON object:

```json
{"v": 1, "to": "/user/checkout/session-7#3",
 "from": "tapio://web@10.0.0.9:25520/user/cart#11",
 "t": "orders.protocol.Reserve",
 "p": {"sku": "X-1", "qty": 2,
       "reply_to": "tapio://web@10.0.0.9:25520/user/cart#11"}}
```

`to` carries no address, because a frame arriving on a link is addressed to
the system that received it. `from` is complete, because the receiver may need
to reply to a system it has not talked to yet.

`t` is a registry key and never an import path, for the reason
[tapio.remote.registry][] gives. The length prefix is checked before the body
is read, so an oversized frame costs a header and a refusal instead of the
memory the peer asked for.

Encoding is `model_dump_json`. Decoding is `model_validate_json` inside the
reading system's deserialization context, so the contents check that
`validate_on_tell` governs locally has already happened: a message off the
wire is validated by construction, strictly, with no way to skip it.
"""

import json
from dataclasses import dataclass
from typing import Any, Final, final

from pydantic import ValidationError

from tapio.actor.dead_letters import DeadLetterOffice, DeadLetterReason
from tapio.actor.path import ActorPath
from tapio.actor.ref import ActorRef
from tapio.errors import (
    FrameTooLargeError,
    MailboxFullError,
    MessageDecodingError,
    MessageTypeError,
    RefResolutionError,
)
from tapio.message import Message
from tapio.remote.address import Address, format_ref
from tapio.remote.context import DeserializationContext, use_context
from tapio.remote.registry import registered_key, type_for_key

__all__ = [
    "FRAME_VERSION",
    "LENGTH_PREFIX",
    "Frame",
    "UndecodableFrame",
    "decode",
    "encode",
    "format_target",
    "frame_length",
    "parse_target",
    "receive_frame",
]

FRAME_VERSION: Final = 1
"""The wire format version, carried in every frame as `v`."""

LENGTH_PREFIX: Final = 4
"""Bytes of big-endian length in front of every frame body."""


@final
@dataclass(frozen=True, slots=True)
class Frame:
    """One decoded frame, before its payload has become a message.

    The payload stays as JSON text at this stage on purpose. Whether it can be
    built depends on the type key, and a frame naming a type this system has
    never heard of has to be reportable without building anything.
    """

    version: int
    """The sender's wire format version."""

    to: ActorPath
    """The recipient, in the receiving system's own path space."""

    sender: str | None
    """The full string form of the sending ref, or `None` if it sent anonymously."""

    key: str
    """The payload's registry key."""

    payload: str
    """The payload, as the JSON text it arrived as."""


class UndecodableFrame(Message):
    """A frame that never became a message, so a dead letter can still report it.

    Every other dead letter carries the message that was not delivered. A
    frame refused for its size, its version or an unknown type key has no
    message to carry. Reporting nothing would leave the failures a peer can
    cause as the only ones you cannot see.
    """

    type_key: str | None = None
    """The registry key the frame named, when the frame parsed far enough."""

    sender: str | None = None
    """The sending ref's string form, when the frame parsed far enough."""

    size: int = 0
    """How many bytes arrived, which for a frame refused on its declared length
    is not the size it claimed. The claim is in the dead letter's detail."""


def encode(
    message: Message,
    *,
    to: ActorPath,
    sender: ActorRef[Any] | None = None,
    max_frame_bytes: int | None = None,
) -> bytes:
    """Write a message and its addressing into a length-prefixed frame.

    Args:
        message: The message to send.
        to: The recipient's path, including its incarnation uid.
        sender: The ref a reply would go to, written out in full.
        max_frame_bytes: Refuse a frame larger than this, if given.

    Returns:
        The complete frame, length prefix included.

    Raises:
        MessageRegistrationError: If the message's type has no wire key.
        FrameTooLargeError: If the encoded frame exceeds `max_frame_bytes`.
    """
    key = registered_key(type(message))
    header = {
        "v": FRAME_VERSION,
        "to": format_target(to),
        "from": format_ref(sender.address, sender.path) if sender else None,
        "t": key,
    }
    # Spliced together rather than built as one dict and dumped. The payload
    # is already JSON, and parsing it just to serialize it again would double
    # the cost of every send.
    prefix = json.dumps(header, separators=(",", ":"))[:-1]
    body = (prefix + ',"p":' + message.model_dump_json() + "}").encode()
    if max_frame_bytes is not None and len(body) > max_frame_bytes:
        msg = (
            f"{type(message).__name__} to {to} encodes to {len(body)} bytes, "
            f"over the {max_frame_bytes} byte frame limit"
        )
        raise FrameTooLargeError(msg)
    return len(body).to_bytes(LENGTH_PREFIX, "big") + body


def frame_length(prefix: bytes, *, max_frame_bytes: int | None = None) -> int:
    """Read a frame's declared body length, and refuse an oversized one.

    Called with the length prefix alone, before the body is read, so a peer
    announcing a gigabyte costs this check instead of a gigabyte.

    Args:
        prefix: Exactly `LENGTH_PREFIX` bytes.
        max_frame_bytes: The limit, if there is one.

    Returns:
        The body length in bytes.

    Raises:
        MessageDecodingError: If the prefix is not `LENGTH_PREFIX` bytes long.
        FrameTooLargeError: If the declared length exceeds `max_frame_bytes`.
    """
    if len(prefix) != LENGTH_PREFIX:
        msg = f"a frame length prefix is {LENGTH_PREFIX} bytes, got {len(prefix)}"
        raise MessageDecodingError(msg)
    length = int.from_bytes(prefix, "big")
    if max_frame_bytes is not None and length > max_frame_bytes:
        msg = (
            f"frame declares {length} bytes, over the {max_frame_bytes} byte "
            "limit; refused without reading the body"
        )
        raise FrameTooLargeError(msg)
    return length


def decode(data: bytes, *, system: str, max_frame_bytes: int | None = None) -> Frame:
    """Read one complete frame, without building its payload.

    Args:
        data: The frame, length prefix included.
        system: The reading system's name, which the recipient path takes.
        max_frame_bytes: Refuse a frame declaring more than this, if given.

    Returns:
        The decoded frame.

    Raises:
        MessageDecodingError: If the frame is truncated, is not JSON, is not of
            a version this system speaks, or is missing a field.
        FrameTooLargeError: If the declared body length exceeds the limit.
    """
    length = frame_length(data[:LENGTH_PREFIX], max_frame_bytes=max_frame_bytes)
    body = data[LENGTH_PREFIX : LENGTH_PREFIX + length]
    if len(body) != length:
        msg = f"frame declares {length} bytes and carries {len(body)}"
        raise MessageDecodingError(msg)
    try:
        parsed = json.loads(body)
    except ValueError as error:
        raise MessageDecodingError(f"frame body is not JSON: {error}") from error
    if not isinstance(parsed, dict):
        msg = f"a frame body is a JSON object, got {type(parsed).__name__}"
        raise MessageDecodingError(msg)
    version = parsed.get("v")
    if version != FRAME_VERSION:
        msg = (
            f"frame speaks wire version {version!r}, this system speaks {FRAME_VERSION}"
        )
        raise MessageDecodingError(msg)
    try:
        to = parse_target(system, parsed["to"])
        key = parsed["t"]
        payload = parsed["p"]
    except (KeyError, TypeError, ValueError) as error:
        raise MessageDecodingError(f"malformed frame: {error}") from error
    if not isinstance(key, str):
        msg = f"a frame's type key is a string, got {type(key).__name__}"
        raise MessageDecodingError(msg)
    sender = parsed.get("from")
    return Frame(
        version=version,
        to=to,
        sender=sender if isinstance(sender, str) else None,
        key=key,
        payload=json.dumps(payload),
    )


def receive_frame(
    data: bytes,
    *,
    context: DeserializationContext,
    dead_letters: DeadLetterOffice,
    max_frame_bytes: int | None = None,
    peer: Address | None = None,
) -> None:
    """Take a frame off a link and deliver what is in it, or account for it.

    This is the receiving half of remoting, and it never raises. Everything a
    peer can get wrong becomes a dead letter on this system's stream: a bad
    size, a bad version, an unknown type key, a payload that will not
    validate, a recipient that has stopped, and a message the recipient does
    not accept. Dead letters are not sent back. A link that just failed to
    deliver a message is not a link to report failures over, and a report over
    a working link would arrive long after the sender stopped caring.

    Args:
        data: One complete frame, length prefix included.
        context: The system reading it, which resolves the refs inside.
        dead_letters: Where anything undeliverable is accounted for.
        max_frame_bytes: The size limit this system enforces, if any.
        peer: The address the frame arrived from, recorded on any dead letter
            so a subscriber can tell a missing actor from a missing node.
    """
    try:
        frame = decode(
            data, system=context.address.system, max_frame_bytes=max_frame_bytes
        )
    except FrameTooLargeError as error:
        _refuse(
            dead_letters,
            context,
            peer,
            DeadLetterReason.FRAME_TOO_LARGE,
            str(error),
            size=len(data),
        )
        return
    except MessageDecodingError as error:
        _refuse(
            dead_letters,
            context,
            peer,
            DeadLetterReason.MALFORMED_FRAME,
            str(error),
            size=len(data),
        )
        return

    msg_type = type_for_key(frame.key)
    if msg_type is None:
        _refuse(
            dead_letters,
            context,
            peer,
            DeadLetterReason.UNKNOWN_MESSAGE_TYPE,
            f"no message type is registered under {frame.key!r}",
            recipient=frame.to,
            type_key=frame.key,
            sender=frame.sender,
            size=len(data),
        )
        return

    try:
        with use_context(context):
            message = msg_type.model_validate_json(frame.payload)
    except (ValidationError, RefResolutionError) as error:
        _refuse(
            dead_letters,
            context,
            peer,
            DeadLetterReason.MALFORMED_FRAME,
            f"payload for {frame.key!r} did not validate: {error}",
            recipient=frame.to,
            type_key=frame.key,
            sender=frame.sender,
            size=len(data),
        )
        return

    recipient = context.resolve_path(context.address, frame.to)
    try:
        recipient.tell(message)
    except MessageTypeError as error:
        # The check that decides. The sender's declaration and the receiving
        # actor's real protocol are deployed separately, so only this one can
        # be trusted.
        dead_letters.publish(
            message,
            frame.to,
            DeadLetterReason.WRONG_MESSAGE_TYPE,
            peer=peer,
            detail=str(error),
        )
    except ValidationError as error:
        dead_letters.publish(
            message,
            frame.to,
            DeadLetterReason.MALFORMED_FRAME,
            peer=peer,
            detail=str(error),
        )
    except MailboxFullError as error:
        # A local sender would get this raised at its call site and could
        # retry or drop the message. There is no local sender here. The one
        # who sent this is across a link and has moved on.
        dead_letters.publish(
            message,
            frame.to,
            DeadLetterReason.MAILBOX_FULL,
            peer=peer,
            detail=str(error),
        )


def _refuse(
    dead_letters: DeadLetterOffice,
    context: DeserializationContext,
    peer: Address | None,
    reason: str,
    detail: str,
    *,
    recipient: ActorPath | None = None,
    type_key: str | None = None,
    sender: str | None = None,
    size: int = 0,
) -> None:
    """Account for a frame that never became a message."""
    dead_letters.publish(
        UndecodableFrame(type_key=type_key, sender=sender, size=size),
        recipient if recipient is not None else ActorPath.root(context.address.system),
        reason,
        peer=peer,
        detail=detail,
    )


def format_target(path: ActorPath) -> str:
    """Write a path the way a frame carries it, with no address.

    A frame arriving on a link is addressed to the system that received it, so
    the address would say nothing. The uid still travels, because it is what
    tells one incarnation of a path from the next.

    Args:
        path: The path to write.

    Returns:
        `/user/checkout/session-7#3`, or without the fragment when there is no
        incarnation uid.
    """
    body = "/".join(path.elements)
    fragment = f"#{path.uid}" if path.uid else ""
    return f"/{body}{fragment}"


def parse_target(system: str, text: object) -> ActorPath:
    """Read a path a frame carried back into a path in a named system.

    Args:
        system: Whose path space the text belongs to. That is the reading
            system for a recipient, and the sending one for a path that came
            back after crossing in the other direction, as a watch does.
        text: The path as the frame carried it.

    Returns:
        The path.

    Raises:
        ValueError: If the text is not a path with an optional uid fragment.
    """
    if not isinstance(text, str) or not text.startswith("/"):
        msg = f"a frame's recipient is an absolute path, got {text!r}"
        raise ValueError(msg)
    body, _, fragment = text.partition("#")
    elements = tuple(element for element in body.split("/") if element)
    return ActorPath(system=system, elements=elements, uid=int(fragment or 0))
