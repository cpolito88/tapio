"""Delivery-time message validation.

"Every message is validated" means three separate things:

1. Construction: ordinary Pydantic `__init__` validation. It already happens
   and costs nothing extra.
2. On send, the type: an `isinstance` against the recipient's declared message
   type. Always on, because it is what keeps a mailbox's type contract honest
   when step 3 is switched off.
3. On send, the contents: a full re-validation whose result is discarded.
   Controlled by the `validate_on_tell` setting.

Discarding the result in step 3 is deliberate. With
`revalidate_instances="always"` the call returns a new instance, and the
mailbox must receive the original object. Validation is a pure check, so
message identity never depends on a setting: `validate_on_tell` changes the
cost and nothing else.

Both checks are resolved once, into a single bound function, so no call site
branches on a setting.
"""

import functools
import operator
import types
import typing
from collections.abc import Callable
from typing import Any, TypeAlias

from pydantic import BaseModel, TypeAdapter

from tapio.actor.path import ActorPath
from tapio.errors import MessageTypeError
from tapio.message import Message
from tapio.settings import TapioSettings

__all__ = ["MessageType", "MessageValidator", "normalize_msg_type", "resolve_validator"]

MessageType: TypeAlias = type[Message] | types.UnionType
"""A declared message type: one `Message` subclass, or a union of them."""

MessageValidator: TypeAlias = Callable[[Any], None]
"""Checks one message, raising on rejection and returning nothing on success."""


def normalize_msg_type(msg_type: object, *, origin: str) -> MessageType:
    """Check a declared message type and put it in a form `isinstance` accepts.

    `typing.Union[A, B]` is rewritten to `A | B`. They mean the same type, but
    only the second works as an argument to `isinstance`.

    Args:
        msg_type: The declared type to check.
        origin: What declared it, named in the error message.

    Returns:
        The normalized type.

    Raises:
        MessageTypeError: If it is not a `Message` subclass or a union of them.
    """
    if typing.get_origin(msg_type) is typing.Union:
        msg_type = functools.reduce(operator.or_, typing.get_args(msg_type))

    if isinstance(msg_type, types.UnionType):
        members = typing.get_args(msg_type)
    elif isinstance(msg_type, type):
        members = (msg_type,)
    else:
        msg = (
            f"{origin} declares an unusable message type: {msg_type!r} is "
            "neither a class nor a union of classes"
        )
        raise MessageTypeError(msg)

    for member in members:
        if isinstance(member, type) and issubclass(member, Message):
            continue
        _reject_member(member, origin=origin)

    return typing.cast(MessageType, msg_type)


def _reject_member(member: object, *, origin: str) -> None:
    """Explain why a declared type member cannot be used, then raise."""
    if isinstance(member, type) and issubclass(member, BaseModel):
        # This is what the check prevents. Re-validating a plain BaseModel
        # instance returns it untouched, so the delivery-time guarantee would
        # be a silent no-op.
        msg = (
            f"{origin} declares {member.__name__}, which subclasses BaseModel "
            "rather than tapio.Message. Pydantic defaults revalidate_instances "
            "to 'never', so re-validating it on delivery would check nothing at "
            f"all. Subclass tapio.Message instead: "
            f"class {member.__name__}(Message): ..."
        )
    else:
        name = getattr(member, "__name__", repr(member))
        msg = (
            f"{origin} declares {name}, which is not a tapio.Message subclass. "
            "Messages must subclass tapio.Message so they are frozen and "
            "re-validated on delivery."
        )
    raise MessageTypeError(msg)


def resolve_validator(
    *,
    msg_type: MessageType,
    settings: TapioSettings,
    target: ActorPath | None = None,
) -> MessageValidator:
    """Build the validation function for one recipient.

    Args:
        msg_type: The recipient's declared message type, already normalized.
        settings: The system settings. `validate_on_tell` selects the variant.
        target: The recipient's path, named in error messages when known.

    Returns:
        A function that raises [MessageTypeError][tapio.errors.MessageTypeError]
        on a type mismatch and `pydantic.ValidationError` on malformed contents.
    """
    where = f" sent to {target}" if target is not None else ""
    expected = _describe(msg_type)

    def check_type(message: Any) -> None:
        if not isinstance(message, msg_type):
            msg = (
                f"{type(message).__name__}{where} does not match the declared "
                f"message type {expected}"
            )
            raise MessageTypeError(msg)

    if not settings.validate_on_tell:
        return check_type

    # Built once. Building a TypeAdapter per send would cost far more than the
    # validation itself.
    adapter: TypeAdapter[Any] = TypeAdapter(msg_type)

    def check_type_and_contents(message: Any) -> None:
        check_type(message)
        # Strict, and the result is dropped on purpose. The recipient gets the
        # original object either way. See the module docstring.
        adapter.validate_python(message, strict=True)

    return check_type_and_contents


def _describe(msg_type: MessageType) -> str:
    """Render a declared message type the way a user would have written it."""
    if isinstance(msg_type, types.UnionType):
        return " | ".join(m.__name__ for m in typing.get_args(msg_type))
    return msg_type.__name__
