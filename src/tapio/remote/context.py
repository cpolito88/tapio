"""The ambient system a ref deserializes against.

Turning `tapio://orders@10.0.0.4:25520/user/checkout#3` back into a working ref
takes something a Pydantic validator has no way to reach on its own: the system
doing the reading. It has to know whether that address is its own, so it can
hand back the live local ref rather than a proxy to itself, and it has to own
the association a foreign address resolves through.

So the receiving end sets an ambient context for the duration of a decode, and
the ref validator reads it. Outside one there is no honest answer, and a
`RefResolutionError` says so: a ref is a handle into a live runtime, and there
is no meaningful ref outside of one.
"""

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from tapio.errors import RefResolutionError
from tapio.remote.address import Address, parse_ref

if TYPE_CHECKING:
    from tapio.actor.path import ActorPath
    from tapio.actor.ref import ActorRef

__all__ = [
    "DeserializationContext",
    "current_context",
    "resolve_ref",
    "use_context",
]


@runtime_checkable
class DeserializationContext(Protocol):
    """What a ref needs from the system that is reading it."""

    @property
    def address(self) -> Address:
        """The reading system's own canonical address."""
        ...

    def resolve(self, address: Address, path: "ActorPath") -> "ActorRef[Any]":
        """Turn an address and a path into a ref that can be told things.

        Never raises about the target: an actor that has stopped, an
        incarnation that has been replaced and a peer with no link to it all
        resolve to something whose `tell` produces a dead letter.
        """
        ...


_CONTEXT: ContextVar[DeserializationContext | None] = ContextVar(
    "tapio_deserialization_context", default=None
)


@contextmanager
def use_context(context: DeserializationContext) -> Iterator[None]:
    """Make `context` the system refs deserialize against inside the block.

    Args:
        context: The system doing the reading.

    Yields:
        Nothing. The context is ambient for the duration of the block.
    """
    token = _CONTEXT.set(context)
    try:
        yield
    finally:
        _CONTEXT.reset(token)


def current_context() -> DeserializationContext | None:
    """Return the system refs are deserializing against, if there is one."""
    return _CONTEXT.get()


def resolve_ref(text: str) -> "ActorRef[Any]":
    """Turn a ref's string form into a live ref, against the ambient system.

    Args:
        text: The full string form of a ref.

    Returns:
        A ref for the reading system's own actor when the address is its own,
        and a ref through the association for that address otherwise.

    Raises:
        RefResolutionError: If the text is not a ref string, or if no system is
            in scope to resolve it against.
    """
    context = _CONTEXT.get()
    if context is None:
        msg = (
            f"cannot resolve {text!r} without a system: a ref is a handle into "
            "a live runtime, so it deserializes only inside a system's decode "
            "path or an explicit `with system.as_deserialization_context():` "
            "block"
        )
        raise RefResolutionError(msg)
    try:
        address, path = parse_ref(text)
    except ValueError as error:
        raise RefResolutionError(str(error)) from error
    return context.resolve(address, path)
