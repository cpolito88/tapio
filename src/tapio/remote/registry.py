"""Two registries: message types by key, and live refs by path.

**Message types.** A frame names its payload's type with a key, and that key is
looked up in a dict. It is never an import path. Resolving a dotted name that
arrived on a socket into an importable object is remote code execution, and it
is how this goes wrong in libraries that treat the wire's type name as a
Python name. An unregistered key is a dead letter naming the key, and nothing
is imported to find out what it might have meant.

**Live refs.** A path and an incarnation uid look up the ref to deliver into.
Cells register when they start and deregister in their termination sequence, so
the registry holds exactly the live actors: a uid that no longer matches
resolves to nothing rather than to whoever occupies that path now. That
symmetry makes registry cleanliness part of the same invariant as leaked tasks,
and a system that has terminated leaves an empty one behind.
"""

from collections.abc import Callable
from typing import Any, TypeVar, cast

from tapio.actor.path import ActorPath
from tapio.actor.ref import ActorRef
from tapio.errors import MessageRegistrationError
from tapio.message import Message

__all__ = [
    "RefRegistry",
    "key_for_type",
    "register_message",
    "registered_key",
    "type_for_key",
]

M = TypeVar("M", bound=type[Message])

_BY_KEY: dict[str, type[Message]] = {}
_BY_TYPE: dict[type[Message], str] = {}


def register_message(key: str | None = None) -> Callable[[M], M]:
    """Register a message type under the key that names it on the wire.

    The default key is `module.qualname`, so the decorator usually takes no
    argument. The explicit form is what lets a class be renamed or moved
    without breaking a peer still running the previous version.

    ```python
    @register_message()
    class Reserve(Message): ...

    @register_message("orders.protocol.Reserve")
    class ReserveV2(Message): ...
    ```

    Args:
        key: The wire key. `module.qualname` when omitted.

    Returns:
        The decorator, which returns the class unchanged.

    Raises:
        MessageRegistrationError: If the key is already taken, at import time
            rather than letting the later class quietly win.
    """

    def decorate(cls: M) -> M:
        wire_key = key if key is not None else f"{cls.__module__}.{cls.__qualname__}"
        # Checked at runtime as well as statically: the decorator is applied
        # by user code that a type checker may never have seen, and a registry
        # entry that is not a Message would fail on the far side of a wire.
        if not issubclass(cast(type, cls), Message):
            msg = (
                f"cannot register {cls!r} under {wire_key!r}: only tapio.Message "
                "subclasses cross the wire, since only they are frozen and "
                "re-validated on delivery"
            )
            raise MessageRegistrationError(msg)
        taken = _BY_KEY.get(wire_key)
        if taken is not None and taken is not cls:
            msg = (
                f"cannot register {cls.__module__}.{cls.__qualname__} under "
                f"{wire_key!r}: {taken.__module__}.{taken.__qualname__} already "
                "has that key. Two classes sharing a key would decode as "
                "whichever imported last, so pass an explicit key to one of them"
            )
            raise MessageRegistrationError(msg)
        _BY_KEY[wire_key] = cls
        _BY_TYPE[cls] = wire_key
        return cls

    return decorate


def type_for_key(key: str) -> type[Message] | None:
    """Return the message type registered under a wire key, if any."""
    return _BY_KEY.get(key)


def key_for_type(msg_type: type[Message]) -> str | None:
    """Return the wire key a message type was registered under, if any."""
    return _BY_TYPE.get(msg_type)


def registered_key(msg_type: type[Message]) -> str:
    """Return a message type's wire key, or say how to give it one.

    Args:
        msg_type: The type about to be written to a frame.

    Returns:
        The key it was registered under.

    Raises:
        MessageRegistrationError: If it was never registered.
    """
    key = _BY_TYPE.get(msg_type)
    if key is None:
        msg = (
            f"{msg_type.__module__}.{msg_type.__qualname__} has no wire key: "
            "decorate it with @register_message() so a peer can name it. A key "
            "is never an import path, so an unregistered type cannot be "
            "reconstructed from the wire"
        )
        raise MessageRegistrationError(msg)
    return key


class RefRegistry:
    """The live refs of one system, by path and incarnation uid.

    Not process-wide, unlike the message-type registry: two systems in one
    process share nothing, so each keeps its own.
    """

    __slots__ = ("_refs",)

    def __init__(self) -> None:
        """Create an empty registry."""
        self._refs: dict[ActorPath, ActorRef[Any]] = {}

    def register(self, ref: ActorRef[Any]) -> None:
        """Record a ref as the live occupant of its path and uid."""
        self._refs[ref.path] = ref

    def deregister(self, path: ActorPath) -> None:
        """Forget a path, whether or not anything was registered under it."""
        self._refs.pop(path, None)

    def lookup(self, path: ActorPath) -> ActorRef[Any] | None:
        """Return the live ref at a path and uid, or `None`.

        `None` covers both halves of the incarnation rule: nothing was ever
        there, or what was there has stopped and its uid will never be minted
        again.
        """
        return self._refs.get(path)

    def paths(self) -> tuple[ActorPath, ...]:
        """Every path currently registered, which is what a leak test reads."""
        return tuple(self._refs)

    def __len__(self) -> int:
        """How many live refs are registered."""
        return len(self._refs)

    def __repr__(self) -> str:
        """Render the size, which is what a reader wants to see."""
        return f"RefRegistry({len(self._refs)} refs)"
