"""Two registries: message types by key, and live refs by path.

**Message types.** A frame names its payload's type with a key, and that key
is looked up in a dict. It is never an import path. Turning a dotted name that
arrived on a socket into an importable object is remote code execution. An
unregistered key becomes a dead letter naming the key, and nothing is imported
to find out what it might have meant.

That table is append-only for the life of the process, on purpose. There is no
deregister, no override and no reset, and a duplicate key raises at import
rather than winning. A key is a promise about the wire: it has to mean the same
type on both ends of a link, and on the node that reads a frame written before
a restart. A type that could be swapped out under a key already in use turns a
decoding error into a silently wrong object, which is exactly what naming types
by key rather than by import path exists to prevent. Anything that wants to
retire a key retires it by never sending it again.

**Live refs.** A path and an incarnation uid look up the ref to deliver into.
Cells register when they start and deregister when they stop, so the registry
holds exactly the live actors. A uid that no longer matches resolves to
nothing rather than to whoever holds that path now. A system that has
terminated leaves an empty registry behind, which the tests check.

**Well-known names.** An actor may also ask to be reachable by its bare path,
with no uid. That is the opposite of the guarantee above, so it is opt-in and
it exists for one situation: a peer that has to address something before it
can know any uid. Bootstrapping a cluster is that situation, since a seed node
is named by an address in a configuration file and nothing else. A ref that
was written down always carries its uid, so nothing becomes bare by accident,
and asking for a well-known name is a decision an actor makes about itself.
The alias is dropped when its actor stops, in the same call that deregisters
the ref, so the registry stays exactly as empty as it was before.
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
    argument. Use the explicit form to rename or move a class without breaking
    a peer still running the previous version.

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
        MessageRegistrationError: If the key is already taken. It is raised at
            import time, rather than letting the later class win silently.
    """

    def decorate(cls: M) -> M:
        wire_key = key if key is not None else f"{cls.__module__}.{cls.__qualname__}"
        # Checked at runtime as well as statically. The decorator is applied
        # by user code a type checker may never have seen, and an entry that
        # is not a Message would fail on the far side of a link.
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

    Unlike the message-type registry, this is not process-wide. Two systems in
    one process share nothing, so each keeps its own.
    """

    __slots__ = ("_named", "_refs")

    def __init__(self) -> None:
        """Create an empty registry."""
        self._refs: dict[ActorPath, ActorRef[Any]] = {}
        self._named: dict[ActorPath, ActorRef[Any]] = {}

    def register(self, ref: ActorRef[Any]) -> None:
        """Record a ref as the live occupant of its path and uid."""
        self._refs[ref.path] = ref

    def register_well_known(self, ref: ActorRef[Any]) -> None:
        """Also reach this actor by its bare path, whatever its incarnation.

        For an actor a peer has to address before it can know any uid, which
        in practice means a bootstrap endpoint named in configuration. It is
        deliberately narrow: only actors that ask for it are reachable this
        way, and a ref written down elsewhere still carries its uid and still
        addresses one incarnation only.

        Args:
            ref: The actor to publish. The name is its own path without the
                uid, so an actor cannot claim somebody else's.
        """
        self._named[ref.path.with_uid(0)] = ref

    def deregister(self, path: ActorPath) -> None:
        """Forget a path, whether or not anything was registered under it.

        Any well-known name this actor held goes with it, so an alias cannot
        outlive the actor it names or survive into the next incarnation.
        """
        self._refs.pop(path, None)
        name = path.with_uid(0)
        named = self._named.get(name)
        if named is not None and named.path == path:
            del self._named[name]

    def lookup(self, path: ActorPath) -> ActorRef[Any] | None:
        """Return the live ref at a path and uid, or `None`.

        `None` covers both cases: nothing was ever there, or what was there
        has stopped and its uid will never be used again. A path with no uid
        finds only an actor that published itself as a well-known name.
        """
        found = self._refs.get(path)
        if found is not None or path.uid:
            return found
        return self._named.get(path)

    def paths(self) -> tuple[ActorPath, ...]:
        """Every path currently registered, which is what a leak test reads.

        Refs only. A well-known alias is a second key onto a ref that is
        already here, so counting it would report one actor twice. Read
        [names][tapio.remote.registry.RefRegistry.names] for the aliases: a
        leak check wants both, because they are cleared by the same call and
        a bug in it would leave one of them behind.
        """
        return tuple(self._refs)

    def names(self) -> tuple[ActorPath, ...]:
        """Every well-known alias currently published, without its uid.

        The companion to `paths`, and exposed for the same reason. An alias
        that outlived its actor would hand a peer the next occupant of that
        path, which is the whole thing the incarnation uid exists to prevent,
        so a test has to be able to see that the alias went with the actor.
        """
        return tuple(self._named)

    def __len__(self) -> int:
        """How many live refs are registered, aliases not counted twice."""
        return len(self._refs)

    def __repr__(self) -> str:
        """Render the size, and the aliases when there are any."""
        if not self._named:
            return f"RefRegistry({len(self._refs)} refs)"
        return f"RefRegistry({len(self._refs)} refs, {len(self._named)} well-known)"
