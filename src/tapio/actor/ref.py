"""`ActorRef`: a local handle to an actor, usable as a Pydantic field."""

from collections.abc import Callable
from datetime import timedelta
from typing import Any, Generic, TypeVar

from pydantic import GetCoreSchemaHandler
from pydantic_core import core_schema

from tapio.actor.path import ActorPath
from tapio.message import Message
from tapio.remote.address import Address, format_ref
from tapio.remote.context import resolve_ref

__all__ = ["ActorRef"]

T = TypeVar("T", bound=Message)
R = TypeVar("R", bound=Message)


class ActorRef(Generic[T]):
    """A handle for sending messages to one actor.

    A ref stays a valid handle after its actor dies. Sending to it never
    raises because the target is gone: the message becomes a dead letter
    instead. An "is it alive?" answer would be out of date as soon as you had
    it, which is why death watch is the supported way to ask.

    The type parameter is static only. Generics are erased, so nothing checks
    it at runtime and `ActorRef[Greeted]` and `ActorRef[Foo]` validate the
    same. A type checker catches the mismatch at the call site. At runtime the
    check lives on the receiving actor, against its declared message type.

    Using one as a Pydantic field:

    * Validation of a live ref is an is-instance check and nothing more. It
      does not check that the target is still alive. That would be a race,
      since the target can die between the check and the send, and a dead
      target is not a schema error.
    * Serialization gives the ref's full string form, including the address
      and the incarnation uid, which is what a peer needs to reply to it.
    * Validation of that string resolves it against the system reading it. So
      `model_dump()` works anywhere, while `model_validate()` on the result
      works only inside a system's decode path or an explicit
      `with system.as_deserialization_context():` block. A ref is a handle
      into a live runtime, and there is no meaningful ref without one.
    """

    __slots__ = ("_path",)

    def __init__(self, path: ActorPath) -> None:
        """Bind a ref to an actor path."""
        self._path = path

    @property
    def path(self) -> ActorPath:
        """Where this ref points."""
        return self._path

    @property
    def address(self) -> Address:
        """The address this ref writes itself down with.

        On this base class it is the system name and nothing else, which is
        what a ref from a system with remoting switched off looks like. A peer
        reading it can tell which system it names, and that there is nowhere
        to dial. The refs a running system hands out override this with the
        canonical address that system advertises.
        """
        return Address(system=self._path.system)

    def tell(self, message: T) -> None:
        """Send a message, without waiting and without blocking.

        Args:
            message: The message to deliver.

        Raises:
            NotImplementedError: Always, on this base class. Delivery belongs
                to the concrete refs a running actor system hands out.
        """
        raise NotImplementedError(self._undeliverable())

    async def offer(self, message: T) -> None:
        """Send a message, waiting for the recipient's mailbox to have room.

        Backpressure belongs to the mailbox, not to the send, so on an
        unbounded mailbox this is `tell` with an `await` in front of it.

        Args:
            message: The message to deliver.

        Raises:
            NotImplementedError: Always, on this base class. Delivery belongs
                to the concrete refs a running actor system hands out.
        """
        raise NotImplementedError(self._undeliverable())

    async def ask(
        self,
        make: "Callable[[ActorRef[R]], T]",
        *,
        expect: type[R],
        timeout: timedelta | None = None,  # noqa: ASYNC109 - the ask deadline
    ) -> R:
        """Send one message and await one reply.

        Args:
            make: Builds the request from the ref the reply should go to.
            expect: The reply type, which is required.
            timeout: How long to wait. The system's `ask_timeout` when omitted.

        Returns:
            The reply.

        Raises:
            NotImplementedError: Always, on this base class. Delivery belongs
                to the concrete refs a running actor system hands out.
        """
        raise NotImplementedError(self._undeliverable())

    def _undeliverable(self) -> str:
        """Explain that this ref is not attached to a running actor."""
        return (
            f"{type(self).__name__} cannot deliver messages; a ref obtained "
            "from a running actor system can"
        )

    def __eq__(self, other: object) -> bool:
        """Refs are equal when they address the same incarnation."""
        if not isinstance(other, ActorRef):
            return NotImplemented
        return self._path == other._path

    def __hash__(self) -> int:
        """Hash by path, so refs work as dict keys and set members."""
        return hash(self._path)

    def __repr__(self) -> str:
        """Render as the class name and the path string."""
        return f"{type(self).__name__}({str(self._path)!r})"

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: GetCoreSchemaHandler,
    ) -> core_schema.CoreSchema:
        """Make `ActorRef` a legal Pydantic field type.

        The schema ignores the type parameter, as documented on the class.
        """
        return core_schema.no_info_plain_validator_function(
            _validate_ref,
            serialization=core_schema.plain_serializer_function_ser_schema(
                _serialize_ref,
                return_schema=core_schema.str_schema(),
                when_used="always",
            ),
        )


def _validate_ref(value: object) -> ActorRef[Any]:
    """Accept a live ref, and resolve a string against the reading system."""
    if isinstance(value, ActorRef):
        return value
    if isinstance(value, str):
        return resolve_ref(value)
    # A ValueError is what Pydantic wants here. It folds one into the
    # enclosing ValidationError alongside any other field errors.
    msg = f"expected an ActorRef, got {type(value).__name__}"
    raise ValueError(msg)


def _serialize_ref(ref: ActorRef[Any]) -> str:
    """Serialize a ref to its full string form, address and uid included."""
    return format_ref(ref.address, ref.path)
