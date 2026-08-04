"""`ActorRef`: a local handle to an actor, usable as a Pydantic field."""

from collections.abc import Callable
from datetime import timedelta
from typing import Any, Generic, TypeVar

from pydantic import GetCoreSchemaHandler
from pydantic_core import core_schema

from tapio.actor.path import ActorPath
from tapio.errors import ActorRefDeserializationError
from tapio.message import Message

__all__ = ["ActorRef"]

T = TypeVar("T", bound=Message)
R = TypeVar("R", bound=Message)


class ActorRef(Generic[T]):
    """A handle for sending messages to one actor.

    Every `ActorRef` is local today, and a ref cannot be resolved from a
    string: see the round-trip note below.

    A ref stays a valid handle after its actor dies, so sending to it never
    raises on account of the target being gone; the message goes to dead
    letters instead. A point-in-time "is it alive?" answer is stale the moment
    you have it, which is why death watch, not a predicate, is the supported
    way to ask.

    The type parameter is static only. Nothing checks it at runtime, so
    `ActorRef[Greeted]` and `ActorRef[Foo]` validate identically, because
    generics are erased. A type checker catches the mismatch at the call site,
    and the runtime check that catches it otherwise lives on the receiving
    actor, keyed by its declared message type.

    Using one as a Pydantic field:

    * Validation is an is-instance check and nothing else. It deliberately does
      not check that the target is still alive: that is a race, since the
      target can die between the check and the send, and a dead target is not a
      schema error.
    * Serialization is the actor path string, which is a debugging and logging
      affordance today and the basis of the wire format once remoting lands.
    * Deserialization is not supported. A model containing an `ActorRef`
      therefore does *not* round-trip: `model_dump()` succeeds, and feeding its
      output back to `model_validate()` raises
      [ActorRefDeserializationError][tapio.errors.ActorRefDeserializationError].
      This reads as a bug if you meet it unwarned, so it is stated plainly here.
    """

    __slots__ = ("_path",)

    def __init__(self, path: ActorPath) -> None:
        """Bind a ref to an actor path."""
        self._path = path

    @property
    def path(self) -> ActorPath:
        """Where this ref points."""
        return self._path

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

        Backpressure belongs to the mailbox rather than to the send, so on an
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
    """Accept a live ref, and reject a path string with a pointed error."""
    if isinstance(value, ActorRef):
        return value
    if isinstance(value, str):
        msg = (
            f"cannot rebuild an ActorRef from {value!r}: resolving a path back "
            "to a live ref needs a registry that tapio does not have yet. "
            "Models containing an ActorRef serialize for logging and "
            "debugging, but do not round-trip. Remoting is the feature that "
            "will make this work, since a ref has to cross a wire before it "
            "has to come back from one; until then, pass the ref itself."
        )
        raise ActorRefDeserializationError(msg)
    # A ValueError here is the right shape: Pydantic folds it into the
    # enclosing ValidationError alongside any other field errors.
    msg = f"expected an ActorRef, got {type(value).__name__}"
    raise ValueError(msg)


def _serialize_ref(ref: ActorRef[Any]) -> str:
    """Serialize a ref to its path string."""
    return str(ref.path)
