"""Message adapters: taking delivery of a protocol you do not own.

An actor's declared message type is a contract, and the delivery-time check
keeps it honest. That creates a problem as soon as two actors written by
different people talk: the service you called replies with *its* reply type,
which is not in your protocol. Widening your own type to admit it is wrong
twice over. It lets anyone send you that message, and it puts a foreign
vocabulary inside your handlers.

An adapter is a ref you hand out instead. It accepts the other protocol's
message, translates it into one of yours, and delivers the result onto your
own user lane. A translated message is therefore ordinary traffic: it queues
where it arrived, it cannot re-enter a running handler, and it is validated
against your declared type.

The translation runs in the owning actor, not in the sender, which is why the
wrapper below exists instead of applying the function at the send site. The
function is the owner's code, so a failure in it is the owner's failure and
becomes a supervision decision, as if the message had been mistranslated
inside a handler. A sender that knows nothing about the adapter must not have
the owner's bug raised into it.
"""

from collections.abc import Callable
from typing import TYPE_CHECKING, Annotated, Any, TypeAlias, TypeVar, cast

from pydantic import PlainSerializer, PlainValidator

from tapio.actor.dead_letters import Carrier
from tapio.actor.path import ActorPath
from tapio.actor.ref import ActorRef
from tapio.message import Message
from tapio.remote.address import Address
from tapio.validation import MessageValidator

if TYPE_CHECKING:
    from tapio.actor.cell import ActorCell

__all__ = ["AdaptedMessage", "AdapterRef"]

T = TypeVar("T", bound=Message)
U = TypeVar("U", bound=Message)

Adapt: TypeAlias = Callable[[Any], Message]
"""Translates one protocol's message into the owning actor's own."""


def _carry_adapt(value: object) -> Adapt:
    """Keep a translation function as itself.

    Pydantic has no schema for a callable, and this one is not data. It is the
    owner's code travelling with the message so the cell can apply it at the
    right moment. Validation is a callable check, and the object is passed
    through untouched.
    """
    if callable(value):
        return cast(Adapt, value)
    msg = f"a message adapter carries a callable, not {type(value).__name__}"
    raise ValueError(msg)


def _describe_adapt(adapt: Adapt) -> str:
    """Render a translation function by name, for the rare dump."""
    return getattr(adapt, "__qualname__", None) or repr(adapt)


AdaptFunction: TypeAlias = Annotated[
    Adapt,
    PlainValidator(_carry_adapt),
    PlainSerializer(_describe_adapt, return_type=str, when_used="always"),
]
"""The translation an adapted message carries, kept as the function it is.

A function has no data representation, so a dump renders its name instead.
Nothing in the runtime dumps one. This exists so that a `model_dump` on a
wrapper caught mid-flight while debugging prints something instead of raising.
"""


class AdaptedMessage(Carrier):
    """One message on its way through an adapter, not yet translated.

    Internal and short-lived. It exists between the adapter ref that accepted
    a foreign message and the cell that unwraps it on the way into the
    behavior. User code never sees one. A dead letter reports the payload
    rather than this wrapper, because the wrapper is only how the message
    travelled.
    """

    adapt: AdaptFunction
    """What turns it into one of the owner's messages."""

    def translate(self) -> Message:
        """Apply the translation, in the actor that owns it.

        Returns:
            The owner's own message.
        """
        return self.adapt(self.payload)

    def __repr__(self) -> str:
        """Render the payload and the function it is waiting for."""
        return f"AdaptedMessage({self.payload!r}, {_describe_adapt(self.adapt)})"


class AdapterRef(ActorRef[U]):
    """A ref that translates what it is told and delivers it to one actor.

    Handed out by
    [ActorContext.message_adapter][tapio.actor.context.ActorContext.message_adapter].
    It behaves like any other ref. It never blocks, it is safe to use from any
    thread, and it stays a valid handle after its actor dies. What it cannot
    deliver becomes a dead letter reporting the message the sender sent.

    It is not an actor. It has no mailbox, no cell and no children, so it
    cannot be watched or asked. Watch the actor that owns it instead.
    """

    __slots__ = ("_adapt", "_cell", "_validate")

    def __init__(
        self,
        *,
        cell: "ActorCell[Any]",
        path: ActorPath,
        adapt: Adapt,
        validate: MessageValidator,
    ) -> None:
        """Bind an adapter to the actor it delivers into.

        Args:
            cell: The owning actor.
            path: Where this adapter is addressed, under its owner.
            adapt: Translates an accepted message into the owner's own.
            validate: Checks an arriving message against the type this adapter
                declares it accepts.
        """
        super().__init__(path)
        self._cell = cell
        self._adapt = adapt
        self._validate = validate

    @property
    def address(self) -> Address:
        """The canonical address of the system the owning actor runs in.

        An adapter is addressable like the actor behind it. Without this it
        would write itself down with no host, and a peer handed one in a
        `reply_to` would read it as a ref with nowhere to dial.
        """
        return self._cell.runtime.address

    def tell(self, message: U) -> None:
        """Accept a message, to be translated and delivered to the owner.

        The split is the same as any other send. An error about the message
        raises here, on the calling thread, because the sender wrote it.
        Errors about the recipient become dead letters, and so does a
        translation the owner never gets to run.

        The translation does not happen here. It is the owner's code, so it
        runs in the owner. A failure in it is then the owner's supervision
        decision, not an exception in a caller that has never heard of this
        adapter.

        Args:
            message: The message to translate and deliver.

        Raises:
            MessageTypeError: If it does not match the type this adapter
                accepts.
            MailboxFullError: If the owner's mailbox is full under
                `OverflowStrategy.FAIL` and the caller is on the system's loop.
            pydantic.ValidationError: If content validation is on and the
                message does not satisfy its own model.
        """
        self._validate(message)
        envelope = AdaptedMessage(payload=message, adapt=self._adapt)
        cell = self._cell
        dispatcher = cell.runtime.dispatcher
        if dispatcher.is_current():
            cell.deliver(envelope)
            return
        try:
            dispatcher.call_soon_threadsafe(cell.deliver_offloop, envelope)
        except RuntimeError:
            cell.log.warning(
                "dead letter: %s sent to an adapter after the loop closed",
                type(message).__name__,
            )

    async def offer(self, message: U) -> None:
        """Accept a message, waiting for the owner's mailbox to have room.

        Args:
            message: The message to translate and deliver.

        Raises:
            MessageTypeError: If it does not match the type this adapter
                accepts.
            RuntimeError: If called off the system's loop, as for any `offer`.
            pydantic.ValidationError: If content validation is on and the
                message does not satisfy its own model.
        """
        self._validate(message)
        cell = self._cell
        if not cell.runtime.dispatcher.is_current():
            msg = (
                f"offer to the adapter at {self.path} must run on the system's "
                "loop; `tell` is the thread-safe send"
            )
            raise RuntimeError(msg)
        await cell.offer(AdaptedMessage(payload=message, adapt=self._adapt))

    def __repr__(self) -> str:
        """Render the adapter, its owner, and what it translates with."""
        return f"AdapterRef({str(self.path)!r}, adapt={_describe_adapt(self._adapt)})"
