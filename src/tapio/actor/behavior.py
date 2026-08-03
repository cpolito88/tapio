"""Behaviors: what an actor does with the next message, and what it becomes.

Two styles sit over the same runtime, functional (`Behaviors.receive`) and
class-based ([AbstractBehavior][tapio.actor.behavior.AbstractBehavior]). Both
produce a `Behavior`.

Every behavior that handles messages carries its message type as *data*, in
`msg_type`. This is the one place Python's type erasure costs something a
compiled actor library gets for free: `Behavior[T]`'s parameter does not
survive to runtime, so the delivery-time type check has to be re-derived from
something that does.
"""

import inspect
import typing
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from typing import Any, Generic, TypeVar

from tapio.actor.context import ActorContext
from tapio.errors import BehaviorTypeError
from tapio.message import Message
from tapio.validation import MessageType, normalize_msg_type

__all__ = [
    "AbstractBehavior",
    "Behavior",
    "Behaviors",
    "ReceivingBehavior",
    "SetupBehavior",
    "resolve_handler_msg_type",
]

T = TypeVar("T", bound=Message)


class Behavior(ABC, Generic[T]):
    """What an actor does next.

    A handler returns one of these: itself (`same`), a new behavior, or a
    terminal one (`stopped`).
    """

    msg_type: MessageType | None = None
    """The declared message type, or `None` for behaviors that carry none.

    `same()`, `stopped()` and friends carry no type: they resolve against the
    type the actor already has, not independently. `setup` is `None` too, since
    its type is whatever the behavior it produces declares.
    """


class _Sentinel(Behavior[Any]):
    """A behavior with no handler, interpreted by the runtime by identity."""

    def __init__(self, name: str) -> None:
        """Name the sentinel, for its repr."""
        self._name = name

    def __repr__(self) -> str:
        """Render as the factory call that produces it."""
        return f"Behaviors.{self._name}()"


_SAME = _Sentinel("same")
_STOPPED = _Sentinel("stopped")
_EMPTY = _Sentinel("empty")
_IGNORE = _Sentinel("ignore")
_UNHANDLED = _Sentinel("unhandled")


class ReceivingBehavior(Behavior[T], ABC):
    """A behavior that handles messages."""

    @abstractmethod
    async def receive(self, ctx: ActorContext[T], message: T) -> Behavior[T]:
        """Handle one message and return what the actor does next."""


class _ReceiveBehavior(ReceivingBehavior[T]):
    """Wraps a two-argument `(ctx, message)` handler."""

    def __init__(
        self,
        on_message: Callable[[ActorContext[T], T], Awaitable[Behavior[T]]],
        msg_type: MessageType,
    ) -> None:
        """Bind the handler and the message type it declares."""
        self._on_message = on_message
        self.msg_type = msg_type

    async def receive(self, ctx: ActorContext[T], message: T) -> Behavior[T]:
        """Delegate to the wrapped handler."""
        return await self._on_message(ctx, message)

    def __repr__(self) -> str:
        """Name the wrapped handler, which is what identifies this behavior."""
        return f"Behaviors.receive({_name_of(self._on_message)})"


class _ReceiveMessageBehavior(ReceivingBehavior[T]):
    """Wraps a one-argument `(message)` handler, for actors that ignore ctx."""

    def __init__(
        self,
        on_message: Callable[[T], Awaitable[Behavior[T]]],
        msg_type: MessageType,
    ) -> None:
        """Bind the handler and the message type it declares."""
        self._on_message = on_message
        self.msg_type = msg_type

    async def receive(self, ctx: ActorContext[T], message: T) -> Behavior[T]:
        """Delegate to the wrapped handler, dropping the context."""
        return await self._on_message(message)

    def __repr__(self) -> str:
        """Name the wrapped handler, which is what identifies this behavior."""
        return f"Behaviors.receive_message({_name_of(self._on_message)})"


class SetupBehavior(Behavior[T]):
    """Deferred construction: the factory runs when the actor starts.

    This is also what a restart re-runs, which is why children spawned in
    `setup` come back after one and children spawned in a message handler do
    not.
    """

    def __init__(self, factory: Callable[[ActorContext[T]], Behavior[T]]) -> None:
        """Bind the factory to run when the actor starts."""
        self._factory = factory

    def setup(self, ctx: ActorContext[T]) -> Behavior[T]:
        """Produce the actual behavior for a starting actor."""
        return self._factory(ctx)

    def __repr__(self) -> str:
        """Name the wrapped factory."""
        return f"Behaviors.setup({_name_of(self._factory)})"


class AbstractBehavior(ReceivingBehavior[T], ABC):
    """Base class for the class-based style, for actors that hold state.

    The message type is read from the type parameter when the class is created:

    ```python
    class Counter(AbstractBehavior[Increment | GetCount]):
        def __init__(self, ctx: ActorContext[Increment | GetCount]) -> None:
            super().__init__(ctx)
            self._count = 0
    ```

    Set `msg_type` as a class attribute to override that, which is needed when
    the parameter is a string forward reference and so cannot be resolved at
    class creation. Either way an unresolvable type raises
    [BehaviorTypeError][tapio.errors.BehaviorTypeError] at class definition
    rather than at spawn.

    An abstract subclass, one that leaves `on_message` abstract, is exempt, so
    intermediate bases in a hierarchy need no type of their own.
    """

    def __init__(self, ctx: ActorContext[T]) -> None:
        """Bind the behavior to its context."""
        self._ctx = ctx

    @property
    def ctx(self) -> ActorContext[T]:
        """The context this behavior was constructed with."""
        return self._ctx

    @abstractmethod
    async def on_message(self, message: T) -> Behavior[T]:
        """Handle one message and return what the actor does next."""

    async def receive(self, ctx: ActorContext[T], message: T) -> Behavior[T]:
        """Delegate to `on_message`, since the context is already held."""
        return await self.on_message(message)

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Resolve and freeze the subclass's message type."""
        super().__init_subclass__(**kwargs)

        declared = cls.__dict__.get("msg_type")
        if declared is None:
            declared = _msg_type_from_type_parameter(cls)
        if declared is None:
            if getattr(cls, "msg_type", None) is not None:
                return  # an ancestor resolved one already; inherit it as-is
            if getattr(cls.on_message, "__isabstractmethod__", False):
                return  # still abstract: nothing to spawn, nothing to check
            msg = (
                f"cannot resolve the message type of {cls.__name__}: subclass "
                "AbstractBehavior[YourMessage] with a concrete type, or set a "
                "'msg_type' class attribute. A behavior with no resolvable "
                "message type is never spawned, because skipping the "
                "delivery-time type check is the failure mode that check exists "
                "to prevent."
            )
            raise BehaviorTypeError(msg)

        cls.msg_type = normalize_msg_type(declared, origin=cls.__name__)


def _msg_type_from_type_parameter(cls: type) -> object | None:
    """Read `AbstractBehavior[X]`'s `X` off a subclass, if it is resolvable."""
    for base in getattr(cls, "__orig_bases__", ()):
        origin = typing.get_origin(base)
        if not (isinstance(origin, type) and issubclass(origin, Behavior)):
            continue
        args = typing.get_args(base)
        if not args:
            continue
        candidate = args[0]
        # A TypeVar means a still-generic intermediate class. A ForwardRef or a
        # bare string means the name was not resolvable here. Both fall through
        # to the explicit-override path rather than being guessed at.
        if isinstance(candidate, TypeVar | str | typing.ForwardRef):
            continue
        return typing.cast(object, candidate)
    return None


def resolve_handler_msg_type(
    handler: Callable[..., Any],
    *,
    explicit: object | None,
    message_param_index: int,
) -> MessageType:
    """Work out which message type a handler declares.

    Explicit wins, always: when `msg_type` is passed it is used verbatim, and
    that is the documented form. Otherwise the handler's message parameter
    annotation is read. Failure is loud, never a silent fallback to an
    unchecked delivery path.

    Args:
        handler: The function whose annotation to read.
        explicit: A `msg_type` passed by the caller, or `None`.
        message_param_index: Which positional parameter carries the message.

    Returns:
        The normalized message type.

    Raises:
        BehaviorTypeError: If no type could be resolved.
    """
    name = _name_of(handler)
    if explicit is not None:
        return normalize_msg_type(explicit, origin=name)

    try:
        hints = typing.get_type_hints(handler)
    except Exception as exc:  # every failure to read hints is the same failure
        msg = (
            f"cannot read the annotations of {name}: {exc}. Pass msg_type= "
            "explicitly to say what this behavior receives."
        )
        raise BehaviorTypeError(msg) from exc

    try:
        parameters = [
            p
            for p in inspect.signature(handler).parameters.values()
            if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
        ]
    except (TypeError, ValueError) as exc:
        msg = (
            f"cannot read the signature of {name}: {exc}. Pass msg_type= "
            "explicitly to say what this behavior receives."
        )
        raise BehaviorTypeError(msg) from exc

    if len(parameters) <= message_param_index:
        msg = (
            f"{name} takes {len(parameters)} positional parameter(s), so it has "
            f"no message parameter at position {message_param_index}. A receive "
            "handler takes (ctx, message); receive_message takes (message)."
        )
        raise BehaviorTypeError(msg)

    parameter = parameters[message_param_index]
    annotation = hints.get(parameter.name)
    if annotation is None:
        msg = (
            f"cannot resolve the message type of {name}: its message parameter "
            f"{parameter.name!r} has no annotation. Annotate it, or pass "
            "msg_type= explicitly. A behavior with no resolvable message type "
            "is never spawned, because skipping the delivery-time type check is "
            "the failure mode that check exists to prevent."
        )
        raise BehaviorTypeError(msg)

    return normalize_msg_type(annotation, origin=name)


def _name_of(obj: object) -> str:
    """Best available name for a callable, for error messages and reprs."""
    return getattr(obj, "__qualname__", None) or repr(obj)


class Behaviors:
    """Factories for the functional style.

    A namespace rather than a module of loose functions, so that
    `Behaviors.same()` reads the way it does in the actor libraries this
    borrows from.
    """

    @staticmethod
    def receive(
        on_message: Callable[[ActorContext[T], T], Awaitable[Behavior[T]]],
        msg_type: MessageType | None = None,
    ) -> Behavior[T]:
        """Handle messages with a `(ctx, message)` function.

        Args:
            on_message: The handler.
            msg_type: What this behavior receives. Read from the handler's
                annotation when omitted.

        Returns:
            The behavior.
        """
        resolved = resolve_handler_msg_type(
            on_message, explicit=msg_type, message_param_index=1
        )
        return _ReceiveBehavior(on_message, resolved)

    @staticmethod
    def receive_message(
        on_message: Callable[[T], Awaitable[Behavior[T]]],
        msg_type: MessageType | None = None,
    ) -> Behavior[T]:
        """Handle messages with a `(message)` function, ignoring the context.

        Args:
            on_message: The handler.
            msg_type: What this behavior receives. Read from the handler's
                annotation when omitted.

        Returns:
            The behavior.
        """
        resolved = resolve_handler_msg_type(
            on_message, explicit=msg_type, message_param_index=0
        )
        return _ReceiveMessageBehavior(on_message, resolved)

    @staticmethod
    def setup(factory: Callable[[ActorContext[T]], Behavior[T]]) -> Behavior[T]:
        """Defer construction until the actor starts, and re-run it on restart.

        Args:
            factory: Called with the context to produce the real behavior.

        Returns:
            The behavior.
        """
        return SetupBehavior(factory)

    @staticmethod
    def same() -> Behavior[T]:
        """Keep the current behavior, with whatever state it holds."""
        return typing.cast(Behavior[T], _SAME)

    @staticmethod
    def stopped() -> Behavior[T]:
        """Stop this actor, running its post-stop signal on the way out."""
        return typing.cast(Behavior[T], _STOPPED)

    @staticmethod
    def empty() -> Behavior[T]:
        """Handle nothing: user messages are unhandled, signals still arrive."""
        return typing.cast(Behavior[T], _EMPTY)

    @staticmethod
    def ignore() -> Behavior[T]:
        """Consume every message and do nothing with it."""
        return typing.cast(Behavior[T], _IGNORE)

    @staticmethod
    def unhandled() -> Behavior[T]:
        """Report that this message was not handled, keeping the behavior."""
        return typing.cast(Behavior[T], _UNHANDLED)
