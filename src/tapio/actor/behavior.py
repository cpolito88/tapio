"""Behaviors: what an actor does with the next message, and what it becomes.

Two styles sit over the same runtime, functional (`Behaviors.receive`) and
class-based ([AbstractBehavior][tapio.actor.behavior.AbstractBehavior]). Both
produce a `Behavior`.

Every behavior that handles messages carries its message type as *data*, in
`msg_type`. This is the one place Python's type erasure costs something a
compiled actor library gets for free: `Behavior[T]`'s parameter does not
survive to runtime, so the delivery-time type check has to be re-derived from
something that does.

Lifecycle signals arrive through a second, optional handler. Most behaviors
never declare one, which is why it is a keyword argument rather than a second
required function: an actor that does not care when a watched peer stops
should not have to say so.
"""

import enum
import inspect
import typing
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from tapio.actor.context import ActorContext
from tapio.actor.signals import Signal
from tapio.actor.supervision import SupervisorStrategy
from tapio.errors import BehaviorTypeError
from tapio.message import Message
from tapio.validation import MessageType, normalize_msg_type

if TYPE_CHECKING:  # both modules import this one, so importing them back at
    from tapio.actor.stash import StashBuffer  # runtime would be a cycle
    from tapio.actor.timers import TimerScheduler

__all__ = [
    "AbstractBehavior",
    "Behavior",
    "Behaviors",
    "Directive",
    "ReceivingBehavior",
    "SetupBehavior",
    "SignalHandler",
    "Supervise",
    "SuperviseBehavior",
    "WithStashBehavior",
    "WithTimersBehavior",
    "directive_of",
    "resolve_handler_msg_type",
]

T = TypeVar("T", bound=Message)

SignalHandler: typing.TypeAlias = Callable[
    [ActorContext[T], Signal], Awaitable["Behavior[T]"]
]
"""What a behavior runs when a lifecycle signal arrives."""


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


class Directive(enum.Enum):
    """What a behavior that carries no handler asks the runtime to do.

    These are returned as behaviors, `Behaviors.same()` and friends, so a
    handler has one return type. The runtime reads the directive back off the
    sentinel with [directive_of][tapio.actor.behavior.directive_of] rather than
    comparing against private module constants.
    """

    SAME = "same"
    """Keep the current behavior and its state."""

    STOPPED = "stopped"
    """Stop this actor."""

    EMPTY = "empty"
    """Handle no user message; signals still arrive."""

    IGNORE = "ignore"
    """Consume every message and do nothing."""

    UNHANDLED = "unhandled"
    """Report the message as unhandled, keeping the current behavior."""


class _Sentinel(Behavior[Any]):
    """A behavior with no handler, interpreted by the runtime by identity."""

    def __init__(self, directive: Directive) -> None:
        """Bind the sentinel to the directive it carries."""
        self.directive = directive

    def __repr__(self) -> str:
        """Render as the factory call that produces it."""
        return f"Behaviors.{self.directive.value}()"


_SAME = _Sentinel(Directive.SAME)
_STOPPED = _Sentinel(Directive.STOPPED)
_EMPTY = _Sentinel(Directive.EMPTY)
_IGNORE = _Sentinel(Directive.IGNORE)
_UNHANDLED = _Sentinel(Directive.UNHANDLED)


def directive_of(behavior: Behavior[Any]) -> Directive | None:
    """Return the directive a behavior carries, or `None` if it handles messages."""
    if isinstance(behavior, _Sentinel):
        return behavior.directive
    return None


class ReceivingBehavior(Behavior[T], ABC):
    """A behavior that handles messages, and possibly signals."""

    @abstractmethod
    async def receive(self, ctx: ActorContext[T], message: T) -> Behavior[T]:
        """Handle one message and return what the actor does next."""

    async def receive_signal(self, ctx: ActorContext[T], signal: Signal) -> Behavior[T]:
        """Handle one lifecycle signal and return what the actor does next.

        The default reports the signal as unhandled, which is not a failure: a
        behavior that never watches anything has nothing to say about a
        `Terminated`, and `PostStop` matters only to an actor holding a
        resource.

        Args:
            ctx: This actor's context.
            signal: The signal that arrived.

        Returns:
            What the actor does next.
        """
        return typing.cast(Behavior[T], _UNHANDLED)


class _ReceiveBehavior(ReceivingBehavior[T]):
    """Wraps a two-argument `(ctx, message)` handler."""

    def __init__(
        self,
        on_message: Callable[[ActorContext[T], T], Awaitable[Behavior[T]]],
        msg_type: MessageType,
        on_signal: SignalHandler[T] | None = None,
    ) -> None:
        """Bind the handlers and the message type they declare."""
        self._on_message = on_message
        self._on_signal = on_signal
        self.msg_type = msg_type

    async def receive(self, ctx: ActorContext[T], message: T) -> Behavior[T]:
        """Delegate to the wrapped handler."""
        return await self._on_message(ctx, message)

    async def receive_signal(self, ctx: ActorContext[T], signal: Signal) -> Behavior[T]:
        """Delegate to the signal handler, if this behavior declared one."""
        if self._on_signal is None:
            return typing.cast(Behavior[T], _UNHANDLED)
        return await self._on_signal(ctx, signal)

    def __repr__(self) -> str:
        """Name the wrapped handler, which is what identifies this behavior."""
        return f"Behaviors.receive({_name_of(self._on_message)})"


class _ReceiveMessageBehavior(ReceivingBehavior[T]):
    """Wraps a one-argument `(message)` handler, for actors that ignore ctx."""

    def __init__(
        self,
        on_message: Callable[[T], Awaitable[Behavior[T]]],
        msg_type: MessageType,
        on_signal: SignalHandler[T] | None = None,
    ) -> None:
        """Bind the handlers and the message type they declare."""
        self._on_message = on_message
        self._on_signal = on_signal
        self.msg_type = msg_type

    async def receive(self, ctx: ActorContext[T], message: T) -> Behavior[T]:
        """Delegate to the wrapped handler, dropping the context."""
        return await self._on_message(message)

    async def receive_signal(self, ctx: ActorContext[T], signal: Signal) -> Behavior[T]:
        """Delegate to the signal handler, if this behavior declared one.

        The signal handler takes the context even here, where the message
        handler does not: an actor reacting to a `Terminated` almost always
        wants to spawn a replacement or log against its own path.
        """
        if self._on_signal is None:
            return typing.cast(Behavior[T], _UNHANDLED)
        return await self._on_signal(ctx, signal)

    def __repr__(self) -> str:
        """Name the wrapped handler, which is what identifies this behavior."""
        return f"Behaviors.receive_message({_name_of(self._on_message)})"


class SuperviseBehavior(Behavior[T]):
    """A behavior wrapped in one supervision strategy.

    Produced by [Behaviors.supervise][tapio.actor.behavior.Behaviors.supervise]
    and unwrapped by the cell when the actor starts, so it never reaches a
    message handler. Wrappers nest, and the outermost is consulted first.
    """

    def __init__(
        self,
        behavior: Behavior[T],
        strategy: SupervisorStrategy,
        on: type[Exception] | tuple[type[Exception], ...],
    ) -> None:
        """Bind a behavior to the strategy that governs its failures."""
        self._behavior = behavior
        self.strategy = strategy
        self.on = on
        # Whatever the wrapped behavior declares, including None when it is a
        # `setup` whose type is only known once it has run.
        self.msg_type = behavior.msg_type

    @property
    def behavior(self) -> Behavior[T]:
        """The behavior this strategy governs."""
        return self._behavior

    def __repr__(self) -> str:
        """Render as the pair of calls that produces it."""
        return (
            f"Behaviors.supervise({self._behavior!r})"
            f".on_failure({self.strategy!r}, on={_name_of_type(self.on)})"
        )


class Supervise(Generic[T]):
    """The half-built result of `Behaviors.supervise`, awaiting a strategy.

    Two calls rather than one because the failures being governed and the
    decision taken about them are separate choices, and reading them apart is
    what makes a nested supervision stack legible.
    """

    __slots__ = ("_behavior",)

    def __init__(self, behavior: Behavior[T]) -> None:
        """Bind the behavior whose failures are about to be governed."""
        self._behavior = behavior

    def on_failure(
        self,
        strategy: SupervisorStrategy,
        *,
        on: type[Exception] | tuple[type[Exception], ...] = Exception,
    ) -> Behavior[T]:
        """Apply a strategy to the failures this actor raises.

        Args:
            strategy: What to do when a matching failure happens.
            on: Which exceptions it governs. Anything else falls through to the
                next wrapper out, and to `stop` if none matches.

        Returns:
            The supervised behavior, to spawn or to wrap again.
        """
        return SuperviseBehavior(self._behavior, strategy, on)

    def __repr__(self) -> str:
        """Render the behavior still waiting for its strategy."""
        return f"Behaviors.supervise({self._behavior!r})"


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


class WithTimersBehavior(Behavior[T]):
    """Deferred construction that also hands over a timer scheduler.

    Like `setup`, and for the same reason: the scheduler belongs to the cell,
    so it cannot exist until there is one. A restart re-runs the factory
    against the same scheduler, whose timers the cell has just cancelled.
    """

    def __init__(self, factory: "Callable[[TimerScheduler[T]], Behavior[T]]") -> None:
        """Bind the factory to run when the actor starts."""
        self._factory = factory

    def with_timers(self, timers: "TimerScheduler[T]") -> Behavior[T]:
        """Produce the actual behavior for a starting actor."""
        return self._factory(timers)

    def __repr__(self) -> str:
        """Name the wrapped factory."""
        return f"Behaviors.with_timers({_name_of(self._factory)})"


class WithStashBehavior(Behavior[T]):
    """Deferred construction that also hands over a stash buffer.

    The capacity is declared here rather than on the buffer the factory
    receives, because the cell owns the buffer across incarnations and a
    restart must not be able to quietly resize it.
    """

    def __init__(
        self, capacity: int, factory: "Callable[[StashBuffer[T]], Behavior[T]]"
    ) -> None:
        """Bind the factory and the capacity of the buffer it will be given."""
        self._factory = factory
        self.capacity = capacity

    def with_stash(self, stash: "StashBuffer[T]") -> Behavior[T]:
        """Produce the actual behavior for a starting actor."""
        return self._factory(stash)

    def __repr__(self) -> str:
        """Name the capacity and the wrapped factory."""
        return f"Behaviors.with_stash({self.capacity}, {_name_of(self._factory)})"


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

    async def on_signal(self, signal: Signal) -> Behavior[T]:
        """Handle one lifecycle signal, if this actor cares about any.

        Override to react to `PostStop`, `PreRestart` or a `Terminated` from a
        watched actor. The default reports the signal as unhandled, which is
        not a failure.

        Args:
            signal: The signal that arrived.

        Returns:
            What the actor does next.
        """
        return typing.cast(Behavior[T], _UNHANDLED)

    async def receive(self, ctx: ActorContext[T], message: T) -> Behavior[T]:
        """Delegate to `on_message`, since the context is already held."""
        return await self.on_message(message)

    async def receive_signal(self, ctx: ActorContext[T], signal: Signal) -> Behavior[T]:
        """Delegate to `on_signal`, since the context is already held."""
        return await self.on_signal(signal)

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


def _name_of_type(on: type[Exception] | tuple[type[Exception], ...]) -> str:
    """Render one exception type, or a tuple of them, as it was written."""
    if isinstance(on, tuple):
        return f"({', '.join(exc.__name__ for exc in on)})"
    return on.__name__


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
        *,
        on_signal: SignalHandler[T] | None = None,
    ) -> Behavior[T]:
        """Handle messages with a `(ctx, message)` function.

        Args:
            on_message: The handler.
            msg_type: What this behavior receives. Read from the handler's
                annotation when omitted.
            on_signal: Called with `(ctx, signal)` for lifecycle signals.
                Without one, signals are reported as unhandled.

        Returns:
            The behavior.
        """
        resolved = resolve_handler_msg_type(
            on_message, explicit=msg_type, message_param_index=1
        )
        return _ReceiveBehavior(on_message, resolved, on_signal)

    @staticmethod
    def receive_message(
        on_message: Callable[[T], Awaitable[Behavior[T]]],
        msg_type: MessageType | None = None,
        *,
        on_signal: SignalHandler[T] | None = None,
    ) -> Behavior[T]:
        """Handle messages with a `(message)` function, ignoring the context.

        Args:
            on_message: The handler.
            msg_type: What this behavior receives. Read from the handler's
                annotation when omitted.
            on_signal: Called with `(ctx, signal)` for lifecycle signals.
                Without one, signals are reported as unhandled.

        Returns:
            The behavior.
        """
        resolved = resolve_handler_msg_type(
            on_message, explicit=msg_type, message_param_index=0
        )
        return _ReceiveMessageBehavior(on_message, resolved, on_signal)

    @staticmethod
    def supervise(behavior: Behavior[T]) -> Supervise[T]:
        """Govern a behavior's failures with a strategy.

        ```python
        Behaviors.supervise(worker()).on_failure(
            SupervisorStrategy.restart(max_restarts=3, window=timedelta(seconds=1)),
            on=ConnectionError,
        )
        ```

        Wrappers nest, and the outermost is consulted first, so a specific
        exception governed by an inner wrapper must be wrapped again outside it
        to win. A failure matching nothing stops the actor, which is what an
        unsupervised actor already does.

        Supervision belongs to the actor rather than to the behavior it
        currently holds: switching behavior keeps the strategies, and a restart
        reinstates the ones the actor was spawned with.

        Args:
            behavior: What the actor does.

        Returns:
            A builder whose `on_failure` produces the supervised behavior.
        """
        return Supervise(behavior)

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
    def with_timers(
        factory: "Callable[[TimerScheduler[T]], Behavior[T]]",
    ) -> Behavior[T]:
        """Defer construction, handing the behavior a scheduler for its timers.

        ```python
        Behaviors.with_timers(
            lambda timers: poller(timers, every=timedelta(seconds=30))
        )
        ```

        A timer sends the actor a message on its own user lane, so a tick is
        ordinary traffic: it queues behind whatever is already there and never
        re-enters a busy handler.

        The scheduler belongs to the cell, and the cell cancels every timer it
        holds when the actor restarts or stops. A tick from an incarnation that
        has gone away therefore cannot arrive at the one that replaced it, and
        the factory below runs again on restart to schedule what the new
        incarnation needs.

        Args:
            factory: Called with the scheduler to produce the real behavior.

        Returns:
            The behavior.
        """
        return WithTimersBehavior(factory)

    @staticmethod
    def with_stash(
        capacity: int,
        factory: "Callable[[StashBuffer[T]], Behavior[T]]",
    ) -> Behavior[T]:
        """Defer construction, handing the behavior a buffer to hold messages in.

        ```python
        Behaviors.with_stash(100, lambda stash: loading(stash))
        ```

        For an actor that cannot answer yet: put what arrives aside, and
        `return stash.unstash_all(ready_behavior)` once it can. The held
        messages go back to the front of the mailbox, ahead of anything that
        queued up since, and the buffer is left empty.

        The capacity is required. A stash holds traffic the actor is by
        definition not keeping up with, so an unbounded one is a memory leak
        with an excuse; overflow raises `StashOverflowError` in the actor that
        stashed, where the decision about what to do belongs.

        A restart empties the buffer, since messages held by the state that
        just failed are not the new state's to answer, and what is discarded is
        published as a dead letter rather than dropped.

        Args:
            capacity: How many messages the buffer can hold.
            factory: Called with the buffer to produce the real behavior.

        Returns:
            The behavior.
        """
        return WithStashBehavior(capacity, factory)

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
