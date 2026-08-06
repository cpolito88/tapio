"""`BehaviorTestKit`: one behavior, no system, no mailbox, no scheduling.

A behavior is a function from a message to the next behavior. Most of what
there is to test about one is that function: what it returns, what it sends,
and what it spawns. None of that needs a running actor system, and testing it
without one makes the test deterministic by construction. There is no loop to
yield to, so there is nothing to wait for and nothing to poll.

```python
kit = BehaviorTestKit(counter(), msg_type=Increment | GetCount)
await kit.run(Increment())
await kit.run(GetCount(reply_to=kit.self_ref))

assert kit.self_inbox == [Count(value=1)]
assert kit.effects == (Spawned("worker"),)
```

`run` is awaited, unlike Pekko's, because tapio's handlers are coroutines. It
is still synchronous in the sense that matters: it runs the handler to
completion and returns, with nothing else running in between.

Two things it deliberately does not do.

**It does not supervise.** A handler that raises raises into the test, where a
unit test wants it. Supervision is about what a cell does with that failure,
so it is tested with a real system and a `TestProbe`.

**It does not deliver.** A ref handed out here records what it was told
instead of enqueuing it. That is what makes the assertions above possible, and
it is why the message a `reply_to` receives is the object that was sent.
"""

from typing import Any, Generic, TypeVar, final

from tapio.actor.behavior import (
    Behavior,
    Directive,
    ReceivingBehavior,
    SetupBehavior,
    SuperviseBehavior,
    WithStashBehavior,
    WithTimersBehavior,
    directive_of,
)
from tapio.actor.context import ActorContext
from tapio.actor.mailbox import MailboxConfig
from tapio.actor.path import ActorPath
from tapio.actor.ref import ActorRef
from tapio.actor.signals import Signal
from tapio.actor.supervision import SupervisorStrategy
from tapio.errors import BehaviorTypeError, TapioError
from tapio.logging import ActorLogAdapter, actor_logger
from tapio.message import Message
from tapio.settings import TapioSettings
from tapio.validation import MessageType, normalize_msg_type, resolve_validator

__all__ = ["BehaviorTestKit", "Effect", "RecordingRef", "Spawned", "Watched"]

T = TypeVar("T", bound=Message)
U = TypeVar("U", bound=Message)
R = TypeVar("R")


class Effect:
    """Something a behavior asked its context to do."""

    __slots__ = ()


@final
class Spawned(Effect):
    """The behavior started a child."""

    __slots__ = ("behavior", "inbox", "mailbox", "name", "ref")

    def __init__(
        self,
        name: str,
        behavior: Behavior[Any],
        mailbox: MailboxConfig | None,
        ref: "RecordingRef[Any]",
    ) -> None:
        """Record one spawn, and the ref that was handed back for it."""
        self.name = name
        """The child's name. Generated names begin with `$`."""
        self.behavior = behavior
        """The behavior it was started with, to assert on or to test in turn."""
        self.mailbox = mailbox
        """The mailbox configuration it was given, or `None` for the default."""
        self.ref = ref
        """The ref the behavior received back."""
        self.inbox = ref.inbox
        """What the behavior has sent to that child so far."""

    def __eq__(self, other: object) -> bool:
        """Equal to another spawn of the same name, and to that name.

        Comparing against the bare name is what makes the common assertion
        read well: `assert kit.effects == ("worker",)`. Comparing the behavior
        objects would test identity of a closure, which says nothing.
        """
        if isinstance(other, Spawned):
            return self.name == other.name
        if isinstance(other, str):
            return self.name == other
        return NotImplemented

    def __hash__(self) -> int:
        """Hash by name, to match equality."""
        return hash(self.name)

    def __repr__(self) -> str:
        """Render the name and what it was started with."""
        return f"Spawned({self.name!r}, {self.behavior!r})"


@final
class Watched(Effect):
    """The behavior started or stopped watching another actor."""

    __slots__ = ("ref", "watching")

    def __init__(self, ref: ActorRef[Any], *, watching: bool) -> None:
        """Record one watch or unwatch."""
        self.ref = ref
        """The actor it was about."""
        self.watching = watching
        """`True` for a watch, `False` for an unwatch."""

    def __eq__(self, other: object) -> bool:
        """Equal when the same actor was watched, or unwatched, either way."""
        if isinstance(other, Watched):
            return self.ref.path == other.ref.path and self.watching == other.watching
        return NotImplemented

    def __hash__(self) -> int:
        """Hash by path and direction, to match equality."""
        return hash((self.ref.path, self.watching))

    def __repr__(self) -> str:
        """Render which way round it was, and about whom."""
        return f"{'Watched' if self.watching else 'Unwatched'}({str(self.ref.path)!r})"


class RecordingRef(ActorRef[T]):
    """A ref that writes messages down instead of delivering them.

    It is what a behavior under test sends to. There is no cell behind it, so
    it cannot be watched and cannot be asked: those need a running system and
    a [TestProbe][tapio.testkit.probe.TestProbe].
    """

    __slots__ = ("inbox",)

    def __init__(self, path: ActorPath) -> None:
        """Create an empty recording ref at a path."""
        super().__init__(path)
        self.inbox: list[T] = []
        """Everything sent to this ref, in order, as the objects sent."""

    def tell(self, message: T) -> None:
        """Record a message.

        Args:
            message: What was sent. It is kept as it is, so an assertion can
                compare by identity as well as by equality.
        """
        self.inbox.append(message)

    async def offer(self, message: T) -> None:
        """Record a message. Nothing here has a mailbox to be full.

        Args:
            message: What was sent.
        """
        self.inbox.append(message)

    def __repr__(self) -> str:
        """Render the path and how much it is holding."""
        return f"RecordingRef({str(self.path)!r}, {len(self.inbox)} message(s))"


class BehaviorTestKit(Generic[T]):
    """One behavior, run by hand, with everything it did written down."""

    __slots__ = (
        "_behavior",
        "_ctx",
        "_settings",
        "_stopped",
        "_supervision",
        "_validate",
    )

    def __init__(
        self,
        behavior: Behavior[T],
        *,
        msg_type: MessageType | None = None,
        name: str = "test",
        system: str = "test",
        settings: TapioSettings | None = None,
    ) -> None:
        """Prepare a behavior for running, evaluating any `setup` at once.

        Args:
            behavior: What to test. `Behaviors.setup(...)` is evaluated here,
                as a cell would evaluate it on spawn.
            msg_type: What it accepts, when the behavior itself cannot say.
                Read from the behavior otherwise.
            name: The actor name this behavior thinks it has.
            system: The system name its path sits in.
            settings: Tunables, for a test about `validate_on_tell`. Read from
                the environment when omitted.

        Raises:
            BehaviorTypeError: If the message type cannot be resolved, exactly
                as a spawn would fail.
            TapioError: If the behavior needs a cell to exist at all, which
                `with_timers` and `with_stash` do.
        """
        self._settings = settings if settings is not None else TapioSettings()
        path = ActorPath.root(system).child("user").child(name, uid=1)
        self._ctx: _KitContext[T] = _KitContext(path)
        self._supervision: list[SupervisorStrategy] = []
        self._stopped = False
        resolved = self._resolve(behavior)
        declared = msg_type if msg_type is not None else resolved.msg_type
        if declared is None:
            msg = (
                f"cannot test {resolved!r}: it carries no message type. Pass "
                "msg_type= to say what it receives, as a spawn would require."
            )
            raise BehaviorTypeError(msg)
        self._validate = resolve_validator(
            msg_type=normalize_msg_type(declared, origin=f"a test kit for {name}"),
            settings=self._settings,
            target=path,
        )
        self._behavior = resolved

    @property
    def self_ref(self) -> "RecordingRef[T]":
        """A ref to the behavior under test, which records rather than delivers.

        Hand it to the code under test as a `reply_to`, then read `self_inbox`.
        """
        return self._ctx.self_recording

    @property
    def self_inbox(self) -> list[T]:
        """What the behavior has sent to itself, in order."""
        return self._ctx.self_recording.inbox

    @property
    def ctx(self) -> ActorContext[T]:
        """The context the behavior is run with."""
        return self._ctx

    @property
    def effects(self) -> tuple[Effect, ...]:
        """Everything the behavior asked its context to do, in order."""
        return tuple(self._ctx.effects)

    @property
    def children(self) -> tuple[Spawned, ...]:
        """Just the spawns, which is what most tests are asking about."""
        return tuple(e for e in self._ctx.effects if isinstance(e, Spawned))

    def child(self, name: str) -> Spawned:
        """Return one spawn by name.

        Args:
            name: The child's name.

        Returns:
            The recorded spawn, including the ref and what has been sent to it.

        Raises:
            AssertionError: If nothing by that name was spawned.
        """
        for effect in self.children:
            if effect.name == name:
                return effect
        spawned = [effect.name for effect in self.children]
        msg = f"no child named {name!r} was spawned; {spawned} were"
        raise AssertionError(msg)

    @property
    def supervision(self) -> tuple[SupervisorStrategy, ...]:
        """The strategies wrapped around this behavior, outermost first.

        The kit does not apply them: a handler that raises raises into the
        test. What can be asserted here is that the behavior declared what it
        meant to declare.
        """
        return tuple(self._supervision)

    @property
    def behavior(self) -> Behavior[T]:
        """What the actor would handle the next message with."""
        return self._behavior

    @property
    def is_stopped(self) -> bool:
        """Whether the behavior has returned `Behaviors.stopped()`."""
        return self._stopped

    async def run(self, message: T) -> Behavior[T]:
        """Handle one message, and become whatever it returned.

        Args:
            message: The message to handle. It is validated against the
                declared message type first, exactly as delivery would.

        Returns:
            What the behavior returned, before it was resolved against the
            current one.

        Raises:
            MessageTypeError: If the message is not of the declared type.
            AssertionError: If the behavior has already stopped.
            Exception: Whatever the handler raised. The kit does not
                supervise, so a failure surfaces in the test.
        """
        self._validate(message)
        behavior = self._receiving("handle a message")
        nxt = await behavior.receive(self._ctx, message)
        self._become(nxt)
        return nxt

    async def signal(self, signal: Signal) -> Behavior[T]:
        """Deliver one lifecycle signal, and become whatever it returned.

        Args:
            signal: The signal to deliver.

        Returns:
            What the behavior returned.

        Raises:
            AssertionError: If the behavior has already stopped.
        """
        behavior = self._receiving("take a signal")
        nxt = await behavior.receive_signal(self._ctx, signal)
        self._become(nxt)
        return nxt

    def _receiving(self, action: str) -> ReceivingBehavior[T]:
        """Return the current behavior, if it is one that can still run."""
        if self._stopped:
            msg = f"the behavior under test has stopped and cannot {action}"
            raise AssertionError(msg)
        if not isinstance(self._behavior, ReceivingBehavior):
            msg = (
                f"the behavior under test is {self._behavior!r}, which handles "
                f"nothing, so it cannot {action}"
            )
            raise AssertionError(msg)
        return self._behavior

    def _become(self, nxt: Behavior[T]) -> None:
        """Apply what a handler returned, the way a cell would."""
        directive = directive_of(nxt)
        if directive is Directive.STOPPED:
            self._stopped = True
            return
        if directive in (Directive.SAME, Directive.UNHANDLED, None):
            if directive is None:
                self._behavior = self._resolve(nxt)
            return
        # `empty` and `ignore` are behaviors in their own right, and a cell
        # keeps them as the current one.
        self._behavior = nxt

    def _resolve(self, behavior: Behavior[T]) -> Behavior[T]:
        """Unwrap supervision and run deferred construction, as a spawn does."""
        current = behavior
        while True:
            if isinstance(current, SuperviseBehavior):
                self._supervision.append(current.strategy)
                current = current.behavior
                continue
            if isinstance(current, SetupBehavior):
                current = current.setup(self._ctx)
                continue
            if isinstance(current, WithTimersBehavior | WithStashBehavior):
                kind = (
                    "timers"
                    if isinstance(current, WithTimersBehavior)
                    else "a stash buffer"
                )
                msg = (
                    f"cannot test {current!r} without a running system: it needs "
                    f"{kind}, which belong to a cell and outlive an incarnation. "
                    "Start an ActorSystem and use a TestProbe for this one."
                )
                raise TapioError(msg)
            return current

    def __repr__(self) -> str:
        """Render the current behavior and whether it is still running."""
        state = "stopped" if self._stopped else repr(self._behavior)
        return f"BehaviorTestKit({state})"


class _KitContext(ActorContext[T]):
    """A context with no cell: it records what it is asked to do."""

    __slots__ = ("_log", "_path", "_recorded", "_self", "effects")

    def __init__(self, path: ActorPath) -> None:
        """Bind the context to the path its behavior thinks it has."""
        self._path = path
        self._log = actor_logger(path)
        self._self: RecordingRef[T] = RecordingRef(path)
        self._recorded = 0
        self.effects: list[Effect] = []

    @property
    def path(self) -> ActorPath:
        """Where this actor would sit in a tree."""
        return self._path

    @property
    def self_ref(self) -> ActorRef[T]:
        """A ref to the behavior under test."""
        return self._self

    @property
    def self_recording(self) -> RecordingRef[T]:
        """The same ref, typed so a test can read its inbox."""
        return self._self

    @property
    def log(self) -> ActorLogAdapter:
        """A logger tagged with the path, as a real one would be."""
        return self._log

    def spawn(
        self,
        behavior: Behavior[U],
        name: str,
        mailbox: MailboxConfig | None = None,
    ) -> ActorRef[U]:
        """Record a spawn and hand back a ref that records what it is told."""
        ref: RecordingRef[U] = RecordingRef(self._path.child(name, uid=1))
        self.effects.append(Spawned(name, behavior, mailbox, ref))
        return ref

    def spawn_anonymous(
        self, behavior: Behavior[U], mailbox: MailboxConfig | None = None
    ) -> ActorRef[U]:
        """Record a spawn under a generated name, as a cell would generate one."""
        self._recorded += 1
        return self.spawn(behavior, f"${self._recorded}", mailbox)

    def message_adapter(
        self, adapt: "Any", msg_type: MessageType | None = None
    ) -> ActorRef[Any]:
        """Hand back a ref that records what the adapter would have translated.

        The translation is not run. What an adapter does with a message is the
        adapting function's business, and it is an ordinary function a test can
        call directly.
        """
        self._recorded += 1
        return RecordingRef[Any](self._path.child(f"$adapter-{self._recorded}", uid=1))

    async def run_blocking(self, fn: "Any", /, *args: Any, **kwargs: Any) -> Any:
        """Call the function here and now, since there is no loop to protect.

        The point of the real one is to keep a blocking call off the event
        loop. There is no system here and nothing else running, so calling it
        directly is both simpler and a truer picture of what the handler does
        with the result.
        """
        return fn(*args, **kwargs)

    async def resolve(self, uri: str, *, expect: type[U]) -> ActorRef[U]:
        """Refuse: resolving a ref needs a system to resolve it against."""
        msg = (
            f"cannot resolve {uri!r} in a BehaviorTestKit: a ref is a handle "
            "into a live runtime, and there is none here. Start an ActorSystem "
            "for a test that resolves."
        )
        raise TapioError(msg)

    def watch(self, ref: ActorRef[Any]) -> None:
        """Record a watch. Nothing here can stop, so nothing is delivered."""
        self.effects.append(Watched(ref, watching=True))

    def unwatch(self, ref: ActorRef[Any]) -> None:
        """Record an unwatch."""
        self.effects.append(Watched(ref, watching=False))

    def __repr__(self) -> str:
        """Render the path and how much has been recorded."""
        return f"_KitContext({str(self._path)!r}, {len(self.effects)} effect(s))"
