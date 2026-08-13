"""`ActorCell`: one actor, one task, one mailbox.

The cell is the runtime object behind every ref. It owns the receive loop, the
current behavior, the children, the watchers, the supervision decisions, and
the termination sequence.

There is deliberately no `TaskGroup` here. In a group, one task raising
cancels its siblings, which is the inverse of supervision: a child failing
must leave its siblings untouched. It would also never fire, since the loop
below turns every exception into a decision and nothing escapes for a group to
react to. Each cell creates one task and owns an explicit children map
instead. The absence of orphaned tasks is an invariant the runtime holds and
the test suite asserts.
"""

import asyncio
import contextlib
import itertools
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Generic, TypeAlias, TypeVar, cast

from tapio.actor.adapter import AdaptedMessage, AdapterRef
from tapio.actor.ask import ask as run_ask
from tapio.actor.behavior import (
    Behavior,
    Directive,
    ReceivingBehavior,
    SetupBehavior,
    SuperviseBehavior,
    WithStashBehavior,
    WithTimersBehavior,
    directive_of,
    resolve_handler_msg_type,
)
from tapio.actor.context import ActorContext
from tapio.actor.dead_letters import Carrier, DeadLetterOffice, DeadLetterReason
from tapio.actor.events import EventStream
from tapio.actor.mailbox import Envelope, Mailbox, MailboxConfig
from tapio.actor.path import ActorPath
from tapio.actor.ref import ActorRef
from tapio.actor.restarts import RestartLog
from tapio.actor.signals import ChildFailed, PostStop, PreRestart, Signal, Terminated
from tapio.actor.stash import StashBuffer, UnstashBehavior
from tapio.actor.supervision import Decision, SupervisorStrategy
from tapio.actor.timers import TimerScheduler
from tapio.actor.watch import Watcher, WatchTarget
from tapio.dispatch.blocking import BlockingPool, describe_blocking
from tapio.dispatch.dispatcher import Dispatcher
from tapio.errors import (
    ActorNameError,
    ActorSystemTerminating,
    BehaviorTypeError,
    MailboxFullError,
    RefResolutionError,
    WatchError,
)
from tapio.logging import ActorLogAdapter, actor_logger, runtime_logger
from tapio.message import Message
from tapio.remote.address import Address
from tapio.remote.registry import RefRegistry
from tapio.settings import TapioSettings
from tapio.validation import MessageType, MessageValidator, resolve_validator

__all__ = ["ActorCell", "ActorRuntime", "LocalActorRef", "RefResolver"]

RefResolver: TypeAlias = Callable[[str, type[Message]], Awaitable["ActorRef[Any]"]]
"""Turns a ref's string form into a ref, against the system that holds it.

The system installs one on its runtime so that `ctx.resolve` is the same call
as `system.resolve` rather than a second implementation of it, and so that a
cell needs no reference to the system to make it.
"""

T = TypeVar("T", bound=Message)
U = TypeVar("U", bound=Message)
R = TypeVar("R", bound=Message)
B = TypeVar("B")

_log = runtime_logger("runtime")

_MAX_SETUP_DEPTH = 100
"""How many rounds of deferred construction to allow before calling it a loop."""

_STOP = SupervisorStrategy.stop()
"""What a failure nobody wrote a strategy for gets.

Stop rather than restart: an actor that failed for a reason nobody anticipated
is in a state nobody described, and restarting it on a loop turns one bug into
a busy one.
"""


def _accept_anything(message: Message) -> None:
    """Stand in for validation on a cell that never resolved a message type."""


@dataclass(frozen=True, slots=True)
class _Supervisor:
    """One `supervise(...).on_failure(...)` layer, as the cell holds it."""

    on: type[Exception] | tuple[type[Exception], ...]
    """Which failures this layer governs."""

    strategy: SupervisorStrategy
    """What it decides about them."""


_UNSUPERVISED = _Supervisor(on=Exception, strategy=_STOP)
"""The layer a failure nobody wrote a strategy for falls back to.

A layer rather than a bare strategy so that every failure has a key to be
counted under, even though this one stops and so never records a restart.
"""


@dataclass(eq=False)
class ActorRuntime:
    """The slice of an actor system that a cell needs.

    Passing this rather than the system itself keeps the dependency one-way,
    and lets a test exercise a cell without starting the guardians.
    """

    name: str
    """The system name, and so the first element of every path."""

    address: Address
    """The canonical address every ref from this system writes itself with."""

    refs: RefRegistry
    """The live refs of this system, by path and incarnation uid.

    A cell registers when it starts and deregisters in its termination
    sequence, which is what lets a ref that crossed a wire find its way back to
    a live actor, and what makes a stale uid resolve to nothing rather than to
    the newcomer at that path.
    """

    settings: TapioSettings
    """Tunables shared by every cell in the system."""

    dispatcher: Dispatcher
    """The loop cells create their tasks on, and whose clock times shutdown."""

    dead_letters: DeadLetterOffice
    """Where a message goes when its recipient cannot take it."""

    blocking: BlockingPool = field(
        default_factory=lambda: BlockingPool(size=1, system="test")
    )
    """The threads `ctx.run_blocking` pushes blocking calls onto.

    The default is a one-thread pool, for a cell built directly in a test with
    no system around it. A real system passes its own, sized by
    `blocking_pool_size`, and shuts it down with the tree.
    """

    events: EventStream = field(default_factory=EventStream)
    """What this system publishes about itself, for whoever subscribed.

    Runtime facts rather than traffic: today, that a peer became unreachable.
    A default is provided so a cell built directly in a test needs no stream
    of its own.
    """

    terminated: bool = False
    """Whether the system has finished shutting down.

    Read only to tell one dead-letter reason from another: a message sent to a
    stopped actor and a message sent after the system went away are different
    diagnoses, and the sender usually cares which.
    """

    resolver: RefResolver | None = None
    """How this system turns a ref's string form into a ref.

    Installed by the system, and `None` only for a cell built directly in a
    test, which has no system to resolve against.
    """

    guardian_failure: Callable[[ActorPath, BaseException], None] | None = None
    """Called when a failure escalates all the way to a guardian.

    A guardian has no application behavior and no parent, so a failure that
    reaches one has run out of actors willing to take responsibility. The
    system installs a callback that terminates the tree and keeps the cause for
    `when_terminated`.
    """

    # Quoted: itertools.count is only subscriptable to a type checker.
    _uids: "itertools.count[int]" = field(default_factory=lambda: itertools.count(1))

    def next_uid(self) -> int:
        """Return the next incarnation uid.

        Uid 0 means "no incarnation", so the counter starts at 1. The uid
        stops a ref to a dead actor from addressing a new actor spawned under
        the same name.
        """
        return next(self._uids)


class LocalActorRef(ActorRef[T]):
    """A ref that delivers into a local cell's mailbox.

    It stays a valid handle after its actor dies. Sending to a dead actor is
    not an error, it is a dead letter, because a liveness check is out of date
    as soon as you have the answer.
    """

    __slots__ = ("_cell",)

    def __init__(self, cell: "ActorCell[T]") -> None:
        """Bind the ref to its cell."""
        super().__init__(cell.path)
        self._cell = cell

    @property
    def address(self) -> Address:
        """The canonical address of the system this actor runs in."""
        return self._cell.runtime.address

    @property
    def cell(self) -> "ActorCell[T]":
        """The cell this ref delivers into.

        For the runtime, which needs the object behind the handle to register
        a death watch. Application code needs only the ref.
        """
        return self._cell

    def watch_target(self) -> WatchTarget:
        """Return the cell, which is what a death watch is registered on."""
        return self._cell

    def tell(self, message: T) -> None:
        """Deliver a message, without waiting and without blocking.

        Safe to call from any thread. Validation runs on the calling thread,
        before any hop, so an error about the message reaches the code that
        wrote it. Delivery then happens on the system's loop.

        The rule is: the message is yours, the recipient is not. Errors about
        the message raise here. Errors about the recipient, such as a stopped
        actor or a full mailbox, are settled on the target's loop after this
        call has returned. There is no caller left to raise into by then, so
        they become dead letters.

        Args:
            message: The message to deliver. The recipient always receives this
                exact object.

        Raises:
            MessageTypeError: If the message does not match the target's
                declared message type.
            MailboxFullError: If the target's mailbox is full under
                `OverflowStrategy.FAIL` *and* the caller is on the system's own
                loop. From another thread the same overflow dead-letters, since
                there is no caller left to raise into.
            pydantic.ValidationError: If content validation is on and the
                message does not satisfy its own model.
        """
        cell = self._cell
        cell.validate(message)
        dispatcher = cell.runtime.dispatcher
        if dispatcher.is_current():
            cell.deliver(message)
            return
        try:
            dispatcher.call_soon_threadsafe(cell.deliver_offloop, message)
        except RuntimeError:
            # The loop is closed, so there is nothing to schedule onto and no
            # subscriber left to notify. Logging is all that remains.
            cell.log.warning(
                "dead letter: %s sent after the loop closed",
                type(message).__name__,
            )

    async def offer(self, message: T) -> None:
        """Deliver a message, waiting for mailbox capacity if it is full.

        Backpressure belongs to the mailbox, not to the send, so this is
        `tell` plus waiting. On an unbounded mailbox they are the same thing.

        Args:
            message: The message to deliver.

        Raises:
            MessageTypeError: If the message does not match the target's
                declared message type.
            RuntimeError: If called from a thread that is not running the
                system's loop. Waiting for capacity across a thread boundary
                is not supported. Use `tell` from other threads.
            pydantic.ValidationError: If content validation is on and the
                message does not satisfy its own model.
        """
        cell = self._cell
        cell.validate(message)
        if not cell.runtime.dispatcher.is_current():
            msg = (
                f"offer to {cell.path} must run on the system's loop; `tell` "
                "is the thread-safe send"
            )
            raise RuntimeError(msg)
        await cell.offer(message)

    async def ask(
        self,
        make: Callable[[ActorRef[R]], T],
        *,
        expect: type[R],
        timeout: timedelta | None = None,  # noqa: ASYNC109 - the ask deadline
    ) -> R:
        """Send one message and await one reply.

        ```python
        reply = await ref.ask(
            lambda reply_to: Query(reply_to=reply_to),
            expect=QueryResult,
            timeout=timedelta(seconds=2),
        )
        ```

        Awaiting from inside a handler stops that actor reading its mailbox
        until the reply lands. That is sometimes what you want, but usually
        not: an actor that asks and waits cannot answer anyone else.

        Args:
            make: Builds the request from the ref the reply should go to.
            expect: The reply type, which is required rather than inferred.
            timeout: How long to wait. The system's `ask_timeout` when omitted.

        Returns:
            The reply, which is the object the responder passed.

        Raises:
            AskTimeoutError: If no reply arrived in time.
            AskTargetTerminated: If the target stopped without replying.
            AskTypeError: If the reply was not an `expect`.
            MessageTypeError: If the request does not match the target's
                declared message type.
            RuntimeError: If called off the system's loop.
            pydantic.ValidationError: If content validation is on and either
                the request or the reply does not satisfy its own model.
        """
        return await run_ask(self, make, expect=expect, timeout=timeout)


class ActorCell(Generic[T]):
    """One actor: a mailbox, a behavior, a task, its children and its watchers."""

    def __init__(
        self,
        *,
        runtime: ActorRuntime,
        path: ActorPath,
        behavior: Behavior[T],
        parent: "ActorCell[Any] | None" = None,
        mailbox: MailboxConfig | None = None,
    ) -> None:
        """Create a cell. Nothing runs until `start` is called."""
        self._runtime = runtime
        self._path = path
        self._parent = parent
        # The behavior as first given, not as it is now. A restart evaluates
        # this one again, so an actor that has switched behavior several times
        # still comes back as what it started as.
        self._initial = behavior
        self._behavior: Behavior[T] = behavior
        self._supervisors: tuple[_Supervisor, ...] = ()
        # Per supervisor, not per actor. Each layer brings its own limit,
        # window and backoff, and they are separate budgets: one layer's
        # failures must not spend, prune or count against another's.
        self._restarts = RestartLog()
        self._mailbox = Mailbox(
            mailbox if mailbox is not None else runtime.settings.default_mailbox
        )
        self._children: dict[str, ActorCell[Any]] = {}
        self._anonymous = itertools.count(1)
        self._adapters = itertools.count(1)
        self._adapter_paths: set[ActorPath] = set()
        # Both sides of every watch, so that neither a watcher nor a watched
        # actor leaves an entry behind in the other when it stops. Not every
        # watcher is a cell: an ask's promise watches its target too.
        self._watchers: dict[ActorPath, Watcher] = {}
        self._watching: dict[ActorPath, WatchTarget] = {}
        self._log = actor_logger(path)
        # Both are owned by the cell rather than by a behavior, and both
        # outlive an incarnation. A restart empties them and hands the same
        # objects to the behavior the factory produces.
        self._timers: TimerScheduler[T] = TimerScheduler(self)
        self._stash: StashBuffer[T] | None = None
        self._ctx: ActorContext[T] = _CellContext(self)
        self._ref: LocalActorRef[T] = LocalActorRef(self)
        self._task: asyncio.Task[None] | None = None
        self._terminated: asyncio.Future[None] = runtime.dispatcher.loop.create_future()
        self._alive = True
        self._terminating = False
        self._current: Message | None = None
        # Both are replaced in `start` once the behavior has declared its
        # message type. A cell that stops during setup never gets one, and
        # there is no type check to make without it.
        self._msg_type: MessageType | None = None
        self._validate: MessageValidator = _accept_anything

    @property
    def path(self) -> ActorPath:
        """Where this actor sits in the tree."""
        return self._path

    @property
    def ref(self) -> ActorRef[T]:
        """The ref others use to reach this actor."""
        return self._ref

    @property
    def log(self) -> ActorLogAdapter:
        """This actor's path-tagged logger."""
        return self._log

    @property
    def runtime(self) -> ActorRuntime:
        """The system slice this cell runs in."""
        return self._runtime

    @property
    def watchers(self) -> tuple[ActorPath, ...]:
        """Who has asked to be told when this actor stops.

        Exposed so a test can assert that the watch was released. A map that
        outlives the actor it names is the leak death watch exists to prevent.
        """
        return tuple(self._watchers)

    @property
    def timers(self) -> TimerScheduler[T]:
        """The timers this actor has running.

        Exposed for the same reason as `watchers`: a test has to be able to
        assert that a restart cancelled the timers.
        """
        return self._timers

    @property
    def msg_type(self) -> MessageType | None:
        """What this actor accepts, fixed when it started.

        It is `None` only for a cell that stopped during deferred construction
        and never declared one. Anything that has to accept exactly what
        another actor accepts reads this. A router takes its own message type
        from the routees it just spawned, rather than being told it twice.
        """
        return self._msg_type

    @property
    def is_alive(self) -> bool:
        """Whether this actor is still accepting messages.

        For the runtime's own use. Application code should watch the actor
        instead, since a liveness answer is out of date once it is read.
        """
        return self._alive

    def start(self) -> None:
        """Evaluate the behavior, resolve validation, and start the loop.

        Deferred construction runs here, synchronously, rather than inside the
        new task. The message type comes from the behavior it produces, and
        the ref can be used as soon as `spawn` returns, so the type has to be
        known by then.
        """
        behavior = self._evaluate(self._initial)
        self._behavior = behavior
        if directive_of(behavior) is Directive.STOPPED:
            # Deferred construction decided there was nothing to run. The
            # caller still gets a ref, and what it sends dead-letters.
            self._finish()
            return

        msg_type = behavior.msg_type
        if msg_type is None:
            msg = (
                f"cannot spawn {self._path}: {behavior!r} carries no message "
                "type. Directives like Behaviors.same() and Behaviors.ignore() "
                "resolve against the type an actor already has, so they cannot "
                "be the behavior an actor starts with."
            )
            raise BehaviorTypeError(msg)

        self._msg_type = msg_type
        self._validate = resolve_validator(
            msg_type=msg_type,
            settings=self._runtime.settings,
            target=self._path,
        )
        # Registered here rather than in the constructor, so a cell that never
        # starts is never addressable. Deregistered in `_finish`, so the
        # registry holds exactly the live actors.
        self._runtime.refs.register(self._ref)
        self._task = self._runtime.dispatcher.spawn_task(
            self._run(), name=f"tapio-cell:{self._path}"
        )

    def validate(self, message: Message) -> None:
        """Check a message against this actor's declared type and model.

        Called on the sender's thread, before any hop onto the system's loop,
        because an error about the message is the sender's to handle.
        """
        self._validate(message)

    def deliver(self, message: Message) -> None:
        """Put an already-validated message on the user lane.

        Raises:
            MailboxFullError: If the mailbox is full under
                `OverflowStrategy.FAIL`.
        """
        if not self._alive:
            self._dead_letter(message, self._unreachable_reason())
            return
        displaced = self._mailbox.put(message)
        if displaced is not None:
            self._dead_letter(displaced, DeadLetterReason.MAILBOX_FULL)

    def deliver_offloop(self, message: Message) -> None:
        """Deliver a message that arrived from another thread.

        The same as `deliver`, except that a full `FAIL` mailbox cannot raise.
        The sender is on another thread and has moved on, so the overflow
        becomes a dead letter like every other recipient error.
        """
        try:
            self.deliver(message)
        except MailboxFullError:
            self._dead_letter(message, DeadLetterReason.MAILBOX_FULL)

    async def offer(self, message: Message) -> None:
        """Put an already-validated message on the user lane, waiting if full.

        Liveness is read through a call rather than the attribute, so the
        second check is accurate. The actor can stop while this sender is
        waiting, which is the case the check below exists for.
        """
        if not self._still_alive():
            self._dead_letter(message, self._unreachable_reason())
            return
        await self._mailbox.offer(message)
        if not self._still_alive():
            # The actor stopped while this sender was parked. Whatever it just
            # enqueued will never be read, so account for it rather than
            # leaving it in a mailbox nobody owns.
            self._drain_to_dead_letters()

    def spawn(
        self,
        behavior: Behavior[U],
        name: str,
        mailbox: MailboxConfig | None = None,
    ) -> ActorRef[U]:
        """Start a child under this actor."""
        if self._terminating:
            msg = (
                f"cannot spawn {name!r} under {self._path}: this actor is "
                "terminating. A spawn during shutdown is an ordering bug, and "
                "raising surfaces it instead of leaving an inert actor behind."
            )
            raise ActorSystemTerminating(msg)
        if name in self._children:
            msg = (
                f"{self._path} already has a live child named {name!r}; actor "
                "names are unique among siblings"
            )
            raise ActorNameError(msg)
        return self._spawn_child(behavior, name, mailbox)

    def spawn_anonymous(
        self, behavior: Behavior[U], mailbox: MailboxConfig | None = None
    ) -> ActorRef[U]:
        """Start a child under a generated name."""
        if self._terminating:
            msg = (
                f"cannot spawn an anonymous child under {self._path}: this "
                "actor is terminating"
            )
            raise ActorSystemTerminating(msg)
        return self._spawn_child(behavior, f"${next(self._anonymous)}", mailbox)

    def message_adapter(
        self, adapt: Callable[[U], T], msg_type: MessageType | None = None
    ) -> ActorRef[U]:
        """Hand out a ref that translates another protocol into this one.

        Each call makes a new adapter with its own address, and refs already
        handed out keep working. An adapter is bound to the actor, not to the
        incarnation that created it, so a restart does not turn replies into
        dead letters for someone still holding a ref.

        That also means nothing releases one on its own. An adapter per
        protocol, made in `setup`, costs one registry entry for the life of
        the actor and is what most actors want. An adapter per request wants
        `AdapterRef.release`, or the entries accumulate for as long as the
        actor runs.
        """
        resolved = resolve_handler_msg_type(
            adapt, explicit=msg_type, message_param_index=0
        )
        path = self._path.child(
            f"$adapter-{next(self._adapters)}", uid=self._runtime.next_uid()
        )
        ref: AdapterRef[U] = AdapterRef(
            cell=self,
            path=path,
            adapt=cast("Callable[[Any], Message]", adapt),
            validate=resolve_validator(
                msg_type=resolved, settings=self._runtime.settings, target=path
            ),
        )
        # An adapter is addressable like the actor behind it, so a ref handed
        # to a peer resolves on the way back. It lives and dies with its owner,
        # which is why the owner is what deregisters it.
        self._runtime.refs.register(ref)
        self._adapter_paths.add(path)
        return ref

    def release_adapter(self, path: ActorPath) -> None:
        """Take one adapter out of the registry, leaving this actor running.

        Called by `AdapterRef.release`. Releasing one that is not this actor's,
        or one that has already gone, does nothing: the ref keeps its own
        released flag, so the call stays idempotent from either side.

        Args:
            path: The adapter's path.
        """
        if path in self._adapter_paths:
            self._adapter_paths.discard(path)
            self._runtime.refs.deregister(path)

    async def run_blocking(
        self, fn: Callable[..., B], /, *args: Any, **kwargs: Any
    ) -> B:
        """Run a blocking call on the system's pool, parking this actor.

        The actor is awaiting for the duration, so its mailbox fills up behind
        the call. The loop stays free, which is the whole point, and this
        actor does not.

        Args:
            fn: The blocking callable.
            *args: Its positional arguments.
            **kwargs: Its keyword arguments.

        Returns:
            Whatever `fn` returned.

        Raises:
            ActorSystemTerminating: If the pool is shut down, which means the
                system is going away and the call would never be run.
        """
        try:
            return await self._runtime.blocking.submit(
                self._runtime.dispatcher.loop, fn, *args, **kwargs
            )
        except RuntimeError as error:
            if not self._runtime.blocking.is_accepting:
                msg = (
                    f"cannot run {describe_blocking(fn)} for {self._path}: the "
                    "system is shutting down, so its blocking pool takes no "
                    "more work"
                )
                raise ActorSystemTerminating(msg) from error
            raise

    def watch(self, ref: ActorRef[Any]) -> None:
        """Ask to be told when another actor stops.

        The ref may point at an actor on a peer, and the call is the same. So
        is the signal: `Terminated` arrives when that actor stops, and also
        when the peer holding it becomes unreachable, which is a judgement
        that can be wrong. There is no way to tell those apart from here, and
        this is the one place remoting is not transparent by design rather
        than by omission.

        Args:
            ref: The actor to watch.

        Raises:
            WatchError: If the ref has no live actor behind it, or if an actor
                tries to watch itself.
        """
        target = _watch_target(ref)
        if target is self:
            msg = (
                f"{self._path} cannot watch itself: the signal would be "
                "delivered to a mailbox nobody is left to read"
            )
            raise WatchError(msg)
        if not target.is_alive:
            # Already gone. Delivering at once rather than refusing keeps the
            # caller's code the same either way: watching is how you ask, and
            # the answer does not depend on how the race came out.
            self._mailbox.put_system(Terminated(ref))
            return
        target.add_watcher(self)
        self._watching[target.path] = target

    def unwatch(self, ref: ActorRef[Any]) -> None:
        """Stop being told when another actor stops.

        Harmless if this actor was not watching it. It does not retract a
        `Terminated` that is already queued, since by then it is true.

        Args:
            ref: The actor to stop watching.
        """
        target = self._watching.pop(ref.path, None)
        if target is not None:
            target.remove_watcher(self)

    def add_watcher(self, watcher: Watcher) -> None:
        """Register something to be told when this actor stops.

        Keyed by path, so watching twice still delivers exactly one signal.
        """
        self._watchers[watcher.path] = watcher

    def remove_watcher(self, watcher: Watcher) -> None:
        """Deregister a watcher."""
        self._watchers.pop(watcher.path, None)

    def child_failed(self, child: ActorRef[Any], error: Exception) -> None:
        """Take a child's escalated failure as this actor's own.

        It travels on the system lane, so it outranks any queued user traffic.
        It is an ordinary signal rather than an exception injected across a
        task boundary. There is no clean way to do that, and its order against
        this actor's in-flight message would be undefined.
        """
        if not self._alive or self._terminating:
            self._log.warning(
                "child %s failed while this actor was already stopping",
                child.path,
                exc_info=error,
            )
            return
        self._mailbox.put_system(ChildFailed(child, error))

    def _spawn_child(
        self,
        behavior: Behavior[U],
        name: str,
        mailbox: MailboxConfig | None = None,
    ) -> ActorRef[U]:
        """Create, register and start a child cell."""
        child: ActorCell[U] = ActorCell(
            runtime=self._runtime,
            path=self._path.child(name, uid=self._runtime.next_uid()),
            behavior=behavior,
            parent=self,
            mailbox=mailbox,
        )
        self._children[name] = child
        try:
            child.start()
        except BaseException:
            self._children.pop(name, None)
            raise
        return child.ref

    async def stop(self, deadline: float) -> None:
        """Stop this actor and everything under it, racing one deadline.

        The deadline belongs to the whole tree, not to this cell. A per-cell
        timeout would make worst-case shutdown the depth times the timeout,
        which is not what "shut down within ten seconds" is meant to mean.

        Args:
            deadline: A point on the loop's clock, shared by every cell in the
                sweep.
        """
        self._terminating = True
        await self._stop_children(deadline)

        if self._task is None:
            self._finish()
            return

        self._mailbox.put_system(PostStop())
        # Shielded, and the task rather than the termination future: a caller
        # that gives up waiting must not cancel the stop it asked for, and the
        # task is only done once its own cleanup has run.
        try:
            async with asyncio.timeout_at(deadline):
                await asyncio.shield(self._task)
        except TimeoutError:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.shield(self._task)
            self._log.warning(
                "did not stop within the shutdown deadline while handling %s; "
                "cancelled",
                type(self._current).__name__
                if self._current is not None
                else "no message",
            )

    def abort(self) -> None:
        """Cancel this actor's task without waiting for it to finish."""
        self._terminating = True
        if self._task is not None and not self._task.done():
            self._task.cancel()

    async def _stop_children(self, deadline: float) -> None:
        """Stop every child, bottom-up, against the shared deadline.

        The list is taken eagerly because children deregister themselves from
        the map as they stop.
        """
        children = list(self._children.values())
        if children:
            await asyncio.gather(*(child.stop(deadline) for child in children))

    async def _run(self) -> None:
        """The receive loop: the whole of what an actor does."""
        try:
            while self._alive:
                envelope = await self._mailbox.get()
                if isinstance(envelope, Signal):
                    await self._on_signal(envelope)
                else:
                    await self._on_message(envelope)
        finally:
            # Cancellation reaches here too. It is a BaseException, so it is
            # never caught below and never becomes a supervision decision: a
            # cancelled actor stops, it does not restart.
            self._finish()

    async def _on_signal(self, signal: Signal) -> None:
        """Handle a system-lane signal."""
        if isinstance(signal, PostStop):
            await self._run_lifecycle_hook(signal)
            self._alive = False
            return
        if isinstance(signal, ChildFailed):
            # A child that escalated is this actor's failure now, with the
            # child's error rather than one of its own.
            await self._on_failure(signal.error)
            return
        try:
            nxt = await self._deliver_signal(signal)
        except Exception as error:
            await self._on_failure(error)
            return
        if nxt is not None:
            await self._become(nxt, signal)

    async def _on_message(self, message: Message) -> None:
        """Run one user message through the current behavior."""
        if isinstance(message, AdaptedMessage):
            try:
                message = self._translate(message)
            except Exception as error:
                # The translation is this actor's own code, which is the whole
                # reason it runs here rather than at the send site: a mistake
                # in it is this actor's failure, not the sender's.
                await self._on_failure(error)
                return

        behavior = self._behavior
        if directive_of(behavior) is Directive.IGNORE:
            return
        if not isinstance(behavior, ReceivingBehavior):
            self._unhandled(message)
            return

        self._current = message
        try:
            nxt = await behavior.receive(self._ctx, cast("T", message))
        except Exception as error:
            # The exception never leaves this loop: it becomes a decision, and
            # the sender hears nothing about it.
            await self._on_failure(error)
            return
        finally:
            self._current = None
        await self._become(nxt, message)

    def _translate(self, envelope: AdaptedMessage) -> Message:
        """Turn a message that arrived through an adapter into one of this actor's.

        The result is validated like anything else delivered here. It has to
        be, because an adapter is the one path onto this lane that did not
        check the declared type on the way in. A translation that produces the
        wrong message is a bug worth hearing about, not something the handler
        should have to guard against.
        """
        self._current = envelope.payload
        try:
            translated = envelope.translate()
            self._validate(translated)
        finally:
            self._current = None
        return translated

    async def _deliver_signal(self, signal: Signal) -> Behavior[T] | None:
        """Hand a signal to the behavior, or `None` if it has no handler."""
        behavior = self._behavior
        if not isinstance(behavior, ReceivingBehavior):
            return None
        return await behavior.receive_signal(self._ctx, signal)

    async def _run_lifecycle_hook(self, signal: Signal) -> None:
        """Deliver `PostStop` or `PreRestart`, whose result cannot change anything.

        A failure here is logged rather than supervised. The actor is already
        stopping or restarting, and taking a second decision on top of the
        first would leave the sequence in an undefined state.
        """
        try:
            await self._deliver_signal(signal)
        except Exception:
            self._log.exception("failed while handling %s", type(signal).__name__)

    async def _on_failure(self, error: Exception) -> None:
        """Turn a failure into a supervision decision and apply it."""
        if self._parent is None:
            # A guardian. Nobody above it takes responsibility, by
            # construction, so the whole system comes down with the cause.
            await self._fail_the_system(error)
            return
        if self._terminating:
            self._log.warning(
                "failed while already stopping; the decision is moot",
                exc_info=error,
            )
            return

        supervisor = self._supervisor_for(error)
        strategy = supervisor.strategy
        match strategy.decision:
            case Decision.RESUME:
                self._log.warning(
                    "resumed after a failure in %s",
                    self._describe_current(),
                    exc_info=error,
                )
            case Decision.RESTART:
                await self._restart(error, supervisor)
            case Decision.STOP:
                self._log.error(
                    "stopping after a failure in %s",
                    self._describe_current(),
                    exc_info=error,
                )
                await self._stop_self()
            case Decision.ESCALATE:
                await self._escalate(error)

    def _supervisor_for(self, error: Exception) -> _Supervisor:
        """Find the layer governing a failure, outermost wrapper first.

        The layer rather than its strategy, because it is also the key a
        restart is counted under. Two layers can decide the same thing and
        still hold separate budgets.
        """
        for supervisor in self._supervisors:
            if isinstance(error, supervisor.on):
                return supervisor
        return _UNSUPERVISED

    async def _restart(self, error: Exception, supervisor: _Supervisor) -> None:
        """Rebuild this actor from the behavior it was spawned with.

        Every part of this is a deliberate choice. Children are stopped, and
        respawned by the setup that runs again. The mailbox survives with both
        lanes intact. The failed message does not. Watchers are told nothing,
        because the ref, the path and the uid are unchanged, and only the
        incarnation behind them is new.
        """
        strategy = supervisor.strategy
        if not self._restarts.record(
            supervisor, strategy, self._runtime.dispatcher.now()
        ):
            self._log.error(
                "restart limit of %d in %s is exhausted; stopping",
                strategy.max_restarts,
                strategy.window,
                exc_info=error,
            )
            await self._stop_self()
            return

        self._log.warning(
            "restarting after a failure in %s", self._describe_current(), exc_info=error
        )
        await self._run_lifecycle_hook(PreRestart())
        # Both belong to the incarnation that just failed. A tick scheduled by
        # it must not arrive at its replacement, and messages it put aside are
        # not the replacement's to answer.
        self._timers.cancel_all()
        self._discard_stash()
        await self._stop_children(self._own_deadline())

        if strategy.backoff is not None:
            delay = strategy.backoff.delay(
                self._restarts.count(supervisor), jitter=random.random()
            )
            self._log.debug("backing off for %.3fs before restarting", delay)
            if not await self._backoff(delay):
                return

        try:
            self._behavior = self._evaluate(self._initial)
        except Exception:
            # The behavior itself cannot be rebuilt, so there is nothing to
            # restart into. Failing the restart the same way twice is a loop,
            # and stopping is the honest end of it.
            self._log.exception("failed while restarting; stopping")
            await self._stop_self()
            return
        if directive_of(self._behavior) is Directive.STOPPED:
            await self._stop_self()

    async def _backoff(self, seconds: float) -> bool:
        """Wait out a backoff window, staying responsive to a stop.

        The cell stops taking user messages and its mailbox keeps filling. The
        actor is absent, not dead, and `tell` stays total. On an unbounded
        mailbox a long window therefore costs memory in proportion to the
        inbound rate times the window, which is why the docs recommend a
        bounded mailbox for actors that back off.

        Args:
            seconds: How long to wait.

        Returns:
            `True` when the window elapsed, `False` when a stop arrived instead
            and this actor is already on its way out.
        """
        held: list[Signal] = []
        try:
            async with asyncio.timeout(seconds):
                while True:
                    signal = await self._mailbox.get_system()
                    if isinstance(signal, PostStop):
                        await self._run_lifecycle_hook(signal)
                        self._alive = False
                        return False
                    # Anything else waits its turn: the actor that would react
                    # to it does not exist for the length of this window.
                    held.append(signal)
        except TimeoutError:
            return True
        finally:
            for signal in held:
                self._mailbox.put_system(signal)

    async def _escalate(self, error: Exception) -> None:
        """Stop, and make this failure the parent's own."""
        # A note rather than a wrapper exception. The chain then reads in the
        # traceback of the original error, which is what anyone debugging this
        # wants, and nothing has to be unwrapped.
        error.add_note(f"escalated from {self._path}")
        parent = self._parent
        if parent is not None:
            parent.child_failed(self._ref, error)
        await self._stop_self()

    async def _fail_the_system(self, error: Exception) -> None:
        """End the system, because a failure reached a guardian.

        A guardian is the top of the tree, so an escalation that reaches here
        has run out of actors willing to take responsibility for it. Carrying
        on with a subtree silently missing is worse than stopping. The service
        embedding tapio awaits `when_terminated`, sees the cause, and decides
        whether to exit or rebuild. That decision belongs to it.
        """
        error.add_note(f"escalated to {self._path}")
        self._log.error(
            "a failure escalated to the guardian and nobody took responsibility; "
            "terminating the system",
            exc_info=error,
        )
        report = self._runtime.guardian_failure
        if report is not None:
            report(self._path, error)
        await self._stop_self()

    def _describe_current(self) -> str:
        """Name the message being handled, for a log line about a failure."""
        return type(self._current).__name__ if self._current is not None else "a signal"

    def _unhandled(self, envelope: Envelope) -> None:
        """Report an envelope the current behavior does not handle."""
        self._log.debug("unhandled %s", type(envelope).__name__)

    async def _become(self, nxt: Behavior[T], envelope: Envelope) -> None:
        """Apply what a handler returned.

        The cell's declared message type is fixed at spawn. It is the contract
        every ref to this actor was validated against, so switching behavior
        changes what the actor does, never what it accepts.
        """
        directive = directive_of(nxt)
        if directive is Directive.SAME:
            return
        if directive is Directive.UNHANDLED:
            self._unhandled(envelope)
            return
        if directive is Directive.STOPPED:
            await self._stop_self()
            return
        self._behavior = self._evaluate(nxt, keep_supervisors=True)

    async def _stop_self(self) -> None:
        """Stop because this actor asked to, rather than because a parent did."""
        if self._terminating and not self._alive:
            return
        self._terminating = True
        await self._stop_children(self._own_deadline())
        await self._run_lifecycle_hook(PostStop())
        self._alive = False

    def _own_deadline(self) -> float:
        """A deadline for a stop this actor started, rather than the tree's."""
        seconds = self._runtime.settings.shutdown_timeout.total_seconds()
        return self._runtime.dispatcher.now() + seconds

    def _evaluate(
        self, behavior: Behavior[T], *, keep_supervisors: bool = False
    ) -> Behavior[T]:
        """Unwrap supervision and run deferred construction until a real behavior.

        Both wrappers are unwrapped in one loop, because either can enclose
        the other. People write `supervise(setup(...))`, and they also write a
        `setup` that returns a supervised behavior.

        Args:
            behavior: What to evaluate.
            keep_supervisors: Keep the strategies already in force when the
                new behavior declares none. Set when a handler returned a
                behavior, because supervision belongs to the actor and not to
                the behavior it currently holds. Left off at start and at
                restart, where the strategies are being established.

        Returns:
            The behavior the actor will run.
        """
        supervisors: list[_Supervisor] = []
        seen = 0
        while True:
            if isinstance(behavior, SuperviseBehavior):
                supervisors.append(_Supervisor(behavior.on, behavior.strategy))
                behavior = behavior.behavior
            elif isinstance(behavior, SetupBehavior):
                behavior = behavior.setup(self._ctx)
            elif isinstance(behavior, WithTimersBehavior):
                behavior = behavior.with_timers(self._timers)
            elif isinstance(behavior, WithStashBehavior):
                behavior = behavior.with_stash(self._stash_buffer(behavior.capacity))
            elif isinstance(behavior, UnstashBehavior):
                self._unstash(behavior.buffer)
                behavior = behavior.behavior
            else:
                break
            seen += 1
            if seen > _MAX_SETUP_DEPTH:
                msg = (
                    f"{self._path}: deferred construction is still returning "
                    f"another Behaviors.setup after {_MAX_SETUP_DEPTH} rounds"
                )
                raise BehaviorTypeError(msg)

        if supervisors or not keep_supervisors:
            self._supervisors = tuple(supervisors)
        return behavior

    def _finish(self) -> None:
        """Run the stop hook, release children, and mark this cell terminated.

        Synchronous on purpose. It runs from the loop's `finally`, including
        while the task is being cancelled, and awaiting anything there could
        leave the sequence that releases the actor unfinished.
        """
        if self._terminated.done():
            return
        self._alive = False
        self._terminating = True
        for child in list(self._children.values()):
            child.abort()
        self._children.clear()
        # Wake any parked sender first, then account for what is left. A
        # message that was queued and never read is not silently lost.
        self._timers.cancel_all()
        self._mailbox.close()
        self._drain_to_dead_letters()
        self._discard_stash()
        self._release_watches()
        self._deregister_refs()
        if self._parent is not None:
            self._parent._remove_child(self)
        self._log.debug("stopped")
        self._terminated.set_result(None)

    def _deregister_refs(self) -> None:
        """Take this actor and its adapters out of the ref registry.

        An entry left behind would let a stale ref address whoever holds that
        path next.
        """
        refs = self._runtime.refs
        refs.deregister(self._path)
        for path in self._adapter_paths:
            refs.deregister(path)
        self._adapter_paths.clear()

    def _release_watches(self) -> None:
        """Tell the watchers, and leave nothing behind in the watched.

        Both directions matter for the same reason. A map that outlives the
        actor it names is a leak, which is what death watch exists to save
        users from writing themselves.
        """
        for watcher in list(self._watchers.values()):
            watcher.notify_terminated(self._ref)
        self._watchers.clear()
        for target in list(self._watching.values()):
            target.remove_watcher(self)
        self._watching.clear()

    def notify_terminated(self, ref: ActorRef[Any]) -> None:
        """Take delivery of a watched actor's death, on the system lane."""
        self._watching.pop(ref.path, None)
        if self._alive:
            self._mailbox.put_system(Terminated(ref))

    def notify_unreachable(self, ref: ActorRef[Any], detail: str) -> None:
        """Take delivery of a watched actor's peer going out of reach.

        An actor is told the same thing either way. Whether the actor over
        there stopped or the link to it went silent, what this one can do
        about it is identical, so `Terminated` is the signal and the reason is
        logged rather than delivered.
        """
        self._log.info("%s is unreachable: %s", ref.path, detail)
        self.notify_terminated(ref)

    def _remove_child(self, child: "ActorCell[Any]") -> None:
        """Deregister a stopped child, freeing its name for reuse."""
        if self._children.get(child.path.name) is child:
            del self._children[child.path.name]

    def _stash_buffer(self, capacity: int) -> StashBuffer[T]:
        """The buffer this actor stashes into, created once and then kept.

        Created on the first evaluation and reused by every incarnation after
        it, so a restart cannot resize the buffer and the cell always knows
        which one to empty.
        """
        if self._stash is None:
            self._stash = StashBuffer(capacity)
        return self._stash

    def _unstash(self, buffer: StashBuffer[T]) -> None:
        """Put everything held back at the head of the user lane.

        They go back in arrival order, ahead of whatever queued up while the
        actor was not ready. The actor stays an ordinary actor throughout: the
        messages pass through the receive loop one at a time, so signals still
        outrank them and a stop arriving mid-replay is honoured.
        """
        for message in reversed(buffer.take_all()):
            self._mailbox.put_front(message)

    def _discard_stash(self) -> None:
        """Empty the stash, accounting for what it held.

        A restart clears it, because messages held by the state that just
        failed are not the new state's to answer. A stop clears it, because
        there is nobody left to replay them. Neither is a reason to lose them
        silently.
        """
        if self._stash is None:
            return
        for message in self._stash.take_all():
            # Its own reason rather than the mailbox's. A message the actor
            # accepted and then put aside is a different case from one that
            # never got in, and the sender may care which.
            self._dead_letter(message, DeadLetterReason.STASH_DISCARDED)

    def _dead_letter(self, message: Message, reason: str) -> None:
        """Account for a message that had nowhere to go.

        A message travelling inside a wrapper, through an adapter or out to a
        peer, is reported as what its sender sent. The wrapper is only how it
        travelled, and a subscriber matching on message types should not have
        to know about it.
        """
        if isinstance(message, Carrier):
            message = message.payload
        self._runtime.dead_letters.publish(message, self._path, reason)

    def _still_alive(self) -> bool:
        """Whether this actor is still reading its mailbox, checked afresh."""
        return self._alive

    def _unreachable_reason(self) -> str:
        """Say why this actor could not take a message.

        A stopped actor and a stopped system are different problems, and the
        sender usually cares which. One is an ordering bug in the application.
        The other is a send that outlived its runtime.
        """
        if self._runtime.terminated:
            return DeadLetterReason.SYSTEM_TERMINATED
        return DeadLetterReason.RECIPIENT_TERMINATED

    def _drain_to_dead_letters(self) -> None:
        """Send everything left on the user lane to dead letters."""
        reason = self._unreachable_reason()
        while (pending := self._mailbox.take_pending()) is not None:
            self._dead_letter(pending, reason)

    def __repr__(self) -> str:
        """Render the path and the current behavior."""
        return f"ActorCell({str(self._path)!r}, {self._behavior!r})"


def _watch_target(ref: ActorRef[Any]) -> WatchTarget:
    """Find what would arrange a death watch on a ref.

    Raises:
        WatchError: If the ref is not one a running system handed out. A
            dead-letter target is the usual case: it stands for an actor that
            is already gone, and there is no death left to report.
    """
    target = ref.watch_target()
    if target is not None:
        return target
    msg = (
        f"cannot watch {ref!r}: it is not a ref to a live actor, here or on a "
        "peer. Watch a ref obtained from spawn, from resolve, or carried in a "
        "message."
    )
    raise WatchError(msg)


class _CellContext(ActorContext[T]):
    """The context handed to a behavior: a thin view onto its own cell."""

    __slots__ = ("_cell",)

    def __init__(self, cell: ActorCell[T]) -> None:
        """Bind the context to its cell."""
        self._cell = cell

    @property
    def path(self) -> ActorPath:
        """Where this actor sits in the tree."""
        return self._cell.path

    @property
    def self_ref(self) -> ActorRef[T]:
        """A ref to this actor, to hand out in messages."""
        return self._cell.ref

    @property
    def log(self) -> ActorLogAdapter:
        """A logger that tags every record with this actor's path."""
        return self._cell.log

    def spawn(
        self,
        behavior: Behavior[U],
        name: str,
        mailbox: MailboxConfig | None = None,
    ) -> ActorRef[U]:
        """Start a child actor under this one."""
        return self._cell.spawn(behavior, name, mailbox)

    def spawn_anonymous(
        self, behavior: Behavior[U], mailbox: MailboxConfig | None = None
    ) -> ActorRef[U]:
        """Start a child under a generated name."""
        return self._cell.spawn_anonymous(behavior, mailbox)

    def message_adapter(
        self, adapt: Callable[[U], T], msg_type: MessageType | None = None
    ) -> ActorRef[U]:
        """Hand out a ref that translates another protocol into this actor's."""
        return self._cell.message_adapter(adapt, msg_type)

    async def run_blocking(
        self, fn: Callable[..., B], /, *args: Any, **kwargs: Any
    ) -> B:
        """Run a call that blocks on the system's thread pool."""
        return await self._cell.run_blocking(fn, *args, **kwargs)

    async def resolve(self, uri: str, *, expect: type[U]) -> ActorRef[U]:
        """Turn a ref's string form into a ref, local or remote."""
        resolver = self._cell.runtime.resolver
        if resolver is None:  # pragma: no cover - every system installs one
            msg = (
                f"cannot resolve {uri!r}: this actor's runtime has no system behind it"
            )
            raise RefResolutionError(msg)
        return cast("ActorRef[U]", await resolver(uri, expect))

    def watch(self, ref: ActorRef[Any]) -> None:
        """Ask to be sent `Terminated` when another actor stops."""
        self._cell.watch(ref)

    def unwatch(self, ref: ActorRef[Any]) -> None:
        """Stop watching an actor."""
        self._cell.unwatch(ref)

    def __repr__(self) -> str:
        """Render the actor this context belongs to."""
        return f"ActorContext({str(self._cell.path)!r})"
