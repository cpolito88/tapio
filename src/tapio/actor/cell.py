"""`ActorCell`: one actor, one task, one mailbox.

The cell is the runtime object behind every ref. It owns the receive loop, the
current behavior, the children, the watchers, the supervision decisions, and
the termination sequence.

There is deliberately no `TaskGroup` here. A group's defining behaviour is that
one task raising cancels its siblings, which is the exact inverse of
supervision, where a child failing must leave its siblings untouched. It would
also never fire: the loop below converts every exception into a decision, so
nothing escapes for a group to react to. Instead each cell creates one task and
owns an explicit children map, and the absence of orphaned tasks is an invariant
the runtime holds and the test suite asserts.
"""

import asyncio
import contextlib
import itertools
import random
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Generic, TypeVar, cast

from tapio.actor.ask import ask as run_ask
from tapio.actor.behavior import (
    Behavior,
    Directive,
    ReceivingBehavior,
    SetupBehavior,
    SuperviseBehavior,
    directive_of,
)
from tapio.actor.context import ActorContext
from tapio.actor.dead_letters import DeadLetterOffice, DeadLetterReason
from tapio.actor.mailbox import Envelope, Mailbox, MailboxConfig
from tapio.actor.path import ActorPath
from tapio.actor.ref import ActorRef
from tapio.actor.signals import ChildFailed, PostStop, PreRestart, Signal, Terminated
from tapio.actor.supervision import Decision, SupervisorStrategy
from tapio.actor.watch import Watcher
from tapio.dispatch.dispatcher import Dispatcher
from tapio.errors import (
    ActorNameError,
    ActorSystemTerminating,
    BehaviorTypeError,
    MailboxFullError,
    WatchError,
)
from tapio.logging import ActorLogAdapter, actor_logger, runtime_logger
from tapio.message import Message
from tapio.settings import TapioSettings
from tapio.validation import MessageValidator, resolve_validator

__all__ = ["ActorCell", "ActorRuntime", "LocalActorRef"]

T = TypeVar("T", bound=Message)
U = TypeVar("U", bound=Message)
R = TypeVar("R", bound=Message)

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


@dataclass(eq=False)
class ActorRuntime:
    """The slice of an actor system that a cell needs.

    Passing this rather than the system itself keeps the dependency one-way,
    and means a cell can be exercised in a test without standing up guardians.
    """

    name: str
    """The system name, and so the first element of every path."""

    settings: TapioSettings
    """Tunables shared by every cell in the system."""

    dispatcher: Dispatcher
    """The loop cells create their tasks on, and whose clock times shutdown."""

    dead_letters: DeadLetterOffice
    """Where a message goes when its recipient cannot take it."""

    terminated: bool = False
    """Whether the system has finished shutting down.

    Read only to tell one dead-letter reason from another: a message sent to a
    stopped actor and a message sent after the system went away are different
    diagnoses, and the sender usually cares which.
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

        Uid 0 means "no incarnation", so the counter starts at 1. The uid is
        what stops a ref to a dead actor from silently addressing a new actor
        spawned under the same name.
        """
        return next(self._uids)


class LocalActorRef(ActorRef[T]):
    """A ref that delivers into a local cell's mailbox.

    It stays a valid handle after its actor dies. Sending to a dead actor is
    not an error, it is a dead letter, because a point-in-time liveness check
    is stale the moment you have it.
    """

    __slots__ = ("_cell",)

    def __init__(self, cell: "ActorCell[T]") -> None:
        """Bind the ref to its cell."""
        super().__init__(cell.path)
        self._cell = cell

    @property
    def cell(self) -> "ActorCell[T]":
        """The cell this ref delivers into.

        For the runtime, which needs the object behind the handle to register a
        death watch. Application code has a ref and needs nothing more.
        """
        return self._cell

    def tell(self, message: T) -> None:
        """Deliver a message, without waiting and without blocking.

        Safe to call from any thread. Validation runs on the calling thread,
        before any hop, so an error about the message reaches the code that
        wrote it; delivery then happens on the system's loop.

        The split is one line: **the message is yours, the recipient is not.**
        Errors about the message raise here. Errors about the recipient, a
        stopped actor or a full mailbox, are resolved on the target's loop
        after this call has returned, where nothing can be raised into the
        caller, so they become dead letters.

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

        Backpressure is a property of the mailbox rather than of the send, so
        this is `tell` plus waiting, and on an unbounded mailbox the two are
        the same thing.

        Args:
            message: The message to deliver.

        Raises:
            MessageTypeError: If the message does not match the target's
                declared message type.
            RuntimeError: If called from a thread that is not running the
                system's loop. Awaiting capacity across a thread boundary is a
                bridge too far; use `tell` from other threads.
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
        until the reply lands, which is occasionally what you want and usually
        not: an actor that asks and waits is one that cannot answer.

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
        # The behavior as first given, not as it is now. A restart re-evaluates
        # this one, so an actor that has switched behaviors several times still
        # comes back as what it originally was.
        self._initial = behavior
        self._behavior: Behavior[T] = behavior
        self._supervisors: tuple[_Supervisor, ...] = ()
        # Timestamps of recent restarts, for the window; the count is kept
        # separately because the backoff exponent is about how many times this
        # actor has failed, not about how many are still inside the window.
        self._restarts: deque[float] = deque()
        self._restart_count = 0
        self._mailbox = Mailbox(
            mailbox if mailbox is not None else runtime.settings.default_mailbox
        )
        self._children: dict[str, ActorCell[Any]] = {}
        self._anonymous = itertools.count(1)
        # Both sides of every watch, so that neither a watcher nor a watched
        # actor leaves an entry behind in the other when it stops. The watchers
        # are not all cells: an ask's promise watches its target too.
        self._watchers: dict[ActorPath, Watcher] = {}
        self._watching: dict[ActorPath, ActorCell[Any]] = {}
        self._log = actor_logger(path)
        self._ctx: ActorContext[T] = _CellContext(self)
        self._ref: LocalActorRef[T] = LocalActorRef(self)
        self._task: asyncio.Task[None] | None = None
        self._terminated: asyncio.Future[None] = runtime.dispatcher.loop.create_future()
        self._alive = True
        self._terminating = False
        self._current: Message | None = None
        # Replaced in `start` once the behavior has declared its message type.
        # A cell that stops during setup never gets one, and there is no honest
        # type check to make without it.
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

        Exposed so that "the watch was released" is a thing a test can assert
        rather than infer: a registry that outlives what it names is exactly
        the leak death watch exists to prevent.
        """
        return tuple(self._watchers)

    @property
    def is_alive(self) -> bool:
        """Whether this actor is still accepting messages.

        For the runtime's own use. Application code watches instead: a
        liveness answer is stale by the time the caller reads it.
        """
        return self._alive

    def start(self) -> None:
        """Evaluate the behavior, resolve validation, and start the loop.

        Deferred construction runs here, synchronously, rather than inside the
        new task: the message type comes from the behavior it produces, and the
        ref is usable the instant `spawn` returns, so the type has to be known
        by then.
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

        self._validate = resolve_validator(
            msg_type=msg_type,
            settings=self._runtime.settings,
            target=self._path,
        )
        self._task = self._runtime.dispatcher.spawn_task(
            self._run(), name=f"tapio-cell:{self._path}"
        )

    def validate(self, message: Message) -> None:
        """Check a message against this actor's declared type and model.

        Called on the sender's thread, before any hop onto the system's loop,
        because an error about the message is the sender's bug to handle.
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

        Identical to `deliver` except that a `FAIL` mailbox at capacity cannot
        raise: the sender is on another thread and has long since moved on, so
        the overflow becomes a dead letter like every other recipient error.
        """
        try:
            self.deliver(message)
        except MailboxFullError:
            self._dead_letter(message, DeadLetterReason.MAILBOX_FULL)

    async def offer(self, message: Message) -> None:
        """Put an already-validated message on the user lane, waiting if full.

        Liveness is read through a call rather than the attribute so that the
        second check is honest: the actor can stop while this sender is parked,
        which is exactly the case the check below exists for.
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

    def watch(self, ref: ActorRef[Any]) -> None:
        """Ask to be told when another actor stops.

        Args:
            ref: The actor to watch.

        Raises:
            WatchError: If the ref has no live cell behind it, or if an actor
                tries to watch itself.
        """
        target = _cell_behind(ref)
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

        Harmless if this actor was not watching it, and it does not retract a
        `Terminated` already queued: by then the fact is true.

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

        On the system lane, so it outranks whatever user traffic is queued, and
        as an ordinary signal rather than an exception injected across a task
        boundary: there is no clean way to do the latter, and its ordering
        against this actor's in-flight message would be undefined.
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

        The deadline is the whole tree's, not this cell's. A per-cell timeout
        would make worst-case shutdown depth times timeout, which is not what
        anyone configuring "shut down within ten seconds" means.

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

    async def _deliver_signal(self, signal: Signal) -> Behavior[T] | None:
        """Hand a signal to the behavior, or `None` if it has no handler."""
        behavior = self._behavior
        if not isinstance(behavior, ReceivingBehavior):
            return None
        return await behavior.receive_signal(self._ctx, signal)

    async def _run_lifecycle_hook(self, signal: Signal) -> None:
        """Deliver `PostStop` or `PreRestart`, whose result cannot change anything.

        A failure here is logged rather than supervised. The actor is already
        stopping or already restarting, and running a second decision over the
        first would leave the sequence in a state with no name.
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

        strategy = self._strategy_for(error)
        match strategy.decision:
            case Decision.RESUME:
                self._log.warning(
                    "resumed after a failure in %s",
                    self._describe_current(),
                    exc_info=error,
                )
            case Decision.RESTART:
                await self._restart(error, strategy)
            case Decision.STOP:
                self._log.error(
                    "stopping after a failure in %s",
                    self._describe_current(),
                    exc_info=error,
                )
                await self._stop_self()
            case Decision.ESCALATE:
                await self._escalate(error)

    def _strategy_for(self, error: Exception) -> SupervisorStrategy:
        """Find the strategy governing a failure, outermost wrapper first."""
        for supervisor in self._supervisors:
            if isinstance(error, supervisor.on):
                return supervisor.strategy
        return _STOP

    async def _restart(self, error: Exception, strategy: SupervisorStrategy) -> None:
        """Rebuild this actor from the behavior it was spawned with.

        Everything here is observable, and every line of it is a decision:
        children are stopped and respawned by the re-run setup, the mailbox
        survives with both lanes intact, the failed message does not, and
        watchers are told nothing, because the ref, the path and the uid are
        unchanged and only the incarnation behind them is new.
        """
        if not self._within_restart_limit(strategy):
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
        await self._stop_children(self._own_deadline())

        if strategy.backoff is not None:
            delay = strategy.backoff.delay(self._restart_count, jitter=random.random())
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

    def _within_restart_limit(self, strategy: SupervisorStrategy) -> bool:
        """Record this restart and say whether it is still inside the limit.

        Timestamps are only kept when there is a limit to count them against,
        so an actor restarting on an unlimited strategy for a month does not
        accumulate a month of them.
        """
        self._restart_count += 1
        if strategy.max_restarts is None:
            return True
        now = self._runtime.dispatcher.now()
        if strategy.window is not None:
            horizon = now - strategy.window.total_seconds()
            while self._restarts and self._restarts[0] < horizon:
                self._restarts.popleft()
        self._restarts.append(now)
        return len(self._restarts) <= strategy.max_restarts

    async def _backoff(self, seconds: float) -> bool:
        """Wait out a backoff window, staying responsive to a stop.

        The cell stops dequeuing user messages and its mailbox keeps filling:
        the actor is absent, not dead, and `tell` stays total. On an unbounded
        mailbox a long window is therefore a memory risk proportional to
        inbound rate times window, which is why the docs recommend a bounded
        mailbox for actors that back off.

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
        # A note rather than a wrapper exception: the chain reads in the
        # traceback of the original error, which is the thing anyone debugging
        # this actually wants, and nothing has to unwrap anything.
        error.add_note(f"escalated from {self._path}")
        parent = self._parent
        if parent is not None:
            parent.child_failed(self._ref, error)
        await self._stop_self()

    async def _fail_the_system(self, error: Exception) -> None:
        """End the system, because a failure reached a guardian.

        A guardian is the top of the tree, so an escalation that arrives here
        has run out of actors willing to take responsibility for it. Carrying
        on with a silently missing subtree is worse than stopping: the service
        embedding tapio awaits `when_terminated`, sees the cause, and decides
        whether to exit or rebuild, which is where that decision belongs.
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

        The cell's declared message type is fixed at spawn: it is the contract
        every ref to this actor was validated against, so switching behaviors
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

        Both wrappers are peeled in one loop because either can enclose the
        other: `supervise(setup(...))` and `setup` returning a supervised
        behavior are both things people write.

        Args:
            behavior: What to evaluate.
            keep_supervisors: Keep the strategies already in force when the new
                behavior declares none. Set when a handler returned a behavior,
                since supervision belongs to the actor rather than to whichever
                behavior it currently holds; left off at start and at restart,
                where the strategies are being established.

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
        when the task is being cancelled, and awaiting anything there would
        risk never completing the very sequence that releases the actor.
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
        self._mailbox.close()
        self._drain_to_dead_letters()
        self._release_watches()
        if self._parent is not None:
            self._parent._remove_child(self)
        self._log.debug("stopped")
        self._terminated.set_result(None)

    def _release_watches(self) -> None:
        """Tell the watchers, and leave nothing behind in the watched.

        Both directions matter for the same reason: a registry that outlives
        the actor it names is the leak this feature exists to save users from
        writing themselves.
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

    def _remove_child(self, child: "ActorCell[Any]") -> None:
        """Deregister a stopped child, freeing its name for reuse."""
        if self._children.get(child.path.name) is child:
            del self._children[child.path.name]

    def _dead_letter(self, message: Message, reason: str) -> None:
        """Account for a message that had nowhere to go."""
        self._runtime.dead_letters.publish(message, self._path, reason)

    def _still_alive(self) -> bool:
        """Whether this actor is still reading its mailbox, checked afresh."""
        return self._alive

    def _unreachable_reason(self) -> str:
        """Say why this actor could not take a message.

        A stopped actor and a stopped system are different diagnoses, and the
        sender usually cares which: one is an ordering bug in the application,
        the other is a send that outlived its runtime.
        """
        if self._runtime.terminated:
            return DeadLetterReason.SYSTEM_TERMINATED
        return DeadLetterReason.RECIPIENT_TERMINATED

    def _drain_to_dead_letters(self) -> None:
        """Send everything left on the user lane to dead letters."""
        reason = self._unreachable_reason()
        while (pending := self._mailbox.take_pending()) is not None:
            self._runtime.dead_letters.publish(pending, self._path, reason)

    def __repr__(self) -> str:
        """Render the path and the current behavior."""
        return f"ActorCell({str(self._path)!r}, {self._behavior!r})"


def _cell_behind(ref: ActorRef[Any]) -> ActorCell[Any]:
    """Find the cell a ref delivers into.

    Raises:
        WatchError: If the ref is not one a running system handed out.
    """
    if isinstance(ref, LocalActorRef):
        return ref.cell
    msg = (
        f"cannot watch {ref!r}: it is not a ref to a live actor in this "
        "system. Watch a ref obtained from spawn or carried in a message."
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

    def watch(self, ref: ActorRef[Any]) -> None:
        """Ask to be sent `Terminated` when another actor stops."""
        self._cell.watch(ref)

    def unwatch(self, ref: ActorRef[Any]) -> None:
        """Stop watching an actor."""
        self._cell.unwatch(ref)

    def __repr__(self) -> str:
        """Render the actor this context belongs to."""
        return f"ActorContext({str(self._cell.path)!r})"
