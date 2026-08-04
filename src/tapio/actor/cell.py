"""`ActorCell`: one actor, one task, one mailbox.

The cell is the runtime object behind every ref. It owns the receive loop, the
current behavior, the children, and the termination sequence.

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
from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar, cast

from tapio.actor.behavior import (
    Behavior,
    Directive,
    ReceivingBehavior,
    SetupBehavior,
    directive_of,
)
from tapio.actor.context import ActorContext
from tapio.actor.dead_letters import DeadLetterOffice, DeadLetterReason
from tapio.actor.mailbox import Mailbox, MailboxConfig
from tapio.actor.path import ActorPath
from tapio.actor.ref import ActorRef
from tapio.actor.signals import PostStop, Signal
from tapio.dispatch.dispatcher import Dispatcher
from tapio.errors import (
    ActorNameError,
    ActorSystemTerminating,
    BehaviorTypeError,
    MailboxFullError,
)
from tapio.logging import ActorLogAdapter, actor_logger, runtime_logger
from tapio.message import Message
from tapio.settings import TapioSettings
from tapio.validation import MessageValidator, resolve_validator

__all__ = ["ActorCell", "ActorRuntime", "LocalActorRef"]

T = TypeVar("T", bound=Message)
U = TypeVar("U", bound=Message)

_log = runtime_logger("runtime")

_MAX_SETUP_DEPTH = 100
"""How many rounds of deferred construction to allow before calling it a loop."""


def _accept_anything(message: Message) -> None:
    """Stand in for validation on a cell that never resolved a message type."""


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


class ActorCell(Generic[T]):
    """One actor: a mailbox, a behavior, a task, and its children."""

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
        self._mailbox = Mailbox(
            mailbox if mailbox is not None else runtime.settings.default_mailbox
        )
        self._children: dict[str, ActorCell[Any]] = {}
        self._anonymous = itertools.count(1)
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
                    self._on_signal(envelope)
                else:
                    await self._on_message(envelope)
        finally:
            # Cancellation reaches here too. It is a BaseException, so it is
            # never caught below and never becomes a supervision decision: a
            # cancelled actor stops, it does not restart.
            self._finish()

    def _on_signal(self, signal: Signal) -> None:
        """Handle a system-lane signal."""
        if isinstance(signal, PostStop):
            # Just the wakeup. The hook itself runs in `_finish`, so that an
            # actor which stops on its own gets it on the same path.
            self._alive = False

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
        except Exception:
            # Until supervision lands, the one decision available is to stop.
            # The exception never leaves this loop either way.
            self._log.exception(
                "failed while handling %s; stopping", type(message).__name__
            )
            await self._stop_self()
            return
        finally:
            self._current = None
        await self._become(nxt, message)

    def _unhandled(self, message: Message) -> None:
        """Report a message the current behavior does not handle."""
        self._log.debug("unhandled %s", type(message).__name__)

    async def _become(self, nxt: Behavior[T], message: Message) -> None:
        """Apply what a handler returned.

        The cell's declared message type is fixed at spawn: it is the contract
        every ref to this actor was validated against, so switching behaviors
        changes what the actor does, never what it accepts.
        """
        directive = directive_of(nxt)
        if directive is Directive.SAME:
            return
        if directive is Directive.UNHANDLED:
            self._unhandled(message)
            return
        if directive is Directive.STOPPED:
            await self._stop_self()
            return
        self._behavior = self._evaluate(nxt)

    async def _stop_self(self) -> None:
        """Stop because this actor asked to, rather than because a parent did."""
        self._terminating = True
        seconds = self._runtime.settings.shutdown_timeout.total_seconds()
        await self._stop_children(self._runtime.dispatcher.now() + seconds)
        self._alive = False

    def _evaluate(self, behavior: Behavior[T]) -> Behavior[T]:
        """Run deferred construction until a real behavior comes out."""
        seen = 0
        while isinstance(behavior, SetupBehavior):
            behavior = behavior.setup(self._ctx)
            seen += 1
            if seen > _MAX_SETUP_DEPTH:
                msg = (
                    f"{self._path}: deferred construction is still returning "
                    f"another Behaviors.setup after {_MAX_SETUP_DEPTH} rounds"
                )
                raise BehaviorTypeError(msg)
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
        if self._parent is not None:
            self._parent._remove_child(self)
        self._log.debug("stopped")
        self._terminated.set_result(None)

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

    def __repr__(self) -> str:
        """Render the actor this context belongs to."""
        return f"ActorContext({str(self._cell.path)!r})"
