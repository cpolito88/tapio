"""`ActorSystem`: the tree, its guardians, and its shutdown.

A system owns two top-level actors. `/user` is the parent of everything the
application spawns; `/system` is reserved for the runtime's own actors, and is
stopped last so that runtime facilities outlive the actors that use them.

Nothing here is global. Several systems can share a process and a loop, and
they share nothing but that loop.
"""

import asyncio
from types import TracebackType
from typing import Self, TypeVar

from tapio.actor.behavior import Behavior, Behaviors
from tapio.actor.cell import ActorCell, ActorRuntime
from tapio.actor.dead_letters import DeadLetterOffice
from tapio.actor.mailbox import MailboxConfig
from tapio.actor.path import ActorPath
from tapio.actor.ref import ActorRef
from tapio.dispatch.dispatcher import Dispatcher
from tapio.errors import ActorSystemTerminating
from tapio.logging import ActorLogAdapter, actor_logger
from tapio.message import Message
from tapio.settings import TapioSettings

__all__ = ["ActorSystem"]

T = TypeVar("T", bound=Message)


class _GuardianMessage(Message):
    """A message type no one can send: guardians receive no user traffic."""


async def _guardian_receive(message: _GuardianMessage) -> Behavior[_GuardianMessage]:
    """Handle nothing. A guardian exists to be a parent, not a recipient."""
    return Behaviors.same()


def _guardian() -> Behavior[_GuardianMessage]:
    """Build a guardian's behavior."""
    return Behaviors.receive_message(_guardian_receive, msg_type=_GuardianMessage)


class ActorSystem:
    """One actor tree, running on the loop that created it.

    Create it inside a coroutine, spawn actors under `/user`, and terminate it
    when done:

    ```python
    async with ActorSystem("hello") as system:
        greeter = system.spawn(greeter_behavior(), name="greeter")
        greeter.tell(Greet(whom="world"))
    ```

    Leaving the `async with` block terminates the system, draining the tree
    bottom-up against a single deadline.
    """

    def __init__(
        self, name: str = "tapio", settings: TapioSettings | None = None
    ) -> None:
        """Start a system and its guardians on the caller's event loop.

        Args:
            name: The system name, which is the authority in every actor path.
            settings: Tunables for this system. Read from the environment when
                omitted.

        Raises:
            RuntimeError: If called outside a running event loop. A system is
                built out of tasks and has nowhere to put them.
            ValueError: If the name would not make a legal actor path.
        """
        self._settings = settings if settings is not None else TapioSettings()
        self._root = ActorPath.root(name)
        dispatcher = Dispatcher.from_running_loop()
        self._dead_letters = DeadLetterOffice(
            log_first=self._settings.dead_letter_log_first,
            summary_interval=(
                self._settings.dead_letter_summary_interval.total_seconds()
            ),
            clock=dispatcher.now,
        )
        self._runtime = ActorRuntime(
            name=name,
            settings=self._settings,
            dispatcher=dispatcher,
            dead_letters=self._dead_letters,
        )
        self._log: ActorLogAdapter = actor_logger(self._root)
        self._terminating = False
        self._terminated: asyncio.Event = asyncio.Event()

        self._user: ActorCell[_GuardianMessage] = self._guardian_cell("user")
        self._system: ActorCell[_GuardianMessage] = self._guardian_cell("system")
        self._log.debug("started")

    def _guardian_cell(self, name: str) -> ActorCell[_GuardianMessage]:
        """Create and start one top-level guardian."""
        cell: ActorCell[_GuardianMessage] = ActorCell(
            runtime=self._runtime,
            path=self._root.child(name),
            behavior=_guardian(),
        )
        cell.start()
        return cell

    @property
    def name(self) -> str:
        """The system name, and the authority in every path below it."""
        return self._runtime.name

    @property
    def settings(self) -> TapioSettings:
        """The tunables this system runs with."""
        return self._settings

    @property
    def log(self) -> ActorLogAdapter:
        """A logger tagged with the system's root path."""
        return self._log

    @property
    def dead_letters(self) -> DeadLetterOffice:
        """Where undeliverable messages go, and what to subscribe to.

        Subscribing is what makes an absence testable: without it, "the message
        was dropped" and "the code never ran" look identical.
        """
        return self._dead_letters

    @property
    def is_terminating(self) -> bool:
        """Whether shutdown has begun."""
        return self._terminating

    def spawn(
        self,
        behavior: Behavior[T],
        name: str,
        mailbox: MailboxConfig | None = None,
    ) -> ActorRef[T]:
        """Start a top-level actor under `/user`.

        Args:
            behavior: What the actor does.
            name: Its name, unique among top-level actors.
            mailbox: Capacity and overflow behaviour. The system's default when
                omitted.

        Returns:
            A ref to the new actor.

        Raises:
            ActorSystemTerminating: If the system is shutting down.
            ActorNameError: If a live top-level actor already has that name.
        """
        self._reject_if_terminating(name)
        return self._user.spawn(behavior, name, mailbox)

    def spawn_anonymous(
        self, behavior: Behavior[T], mailbox: MailboxConfig | None = None
    ) -> ActorRef[T]:
        """Start a top-level actor under a generated name.

        Args:
            behavior: What the actor does.
            mailbox: Capacity and overflow behaviour. The system's default when
                omitted.

        Returns:
            A ref to the new actor.

        Raises:
            ActorSystemTerminating: If the system is shutting down.
        """
        self._reject_if_terminating("an anonymous actor")
        return self._user.spawn_anonymous(behavior, mailbox)

    def _reject_if_terminating(self, name: str) -> None:
        """Refuse a spawn once shutdown has begun."""
        if self._terminating:
            msg = (
                f"cannot spawn {name!r} in {self.name}: the system is shutting "
                "down. A spawn during shutdown is an ordering bug, and raising "
                "surfaces it instead of leaving an inert actor behind."
            )
            raise ActorSystemTerminating(msg)

    async def terminate(self) -> None:
        """Stop every actor, bottom-up, and wait for the tree to drain.

        Every cell races one deadline taken from `shutdown_timeout`, so the
        worst case tracks that setting rather than the depth of the tree. An
        actor still wedged in a handler when the deadline passes is cancelled,
        and the warning names it.

        Calling this more than once is safe: later callers wait for the first.
        """
        if self._terminating:
            await self._terminated.wait()
            return

        self._terminating = True
        deadline = (
            self._runtime.dispatcher.now()
            + self._settings.shutdown_timeout.total_seconds()
        )
        # Application actors first: the system guardian holds runtime
        # facilities that theirs may still be using on the way out.
        await self._user.stop(deadline)
        await self._system.stop(deadline)
        # Set before the event: a send racing shutdown should be told the
        # system is gone, which is a different diagnosis from one actor having
        # stopped while the rest of the tree runs on.
        self._runtime.terminated = True
        self._terminated.set()
        self._log.debug("terminated")

    async def when_terminated(self) -> None:
        """Wait until the system has finished shutting down."""
        await self._terminated.wait()

    async def __aenter__(self) -> Self:
        """Return the running system."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Terminate the system on the way out, however the block ended."""
        await self.terminate()

    def __repr__(self) -> str:
        """Render the system name and whether it is still running."""
        state = "terminating" if self._terminating else "running"
        return f"ActorSystem({self.name!r}, {state})"
