"""`ActorSystem`: the tree, its guardians, and its shutdown.

A system owns two top-level actors. `/user` is the parent of everything the
application spawns; `/system` is reserved for the runtime's own actors, and is
stopped last so that runtime facilities outlive the actors that use them.

Nothing here is global. Several systems can share a process and a loop, and
they share nothing but that loop.
"""

import asyncio
from collections.abc import Callable
from contextlib import AbstractContextManager
from types import TracebackType
from typing import Any, Self, TypeVar

from tapio.actor.behavior import Behavior, Behaviors
from tapio.actor.cell import ActorCell, ActorRuntime
from tapio.actor.dead_letters import DeadLetterOffice, DeadLetterReason, DeadLetterRef
from tapio.actor.mailbox import MailboxConfig
from tapio.actor.path import ActorPath
from tapio.actor.ref import ActorRef
from tapio.dispatch.dispatcher import Dispatcher
from tapio.errors import ActorSystemTerminating
from tapio.logging import ActorLogAdapter, actor_logger
from tapio.message import Message
from tapio.remote.address import Address
from tapio.remote.codec import receive_frame
from tapio.remote.context import use_context
from tapio.remote.registry import RefRegistry
from tapio.settings import RemoteSettings, TapioSettings

__all__ = ["ActorSystem", "PeerResolver"]

T = TypeVar("T", bound=Message)

PeerResolver = Callable[[Address, ActorPath], "ActorRef[Any] | None"]
"""Turns another system's address and a path into a ref that reaches it.

The association layer installs one when remoting is switched on. Without one,
and for an address no link exists to, a foreign ref resolves to a dead-letter
target: `tell` stays total, and the message is accounted for rather than
dropped.
"""


class _GuardianMessage(Message):
    """A message type no one can send: guardians receive no user traffic."""


async def _guardian_receive(message: _GuardianMessage) -> Behavior[_GuardianMessage]:
    """Handle nothing. A guardian exists to be a parent, not a recipient."""
    return Behaviors.same()


def _guardian() -> Behavior[_GuardianMessage]:
    """Build a guardian's behavior."""
    return Behaviors.receive_message(_guardian_receive, msg_type=_GuardianMessage)


def _canonical_address(name: str, remote: RemoteSettings | None) -> Address:
    """Work out the address this system's refs write down.

    Canonical falls back to bind, which is right for the ordinary case of a
    process whose peers dial the interface it listens on, and overridable for
    the ones where they cannot: containers, NAT and port mapping.
    """
    if remote is None:
        return Address(system=name)
    return Address(
        system=name,
        host=remote.canonical_host or remote.bind_host,
        port=remote.canonical_port or remote.bind_port,
    )


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
        self._address = _canonical_address(name, self._settings.remote)
        self._refs = RefRegistry()
        self._peer_resolver: PeerResolver | None = None
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
            address=self._address,
            refs=self._refs,
            settings=self._settings,
            dispatcher=dispatcher,
            dead_letters=self._dead_letters,
            guardian_failure=self._on_guardian_failure,
        )
        self._log: ActorLogAdapter = actor_logger(self._root)
        self._terminating = False
        self._terminated: asyncio.Event = asyncio.Event()
        self._failure: BaseException | None = None

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
    def address(self) -> Address:
        """How peers address this system, and what its refs write down.

        The canonical address when remoting is configured, which is what a peer
        dials and not necessarily what a socket is bound to. Otherwise the
        system name alone: a ref from a system with remoting off says which
        system it belongs to and offers nowhere to dial.
        """
        return self._address

    @property
    def refs(self) -> RefRegistry:
        """The live refs of this system, by path and incarnation uid.

        Exposed for the same reason `watchers` is on a cell: "the registry was
        emptied" has to be something a test can assert rather than infer, and
        an entry outliving its actor is a leak of exactly the kind the runtime
        promises not to have.
        """
        return self._refs

    @property
    def is_terminating(self) -> bool:
        """Whether shutdown has begun."""
        return self._terminating

    def set_peer_resolver(self, resolve: PeerResolver | None) -> None:
        """Install what turns another system's address into a usable ref.

        The association layer calls this when remoting starts. Until then, and
        for any address it has no link to, a foreign ref resolves to a
        dead-letter target.

        Args:
            resolve: Returns a ref for an address it can reach, and `None` for
                one it cannot.
        """
        self._peer_resolver = resolve

    def as_deserialization_context(self) -> AbstractContextManager[None]:
        """Make this system the one refs deserialize against inside the block.

        A ref's string form only means something relative to a system: the
        reading system has to know whether the address is its own, so it can
        hand back the live local ref rather than a proxy to itself. The
        receiving end of a link enters this for the duration of a decode, and
        it is exported so that `Greet.model_validate_json(blob)` is a thing a
        test or a debugging session can do deliberately.

        Returns:
            A context manager. Refs resolve against this system inside it, and
            raise [RefResolutionError][tapio.errors.RefResolutionError] outside.
        """
        return use_context(self)

    def resolve(self, address: Address, path: ActorPath) -> ActorRef[Any]:
        """Turn an address and a path into something that can be told messages.

        This is the whole of ref resolution, and it never raises about the
        target. There are three answers:

        * The address is this system's own, and a live actor answers to that
          path and uid: the live local ref, so a reply to a `reply_to` that
          crossed a link is an ordinary local `tell` on the way back.
        * The address is this system's own and nothing answers: a dead-letter
          target. The uid is what makes this safe. A path on its own is
          reusable, so without it a stale ref would address whoever occupies
          that path now.
        * The address is another system's: the peer resolver's answer, or a
          dead-letter target when there is no link to that address.

        Args:
            address: The system the ref names.
            path: Where in that system it points.

        Returns:
            A ref. Always.
        """
        if address.system == self.name and (
            not address.is_addressable or address == self._address
        ):
            local = self._refs.lookup(path)
            if local is not None:
                return local
            return DeadLetterRef(
                path,
                dead_letters=self._dead_letters,
                reason=DeadLetterReason.UNKNOWN_RECIPIENT,
            )
        if self._peer_resolver is not None:
            remote = self._peer_resolver(address, path)
            if remote is not None:
                return remote
        return DeadLetterRef(
            path,
            dead_letters=self._dead_letters,
            reason=DeadLetterReason.NO_ASSOCIATION,
            peer=address,
        )

    def deliver_frame(self, data: bytes, *, peer: Address | None = None) -> None:
        """Take one frame off a link and deliver what is in it.

        The receiving half of remoting, and the reason it is a plain method:
        every failure a peer can inflict is decided here, with no socket in
        sight, so all of it is testable by handing one system the bytes another
        one produced.

        It never raises. A size, a version, a type key this system does not
        know, a payload that will not validate, a recipient that has stopped
        and a message the recipient does not accept all become dead letters on
        this system's stream, carrying the peer address when there is one.

        Args:
            data: One complete frame, length prefix included.
            peer: Where it came from, recorded on any dead letter.
        """
        remote = self._settings.remote
        receive_frame(
            data,
            context=self,
            dead_letters=self._dead_letters,
            max_frame_bytes=remote.max_frame_bytes if remote is not None else None,
            peer=peer,
        )

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

    def _on_guardian_failure(self, path: ActorPath, error: BaseException) -> None:
        """Bring the system down, because a failure escalated past a guardian.

        Keeping the cause and terminating are two halves of one answer: a
        system with a silently missing subtree is worse than one that stopped,
        and a service embedding tapio awaits `when_terminated` to decide
        whether to exit or rebuild.

        Termination runs as its own task rather than inline, because the caller
        is the guardian's own receive loop and shutdown waits for that loop to
        finish.
        """
        if self._failure is None:
            self._failure = error
        if self._terminating:
            return
        self._runtime.dispatcher.spawn_task(
            self.terminate(), name=f"tapio-terminate:{path}"
        )

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
        """Wait until the system has finished shutting down.

        This is where an escalation that reached a guardian surfaces. Nothing
        else in the runtime can raise it: the failing actor's exception never
        leaves its own receive loop, and its supervisors all declined to take
        responsibility, so the last place to report it is the one the embedding
        service is already waiting on.

        Raises:
            BaseException: The original failure, if the system terminated
                because one escalated to a guardian.
        """
        await self._terminated.wait()
        if self._failure is not None:
            raise self._failure

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
