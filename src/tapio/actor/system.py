"""`ActorSystem`: the tree, its guardians, and its shutdown.

A system owns two top-level actors. `/user` is the parent of everything the
application spawns; `/system` is reserved for the runtime's own actors, and is
stopped last so that runtime facilities outlive the actors that use them.

Nothing here is global. Several systems can share a process and a loop, and
they share nothing but that loop.
"""

import asyncio
import secrets
import socket
from collections.abc import Callable
from contextlib import AbstractContextManager
from types import TracebackType
from typing import Any, Final, Self, TypeVar, cast

from tapio.actor.behavior import Behavior, Behaviors
from tapio.actor.cell import ActorCell, ActorRuntime, LocalActorRef
from tapio.actor.dead_letters import DeadLetterOffice, DeadLetterReason, DeadLetterRef
from tapio.actor.events import EventStream
from tapio.actor.mailbox import MailboxConfig
from tapio.actor.path import ActorPath
from tapio.actor.ref import ActorRef
from tapio.dispatch.blocking import BlockingPool
from tapio.dispatch.dispatcher import Dispatcher
from tapio.errors import ActorSystemTerminating, RefResolutionError
from tapio.logging import ActorLogAdapter, actor_logger
from tapio.message import Message
from tapio.remote.address import Address, parse_ref
from tapio.remote.codec import receive_frame
from tapio.remote.context import use_context
from tapio.remote.endpoint import RemoteEndpoint, open_listener
from tapio.remote.registry import RefRegistry
from tapio.settings import RemoteSettings, TapioSettings
from tapio.validation import normalize_msg_type

__all__ = ["ActorSystem", "PeerResolver"]

T = TypeVar("T", bound=Message)

_UID_BITS: Final = 63
"""How much randomness goes into a system's incarnation uid.

Random rather than sequential: two systems that restart do not coordinate, and
a counter would hand the same uid to different incarnations on different hosts.
"""

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


def _canonical_address(
    name: str, remote: RemoteSettings | None, listener: socket.socket | None
) -> Address:
    """Work out the address this system's refs write down.

    The canonical address falls back to the bind address, which is right when
    peers dial the interface the process listens on. It can be overridden for
    containers, NAT and port mapping, where they cannot. The port comes from
    the socket rather than the setting, so `bind_port=0` advertises the port
    the OS handed out.
    """
    if remote is None or listener is None:
        return Address(system=name)
    return Address(
        system=name,
        host=remote.canonical_host or remote.bind_host,
        port=remote.canonical_port or listener.getsockname()[1],
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
        self._refs = RefRegistry()
        self._peer_resolver: PeerResolver | None = None
        # Minted per incarnation, so a system restarted on the same host and
        # port is a different peer rather than a slow one. Without it, nothing
        # built on a link could tell a restart from a pause, and every ref
        # held against the previous incarnation would resolve to a stranger.
        self._uid = secrets.randbits(_UID_BITS)
        # Bound before anything else, so the canonical address is settled
        # before any ref can write itself down, and so a configuration that
        # would listen to the world fails here rather than at the first
        # connection.
        self._listener = (
            open_listener(self._settings.remote)
            if self._settings.remote is not None
            else None
        )
        try:
            self._build(name)
        except BaseException:
            # The port is bound by now, and nothing else has a reference to
            # this half-built system to close it later. Leaving it open means
            # the next attempt on a fixed port fails to bind, which reads as a
            # configuration problem rather than as the failure that caused it.
            if self._listener is not None:
                self._listener.close()
            raise

    def _build(self, name: str) -> None:
        """Finish construction, with the listening socket already bound.

        Split out so that `__init__` has one place to close that socket if any
        of this raises. `Dispatcher.from_running_loop` alone is enough reason:
        it runs after the bind and raises when there is no running loop.

        Args:
            name: The system name, and the authority in every path below it.
        """
        self._address = _canonical_address(name, self._settings.remote, self._listener)
        dispatcher = Dispatcher.from_running_loop()
        self._dead_letters = DeadLetterOffice(
            log_first=self._settings.dead_letter_log_first,
            summary_interval=(
                self._settings.dead_letter_summary_interval.total_seconds()
            ),
            clock=dispatcher.now,
        )
        self._events = EventStream()
        # Described here, started by the first blocking call. A system that
        # never blocks starts no threads, which is what keeps the thread-leak
        # check meaningful for everything else.
        self._blocking = BlockingPool(
            size=self._settings.blocking_pool_size, system=name
        )
        self._runtime = ActorRuntime(
            name=name,
            address=self._address,
            refs=self._refs,
            settings=self._settings,
            dispatcher=dispatcher,
            dead_letters=self._dead_letters,
            blocking=self._blocking,
            events=self._events,
            guardian_failure=self._on_guardian_failure,
            resolver=lambda uri, expect: self.resolve(uri, expect=expect),
        )
        self._log: ActorLogAdapter = actor_logger(self._root)
        self._terminating = False
        self._terminated: asyncio.Event = asyncio.Event()
        # A guardian failure terminates the tree from a task nobody awaits.
        # The loop holds only a weak reference to a task, so the system holds
        # this one: a shutdown sweep collected halfway through would leave the
        # tree half-stopped with nothing to say so.
        # The drain runs as its own task, held here for two reasons. A caller
        # of terminate() that gives up waiting must not cancel the shutdown it
        # asked for, so the work cannot live on the caller's task. And a
        # guardian failure starts the same drain from a receive loop nobody
        # awaits, where the loop keeps only a weak reference to a task, so the
        # strong one has to be the system's or a sweep can be collected halfway.
        self._draining: asyncio.Task[None] | None = None
        self._failure: BaseException | None = None

        self._user: ActorCell[_GuardianMessage] = self._guardian_cell("user")
        self._system: ActorCell[_GuardianMessage] = self._guardian_cell("system")
        self._remote: RemoteEndpoint | None = None
        if self._listener is not None:
            self._remote = self._start_remoting(self._listener)
        self._log.debug("started")

    def _start_remoting(self, listener: socket.socket) -> RemoteEndpoint:
        """Hang the endpoint under `/system` and start accepting on its socket.

        The endpoint is an actor and the associations are its children, so the
        ordinary stop sweep releases the port and every link under it. No
        separate shutdown step is needed, and none can be forgotten.
        """
        endpoint = RemoteEndpoint(
            runtime=self._runtime,
            uid=self._uid,
            deliver=lambda frame, peer: self.deliver_frame(frame, peer=peer),
            listener=listener,
        )
        ref = self._system.spawn(endpoint.behavior(), "remote")
        endpoint.start(cast("LocalActorRef[Any]", ref).cell)
        self.set_peer_resolver(endpoint.resolve_peer)
        return endpoint

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

        Subscribing is what makes an absence testable. Without it, "the
        message was dropped" and "the code never ran" look the same.
        """
        return self._dead_letters

    @property
    def events(self) -> EventStream:
        """What this system publishes about itself, for whoever subscribes.

        Runtime facts rather than traffic. Today those are
        [PeerUnreachable][tapio.remote.failure.PeerUnreachable] and
        [PeerReachable][tapio.remote.failure.PeerReachable], which is how
        a service learns that a node it was talking to is beyond reach and
        decides whether to log it, alarm, or stop.
        """
        return self._events

    @property
    def address(self) -> Address:
        """How peers address this system, and what its refs write down.

        With remoting configured this is the canonical address, which is what
        a peer dials and not always what the socket is bound to. Otherwise it
        is the system name alone. A ref from a system with remoting off says
        which system it belongs to and gives nowhere to dial.
        """
        return self._address

    @property
    def uid(self) -> int:
        """This incarnation's uid, presented to every peer in the handshake.

        A system restarted on the same host and port is a different peer, and
        the uid is what says so. Without it, a node that restarts inside a
        failure detector's window looks the same as one that was slow.
        """
        return self._uid

    @property
    def remote(self) -> RemoteEndpoint | None:
        """This system's remoting, or `None` when it is switched off.

        It holds the port, the associations, and the resolver behind every
        foreign ref. Exposed so a test or an operator can see which peers are
        associated, instead of inferring it from traffic.
        """
        return self._remote

    @property
    def blocking(self) -> BlockingPool:
        """The threads this system runs blocking calls on.

        Exposed so a test can assert that shutdown left none of them running.
        The pool is the one piece of the runtime that is not a task, so the
        leak invariant does not cover it for free.
        """
        return self._blocking

    @property
    def refs(self) -> RefRegistry:
        """The live refs of this system, by path and incarnation uid.

        Exposed for the same reason as `watchers` on a cell: a test has to be
        able to assert that the registry was emptied. An entry that outlives
        its actor is a leak.
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

        A ref's string form only means something relative to a system. The
        reading system has to know whether the address is its own, so it can
        hand back the live local ref rather than a proxy to itself. The
        receiving end of a link enters this for the duration of a decode. It
        is public so that a test or a debugging session can run
        `Greet.model_validate_json(blob)` on purpose.

        Returns:
            A context manager. Refs resolve against this system inside it, and
            raise [RefResolutionError][tapio.errors.RefResolutionError] outside.
        """
        return use_context(self)

    def resolve_path(self, address: Address, path: ActorPath) -> ActorRef[Any]:
        """Turn an address and a path into something that can be told messages.

        This is all of ref resolution, and it never raises about the target.
        There are three answers:

        * The address is this system's own and a live actor holds that path
          and uid. The answer is the live local ref, so replying to a
          `reply_to` that crossed a link is an ordinary local `tell`.
        * The address is this system's own and nothing holds it. The answer is
          a dead-letter target. The uid is what makes this safe: a path on its
          own is reusable, so without it a stale ref would reach whoever holds
          that path now.
        * The address is another system's. The answer is what the peer
          resolver returns, or a dead-letter target if there is no link.

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

    async def resolve(self, uri: str, *, expect: type[T]) -> ActorRef[T]:
        """Turn a ref's string form into something that can be told messages.

        ```python
        stock = await system.resolve(
            "tapio://inventory@inventory.svc:25520/user/stock", expect=Reserve
        )
        stock.tell(Reserve(sku="X-1", qty=2, reply_to=ctx.self_ref))
        ```

        That is the whole user-facing surface of remote messaging: one
        resolve, then an ordinary ref. Nothing is dialled here. The first send
        through the ref creates the association, and the dial happens behind
        it. So this call does not wait for a peer that may be down, and a
        `tell` to one that never answers dead-letters rather than hanging. The
        ref is bound to the peer and not to a link, so it keeps working after
        a link fails.

        `expect` declares what the actor over there accepts. It is a claim
        about the peer rather than knowledge of it, so it catches a mistake at
        this end. The check that decides runs on the receiving node, against
        the target's real message type.

        Args:
            uri: The full string form, `tapio://sys@host:port/user/x#uid`.
            expect: What the target accepts.

        Returns:
            A ref. The live local one when the address is this system's own,
            since resolving your own address should not put a socket in the
            middle of a local send.

        Raises:
            RefResolutionError: If the text is not a ref, if it names another
                system with no host to dial, or if it names a reachable peer
                and this system has remoting switched off.
            MessageTypeError: If `expect` is not a `Message` subclass or a
                union of them.
        """
        try:
            address, path = parse_ref(uri)
        except ValueError as error:
            raise RefResolutionError(str(error)) from error

        if address.system == self.name and (
            not address.is_addressable or address == self._address
        ):
            return cast("ActorRef[T]", self.resolve_path(address, path))

        msg_type = normalize_msg_type(expect, origin=f"resolving {uri}")
        if not address.is_addressable:
            msg = (
                f"cannot resolve {uri!r}: it names the system {address.system!r} "
                "and no host to dial. A system with remoting switched off writes "
                "its refs down that way, and there is nowhere to send to."
            )
            raise RefResolutionError(msg)
        if self._remote is None:
            msg = (
                f"cannot resolve {uri!r}: this system has remoting switched off. "
                "Pass TapioSettings(remote=RemoteSettings(...)) to reach another "
                "system."
            )
            raise RefResolutionError(msg)
        return cast(
            "ActorRef[T]", self._remote.resolve_expecting(address, path, msg_type)
        )

    def deliver_frame(self, data: bytes, *, peer: Address | None = None) -> None:
        """Take one frame off a link and deliver what is in it.

        This is the receiving half of remoting. It is a plain method because
        every failure a peer can cause is decided here, with no socket
        involved, so a test can cover all of it by handing one system the
        bytes another produced.

        It never raises. A bad size, a bad version, an unknown type key, a
        payload that will not validate, a recipient that has stopped, and a
        message the recipient does not accept all become dead letters on this
        system's stream, carrying the peer address when there is one.

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

    def spawn_system_actor(
        self,
        behavior: Behavior[T],
        name: str,
        mailbox: MailboxConfig | None = None,
    ) -> ActorRef[T]:
        """Start an actor under `/system`, beside remoting.

        For the runtime's own extensions rather than for application actors.
        Clustering uses it, as remoting does: those actors are part of how the
        system works, they must not collide with names a user chose, and a
        failure in one is not the user tree's business.

        Application actors belong under `/user`, through `spawn`. An actor
        started here does not sit under the user guardian, so it does not
        share the shutdown ordering or the escalation behaviour that the rest
        of an application relies on.

        Args:
            behavior: What the actor does.
            name: Its name, unique among system actors.
            mailbox: Capacity and overflow behaviour. The system's default when
                omitted.

        Returns:
            A ref to the new actor.

        Raises:
            ActorSystemTerminating: If the system is shutting down.
            ActorNameError: If a live system actor already has that name.
        """
        self._reject_if_terminating(name)
        return self._system.spawn(behavior, name, mailbox)

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

        Keeping the cause and terminating are two halves of one answer. A
        system with a subtree silently missing is worse than one that stopped,
        and a service embedding tapio awaits `when_terminated` to decide
        whether to exit or rebuild.

        Termination runs as its own task rather than inline, because the
        caller is the guardian's receive loop and shutdown waits for that loop
        to finish.
        """
        if self._failure is None:
            self._failure = error
        if not self._terminating:
            self._begin_termination()

    def _begin_termination(self) -> asyncio.Task[None]:
        """Start the drain once, and return the task that carries it.

        Idempotent: the first caller spawns the drain and every later one gets
        the same task back. Both `terminate` and a guardian failure come
        through here, so there is exactly one drain however the shutdown began.
        """
        if self._draining is None:
            self._terminating = True
            self._draining = self._runtime.dispatcher.spawn_task(
                self._drain(), name=f"tapio-drain:{self.name}"
            )
        return self._draining

    async def _drain(self) -> None:
        """Stop the tree and the pool, then mark the system terminated.

        This is the actual shutdown work. It is owned by the system, not by
        whoever called `terminate`, so a cancelled caller cannot leave the
        tree half-stopped.

        Every cell races one deadline taken from `shutdown_timeout`, so the
        worst case follows that setting and not the depth of the tree. An
        actor still stuck in a handler when the deadline passes is cancelled,
        and the warning names it.
        """
        deadline = (
            self._runtime.dispatcher.now()
            + self._settings.shutdown_timeout.total_seconds()
        )
        # Application actors first. The system guardian holds runtime
        # facilities that they may still be using on the way out.
        await self._user.stop(deadline)
        await self._system.stop(deadline)
        # Against the same deadline as the tree. Threads are not tasks, so
        # nothing above has touched them, and a system that has terminated
        # must leave none behind.
        await self._blocking.shutdown(deadline, now=self._runtime.dispatcher.now)
        # Set before the event. A send racing shutdown should be told that the
        # system is gone, which is a different problem from one actor stopping
        # while the rest of the tree runs on.
        self._runtime.terminated = True
        self._terminated.set()
        self._log.debug("terminated")

    async def terminate(self) -> None:
        """Stop every actor, bottom-up, and wait for the tree to drain.

        Calling this more than once is safe, and so is being cancelled while
        it runs. The drain itself is a task the system owns, so a caller that
        gives up waiting, or is cancelled at a bad moment, does not cancel the
        shutdown: the tree still finishes and later callers still see it. This
        is the guarantee `ActorCell.stop` keeps for a single cell, one level
        up for the whole tree.
        """
        draining = self._begin_termination()
        # Shielded for the reason ActorCell.stop is: a caller that gives up
        # waiting must not cancel the shutdown it asked for.
        await asyncio.shield(draining)

    async def when_terminated(self) -> None:
        """Wait until the system has finished shutting down.

        This is where an escalation that reached a guardian surfaces. Nowhere
        else can raise it. The failing actor's exception never leaves its own
        receive loop, and every supervisor above it declined to take
        responsibility, so the last place to report it is the one the
        embedding service is already waiting on.

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
