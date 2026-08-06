"""`TestProbe`: an actor whose only job is to be asserted about.

A probe is a real actor in a real system. It has a path, a mailbox and a ref
you can put in a `reply_to` field, so the code under test cannot tell it from
any other actor, and nothing about the runtime is stubbed out to make a test
pass.

```python
probe: TestProbe[Greeted] = TestProbe(system, Greeted)
greeter.tell(Greet(whom="world", reply_to=probe.ref))

await probe.expect_message(Greeted(whom="world"))
await probe.expect_no_message()
```

It also watches, which is how a test asserts that an actor stopped without
reaching into the runtime:

```python
probe.watch(worker)
worker.tell(Retire())
await probe.expect_terminated(worker)
```

Every wait has a deadline, and running out of it is an `AssertionError` naming
what was expected and what arrived. A test that hangs tells you nothing; a
test that fails in a second tells you what it was waiting for.
"""

import asyncio
from datetime import timedelta
from typing import Any, Generic, TypeVar

from tapio.actor.behavior import Behavior, Behaviors
from tapio.actor.context import ActorContext
from tapio.actor.mailbox import MailboxConfig
from tapio.actor.path import ActorPath
from tapio.actor.ref import ActorRef
from tapio.actor.signals import Signal, Terminated
from tapio.actor.system import ActorSystem
from tapio.message import Message
from tapio.validation import MessageType

__all__ = ["TestProbe"]

T = TypeVar("T", bound=Message)
M = TypeVar("M", bound=Message)

DEFAULT_TIMEOUT = timedelta(seconds=3)
"""How long a probe waits before it decides nothing is coming.

Long enough that a busy CI runner does not fail a correct test, short enough
that a wrong one fails while you are still looking at it.
"""

NO_MESSAGE_WINDOW = timedelta(milliseconds=100)
"""How long `expect_no_message` watches before it believes the silence.

Proving a negative takes real time, and this is the one wait in the TestKit
that is spent even when the test passes. It is deliberately short: the case it
catches is a message sent immediately, which is the case that happens.
"""


class TestProbe(Generic[T]):
    """An actor that records what it is sent, for a test to assert on."""

    __slots__ = ("_messages", "_ref", "_signals", "_system")

    __test__ = False
    """Keeps pytest from collecting this as a test class.

    The name begins with `Test`, which is pytest's rule for a class holding
    tests, and a class with a constructor cannot be collected. Without this
    flag, importing a probe into a test module is a collection warning in
    somebody else's project.
    """

    def __init__(
        self,
        system: ActorSystem,
        msg_type: MessageType,
        *,
        name: str | None = None,
        mailbox: MailboxConfig | None = None,
    ) -> None:
        """Start a probe as a top-level actor in a running system.

        Args:
            system: The system to spawn it in.
            msg_type: What it accepts. A probe validates on delivery like any
                other actor, so a message of the wrong type dead-letters here
                too, rather than being quietly recorded.
            name: Its actor name. Generated when omitted, so several probes in
                one test need no names at all.
            mailbox: Capacity and overflow behaviour, for a test about
                backpressure. Unbounded when omitted.
        """
        self._system = system
        self._messages: asyncio.Queue[T] = asyncio.Queue()
        self._signals: asyncio.Queue[Signal] = asyncio.Queue()
        behavior = _probe_behavior(self._messages, self._signals, msg_type)
        self._ref: ActorRef[T] = (
            system.spawn_anonymous(behavior, mailbox)
            if name is None
            else system.spawn(behavior, name, mailbox)
        )

    @property
    def ref(self) -> ActorRef[T]:
        """The ref to hand to the code under test."""
        return self._ref

    @property
    def path(self) -> ActorPath:
        """Where this probe sits in the tree."""
        return self._ref.path

    def tell(self, message: T) -> None:
        """Send this probe a message, as anything else would.

        Args:
            message: The message to record.
        """
        self._ref.tell(message)

    def watch(self, target: ActorRef[Any]) -> None:
        """Watch another actor, so its stopping can be expected.

        The probe is an ordinary watcher, so this works on a ref that points
        at another node exactly as it does locally.

        Args:
            target: The actor to watch.
        """
        self._ref.tell(_Watch(target=target))  # type: ignore[arg-type]

    async def receive(
        self,
        timeout: timedelta | None = None,  # noqa: ASYNC109 - a probe deadline
    ) -> T:
        """Take the next message, waiting for one if none has arrived.

        Args:
            timeout: How long to wait. `DEFAULT_TIMEOUT` when omitted.

        Returns:
            The message.

        Raises:
            AssertionError: If nothing arrived in time.
        """
        message: T = await self._next(self._messages, "a message", timeout)
        return message

    async def expect_message(
        self,
        expected: T,
        timeout: timedelta | None = None,  # noqa: ASYNC109 - a probe deadline
    ) -> T:
        """Take the next message and assert it is the one expected.

        Equality rather than identity, so this reads the same for a message
        that crossed a link as for one that did not. A local `tell` does
        deliver the very object that was sent, and `receive` is there for a
        test that wants to say so.

        Args:
            expected: What should arrive.
            timeout: How long to wait. `DEFAULT_TIMEOUT` when omitted.

        Returns:
            The message that arrived.

        Raises:
            AssertionError: If nothing arrived in time, or something else did.
        """
        message = await self.receive(timeout)
        if message != expected:
            msg = f"expected {expected!r} at {self.path}, got {message!r}"
            raise AssertionError(msg)
        return message

    async def expect_message_of(
        self,
        msg_type: type[M],
        timeout: timedelta | None = None,  # noqa: ASYNC109 - a probe deadline
    ) -> M:
        """Take the next message and assert what kind it is.

        For when the contents are not known in advance, which is most replies
        carrying an id or a timestamp.

        Args:
            msg_type: The type it should be.
            timeout: How long to wait. `DEFAULT_TIMEOUT` when omitted.

        Returns:
            The message, narrowed to that type.

        Raises:
            AssertionError: If nothing arrived in time, or the wrong type did.
        """
        message = await self.receive(timeout)
        if not isinstance(message, msg_type):
            msg = (
                f"expected a {msg_type.__name__} at {self.path}, got "
                f"{type(message).__name__}: {message!r}"
            )
            raise AssertionError(msg)
        return message

    async def expect_no_message(self, within: timedelta | None = None) -> None:
        """Assert that nothing arrives for a while.

        This is the one wait a passing test actually spends, so the window is
        short by default. What it catches is a message sent immediately, which
        is the mistake that happens.

        Args:
            within: How long to watch. `NO_MESSAGE_WINDOW` when omitted.

        Raises:
            AssertionError: If a message arrived.
        """
        window = (within if within is not None else NO_MESSAGE_WINDOW).total_seconds()
        try:
            message = await asyncio.wait_for(self._messages.get(), window)
        except TimeoutError:
            return
        msg = f"expected nothing at {self.path} within {window:g}s, got {message!r}"
        raise AssertionError(msg)

    async def expect_terminated(
        self,
        target: ActorRef[Any],
        timeout: timedelta | None = None,  # noqa: ASYNC109 - a probe deadline
    ) -> Terminated:
        """Assert that a watched actor stopped.

        Args:
            target: The actor that should have stopped. It has to have been
                passed to `watch` first.
            timeout: How long to wait. `DEFAULT_TIMEOUT` when omitted.

        Returns:
            The signal that arrived.

        Raises:
            AssertionError: If no signal arrived in time, or one arrived about
                a different actor.
        """
        signal = await self._next(self._signals, f"Terminated({target.path})", timeout)
        if not isinstance(signal, Terminated) or signal.ref.path != target.path:
            msg = f"expected Terminated({target.path}) at {self.path}, got {signal!r}"
            raise AssertionError(msg)
        return signal

    @property
    def pending(self) -> int:
        """How many messages are recorded and not yet taken."""
        return self._messages.qsize()

    def stop(self) -> None:
        """Stop the probe, as an ordinary actor stops.

        Rarely needed: the system's shutdown stops it with everything else.
        Useful for a test where the probe itself has to stop, so that whoever
        was watching *it* hears about that.
        """
        self._ref.tell(_Stop())  # type: ignore[arg-type]

    async def _next(
        self,
        queue: "asyncio.Queue[Any]",
        expected: str,
        timeout: timedelta | None,  # noqa: ASYNC109 - a probe deadline
    ) -> Any:
        """Wait for one item, and say what was expected if none comes."""
        seconds = (timeout if timeout is not None else DEFAULT_TIMEOUT).total_seconds()
        try:
            return await asyncio.wait_for(queue.get(), seconds)
        except TimeoutError:
            msg = f"expected {expected} at {self.path} within {seconds:g}s, got nothing"
            raise AssertionError(msg) from None

    def __repr__(self) -> str:
        """Render where the probe sits and how much it is holding."""
        return f"TestProbe({str(self.path)!r}, pending={self.pending})"


class _Watch(Message):
    """Tells a probe to watch an actor, on its own lane rather than out of band.

    A probe watches by sending itself a message, so the watch is registered by
    the probe's own cell on the probe's own loop. Reaching into the cell from
    the test's thread would race with whatever the probe is doing.
    """

    target: ActorRef[Any]


class _Stop(Message):
    """Tells a probe to stop itself through its behavior."""


def _probe_behavior(
    messages: "asyncio.Queue[Any]",
    signals: "asyncio.Queue[Signal]",
    msg_type: MessageType,
) -> Behavior[Any]:
    """Build the behavior behind a probe: record everything, decide nothing."""

    async def on_message(ctx: ActorContext[Any], message: Any) -> Behavior[Any]:
        if isinstance(message, _Watch):
            ctx.watch(message.target)
            return Behaviors.same()
        if isinstance(message, _Stop):
            return Behaviors.stopped()
        messages.put_nowait(message)
        return Behaviors.same()

    async def on_signal(ctx: ActorContext[Any], signal: Signal) -> Behavior[Any]:
        signals.put_nowait(signal)
        return Behaviors.same()

    # The control messages are part of the declared type, so the cell's own
    # delivery-time check still runs against whatever the probe was told to
    # accept. A message of the wrong type dead-letters here as anywhere else.
    return Behaviors.receive(
        on_message, msg_type=msg_type | _Watch | _Stop, on_signal=on_signal
    )
