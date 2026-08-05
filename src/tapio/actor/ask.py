"""Ask: one request, one reply, and every way that can fail.

`ask` is sugar over the `reply_to` field the examples start with. The sugar is
a `PromiseRef`: a ref with no mailbox and no cell, whose `tell` resolves an
`asyncio.Future` instead of enqueuing anything. The caller awaits that future.

Three things fail an ask. All three are here so that the caller never waits
out a timeout for an answer that is not coming:

* The timeout elapses. This is the only one that costs the full wait.
* The target stops. The promise watches it, so this fails at once.
* The reply is not of the expected type. That is a bug in the responder, and
  it surfaces in the caller rather than as a value whose static type is a lie.

After any of those the promise is settled, and a reply arriving later becomes
a dead letter rather than resolving a future nobody is awaiting.
"""

import asyncio
from collections.abc import Callable
from datetime import timedelta
from typing import TYPE_CHECKING, Any, TypeVar

from pydantic import ValidationError

from tapio.actor.dead_letters import DeadLetterReason
from tapio.actor.path import ActorPath
from tapio.actor.ref import ActorRef
from tapio.errors import (
    AskTargetTerminated,
    AskTimeoutError,
    AskTypeError,
    MessageTypeError,
)
from tapio.logging import runtime_logger
from tapio.message import Message
from tapio.remote.address import Address
from tapio.validation import MessageValidator, normalize_msg_type, resolve_validator

if TYPE_CHECKING:
    from tapio.actor.cell import ActorRuntime, LocalActorRef

__all__ = ["PromiseRef", "ask"]

T = TypeVar("T", bound=Message)
R = TypeVar("R", bound=Message)

_log = runtime_logger("ask")

_PROMISES = "promises"
"""The name of the place under `/system` that promise refs are addressed in."""


class PromiseRef(ActorRef[R]):
    """A ref with no actor behind it, whose `tell` completes one ask.

    It is addressable rather than anonymous because a reply may come back over
    a link and has to find its way. Every promise has a path under
    `/system/promises` and stays registered there while the ask is running, so
    a `reply_to` that crossed a link resolves back to the future being
    awaited.
    """

    __slots__ = ("_expected", "_future", "_runtime", "_settled", "_target", "_validate")

    def __init__(
        self,
        *,
        path: ActorPath,
        runtime: "ActorRuntime",
        validate: MessageValidator,
        expected: str,
        target: ActorPath,
    ) -> None:
        """Create a promise for one ask.

        Args:
            path: Where this promise is addressed, under `/system/promises`.
            runtime: The system slice, for the loop and for dead letters.
            validate: The reply check, resolved exactly as a cell's is.
            expected: The expected reply type, as it reads in an error.
            target: The actor being asked, named in every error.
        """
        super().__init__(path)
        self._runtime = runtime
        self._validate = validate
        self._expected = expected
        self._target = target
        self._future: asyncio.Future[R] = runtime.dispatcher.loop.create_future()
        self._settled = False

    @property
    def address(self) -> Address:
        """The canonical address of the system the asker is running in."""
        return self._runtime.address

    @property
    def future(self) -> "asyncio.Future[R]":
        """The reply, once there is one."""
        return self._future

    def tell(self, message: R) -> None:
        """Reply to the ask this promise stands for.

        Safe to call from any thread, like every other `tell`. Validation does
        not run here, unlike every other `tell`. A reply of the wrong type is
        the asker's problem, not an exception to raise into whoever answered,
        so the check runs on the loop and its failure goes to the awaiting
        caller.

        Args:
            message: The reply. The first one wins. A later one is a dead
                letter, and so is any reply to an ask that has timed out.
        """
        dispatcher = self._runtime.dispatcher
        if dispatcher.is_current():
            self._accept(message)
            return
        try:
            dispatcher.call_soon_threadsafe(self._accept, message)
        except RuntimeError:
            # The loop is closed, so the asker is gone along with it and there
            # is no office left to publish to either.
            _log.warning(
                "dead letter: %s replied to %s after the loop closed",
                type(message).__name__,
                self.path,
            )

    async def offer(self, message: R) -> None:
        """Reply, waiting for capacity that a promise never lacks.

        A promise holds one future rather than a mailbox, so there is nothing
        to fill and nothing to wait for. This is `tell`.

        Args:
            message: The reply.
        """
        self.tell(message)

    def notify_terminated(self, ref: ActorRef[Any]) -> None:
        """Fail the ask because the actor it was waiting on stopped.

        This is why an ask watches its target. Without it, a caller asking an
        actor that has already stopped waits out the full timeout for a reply
        that cannot arrive.

        Args:
            ref: A ref to the actor that stopped.
        """
        if self._settled or self._future.done():
            return
        msg = f"{ref.path} stopped before replying to an ask expecting {self._expected}"
        self._future.set_exception(AskTargetTerminated(msg))

    def settle(self) -> None:
        """Close this promise, whatever became of the ask.

        After this a reply is a dead letter. The future is not abandoned: a
        pending one is cancelled, and an exception that arrived just as the
        caller gave up is retrieved here. An unretrieved one would be reported
        by asyncio at collection time as if something had gone unhandled.

        The promise also stops being addressable, so an ask leaves nothing in
        the ref registry however it ended.
        """
        self._settled = True
        self._runtime.refs.deregister(self.path)
        if not self._future.done():
            self._future.cancel()
        elif not self._future.cancelled():
            self._future.exception()

    def _accept(self, message: R) -> None:
        """Take a reply on the system's loop, and settle the future with it."""
        if self._settled or self._future.done():
            self._runtime.dead_letters.publish(
                message, self.path, DeadLetterReason.ASK_SETTLED
            )
            return
        try:
            self._validate(message)
        except MessageTypeError:
            msg = (
                f"{self._target} replied with {type(message).__name__} to an "
                f"ask expecting {self._expected}"
            )
            self._future.set_exception(AskTypeError(msg))
        except ValidationError as error:
            self._future.set_exception(error)
        else:
            self._future.set_result(message)


async def ask(
    target: "LocalActorRef[T]",
    make: Callable[[ActorRef[R]], T],
    *,
    expect: type[R],
    timeout: timedelta | None = None,  # noqa: ASYNC109 - the ask deadline
) -> R:
    """Send one message and await one reply.

    Args:
        target: The actor to ask.
        make: Builds the request from the ref the reply should go to. It is a
            factory rather than a message because the promise does not exist
            until the ask begins.
        expect: The reply type. Required, because a promise has no cell and so
            no declared message type of its own. Without it, request/response
            would be the one delivery path with no type check.
        timeout: How long to wait. The system's `ask_timeout` when omitted.

    Returns:
        The reply, which is the object the responder passed.

    Raises:
        AskTimeoutError: If no reply arrived in time.
        AskTargetTerminated: If the target stopped without replying, including
            when it had already stopped before the ask began.
        AskTypeError: If a reply arrived that was not an `expect`.
        MessageTypeError: If the request does not match the target's declared
            message type. That is an error about the message, so it belongs to
            the sender, as it does for `tell`.
        RuntimeError: If called from a thread that is not running the system's
            loop.
        pydantic.ValidationError: If content validation is on and either the
            request or the reply does not satisfy its own model.
    """
    cell = target.cell
    runtime = cell.runtime
    if not runtime.dispatcher.is_current():
        msg = (
            f"ask to {cell.path} must run on the system's loop; a reply is "
            "resolved there and cannot be awaited from another thread"
        )
        raise RuntimeError(msg)

    origin = f"an ask to {cell.path}"
    reply_type = normalize_msg_type(expect, origin=origin)
    promise: PromiseRef[R] = PromiseRef(
        path=_promise_path(runtime),
        runtime=runtime,
        validate=resolve_validator(
            msg_type=reply_type, settings=runtime.settings, target=None
        ),
        expected=expect.__name__,
        target=cell.path,
    )

    if not cell.is_alive:
        # Nothing between here and the watch below awaits, and a cell stops on
        # this same loop, so the target cannot slip through the gap.
        msg = (
            f"{cell.path} had already stopped when an ask expecting "
            f"{expect.__name__} was made"
        )
        raise AskTargetTerminated(msg)
    cell.add_watcher(promise)
    # After the early exit above, so that an ask which never starts leaves
    # nothing registered. `settle` in the `finally` below takes it out again.
    runtime.refs.register(promise)

    seconds = (
        timeout if timeout is not None else runtime.settings.ask_timeout
    ).total_seconds()
    try:
        request = make(promise)
        target.tell(request)
        try:
            async with asyncio.timeout(seconds):
                return await promise.future
        except TimeoutError:
            msg = (
                f"no reply from {cell.path} within {seconds:g}s to "
                f"{type(request).__name__}, expecting {expect.__name__}"
            )
            raise AskTimeoutError(msg) from None
    finally:
        # Both have to run however this ended. The promise stops accepting
        # replies, and the target stops holding a watcher for a finished ask.
        promise.settle()
        cell.remove_watcher(promise)


def _promise_path(runtime: "ActorRuntime") -> ActorPath:
    """Address one promise under `/system/promises`.

    The uid comes from the system's incarnation counter, so no two asks in a
    system share a path, even across a restart of whatever made them.
    """
    uid = runtime.next_uid()
    promises = ActorPath.root(runtime.name).child("system").child(_PROMISES)
    return promises.child(str(uid), uid=uid)
