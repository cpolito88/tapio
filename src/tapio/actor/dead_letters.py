"""Dead letters: the one place a message goes when it cannot be delivered.

An `ActorRef` stays a valid handle after its actor dies, so `tell` has to stay
total: it cannot raise about a recipient the sender has no way to check without
racing. The cost of that is messages with nowhere to go, and the answer is to
account for every one of them rather than to drop them silently.

The office is a system-wide sink with two outputs. It publishes to subscribers,
which is what makes an absence testable, and it logs with a throttle, because a
dead actor on the receiving end of a hot send loop would otherwise drown
everything else in the log.
"""

import time
from collections.abc import Callable, Iterator
from typing import Annotated, Any, TypeAlias

from pydantic import PlainValidator, SerializeAsAny

from tapio.actor.path import ActorPath
from tapio.actor.ref import ActorRef
from tapio.logging import runtime_logger
from tapio.message import Message
from tapio.remote.address import Address

__all__ = [
    "CarriedMessage",
    "Carrier",
    "DeadLetter",
    "DeadLetterOffice",
    "DeadLetterReason",
    "DeadLetterRef",
    "Subscription",
]

_log = runtime_logger("dead-letters")


def _carry(value: object) -> Message:
    """Keep an undelivered message exactly as it was sent.

    A field annotated with the `Message` base class cannot be validated
    normally: `Message` sets `revalidate_instances="always"`, so Pydantic would
    rebuild the payload *as a bare `Message`* and every field of the actual
    subclass would be silently dropped. A dead letter that has lost the message
    it is reporting is worse than useless, so validation here is an is-instance
    check and the object is passed through untouched.
    """
    if isinstance(value, Message):
        return value
    msg = f"a dead letter carries a Message, not {type(value).__name__}"
    raise ValueError(msg)


CarriedMessage: TypeAlias = SerializeAsAny[Annotated[Message, PlainValidator(_carry)]]
"""An undelivered message, kept as itself.

`SerializeAsAny` is the other half of `_carry`: without it a dump would emit
only the base class's fields, which for `Message` is none of them.
"""


class Carrier(Message):
    """A message on its way somewhere, wrapped in how it is travelling.

    An adapter's wrapper and an association's outbound frame are both one of
    these: internal, short-lived, and never something a user writes. They share
    a base class so that a dead letter reports the *payload*, since how a
    message travelled is a detail of the runtime and a subscriber matching on
    message types should not have to know about one.
    """

    payload: "CarriedMessage"
    """The message its sender actually sent."""


class DeadLetterReason:
    """Why a message could not be delivered.

    Deliberately a namespace of string constants rather than an enum. The set
    grows as features land, remoting most of all, and a subscriber that treats
    it as closed would break on every release. Match on the ones you care about
    and let the rest fall through.
    """

    RECIPIENT_TERMINATED = "recipient-terminated"
    """The target actor had already stopped."""

    SYSTEM_TERMINATED = "system-terminated"
    """The whole system had shut down before the message was enqueued."""

    ASK_SETTLED = "ask-settled"
    """A reply arrived for an ask that was already over. The ask had timed out,
    been answered, or lost its target, so there was no longer a future for the
    reply to resolve and nobody left awaiting one."""

    STASH_DISCARDED = "stash-discarded"
    """A message was still stashed when its actor restarted or stopped. A
    restart clears the stash, since messages held by the state that just failed
    are not the new state's to answer, and a stop leaves nobody to replay
    them."""

    MAILBOX_FULL = "mailbox-full"
    """A bounded mailbox was at capacity and its overflow strategy discarded
    this message. Which message that is depends on the strategy: the arriving
    one under `DROP_NEW`, the oldest queued one under `DROP_OLDEST`."""

    UNKNOWN_RECIPIENT = "unknown-recipient"
    """No live actor answers to that path and incarnation uid. Either nothing
    was ever there, or the ref is stale: the actor it named has stopped, and
    whoever occupies that path now is a stranger to the sender."""

    UNKNOWN_MESSAGE_TYPE = "unknown-message-type"
    """A frame named a type key this system has not registered. Nothing is
    imported to find out what it might have meant."""

    WRONG_MESSAGE_TYPE = "wrong-message-type"
    """A frame decoded into a message the recipient does not accept. The
    sender's declaration and the recipient's real protocol are independently
    deployed, so this is the check that can be trusted."""

    MALFORMED_FRAME = "malformed-frame"
    """A frame was not readable: not JSON, not a version this system speaks, or
    a payload that failed validation."""

    FRAME_TOO_LARGE = "frame-too-large"
    """A frame exceeded the configured size limit and was refused."""

    NO_ASSOCIATION = "no-association"
    """A ref addressed another system that this one has no link to."""

    OUTBOUND_BUFFER_FULL = "outbound-buffer-full"
    """An association was holding all the frames it will hold for a peer that
    is not reading. Backpressure against a socket, and deliberately not
    backpressure from the receiving actor: nothing in a fire-and-forget wire
    protocol can offer the latter."""

    LINK_FAILED = "link-failed"
    """The link to the peer failed while the message was on it or queued for
    it. At-most-once means exactly this: a message written to a socket that
    then died may or may not have been processed."""


class DeadLetter(Message):
    """One message that was sent and never delivered."""

    message: CarriedMessage
    """The undelivered message itself, the same object the sender passed."""

    recipient: str
    """The path of the actor it was addressed to."""

    reason: str
    """One of the `DeadLetterReason` constants."""

    peer: str | None = None
    """The address involved, when a remote link was. Carried as a field rather
    than folded into `reason` so a subscriber can tell "this actor is gone"
    from "that node is gone" without parsing a string."""

    detail: str | None = None
    """The specifics, when the reason alone does not carry them: the type key
    nobody registered, the two types that did not match, the size that was
    refused."""


class Subscription:
    """A handle for undoing one `subscribe` call."""

    __slots__ = ("_cancel",)

    def __init__(self, cancel: Callable[[], None]) -> None:
        """Bind the subscription to the office that made it."""
        self._cancel = cancel

    def unsubscribe(self) -> None:
        """Stop receiving dead letters. Calling this twice is harmless."""
        self._cancel()

    def __enter__(self) -> "Subscription":
        """Return the subscription, for use as a context manager."""
        return self

    def __exit__(self, *exc: object) -> None:
        """Unsubscribe on the way out of the block."""
        self.unsubscribe()


class DeadLetterOffice:
    """The system-wide sink for undeliverable messages.

    Subscribers see every dead letter. The log sees the first few in full and
    periodic summaries after that, so a hot send loop to a stopped actor costs
    a bounded number of log records rather than one per message.
    """

    __slots__ = (
        "_clock",
        "_last_summary",
        "_log_first",
        "_logged",
        "_next_token",
        "_subscribers",
        "_summary_interval",
        "_suppressed",
        "_total",
    )

    def __init__(
        self,
        *,
        log_first: int,
        summary_interval: float,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        """Create an office.

        Args:
            log_first: How many dead letters to log in full before throttling.
            summary_interval: Seconds between summaries once throttled.
            clock: Monotonic time source, injected so tests need no sleeping.
        """
        self._log_first = log_first
        self._summary_interval = summary_interval
        self._clock = clock
        self._subscribers: dict[int, Callable[[DeadLetter], None]] = {}
        self._next_token = 0
        self._total = 0
        self._logged = 0
        self._suppressed = 0
        self._last_summary = clock()

    @property
    def total(self) -> int:
        """How many dead letters this office has handled."""
        return self._total

    @property
    def logged(self) -> int:
        """How many were logged in full, as opposed to summarised."""
        return self._logged

    def subscribe(self, handler: Callable[[DeadLetter], None]) -> Subscription:
        """Register a handler to receive every dead letter.

        Args:
            handler: Called with each `DeadLetter`. An exception raised here is
                logged and swallowed: one bad subscriber must not stop the
                others or fail the send that produced the event.

        Returns:
            A handle for unsubscribing, usable as a context manager.
        """
        token = self._next_token
        self._next_token += 1
        self._subscribers[token] = handler

        def cancel() -> None:
            self._subscribers.pop(token, None)

        return Subscription(cancel)

    def publish(
        self,
        message: Message,
        recipient: ActorPath,
        reason: str,
        *,
        peer: Address | None = None,
        detail: str | None = None,
    ) -> None:
        """Record an undeliverable message and tell everyone who asked.

        Args:
            message: The message that was not delivered.
            recipient: Where it was addressed.
            reason: One of the `DeadLetterReason` constants.
            peer: The remote address involved, when one was.
            detail: The specifics the reason alone does not carry.
        """
        self._total += 1
        event = DeadLetter(
            message=message,
            recipient=str(recipient),
            reason=reason,
            peer=str(peer) if peer is not None else None,
            detail=detail,
        )
        for handler in list(self._subscribers.values()):
            try:
                handler(event)
            except Exception:
                _log.exception("a dead letter subscriber raised; continuing")
        self._log_throttled(event)

    def _log_throttled(self, event: DeadLetter) -> None:
        """Log the first few in full, then one summary per interval."""
        if self._logged < self._log_first:
            self._logged += 1
            _log.warning(
                "dead letter: %s to %s (%s)",
                type(event.message).__name__,
                event.recipient,
                event.reason,
            )
            return

        self._suppressed += 1
        now = self._clock()
        if now - self._last_summary < self._summary_interval:
            return
        _log.warning(
            "dead letters: %d more in the last %.0fs, %d in total",
            self._suppressed,
            now - self._last_summary,
            self._total,
        )
        self._suppressed = 0
        self._last_summary = now

    def __iter__(self) -> Iterator[Callable[[DeadLetter], None]]:
        """Iterate the current subscribers, which tests find useful."""
        return iter(list(self._subscribers.values()))

    def __repr__(self) -> str:
        """Render the totals, which is what a reader wants to see."""
        return (
            f"DeadLetterOffice(total={self._total}, "
            f"subscribers={len(self._subscribers)})"
        )


class DeadLetterRef(ActorRef[Any]):
    """A ref that accepts anything and delivers none of it.

    What resolving an address gives you when there is nothing behind it: an
    actor that has stopped, an incarnation whose uid has been retired, or a
    system this one has no link to. Resolution has to return *something*
    tellable, because `tell` is total and a ref is a handle rather than a
    liveness claim, and this is the honest something.
    """

    __slots__ = ("_dead_letters", "_peer", "_reason")

    def __init__(
        self,
        path: ActorPath,
        *,
        dead_letters: DeadLetterOffice,
        reason: str,
        peer: Address | None = None,
    ) -> None:
        """Bind a dead-letter target to the office that will hear about it.

        Args:
            path: The path that was asked for.
            dead_letters: Where what is told to this ref is accounted for.
            reason: One of the `DeadLetterReason` constants.
            peer: The address involved, when a remote one was.
        """
        super().__init__(path)
        self._dead_letters = dead_letters
        self._reason = reason
        self._peer = peer

    @property
    def address(self) -> Address:
        """The address that was asked for, reachable or not."""
        return self._peer if self._peer is not None else super().address

    def tell(self, message: Message) -> None:
        """Account for a message that had nowhere to go.

        Args:
            message: The undelivered message.
        """
        self._dead_letters.publish(message, self.path, self._reason, peer=self._peer)

    async def offer(self, message: Message) -> None:
        """Account for a message, since there is no capacity to wait for.

        Args:
            message: The undelivered message.
        """
        self.tell(message)
