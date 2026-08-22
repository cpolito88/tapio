"""The event stream: things the runtime noticed, for whoever wants them.

Dead letters are one such stream and they have their own office, because a
message that went nowhere carries a payload and needs throttled logging. This
is the general one. It carries facts about the system rather than traffic:
today, that a peer became unreachable.

Subscribers pick the event type they care about, so a handler written for one
event is never called with another. The set of event types grows with the
library, and a subscriber that matched on everything would have to keep up.
"""

from collections.abc import Callable, Iterator
from typing import Any, TypeVar

from tapio.logging import runtime_logger
from tapio.message import Message

__all__ = ["EventStream", "Subscription"]

E = TypeVar("E", bound=Message)

_log = runtime_logger("events")


class Subscription:
    """A handle for undoing one `subscribe` call."""

    __slots__ = ("_cancel",)

    def __init__(self, cancel: Callable[[], None]) -> None:
        """Bind the subscription to the stream that made it."""
        self._cancel = cancel

    def unsubscribe(self) -> None:
        """Stop receiving events. Calling this twice is harmless."""
        self._cancel()

    def __enter__(self) -> "Subscription":
        """Return the subscription, for use as a context manager."""
        return self

    def __exit__(self, *exc: object) -> None:
        """Unsubscribe on the way out of the block."""
        self.unsubscribe()


class EventStream:
    """What one system publishes about itself, and who is listening.

    One per system. Publishing is synchronous and runs on the system's loop,
    so a handler must not block and must not raise. One that raises is logged
    and the rest still run: a bad subscriber cannot break the runtime event
    that reached it.
    """

    __slots__ = ("_next_token", "_subscribers", "_total")

    def __init__(self) -> None:
        """Create an empty stream."""
        self._subscribers: dict[int, tuple[type[Message], Callable[[Any], None]]] = {}
        self._next_token = 0
        self._total = 0

    @property
    def total(self) -> int:
        """How many events this stream has published."""
        return self._total

    def subscribe(
        self, event_type: type[E], handler: Callable[[E], None]
    ) -> Subscription:
        """Register a handler for one event type.

        Args:
            event_type: What to be told about. Subclasses count.
            handler: Called with each matching event.

        Returns:
            A handle for unsubscribing, usable as a context manager.
        """
        token = self._next_token
        self._next_token += 1
        self._subscribers[token] = (event_type, handler)

        def cancel() -> None:
            self._subscribers.pop(token, None)

        return Subscription(cancel)

    def publish(self, event: Message) -> None:
        """Hand an event to every subscriber that asked for its type.

        Args:
            event: What happened.
        """
        self._total += 1
        for event_type, handler in list(self._subscribers.values()):
            if not isinstance(event, event_type):
                continue
            try:
                handler(event)
            except Exception:
                _log.exception("an event subscriber raised; continuing")

    def __iter__(self) -> Iterator[type[Message]]:
        """Iterate the event types currently subscribed to, for tests to check."""
        return iter([event_type for event_type, _ in self._subscribers.values()])

    def __repr__(self) -> str:
        """Show the running totals."""
        return f"EventStream(total={self._total}, subscribers={len(self._subscribers)})"
