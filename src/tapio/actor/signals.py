"""Signals: what the runtime tells an actor about its own lifecycle.

Signals travel on the mailbox's system lane, which drains before the user lane,
so a stop can never be stuck behind a backlog of user messages.

They are frozen dataclasses rather than `Message` subclasses. A signal is
produced by the runtime and never crosses a `tell`, so the delivery-time
guarantee `Message` exists to provide has nothing to check here.

`PostStop` is the only signal the runtime raises so far. Restart, death watch
and escalation bring their own, and each arrives with the mechanism that gives
it a meaning beyond a name.
"""

from dataclasses import dataclass

__all__ = ["PostStop", "Signal"]


@dataclass(frozen=True, slots=True)
class Signal:
    """Base class for everything that travels on the system lane."""


@dataclass(frozen=True, slots=True)
class PostStop(Signal):
    """The actor has stopped and will handle no further message.

    Best effort: an actor cancelled at the shutdown deadline while wedged in a
    handler may never observe it. This is where resources get released, so
    "usually runs" is the honest description and it is documented rather than
    promised.
    """
