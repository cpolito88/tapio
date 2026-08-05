"""Signals: what the runtime tells an actor about its own lifecycle.

Signals travel on the mailbox's system lane, which drains before the user
lane. A stop is therefore never stuck behind a backlog of user messages, and a
death watch fires in bounded time however deep the queue is.

They are frozen dataclasses rather than `Message` subclasses. A signal is
produced by the runtime and never crosses a `tell`, so there is nothing for
the delivery-time guarantee to check.

A behavior sees them through its signal handler, `Behaviors.receive(...,
on_signal=...)` or `AbstractBehavior.on_signal`. `ChildFailed` is the
exception: it is the runtime's escalation path, handled by the parent's
supervision rather than by its code.
"""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # ref.py is imported by most of the runtime; keeping the
    from tapio.actor.ref import ActorRef  # edge one-way keeps the import graph flat

__all__ = ["ChildFailed", "PostStop", "PreRestart", "Signal", "Terminated"]


@dataclass(frozen=True, slots=True)
class Signal:
    """Base class for everything that travels on the system lane."""


@dataclass(frozen=True, slots=True)
class PostStop(Signal):
    """The actor has stopped and will handle no further message.

    Best effort. An actor cancelled at the shutdown deadline while stuck in a
    handler may never see it. Resources are released here, so treat it as
    "usually runs" rather than a guarantee.
    """


@dataclass(frozen=True, slots=True)
class PreRestart(Signal):
    """The actor is about to be restarted, and this incarnation is ending.

    Delivered to the incarnation that failed, before its children are stopped
    and before the original behavior is evaluated again. `PostStop` does not
    follow, because a restart is not a stop. Releasing resources twice would
    be as wrong as never releasing them.
    """


@dataclass(frozen=True, slots=True)
class Terminated(Signal):
    """A watched actor has stopped, for any reason including failure.

    Delivered to everyone who called `ctx.watch` on it, exactly once, on the
    system lane. A restart does not produce one, because the ref, path and uid
    are unchanged and only the incarnation behind them is new.
    """

    ref: "ActorRef[Any]"
    """The actor that stopped."""


@dataclass(frozen=True, slots=True)
class ChildFailed(Signal):
    """A child failed and its supervision decision was to escalate.

    The parent's cell treats this as its own failure and takes its own
    decision. Escalation is therefore ordinary message flow, which makes it
    ordered, observable and testable. An exception injected across a task
    boundary would have no defined order against the parent's in-flight
    message.

    Handled by the runtime, never by a behavior's signal handler.
    """

    ref: "ActorRef[Any]"
    """The child that failed."""

    error: Exception
    """What it failed with, carried unchanged up the chain."""
