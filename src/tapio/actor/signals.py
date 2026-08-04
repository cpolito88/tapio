"""Signals: what the runtime tells an actor about its own lifecycle.

Signals travel on the mailbox's system lane, which drains before the user lane,
so a stop can never be stuck behind a backlog of user messages, and a death
watch fires in bounded time however deep the queue is.

They are frozen dataclasses rather than `Message` subclasses. A signal is
produced by the runtime and never crosses a `tell`, so the delivery-time
guarantee `Message` exists to provide has nothing to check here.

A behavior sees them through its signal handler, `Behaviors.receive(...,
on_signal=...)` or `AbstractBehavior.on_signal`. `ChildFailed` is the one
exception: it is the runtime's own escalation path, handled by the parent's
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

    Best effort: an actor cancelled at the shutdown deadline while wedged in a
    handler may never observe it. This is where resources get released, so
    "usually runs" is the honest description and it is documented rather than
    promised.
    """


@dataclass(frozen=True, slots=True)
class PreRestart(Signal):
    """The actor is about to be restarted, and this incarnation is ending.

    Delivered to the incarnation that failed, before its children are stopped
    and before the original behavior is re-evaluated. `PostStop` does *not*
    follow: a restart is not a stop, and an actor that released its resources
    twice would be as surprised as one that never released them.
    """


@dataclass(frozen=True, slots=True)
class Terminated(Signal):
    """A watched actor has stopped, for any reason including failure.

    Delivered to everyone who called `ctx.watch` on it, exactly once, on the
    system lane. A restart does not produce one: the ref, path and uid are
    unchanged, and only the incarnation behind them is new.
    """

    ref: "ActorRef[Any]"
    """The actor that stopped."""


@dataclass(frozen=True, slots=True)
class ChildFailed(Signal):
    """A child failed and its supervision decision was to escalate.

    The parent's cell treats this as its own failure and runs its own decision,
    which is what makes escalation ordinary message flow: orderable, observable
    and testable, rather than an exception injected across a task boundary
    where its ordering against the parent's in-flight message would be
    undefined.

    Handled by the runtime, never by a behavior's signal handler.
    """

    ref: "ActorRef[Any]"
    """The child that failed."""

    error: Exception
    """What it failed with, carried unchanged up the chain."""
