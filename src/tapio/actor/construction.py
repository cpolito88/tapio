"""Deferred construction: turning a wrapped behavior into one that can run.

A behavior can arrive wrapped. `Behaviors.supervise` declares strategies
around it. `Behaviors.setup`, `with_timers` and `with_stash` build it from a
factory that needs something only a running actor has. `unstash_all` replays
held messages before the behavior takes over. Wrappers nest in any order.
People write `supervise(setup(...))`, and they also write a `setup` that
returns a supervised behavior.

The cell unwraps a behavior when an actor starts, when it restarts, and when
a handler returns one. `BehaviorTestKit` unwraps one when it is built and
after each message. All of them call `construct`, so they cannot disagree
about what a wrapper means. Each caller supplies what the factories are given,
through a `ConstructionHost`.

`construct` does not handle a factory that raises. The error propagates to the
caller, because what a failure means depends on where the factory ran: a
spawn raises it, and a handler turns it into a supervision decision.
"""

from dataclasses import dataclass
from typing import Any, Generic, Protocol, TypeVar

from tapio.actor.behavior import (
    Behavior,
    SetupBehavior,
    SuperviseBehavior,
    WithStashBehavior,
    WithTimersBehavior,
)
from tapio.actor.context import ActorContext
from tapio.actor.stash import StashBuffer, UnstashBehavior
from tapio.actor.timers import TimerScheduler
from tapio.errors import BehaviorTypeError
from tapio.message import Message

__all__ = ["MAX_SETUP_DEPTH", "Constructed", "ConstructionHost", "construct"]

T = TypeVar("T", bound=Message)

MAX_SETUP_DEPTH = 100
"""How many wrappers to unwrap before calling deferred construction a loop."""


class ConstructionHost(Protocol[T]):
    """What the factories inside a wrapped behavior are given.

    The cell supplies the real ones. The test kit has no cell, so it supplies
    a context that records and refuses the rest.
    """

    @property
    def ctx(self) -> ActorContext[T]:
        """The context a `setup` factory is called with."""
        ...

    @property
    def timers(self) -> TimerScheduler[T]:
        """The scheduler a `with_timers` factory is called with."""
        ...

    def stash_buffer(self, capacity: int) -> StashBuffer[T]:
        """The buffer a `with_stash` factory is called with."""
        ...

    def unstash(self, buffer: StashBuffer[T]) -> None:
        """Replay what `unstash_all` asked to replay."""
        ...


@dataclass(frozen=True, slots=True)
class Constructed(Generic[T]):
    """A behavior with every wrapper taken off, and the strategies found on it."""

    behavior: Behavior[T]
    """What the actor runs. It is never one of the wrappers above."""

    supervision: tuple[SuperviseBehavior[Any], ...]
    """Every `supervise` layer found while unwrapping, outermost first."""


def construct(behavior: Behavior[T], host: ConstructionHost[T]) -> Constructed[T]:
    """Unwrap supervision and run deferred construction until a real behavior.

    Args:
        behavior: What to unwrap.
        host: What the factories are given.

    Returns:
        The behavior to run, and the supervision declared on the way to it.

    Raises:
        BehaviorTypeError: If the wrappers are still coming after
            `MAX_SETUP_DEPTH` rounds, which is a factory that returns itself.
        Exception: Whatever a factory raised.
    """
    supervision: list[SuperviseBehavior[Any]] = []
    rounds = 0
    while True:
        if isinstance(behavior, SuperviseBehavior):
            supervision.append(behavior)
            behavior = behavior.behavior
        elif isinstance(behavior, SetupBehavior):
            behavior = behavior.setup(host.ctx)
        elif isinstance(behavior, WithTimersBehavior):
            behavior = behavior.with_timers(host.timers)
        elif isinstance(behavior, WithStashBehavior):
            behavior = behavior.with_stash(host.stash_buffer(behavior.capacity))
        elif isinstance(behavior, UnstashBehavior):
            host.unstash(behavior.buffer)
            behavior = behavior.behavior
        else:
            return Constructed(behavior, tuple(supervision))
        rounds += 1
        if rounds > MAX_SETUP_DEPTH:
            msg = (
                f"{host.ctx.path}: deferred construction is still returning "
                f"another Behaviors.setup after {MAX_SETUP_DEPTH} rounds"
            )
            raise BehaviorTypeError(msg)
