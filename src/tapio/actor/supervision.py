"""Supervision: what happens to an actor whose handler raised.

A failure is not an exception the sender sees. It never leaves the failing
actor's receive loop; it becomes a decision taken by whoever declared one, and
the four decisions are all there are: carry on with the state you have
(`resume`), rebuild from the behavior you started with (`restart`), stop
(`stop`), or make it the parent's problem (`escalate`).

The default is `stop`, and it is deliberately not `restart`: an actor that
failed for a reason nobody anticipated is in a state nobody described, and
restarting it in a loop turns one bug into a busy one. Restarting is what you
ask for when you know the failure is transient.
"""

import enum
from dataclasses import dataclass
from datetime import timedelta

__all__ = ["Backoff", "Decision", "SupervisorStrategy"]

_MAX_DOUBLINGS = 30
"""Where the exponential stops doubling, before the arithmetic gets silly.

The cap is `max_backoff` anyway; this only keeps the intermediate from growing
without bound while a long-lived actor keeps failing.
"""


class Decision(enum.Enum):
    """What supervision does with a failed actor."""

    RESUME = "resume"
    """Keep the actor, its behavior, and its state, and take the next message.

    The failed message is gone. Right when the failure is about the message
    rather than about the actor.
    """

    RESTART = "restart"
    """Rebuild the actor from the behavior it was spawned with.

    Children are stopped and respawned by the re-run setup, the mailbox
    survives, and watchers hear nothing: the actor's identity is unchanged and
    only its incarnation is new.
    """

    STOP = "stop"
    """Stop the actor. Its watchers get `Terminated` like any other stop."""

    ESCALATE = "escalate"
    """Stop the actor and hand the failure to its parent as the parent's own.

    The parent then takes its own decision, so a subtree can be restarted by
    the actor that knows how to rebuild it rather than by the one that broke.
    """


@dataclass(frozen=True, slots=True)
class Backoff:
    """Exponential backoff with jitter, for restarts that should not thrash.

    A dependency that just refused a connection will usually refuse the next
    one too, so restarting immediately spends the restart window in a
    millisecond and stops the actor for a fault that would have cleared on its
    own. Waiting, and waiting longer each time, is what makes a restart limit
    a description of "this is not getting better" instead of a race.
    """

    min_backoff: timedelta
    """How long to wait before the first restart."""

    max_backoff: timedelta
    """The ceiling the doubling stops at."""

    random_factor: float = 0.2
    """How much jitter to add, as a fraction of the delay.

    `0.2` means up to twenty percent longer. Jitter matters when a shared
    dependency fails: without it, every actor that noticed at the same moment
    retries at the same moment, forever.
    """

    def __post_init__(self) -> None:
        """Reject a backoff that could not produce a sensible delay.

        Raises:
            ValueError: If either bound is negative, if the maximum is below
                the minimum, or if the random factor is not in `[0, 1]`.
        """
        if self.min_backoff < timedelta(0):
            msg = f"min_backoff must not be negative, got {self.min_backoff}"
            raise ValueError(msg)
        if self.max_backoff < self.min_backoff:
            msg = (
                f"max_backoff ({self.max_backoff}) must be at least min_backoff "
                f"({self.min_backoff})"
            )
            raise ValueError(msg)
        if not 0.0 <= self.random_factor <= 1.0:
            msg = f"random_factor must be in [0, 1], got {self.random_factor}"
            raise ValueError(msg)

    def delay(self, restart: int, *, jitter: float) -> float:
        """How long to wait before the given restart.

        Pure, and the jitter is an argument rather than a call to `random`, so
        the schedule can be asserted exactly in a test and the randomness lives
        at the one call site that wants it.

        Args:
            restart: Which restart this is, counting from one.
            jitter: A value in `[0, 1)`, scaled by `random_factor`.

        Returns:
            The delay in seconds.
        """
        doublings = min(max(restart - 1, 0), _MAX_DOUBLINGS)
        base: float = min(
            self.min_backoff.total_seconds() * float(2**doublings),
            self.max_backoff.total_seconds(),
        )
        return base * (1.0 + self.random_factor * jitter)


@dataclass(frozen=True, slots=True)
class SupervisorStrategy:
    """One decision, plus the limits that apply when it is `restart`.

    Built through the classmethods rather than the constructor, so that a
    strategy reads as the decision it makes:

    ```python
    Behaviors.supervise(worker()).on_failure(
        SupervisorStrategy.restart(max_restarts=3, window=timedelta(seconds=1)),
        on=ConnectionError,
    )
    ```
    """

    decision: Decision
    """What to do with the failed actor."""

    max_restarts: int | None = None
    """How many restarts are allowed inside `window`, or `None` for no limit.

    Exceeding it stops the actor: the failure is not transient after all, and
    an actor restarting forever is a bug that never gets reported.
    """

    window: timedelta | None = None
    """The span `max_restarts` is counted over, or `None` for all time."""

    backoff: Backoff | None = None
    """How long to wait before each restart, or `None` to restart at once."""

    def __post_init__(self) -> None:
        """Reject limits that only make sense on a restart strategy.

        Raises:
            ValueError: If restart limits are set on a strategy that does not
                restart, or if `max_restarts` is not positive.
        """
        restarting = self.decision is Decision.RESTART
        if not restarting and (
            self.max_restarts is not None
            or self.window is not None
            or self.backoff is not None
        ):
            msg = (
                f"restart limits do not apply to a {self.decision.value} "
                "strategy; they are only read when the decision is restart"
            )
            raise ValueError(msg)
        if self.max_restarts is not None and self.max_restarts < 1:
            msg = f"max_restarts must be at least 1, got {self.max_restarts}"
            raise ValueError(msg)

    @classmethod
    def resume(cls) -> "SupervisorStrategy":
        """Keep the actor and its state, and move on to the next message."""
        return cls(Decision.RESUME)

    @classmethod
    def stop(cls) -> "SupervisorStrategy":
        """Stop the actor. This is what an unsupervised actor already does."""
        return cls(Decision.STOP)

    @classmethod
    def escalate(cls) -> "SupervisorStrategy":
        """Stop the actor and hand the failure to its parent."""
        return cls(Decision.ESCALATE)

    @classmethod
    def restart(
        cls,
        *,
        max_restarts: int | None = None,
        window: timedelta | None = None,
        backoff: Backoff | None = None,
    ) -> "SupervisorStrategy":
        """Rebuild the actor from the behavior it was spawned with.

        Args:
            max_restarts: How many restarts to allow within `window`. The actor
                is stopped once that is exceeded.
            window: The span restarts are counted over. Without one, the count
                runs for the life of the actor.
            backoff: How long to wait before each restart. Without one the
                restart is immediate, which is right for a fault that is
                genuinely instantaneous and wrong for anything involving a
                dependency.

        Returns:
            The strategy.
        """
        return cls(
            Decision.RESTART,
            max_restarts=max_restarts,
            window=window,
            backoff=backoff,
        )

    def __repr__(self) -> str:
        """Render as the factory call that produces it."""
        if self.decision is not Decision.RESTART:
            return f"SupervisorStrategy.{self.decision.value}()"
        limits = [
            f"max_restarts={self.max_restarts!r}",
            f"window={self.window!r}",
            f"backoff={self.backoff!r}",
        ]
        return f"SupervisorStrategy.restart({', '.join(limits)})"
