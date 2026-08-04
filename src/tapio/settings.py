"""System-wide settings, read from the environment with a `TAPIO_` prefix."""

from datetime import timedelta

from pydantic_settings import BaseSettings, SettingsConfigDict

from tapio.actor.mailbox import MailboxConfig, OverflowStrategy

__all__ = ["TapioSettings"]


class TapioSettings(BaseSettings):
    """Tunables for one actor system.

    Every field can be set with an environment variable, upper-cased and
    prefixed: `TAPIO_VALIDATE_ON_TELL=0`, `TAPIO_ASK_TIMEOUT=PT2S`.
    """

    model_config = SettingsConfigDict(env_prefix="TAPIO_", frozen=True)

    validate_on_tell: bool = True
    """Re-validate a message's *contents* on delivery, not just its type.

    The type check is unconditional and cheap. This switch controls the
    expensive half, a full re-validation whose result is discarded, so the
    cost is measurable and tunable in one place rather than at call sites.
    Turning it off changes cost and nothing else: the recipient always receives
    the object the sender passed.
    """

    default_mailbox_capacity: int | None = None
    """User-lane capacity for new mailboxes; `None` means unbounded.

    The system lane is always unbounded, whatever this says: a capacity limit
    that could refuse a stop signal would make shutdown unreliable.
    """

    default_mailbox_overflow: OverflowStrategy = OverflowStrategy.FAIL
    """What a bounded mailbox does when full, unless a spawn overrides it.

    Never consulted while `default_mailbox_capacity` is `None`.
    """

    ask_timeout: timedelta = timedelta(seconds=5)
    """Default deadline for `ActorRef.ask` when the call does not give one."""

    shutdown_timeout: timedelta = timedelta(seconds=10)
    """One deadline for the whole tree, not a per-actor timeout.

    Shutdown races a single clock, so worst-case shutdown time tracks this
    value rather than depth times timeout.
    """

    blocking_pool_size: int = 16
    """Threads available to `ctx.run_blocking`.

    A private, bounded pool rather than the loop's default executor, which is
    shared with every other library in the process and whose size tapio does
    not control, so a bound could not be honoured.
    """

    dead_letter_log_first: int = 10
    """Log this many dead letters in full, then switch to periodic summaries.

    A dead actor in a hot send loop must not drown the log.
    """

    dead_letter_summary_interval: timedelta = timedelta(seconds=60)
    """How often to log a summary once `dead_letter_log_first` is spent."""

    @property
    def default_mailbox(self) -> MailboxConfig:
        """The mailbox configuration a spawn gets when it asks for nothing."""
        return MailboxConfig(
            capacity=self.default_mailbox_capacity,
            on_overflow=self.default_mailbox_overflow,
        )
