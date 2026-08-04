"""System-wide settings, read from the environment with a `TAPIO_` prefix."""

from datetime import timedelta

from pydantic_settings import BaseSettings, SettingsConfigDict

from tapio.actor.mailbox import MailboxConfig, OverflowStrategy

__all__ = ["RemoteSettings", "TapioSettings"]


class RemoteSettings(BaseSettings):
    """Where this system listens, and how peers address it.

    Nested under `TapioSettings.remote` rather than spread across it, so "is
    this system reachable from outside the process" is one `is None` check
    instead of a handful of defaults that individually look harmless.

    Bind and canonical are separate because they routinely differ: containers,
    NAT and port mapping all mean the address a peer dials is not the one the
    socket is bound to. What a ref writes down is always the canonical one.
    """

    model_config = SettingsConfigDict(env_prefix="TAPIO_REMOTE_", frozen=True)

    bind_host: str = "127.0.0.1"
    """The interface to listen on. Loopback by default: a port that accepts
    frames naming actor paths and message types is a serious surface, and the
    default is set for someone who has not thought about it yet."""

    bind_port: int = 25520
    """The port to listen on."""

    canonical_host: str | None = None
    """The host peers dial. `bind_host` when omitted."""

    canonical_port: int | None = None
    """The port peers dial. `bind_port` when omitted."""

    max_frame_bytes: int = 4 * 1024 * 1024
    """Refuse a frame larger than this, before its body is read."""


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

    remote: RemoteSettings | None = None
    """How this system is addressed from outside the process.

    `None` means remoting is off, which is the default: a system that has not
    asked to be reachable is not. Refs it hands out still serialize, carrying
    the system name and no host, so a peer reading one can tell which system it
    names and that there is nowhere to dial.
    """

    @property
    def default_mailbox(self) -> MailboxConfig:
        """The mailbox configuration a spawn gets when it asks for nothing."""
        return MailboxConfig(
            capacity=self.default_mailbox_capacity,
            on_overflow=self.default_mailbox_overflow,
        )
