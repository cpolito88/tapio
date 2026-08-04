"""The tapio error hierarchy.

Every error tapio raises derives from `TapioError`, so a caller can
catch the whole library with one clause. Where an error also has an obvious
builtin counterpart it inherits from that too. `MessageTypeError` is a `TypeError` and
`AskTimeoutError` is a `TimeoutError`, so existing `except` clauses keep
working.

Note that no tapio error inherits from `ValueError`. Pydantic converts a
`ValueError` raised inside a validator into a `ValidationError`, which
would bury the message; raising something else lets it propagate intact.
"""

__all__ = [
    "ActorNameError",
    "ActorRefDeserializationError",
    "ActorSystemTerminating",
    "AskTargetTerminated",
    "AskTimeoutError",
    "AskTypeError",
    "BehaviorTypeError",
    "MailboxFullError",
    "MessageTypeError",
    "TapioError",
    "WatchError",
]


class TapioError(Exception):
    """Base class for every error raised by tapio."""


class MessageTypeError(TapioError, TypeError):
    """A message does not match the recipient's declared message type.

    Also raised when a declared message type is not a `tapio.Message`
    subclass, because re-validation on a plain `BaseModel` silently does
    nothing.
    """


class BehaviorTypeError(TapioError, TypeError):
    """A behavior's message type could not be resolved.

    Raised when neither an explicit `msg_type` nor a readable annotation is
    available. A behavior with no resolvable message type is never spawned:
    silently skipping the type check is the failure mode the check exists to
    prevent.
    """


class ActorNameError(TapioError):
    """A child could not be given the name it asked for.

    Names are unique among an actor's live children, since the path they form
    is the actor's identity in logs and in every error message.
    """


class ActorRefDeserializationError(TapioError):
    """An `tapio.actor.ActorRef` was validated from its path string.

    A path cannot be resolved back to a live local ref without a registry, so
    a model containing an `ActorRef` does not round-trip: `model_dump()`
    succeeds and feeding its output back to `model_validate()` raises this.
    """


class WatchError(TapioError):
    """A ref could not be watched.

    Raised for a ref with no live cell behind it, and for an actor watching
    itself, which would promise a signal that cannot be delivered: by the time
    the actor has stopped there is nobody left to read its own mailbox.
    """


class AskTimeoutError(TapioError, TimeoutError):
    """No reply arrived within the ask timeout."""


class AskTypeError(TapioError, TypeError):
    """A reply to an ask did not match the expected reply type."""


class AskTargetTerminated(TapioError):  # noqa: N818 - reads as a state, not a failure
    """The target of an ask stopped before it replied."""


class MailboxFullError(TapioError):
    """A bounded mailbox with the `Fail` overflow strategy was full.

    Raised in the *sender*, since only the sender knows whether to retry, shed,
    or escalate.
    """


class ActorSystemTerminating(TapioError, RuntimeError):  # noqa: N818 - a state
    """An operation was attempted on an actor or system that is shutting down."""
