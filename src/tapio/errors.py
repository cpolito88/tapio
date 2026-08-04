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
    "ActorSystemTerminating",
    "AskTargetTerminated",
    "AskTimeoutError",
    "AskTypeError",
    "BehaviorTypeError",
    "FrameTooLargeError",
    "MailboxFullError",
    "MessageDecodingError",
    "MessageEncodingError",
    "MessageRegistrationError",
    "MessageTypeError",
    "RefResolutionError",
    "StashOverflowError",
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


class RefResolutionError(TapioError):
    """A ref could not be rebuilt from its string form.

    Raised when no system is in scope to resolve it against, and when the
    string is not a ref at all. `model_dump()` on a model holding a ref
    succeeds anywhere; feeding the result back to `model_validate()` succeeds
    only inside a system's decode path or an explicit
    `with system.as_deserialization_context():` block. The asymmetry is
    deliberate: a ref is a handle into a live runtime, and there is no
    meaningful ref outside of one.
    """


class MessageRegistrationError(TapioError):
    """A message type could not be registered, or was never registered.

    Raised at import time for a duplicate wire key, since two classes sharing
    one would otherwise decode as whichever imported last, and at encode time
    for a type that has no key, since a key is never an import path and an
    unregistered type could not be rebuilt by the peer.
    """


class MessageEncodingError(TapioError):
    """A message could not be written to the wire.

    Raised at the send site, because the message belongs to the sender: an
    error about it is the sender's to catch, exactly as it is for a local
    `tell`.
    """


class FrameTooLargeError(MessageEncodingError):
    """A frame exceeded the configured size limit.

    On the way out this raises at the send site. On the way in, the declared
    length is checked before the body is read, so the frame costs a header and
    a refusal rather than the memory it asked for.
    """


class MessageDecodingError(TapioError):
    """A frame could not be read.

    Never raised into application code: the receiving end turns one of these
    into a dead letter naming what was wrong, because the failure belongs to a
    peer and there is no local caller to tell about it.
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


class StashOverflowError(TapioError):
    """A stash was full and one more message was put aside.

    Raised in the *stashing* actor, since only it knows whether the right
    answer is to shed the message, reject it, or let the failure become a
    supervision decision. A stash is bounded for the same reason a mailbox can
    be: it holds traffic the actor is by definition not keeping up with.
    """


class MailboxFullError(TapioError):
    """A bounded mailbox with the `Fail` overflow strategy was full.

    Raised in the *sender*, since only the sender knows whether to retry, shed,
    or escalate.
    """


class ActorSystemTerminating(TapioError, RuntimeError):  # noqa: N818 - a state
    """An operation was attempted on an actor or system that is shutting down."""
