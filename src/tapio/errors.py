"""The tapio error hierarchy.

Every error tapio raises derives from `TapioError`, so a caller can catch the
whole library with one clause. Where an error has an obvious builtin
counterpart it inherits from that too: `MessageTypeError` is a `TypeError` and
`AskTimeoutError` is a `TimeoutError`, so existing `except` clauses keep
working.

No tapio error inherits from `ValueError`. Pydantic turns a `ValueError`
raised inside a validator into a `ValidationError`, which would bury the
message. Raising something else lets it propagate intact.
"""

__all__ = [
    "ActorNameError",
    "ActorSystemTerminating",
    "AskTargetTerminated",
    "AskTargetUnreachable",
    "AskTimeoutError",
    "AskTypeError",
    "BehaviorRegistrationError",
    "BehaviorTypeError",
    "ClusterError",
    "FrameTooLargeError",
    "HandshakeError",
    "InsecureRemoteConfig",
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

    Raised when there is neither an explicit `msg_type` nor a readable
    annotation. A behavior with no message type is never spawned, because
    silently skipping the type check is what the check exists to prevent.
    """


class ActorNameError(TapioError):
    """A child could not be given the name it asked for.

    Names are unique among an actor's live children, because the path they
    form is the actor's identity in logs and in every error message.
    """


class RefResolutionError(TapioError):
    """A ref could not be rebuilt from its string form.

    Raised when no system is in scope to resolve it against, and when the
    string is not a ref at all. `model_dump()` on a model holding a ref works
    anywhere. Feeding the result back to `model_validate()` works only inside
    a system's decode path or an explicit
    `with system.as_deserialization_context():` block. A ref is a handle into
    a live runtime, and there is no meaningful ref without one.
    """


class MessageRegistrationError(TapioError):
    """A message type could not be registered, or was never registered.

    Raised at import time for a duplicate wire key, because two classes
    sharing one would decode as whichever imported last. Raised at encode time
    for a type with no key, because a key is never an import path and a peer
    could not rebuild an unregistered type.
    """


class BehaviorRegistrationError(TapioError):
    """A behavior could not be offered to peers, or was never registered.

    Raised at import time for a duplicate factory key and for a factory whose
    arguments model cannot be resolved, since a factory no peer could call is
    a bug where it is written. Raised at construction for a spawner offering a
    key nothing registered, which is almost always a typo in the allowlist.
    """


class MessageEncodingError(TapioError):
    """A message could not be written to the wire.

    Raised at the send site, because the message belongs to the sender. An
    error about it is the sender's to catch, as it is for a local `tell`.
    """


class FrameTooLargeError(MessageEncodingError):
    """A frame exceeded the configured size limit.

    On the way out it raises at the send site. On the way in, the declared
    length is checked before the body is read, so the frame costs a header and
    a refusal instead of the memory it asked for.
    """


class MessageDecodingError(TapioError):
    """A frame could not be read.

    Never raised into application code. The receiving end turns it into a dead
    letter naming what was wrong, because the failure belongs to a peer and
    there is no local caller to tell.
    """


class InsecureRemoteConfig(TapioError):  # noqa: N818 - names a configuration
    """Remoting was configured to listen beyond loopback with nothing to prove.

    Raised at system construction, so a deployment that would accept frames
    from anything that can reach the port fails to start. The error names both
    settings involved: bind somewhere else, or set a secret.
    """


class HandshakeError(TapioError):
    """A link was refused before it carried a single message.

    The causes are a version this system does not speak, a secret that did not
    match, or a peer that stopped talking part-way through. The connection is
    closed, the reason is logged, and no further frames are read. A wire
    format that half works is worse than one that refuses.
    """


class WatchError(TapioError):
    """A ref could not be watched.

    Raised for a ref with no live cell behind it, and for an actor watching
    itself. The second would promise a signal that cannot be delivered: once
    the actor has stopped, nobody is left to read its mailbox.
    """


class AskTimeoutError(TapioError, TimeoutError):
    """No reply arrived within the ask timeout."""


class AskTypeError(TapioError, TypeError):
    """A reply to an ask did not match the expected reply type."""


class AskTargetTerminated(TapioError):  # noqa: N818 - reads as a state, not a failure
    """The target of an ask stopped before it replied."""


class AskTargetUnreachable(TapioError):  # noqa: N818 - reads as a state
    """The peer holding the target of an ask became unreachable.

    Different from `AskTargetTerminated` on purpose. That one says an actor
    stopped, which is a fact. This one says a link went silent, which is a
    judgement that can be wrong: the actor may be alive on the other side of a
    partition. Both fail the ask at once rather than after the full timeout,
    and which one arrived tells the caller whether retrying elsewhere makes
    sense.
    """


class StashOverflowError(TapioError):
    """A stash was full and one more message was put aside.

    Raised in the actor that stashed, because only it knows whether to drop
    the message, reject it, or let the failure become a supervision decision.
    A stash is bounded for the same reason a mailbox can be: it holds traffic
    the actor is not keeping up with.
    """


class MailboxFullError(TapioError):
    """A bounded mailbox with the `Fail` overflow strategy was full.

    Raised in the sender, because only the sender knows whether to retry, drop
    the message, or escalate.
    """


class ActorSystemTerminating(TapioError, RuntimeError):  # noqa: N818 - a state
    """An operation was attempted on an actor or system that is shutting down."""


class ClusterError(TapioError):
    """Clustering was asked for something it cannot do.

    Raised where the caller can act: a system with no address to be dialled
    at, a join with no seeds to ask, or a join or a leave that did not finish
    in the time allowed. The last of those does not stop the node trying, so
    catching it is a decision about how long to wait rather than about whether
    the cluster is broken.
    """
