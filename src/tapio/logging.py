"""Logging that always says which actor spoke.

A log line from an actor system is close to useless without the actor's path.
"connection refused" is noise; `tapio://app/user/pool/worker-3: connection
refused` is a bug report. So the runtime never hands a behavior a bare
`Logger`. `ctx.log` is an adapter bound to the cell's path, and every record
it emits carries that path in the formatted message and as an `actor_path`
attribute for structured handlers.
"""

import logging
from collections.abc import MutableMapping
from typing import Any

from tapio.actor.path import ActorPath

__all__ = [
    "ActorLogAdapter",
    "actor_logger",
    "describe_callable",
    "runtime_logger",
]

_ROOT: str = "tapio"


class ActorLogAdapter(logging.LoggerAdapter[logging.Logger]):
    """A logger bound to one actor path.

    The path is added twice on purpose. As a prefix, so a plain
    `logging.basicConfig()` setup is readable with no configuration. As a
    record attribute, so a structured handler can index on it without parsing
    the message apart again.
    """

    def __init__(self, logger: logging.Logger, path: ActorPath) -> None:
        """Bind the adapter to an actor path."""
        super().__init__(logger, {"actor_path": str(path)})
        self.path = path

    def process(
        self, msg: Any, kwargs: MutableMapping[str, Any]
    ) -> tuple[Any, MutableMapping[str, Any]]:
        """Prefix the message with the path and merge, not replace, `extra`.

        `LoggerAdapter.process` replaces `kwargs["extra"]` entirely, which
        would drop a caller's own structured fields.
        """
        extra = dict(self.extra or {})
        extra.update(kwargs.get("extra") or {})
        kwargs["extra"] = extra
        return f"{self.path}: {msg}", kwargs


def actor_logger(path: ActorPath) -> ActorLogAdapter:
    """Return the logger for the actor at `path`."""
    return ActorLogAdapter(logging.getLogger(f"{_ROOT}.actor"), path)


def runtime_logger(name: str) -> logging.Logger:
    """Return a runtime logger for messages that belong to no single actor."""
    return logging.getLogger(f"{_ROOT}.{name}")


def describe_callable(obj: object) -> str:
    """Best available name for a callable, for an error message or a repr.

    A function gives its `__qualname__`. Anything without one, a lambda
    included, falls back to `repr`. One implementation because these strings
    describe overlapping failures: an actor can name a handler, an adapting
    function and a blocking call in three different messages, and they should
    read the same way. Improving how a lambda renders is then one change here
    rather than four.

    Args:
        obj: The callable to name. Anything is accepted, since a value that
            was supposed to be callable and is not still has to be named in
            the error that says so.

    Returns:
        Its qualified name, or its `repr` when it has none.
    """
    return getattr(obj, "__qualname__", None) or repr(obj)
