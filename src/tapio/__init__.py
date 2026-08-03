"""tapio: a Pekko-inspired actor toolkit for Python.

Local, typed, asyncio-native actors with supervision, and Pydantic models
throughout.
"""

from tapio.errors import (
    ActorRefDeserializationError,
    ActorSystemTerminating,
    AskTargetTerminated,
    AskTimeoutError,
    AskTypeError,
    BehaviorTypeError,
    MailboxFullError,
    MessageTypeError,
    TapioError,
)
from tapio.message import Message
from tapio.settings import TapioSettings

__all__ = [
    "ActorRefDeserializationError",
    "ActorSystemTerminating",
    "AskTargetTerminated",
    "AskTimeoutError",
    "AskTypeError",
    "BehaviorTypeError",
    "MailboxFullError",
    "Message",
    "MessageTypeError",
    "TapioError",
    "TapioSettings",
    "__version__",
]

__version__ = "0.0.0"
