"""tapio: a Pekko-inspired actor toolkit for Python.

Local, typed, asyncio-native actors with supervision, and Pydantic models
throughout.
"""

from tapio.actor.behavior import AbstractBehavior, Behavior, Behaviors
from tapio.actor.context import ActorContext
from tapio.actor.path import ActorPath
from tapio.actor.ref import ActorRef
from tapio.actor.system import ActorSystem
from tapio.errors import (
    ActorNameError,
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
    "AbstractBehavior",
    "ActorContext",
    "ActorNameError",
    "ActorPath",
    "ActorRef",
    "ActorRefDeserializationError",
    "ActorSystem",
    "ActorSystemTerminating",
    "AskTargetTerminated",
    "AskTimeoutError",
    "AskTypeError",
    "Behavior",
    "BehaviorTypeError",
    "Behaviors",
    "MailboxFullError",
    "Message",
    "MessageTypeError",
    "TapioError",
    "TapioSettings",
    "__version__",
]

__version__ = "0.0.0"
