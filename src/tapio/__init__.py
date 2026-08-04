"""tapio: a Pekko-inspired actor toolkit for Python.

Local, typed, asyncio-native actors with supervision, and Pydantic models
throughout.
"""

from tapio.actor.behavior import AbstractBehavior, Behavior, Behaviors
from tapio.actor.context import ActorContext
from tapio.actor.dead_letters import DeadLetter, DeadLetterReason
from tapio.actor.mailbox import MailboxConfig, OverflowStrategy
from tapio.actor.path import ActorPath
from tapio.actor.ref import ActorRef
from tapio.actor.router import RoundRobin, Routers, RoutingStrategy
from tapio.actor.signals import ChildFailed, PostStop, PreRestart, Signal, Terminated
from tapio.actor.stash import StashBuffer
from tapio.actor.supervision import Backoff, Decision, SupervisorStrategy
from tapio.actor.system import ActorSystem
from tapio.actor.timers import TimerScheduler
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
    StashOverflowError,
    TapioError,
    WatchError,
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
    "Backoff",
    "Behavior",
    "BehaviorTypeError",
    "Behaviors",
    "ChildFailed",
    "DeadLetter",
    "DeadLetterReason",
    "Decision",
    "MailboxConfig",
    "MailboxFullError",
    "Message",
    "MessageTypeError",
    "OverflowStrategy",
    "PostStop",
    "PreRestart",
    "RoundRobin",
    "Routers",
    "RoutingStrategy",
    "Signal",
    "StashBuffer",
    "StashOverflowError",
    "SupervisorStrategy",
    "TapioError",
    "TapioSettings",
    "Terminated",
    "TimerScheduler",
    "WatchError",
    "__version__",
]

__version__ = "0.0.0"
