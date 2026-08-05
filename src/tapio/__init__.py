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
    ActorSystemTerminating,
    AskTargetTerminated,
    AskTimeoutError,
    AskTypeError,
    BehaviorTypeError,
    FrameTooLargeError,
    HandshakeError,
    InsecureRemoteConfig,
    MailboxFullError,
    MessageDecodingError,
    MessageEncodingError,
    MessageRegistrationError,
    MessageTypeError,
    RefResolutionError,
    StashOverflowError,
    TapioError,
    WatchError,
)
from tapio.message import Message
from tapio.remote.address import Address
from tapio.remote.registry import register_message
from tapio.settings import RemoteSettings, TapioSettings, TLSSettings
from tapio.version import __version__

__all__ = [
    "AbstractBehavior",
    "ActorContext",
    "ActorNameError",
    "ActorPath",
    "ActorRef",
    "ActorSystem",
    "ActorSystemTerminating",
    "Address",
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
    "FrameTooLargeError",
    "HandshakeError",
    "InsecureRemoteConfig",
    "MailboxConfig",
    "MailboxFullError",
    "Message",
    "MessageDecodingError",
    "MessageEncodingError",
    "MessageRegistrationError",
    "MessageTypeError",
    "OverflowStrategy",
    "PostStop",
    "PreRestart",
    "RefResolutionError",
    "RemoteSettings",
    "RoundRobin",
    "Routers",
    "RoutingStrategy",
    "Signal",
    "StashBuffer",
    "StashOverflowError",
    "SupervisorStrategy",
    "TLSSettings",
    "TapioError",
    "TapioSettings",
    "Terminated",
    "TimerScheduler",
    "WatchError",
    "__version__",
    "register_message",
]
