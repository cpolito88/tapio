"""Narrowing a public type to the runtime object a test asserts on.

The library hands out deliberately wide types: `ActorSystem.remote` is `None`
unless remoting was configured, and an `ActorRef` is a handle rather than the
cell behind it. Application code needs no more than that. A test about the
runtime does, and this is where it says so, once, instead of at each use.
"""

from typing import Any

from tapio.actor import ActorSystem
from tapio.actor.cell import ActorCell, LocalActorRef
from tapio.actor.ref import ActorRef
from tapio.remote.endpoint import RemoteEndpoint


def endpoint(system: ActorSystem) -> RemoteEndpoint:
    """This system's remoting, which the caller configured.

    Args:
        system: A system started with remote settings.

    Returns:
        Its endpoint.
    """
    remote = system.remote
    assert remote is not None, f"{system.name} was started without remoting"
    return remote


def cell_of(ref: ActorRef[Any]) -> ActorCell[Any]:
    """The cell behind a ref, for the tests that assert on runtime state.

    Args:
        ref: A ref a running system handed out for a local actor.

    Returns:
        The cell it delivers into.
    """
    assert isinstance(ref, LocalActorRef), ref
    return ref.cell
