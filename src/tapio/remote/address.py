"""Addresses: where a system is, and how a ref writes itself down.

A ref's string form is its wire form:

```
tapio://orders@10.0.0.4:25520/user/checkout/session-7#3
        └ system  └ address           └ path          └ uid
```

The address a ref carries is the **canonical** one, which is what a peer
dials. It is not always what a socket is bound to, since containers, NAT and
port mapping routinely make the two differ. A system with remoting switched
off has no host and port at all and writes `tapio://orders/user/x#3`, which a
peer reads as a ref it cannot reach rather than an address to guess at.

The uid is the incarnation guard, and it is why a bare path will not do. Paths
are reusable: stop `/user/worker`, spawn another actor under the same name,
and a ref written down before the stop would now address a stranger. A frame
whose uid does not match the live actor becomes a dead letter.
"""

import re
from dataclasses import dataclass
from typing import Final, Self, final

from tapio.actor.path import SCHEME, ActorPath

__all__ = ["Address", "format_ref", "parse_ref"]

_MAX_PORT: Final = 65535

# A host is either a bracketed IPv6 literal, or a run of characters that
# cannot be confused with the delimiters around it: no "/", ":" or "@".
_HOST: Final = r"\[[0-9A-Fa-f:.]+\]|[^/:@\s]+"

_ADDRESS_RE: Final = re.compile(
    rf"\A{SCHEME}://(?P<system>[^/@\s]+)(?:@(?P<host>{_HOST}):(?P<port>\d+))?\Z"
)

_REF_RE: Final = re.compile(
    rf"\A{SCHEME}://(?P<system>[^/@\s]+)(?:@(?P<host>{_HOST}):(?P<port>\d+))?"
    r"/(?P<path>[^#\s]*)(?:#(?P<uid>\d+))?\Z"
)


@final
@dataclass(frozen=True, slots=True)
class Address:
    """Where one actor system is, as far as other systems are concerned.

    `host` and `port` are set together or not at all. Without them the address
    is unaddressable. It names a system, which is enough to tell a local ref
    from a foreign one, but there is nothing for a peer to dial.
    """

    system: str
    """The system name, which is also the first element of every path below it."""

    host: str | None = None
    """The canonical host peers dial, or `None` when remoting is off."""

    port: int | None = None
    """The canonical port peers dial, or `None` when remoting is off."""

    def __post_init__(self) -> None:
        """Reject a half-written address and a name no path could hold."""
        # It borrows the path rules rather than restating them. The system
        # name heads every path, so anything a path rejects is not a valid
        # system name either.
        ActorPath.root(self.system)
        if (self.host is None) != (self.port is None):
            msg = (
                f"an address needs a host and a port together or neither, got "
                f"host={self.host!r} port={self.port!r}"
            )
            raise ValueError(msg)
        if self.port is not None and not 1 <= self.port <= _MAX_PORT:
            msg = f"invalid port: {self.port!r}"
            raise ValueError(msg)

    @property
    def is_addressable(self) -> bool:
        """Whether a peer could dial this address."""
        return self.host is not None

    @classmethod
    def parse(cls, text: str) -> Self:
        """Read an address back from its string form.

        Args:
            text: `tapio://orders` or `tapio://orders@10.0.0.4:25520`.

        Returns:
            The address.

        Raises:
            ValueError: If the text is not an address in that form.
        """
        match = _ADDRESS_RE.match(text)
        if match is None:
            msg = f"not an actor system address: {text!r}"
            raise ValueError(msg)
        port = match["port"]
        return cls(
            system=match["system"],
            host=match["host"],
            port=int(port) if port is not None else None,
        )

    def __str__(self) -> str:
        """Render as `tapio://orders@10.0.0.4:25520`, or without the host part."""
        if self.host is None:
            return f"{SCHEME}://{self.system}"
        return f"{SCHEME}://{self.system}@{self.host}:{self.port}"

    def __repr__(self) -> str:
        """Render as the string form, which is what a reader wants to see."""
        return f"Address({str(self)!r})"


def format_ref(address: Address, path: ActorPath) -> str:
    """Write a ref down as an address, a path and an incarnation uid.

    Args:
        address: Where the ref's system is.
        path: Where in that system the actor sits.

    Returns:
        The full string form, `tapio://orders@10.0.0.4:25520/user/x#3`.

    Raises:
        ValueError: If the path belongs to a different system than the address.
    """
    if path.system != address.system:
        msg = (
            f"cannot address {path} through {address}: the path names system "
            f"{path.system!r} and the address names {address.system!r}"
        )
        raise ValueError(msg)
    body = "/".join(path.elements)
    fragment = f"#{path.uid}" if path.uid else ""
    return f"{address}/{body}{fragment}"


def parse_ref(text: str) -> tuple[Address, ActorPath]:
    """Read a ref's string form back into an address and a path.

    Args:
        text: The full string form, with or without the host part.

    Returns:
        The address and the path, which still has to be resolved against a
        system before it addresses anything.

    Raises:
        ValueError: If the text is not a ref string, or holds a name no actor
            path could hold.
    """
    match = _REF_RE.match(text)
    if match is None:
        msg = (
            f"not an actor ref: {text!r}. The form is "
            f"{SCHEME}://system[@host:port]/path/to/actor[#uid]"
        )
        raise ValueError(msg)
    port = match["port"]
    address = Address(
        system=match["system"],
        host=match["host"],
        port=int(port) if port is not None else None,
    )
    body = match["path"]
    uid = match["uid"]
    path = ActorPath(
        system=address.system,
        elements=tuple(body.split("/")) if body else (),
        uid=int(uid) if uid is not None else 0,
    )
    return address, path
