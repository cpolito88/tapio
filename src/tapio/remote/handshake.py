"""The handshake: who is on the other end, and may they speak at all.

Before a single message frame crosses a link, both ends say who they are and
prove they hold the shared secret. Three link frames, one round trip and a
half:

```
server -> client   server-hello   name, address, uid, version, nonce
client -> server   client-hello   name, address, uid, version, nonce, proof
server -> client   welcome        proof
```

Both proofs are HMACs of the *other* side's nonce, so neither end can be
replayed at the other and a peer that holds no secret cannot pass for one that
does. Three things get established here, and each of them is load-bearing:

* **Version equality.** Both ends must run the same tapio version. An
  incompatible wire format that half works is worse than one that refuses, and
  a version is a cheap thing to check before anything harder to diagnose goes
  wrong.
* **The canonical address**, which is what this system will use to address the
  peer and to key the association. It is what the peer advertises rather than
  the socket it dialled from, since those differ routinely: containers, NAT,
  port mapping.
* **The system uid**, minted per incarnation. It is what makes a restarted peer
  a *different* peer rather than a slow one, which is the distinction every
  later judgement about reachability rests on.
"""

import hmac
import secrets
from dataclasses import dataclass
from hashlib import sha256
from typing import Final, TypeVar, final

from pydantic import SecretStr, ValidationError

from tapio.errors import HandshakeError
from tapio.remote.address import Address
from tapio.remote.transport import FrameLink, LinkFrame
from tapio.version import __version__

__all__ = ["PeerIdentity", "accept", "introduce"]

_NONCE_BYTES: Final = 16

F = TypeVar("F", bound=LinkFrame)


@final
@dataclass(frozen=True, slots=True)
class PeerIdentity:
    """Who answered on the other end of a link."""

    address: Address
    """The canonical address the peer advertises, which is what it is dialled by."""

    uid: int
    """The peer's incarnation uid. A new one means the old peer died."""

    version: str
    """The tapio version it runs, which equals this system's or it got no further."""


class _ServerHello(LinkFrame):
    """What a listening system says first, including its challenge."""

    link: str = "server-hello"
    system: str
    address: str
    uid: int
    version: str
    nonce: str


class _ClientHello(LinkFrame):
    """The dialler's answer: who it is, its own challenge, and its proof."""

    link: str = "client-hello"
    system: str
    address: str
    uid: int
    version: str
    nonce: str
    proof: str


class _Welcome(LinkFrame):
    """The server's proof, which is what makes the authentication mutual."""

    link: str = "welcome"
    proof: str


async def accept(
    link: FrameLink,
    *,
    address: Address,
    uid: int,
    secret: SecretStr | None,
    timeout: float,  # noqa: ASYNC109 - the handshake deadline
) -> PeerIdentity:
    """Handshake as the system that was dialled.

    Args:
        link: The freshly accepted connection.
        address: This system's canonical address, which the peer will dial.
        uid: This system's incarnation uid.
        secret: The shared secret, or `None` when nothing has to be proved.
        timeout: Seconds allowed for the whole exchange.

    Returns:
        Who dialled in.

    Raises:
        HandshakeError: If the peer speaks a different version, fails the
            challenge, or sends something that is not the expected frame.
        asyncio.IncompleteReadError: If the peer closed first.
        OSError: If the connection failed.
        TimeoutError: If the peer stopped talking part-way through.
    """
    nonce = secrets.token_hex(_NONCE_BYTES)
    await link.write_link(
        _ServerHello(
            system=address.system,
            address=str(address),
            uid=uid,
            version=__version__,
            nonce=nonce,
        )
    )
    hello = _read(_ClientHello, await link.read_link(timeout), "client-hello")
    _check_version(hello.version)
    _check_proof(secret, nonce, hello.proof, who="the peer that dialled in")
    # Everything a peer can be refused for is settled before it is welcomed:
    # a welcome the sender then closes on reads as a peer that vanished, and
    # what happened was a refusal with a reason.
    identity = _identify(hello.system, hello.address, hello.uid, hello.version)
    await link.write_link(_Welcome(proof=_proof(secret, hello.nonce)))
    return identity


async def introduce(
    link: FrameLink,
    *,
    address: Address,
    uid: int,
    secret: SecretStr | None,
    timeout: float,  # noqa: ASYNC109 - the handshake deadline
) -> PeerIdentity:
    """Handshake as the system that dialled.

    Args:
        link: The connection just opened.
        address: This system's canonical address, which the peer will dial to
            reply.
        uid: This system's incarnation uid.
        secret: The shared secret, or `None` when nothing has to be proved.
        timeout: Seconds allowed for the whole exchange.

    Returns:
        Who answered.

    Raises:
        HandshakeError: If the peer speaks a different version, fails the
            challenge, or sends something that is not the expected frame.
        asyncio.IncompleteReadError: If the peer closed first.
        OSError: If the connection failed.
        TimeoutError: If the peer stopped talking part-way through.
    """
    hello = _read(_ServerHello, await link.read_link(timeout), "server-hello")
    _check_version(hello.version)
    nonce = secrets.token_hex(_NONCE_BYTES)
    await link.write_link(
        _ClientHello(
            system=address.system,
            address=str(address),
            uid=uid,
            version=__version__,
            nonce=nonce,
            proof=_proof(secret, hello.nonce),
        )
    )
    welcome = _read(_Welcome, await link.read_link(timeout), "welcome")
    _check_proof(secret, nonce, welcome.proof, who="the peer that was dialled")
    return _identify(hello.system, hello.address, hello.uid, hello.version)


def _proof(secret: SecretStr | None, nonce: str) -> str:
    """Answer a challenge, or answer nothing when there is no secret."""
    if secret is None:
        return ""
    return hmac.new(
        secret.get_secret_value().encode(), nonce.encode(), sha256
    ).hexdigest()


def _check_proof(secret: SecretStr | None, nonce: str, proof: str, *, who: str) -> None:
    """Check an answer to this side's challenge.

    A system with no secret checks nothing, which is the loopback default. A
    system with one refuses a peer that answered without: the empty proof a
    secretless peer sends cannot match, and saying so plainly is better than
    letting the mismatch read as a bad password.

    Raises:
        HandshakeError: If the proof does not match.
    """
    if secret is None:
        return
    if hmac.compare_digest(_proof(secret, nonce), proof):
        return
    detail = "sent no proof at all" if not proof else "failed the challenge"
    msg = (
        f"{who} {detail}. Both ends of a link share one secret; set the same "
        "RemoteSettings(secret=...) on each."
    )
    raise HandshakeError(msg)


def _check_version(version: str) -> None:
    """Refuse a peer running a different tapio.

    Raises:
        HandshakeError: If the versions differ.
    """
    if version == __version__:
        return
    msg = (
        f"the peer runs tapio {version} and this system runs {__version__}. "
        "Both ends of a link run the same version: a wire format that half "
        "matches would corrupt a session rather than refuse one."
    )
    raise HandshakeError(msg)


def _identify(system: str, address: str, uid: int, version: str) -> PeerIdentity:
    """Turn what a peer said about itself into an identity, or refuse it.

    Raises:
        HandshakeError: If the advertised address is unusable, which includes
            a peer with remoting off: it has nothing to be dialled by, so
            there is nothing to associate with.
    """
    try:
        parsed = Address.parse(address)
    except ValueError as error:
        msg = f"the peer advertised {address!r}, which is not an address: {error}"
        raise HandshakeError(msg) from error
    if not parsed.is_addressable:
        msg = (
            f"the peer advertised {address!r}, which names a system and no host "
            "to dial. A system that opens a link advertises the address its "
            "refs are written with."
        )
        raise HandshakeError(msg)
    if parsed.system != system:
        msg = (
            f"the peer calls itself {system!r} and advertises {address!r}, which "
            "names a different system"
        )
        raise HandshakeError(msg)
    return PeerIdentity(address=parsed, uid=uid, version=version)


def _read(model: type[F], body: dict[str, object], expected: str) -> F:
    """Read one link frame into the model it should have been.

    Raises:
        HandshakeError: If it is a different frame, or a malformed one.
    """
    kind = body.get("link")
    if kind != expected:
        msg = f"expected a {expected} frame, got {kind!r}"
        raise HandshakeError(msg)
    try:
        return model.model_validate(body)
    except ValidationError as error:
        msg = f"malformed {expected} frame: {error}"
        raise HandshakeError(msg) from error
