"""Two systems, the messages they send each other, and a peer that misbehaves.

Everything the remoting tests share. Systems always bind port 0, so the OS
picks and no two tests argue over a number somebody chose, and the fake peer
below is what makes the receiving end testable: it dials a real port, does the
handshake by hand, and can then write anything, including the frames a correct
implementation never would.
"""

import hmac
import json
from hashlib import sha256

from tapio import Message
from tapio.actor import ActorRef, ActorSystem, Behavior, Behaviors
from tapio.remote.address import Address, format_ref
from tapio.remote.registry import register_message
from tapio.remote.transport import FrameLink, connect, framed
from tapio.settings import RemoteSettings, TapioSettings
from tapio.version import __version__


@register_message()
class Ping(Message):
    """A request that says where its answer should go."""

    n: int
    reply_to: ActorRef["Pong"]


@register_message()
class Pong(Message):
    """The answer."""

    n: int


@register_message()
class Tick(Message):
    """A one-way message, for the tests about order and volume."""

    n: int


class Unregistered(Message):
    """A message with no wire key, so nothing can name it on a frame."""

    n: int


GHOST = Address(system="ghost", host="127.0.0.1", port=1)
"""An address nothing listens on: port 1 is privileged and unbound here."""


def remoting(**overrides: object) -> TapioSettings:
    """Settings for a system listening on a loopback port of the OS's choosing."""
    return TapioSettings(
        _env_file=None,
        remote=RemoteSettings(_env_file=None, bind_port=0, **overrides),  # type: ignore[arg-type]
    )


def uri(system: ActorSystem, ref: ActorRef[Message]) -> str:
    """The full string form of a ref, as a peer would be handed it."""
    return format_ref(system.address, ref.path)


def echoing() -> Behavior[Ping]:
    """An actor that answers every ping through the ref it was given."""

    async def on_message(message: Ping) -> Behavior[Ping]:
        message.reply_to.tell(Pong(n=message.n))
        return Behaviors.same()

    return Behaviors.receive_message(on_message, msg_type=Ping)


def collecting(seen: list[Pong]) -> Behavior[Pong]:
    """An actor that writes down every answer, as the object it received."""

    async def on_message(message: Pong) -> Behavior[Pong]:
        seen.append(message)
        return Behaviors.same()

    return Behaviors.receive_message(on_message, msg_type=Pong)


def counting(seen: list[int]) -> Behavior[Tick]:
    """An actor that writes down the order it saw things in.

    A negative tick stops it, which is how a test ends an actor without
    reaching for `abort`: the cancellation that follows one lands in the test
    rather than in the runtime.
    """

    async def on_message(message: Tick) -> Behavior[Tick]:
        if message.n < 0:
            return Behaviors.stopped()
        seen.append(message.n)
        return Behaviors.same()

    return Behaviors.receive_message(on_message, msg_type=Tick)


async def dial(
    target: ActorSystem,
    *,
    secret: str | None = None,
    version: str = __version__,
    address: Address = GHOST,
    system: str | None = None,
    proof: str | None = None,
    welcome: bool = True,
) -> FrameLink:
    """Dial a system as a peer that writes its own handshake.

    Every argument is here so a test can get one of them wrong on purpose: the
    version, the proof, or the address a peer claims to be at.

    Args:
        target: The system to dial.
        secret: Used to answer the challenge, when the caller holds one.
        version: The tapio version to claim.
        address: The canonical address to advertise.
        system: The name to claim, when it should not be the one in `address`.
        proof: An answer to the challenge, overriding the one `secret` gives.
        welcome: Read the peer's welcome before returning. Off for the tests
            that expect a refusal, where there is no welcome to wait for.

    Returns:
        The link, ready to be written to or watched for a close.
    """
    port = target.address.port
    assert port is not None
    link = await connect(
        "127.0.0.1", port, max_frame_bytes=1024 * 1024, ssl_context=None
    )
    hello = await link.read_link(2.0)
    answer = proof if proof is not None else _proof(secret, str(hello["nonce"]))
    await link.write_frame(
        framed(
            json.dumps(
                {
                    "link": "client-hello",
                    "system": system if system is not None else address.system,
                    "address": str(address),
                    "uid": 99,
                    "version": version,
                    "nonce": "0" * 32,
                    "proof": answer,
                }
            ).encode()
        )
    )
    if welcome:
        await link.read_link(2.0)
    return link


def _proof(secret: str | None, nonce: str) -> str:
    """Answer a challenge the way a peer holding the secret would."""
    if secret is None:
        return ""
    return hmac.new(secret.encode(), nonce.encode(), sha256).hexdigest()
