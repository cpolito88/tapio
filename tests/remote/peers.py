"""Messages, behaviors and a fake peer, shared by the remoting tests."""

import hmac
import json
from hashlib import sha256

from tapio import Message
from tapio.actor import (
    ActorContext,
    ActorRef,
    ActorSystem,
    Behavior,
    Behaviors,
    Signal,
    Terminated,
)
from tapio.remote.address import Address, format_ref
from tapio.remote.protocol import PROTOCOL_VERSION
from tapio.remote.registry import register_message
from tapio.remote.transport import FrameLink, connect, framed
from tapio.settings import RemoteSettings, TapioSettings
from tapio.version import __version__


@register_message()
class Ping(Message):
    """A request carrying the ref its answer should go to."""

    n: int
    reply_to: ActorRef["Pong"]


@register_message()
class Pong(Message):
    """The answer."""

    n: int


@register_message()
class Tick(Message):
    """A one-way message, used by the tests about order and volume."""

    n: int


class Unregistered(Message):
    """A message with no wire key, so it cannot be encoded."""

    n: int


GHOST = Address(system="ghost", host="127.0.0.1", port=1)
"""An address nothing listens on: port 1 is privileged and unbound."""


def remoting(**overrides: object) -> TapioSettings:
    """Settings for a system listening on a loopback port the OS picks."""
    return TapioSettings(
        _env_file=None,
        remote=RemoteSettings(_env_file=None, bind_port=0, **overrides),  # type: ignore[arg-type]
    )


def uri(system: ActorSystem, ref: ActorRef[Message]) -> str:
    """The full string form of a ref, as a peer would receive it."""
    return format_ref(system.address, ref.path)


def echoing() -> Behavior[Ping]:
    """An actor that answers every ping on the ref the ping carried.

    A negative ping stops it without an answer, so a test can watch an ask
    lose its target through the target's own behavior.
    """

    async def on_message(message: Ping) -> Behavior[Ping]:
        if message.n < 0:
            return Behaviors.stopped()
        message.reply_to.tell(Pong(n=message.n))
        return Behaviors.same()

    return Behaviors.receive_message(on_message, msg_type=Ping)


def ignoring(seen: list[int]) -> Behavior[Ping]:
    """An actor that takes every ping and answers none of them."""

    async def on_message(message: Ping) -> Behavior[Ping]:
        seen.append(message.n)
        return Behaviors.same()

    return Behaviors.receive_message(on_message, msg_type=Ping)


def collecting(seen: list[Pong]) -> Behavior[Pong]:
    """An actor that appends every answer it receives."""

    async def on_message(message: Pong) -> Behavior[Pong]:
        seen.append(message)
        return Behaviors.same()

    return Behaviors.receive_message(on_message, msg_type=Pong)


def counting(seen: list[int]) -> Behavior[Tick]:
    """An actor that appends every tick, in the order it saw them.

    A negative tick stops it, so a test can end it through its behavior
    instead of `abort`.
    """

    async def on_message(message: Tick) -> Behavior[Tick]:
        if message.n < 0:
            return Behaviors.stopped()
        seen.append(message.n)
        return Behaviors.same()

    return Behaviors.receive_message(on_message, msg_type=Tick)


def relaying(target: ActorRef[Ping], seen: list[int]) -> Behavior[Tick]:
    """An actor that asks a peer to reply to an adapter rather than to itself.

    Its own protocol is ticks, and the peer answers in pongs, so what it
    hands over is an adapter ref. That ref has to write itself down with this
    system's address or the answer has nowhere to come back to.
    """

    def build(ctx: ActorContext[Tick]) -> Behavior[Tick]:
        # Negated, so an answer coming back is not mistaken for a new request.
        answers: ActorRef[Pong] = ctx.message_adapter(
            lambda pong: Tick(n=-pong.n), Pong
        )

        async def on_message(message: Tick) -> Behavior[Tick]:
            if message.n > 0:
                target.tell(Ping(n=message.n, reply_to=answers))
                return Behaviors.same()
            seen.append(-message.n)
            return Behaviors.same()

        return Behaviors.receive_message(on_message, msg_type=Tick)

    return Behaviors.setup(build)


def watching(target: ActorRef[Message], seen: list[str]) -> Behavior[Tick]:
    """An actor that watches one ref and records what it is told about it.

    It takes ticks so a test can prove it is still running afterwards, which
    is the difference between "the watch fired" and "the watcher died". A
    negative tick makes it stop watching.
    """

    def build(ctx: ActorContext[Tick]) -> Behavior[Tick]:
        ctx.watch(target)

        async def on_message(message: Tick) -> Behavior[Tick]:
            if message.n < 0:
                ctx.unwatch(target)
                seen.append("unwatched")
                return Behaviors.same()
            seen.append(f"tick {message.n}")
            return Behaviors.same()

        async def on_signal(ctx: ActorContext[Tick], signal: Signal) -> Behavior[Tick]:
            if isinstance(signal, Terminated):
                seen.append(f"terminated {signal.ref.path}")
            return Behaviors.same()

        return Behaviors.receive_message(on_message, msg_type=Tick, on_signal=on_signal)

    return Behaviors.setup(build)


async def dial(
    target: ActorSystem,
    *,
    secret: str | None = None,
    protocol: int = PROTOCOL_VERSION,
    version: str = __version__,
    address: Address = GHOST,
    system: str | None = None,
    proof: str | None = None,
    welcome: bool = True,
    uid: int = 99,
) -> FrameLink:
    """Dial a system as a peer that writes its own handshake.

    Each argument exists so a test can get that part of the handshake wrong
    on purpose.

    Args:
        target: The system to dial.
        secret: Used to answer the challenge, if the caller has one.
        protocol: The wire protocol to claim.
        version: The tapio release to claim, which the peer does not check.
        address: The canonical address to advertise.
        system: The name to claim, if it should differ from `address`.
        proof: An answer to the challenge, overriding what `secret` gives.
        welcome: Read the peer's welcome before returning. Pass False when
            expecting a refusal, since no welcome is coming.
        uid: The incarnation to claim. A second dial with a different one is
            how a restarted peer says it is not the one from before.

    Returns:
        The open link.
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
                    "uid": uid,
                    "protocol": protocol,
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
    """Answer a challenge the way a peer with the secret would."""
    if secret is None:
        return ""
    return hmac.new(secret.encode(), nonce.encode(), sha256).hexdigest()
