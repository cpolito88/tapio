"""Messages, behaviors and a fake peer, shared by the remoting tests."""

import asyncio
import contextlib
import hmac
import json
from collections.abc import AsyncIterator, Callable
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
from tapio.remote.transport import FrameLink, Link, LinkFrame, connect, framed
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


class _WriteFaultLink:
    """A link whose writes break or stall once a few have gone through.

    Deliberately not part of `tapio.testkit.remote`. `LinkFaults` there loses
    frames and never raises, because that is what a partition looks like: the
    whole difficulty is that nothing tells you anything. These two faults are
    the opposite case, a socket reporting its own failure and a peer that
    accepts nothing, and they exist to exercise the write paths rather than
    the detector.

    The filter is installed before any association forms and the handshake
    runs unwrapped, so `after` counts only the frames the association itself
    writes. `after=0` breaks the flush that `_open` does, `after=1` leaves the
    flush alone and breaks the first ordinary send.
    """

    __slots__ = ("_after", "_link", "_stall", "_written")

    def __init__(self, link: FrameLink, after: int, stall: bool) -> None:
        self._link = link
        self._after = after
        self._stall = stall
        self._written = 0

    @property
    def peer(self) -> str:
        return self._link.peer

    async def read_frame(self) -> bytes:
        return await self._link.read_frame()

    async def write_frame(self, data: bytes) -> None:
        if self._written >= self._after:
            if self._stall:
                # Never returns, and never raises. This is a peer holding the
                # connection open while reading nothing, which is what parks a
                # real `drain` for good.
                await asyncio.Event().wait()
            raise OSError(107, "Transport endpoint is not connected")
        self._written += 1
        await self._link.write_frame(data)

    async def write_link(self, message: LinkFrame) -> None:
        await self.write_frame(framed(message.model_dump_json().encode()))

    async def close(self) -> None:
        await self._link.close()

    def __repr__(self) -> str:
        state = "stalling" if self._stall else "failing"
        return f"_WriteFaultLink({self._link.peer!r}, {state} after {self._after})"


def failing_writes(after: int = 0) -> Callable[[FrameLink], Link]:
    """A link filter whose writes raise `OSError` once `after` have succeeded."""
    return lambda link: _WriteFaultLink(link, after, stall=False)


def stalled_writes(after: int = 0) -> Callable[[FrameLink], Link]:
    """A link filter whose writes never return once `after` have succeeded."""
    return lambda link: _WriteFaultLink(link, after, stall=True)


@contextlib.asynccontextmanager
async def silent_peer() -> AsyncIterator[Address]:
    """A peer that accepts a connection and then says nothing at all.

    A dial to it hangs until `handshake_timeout`, which is the window in which
    an association holds frames rather than writing them. That is the only way
    to fill the hold buffer on purpose.

    The accepting tasks are cancelled on the way out. `Server.close` does not
    touch them on 3.11, and one left waiting is a leaked task that the suite's
    own check would report against whichever test ran next.

    Yields:
        The address to resolve against.
    """
    handlers: set[asyncio.Task[None]] = set()

    async def accept(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        task = asyncio.current_task()
        if task is not None:
            handlers.add(task)
        try:
            # No server-hello, ever. The dialling side waits out its handshake
            # deadline, which is exactly the state under test.
            await asyncio.Event().wait()
        finally:
            writer.close()

    server = await asyncio.start_server(accept, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        yield Address(system="silent", host="127.0.0.1", port=port)
    finally:
        for task in handlers:
            task.cancel()
        for task in handlers:
            with contextlib.suppress(asyncio.CancelledError):
                await task
        server.close()
        await server.wait_closed()
