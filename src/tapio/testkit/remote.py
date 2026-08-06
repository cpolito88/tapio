"""Two nodes in one process, and a way to break the link between them.

Everything interesting about remoting happens when the network misbehaves, and
none of it can be tested by waiting. A partition is not a slow link: it is a
link that carries nothing and reports nothing, which is exactly what a test
cannot arrange by hand without a second machine.

So the fault sits inside the link. A wrapper goes between an association and
its socket, and it drops, delays or swallows frames on command. Nothing real
is broken, nothing is unplugged, and the system under test cannot tell the
difference: frames stop arriving, the failure detector gives up, and the
quarantine that follows is the production one.

```python
async with two_nodes() as nodes:
    worker = nodes.beta.spawn(work(), "worker")
    ...
    nodes.partition()      # both directions, both nodes
    ...                    # watchers get Terminated, sends dead-letter
    nodes.heal()           # the packets flow again, and nothing re-associates
    await nodes.alpha.remote.reconnect(nodes.beta.address)
```

The pair runs in one process on loopback ports the OS picks, so a test needs
no orchestration and no port nobody else is using.
"""

import asyncio
import contextlib
from collections.abc import AsyncIterator
from datetime import timedelta
from typing import final

from tapio.actor.system import ActorSystem
from tapio.errors import TapioError
from tapio.remote.transport import FrameLink, Link, LinkFrame, framed
from tapio.settings import RemoteSettings, TapioSettings

__all__ = ["LinkFaults", "TwoNodes", "link_faults", "two_nodes"]


@final
class LinkFaults:
    """What is wrong with one system's links, and how wrong.

    One of these covers every link a system opens or accepts after it is
    installed, because a partition is a property of the network rather than of
    one connection. The settings are read at each frame, so a link already
    open is affected the moment they change.
    """

    __slots__ = ("_delay", "_drops", "_healed", "_partitioned")

    def __init__(self) -> None:
        """Start with links that behave."""
        self._partitioned = False
        self._drops = 0
        self._delay = 0.0
        self._healed = asyncio.Event()
        self._healed.set()

    @property
    def partitioned(self) -> bool:
        """Whether frames are currently going nowhere in either direction."""
        return self._partitioned

    def partition(self) -> None:
        """Cut this system off: nothing it writes leaves, nothing it reads arrives.

        Nothing raises and nothing is told, which is the point. A partition
        looks exactly like a peer that has stopped talking, and telling those
        apart from one node is the problem remoting cannot solve.

        A socket closing on the other side is silenced too. A peer that gives
        up first really does close its connection, and a FIN arriving through
        a partition would hand this side a piece of news the network was
        supposed to be swallowing.
        """
        self._partitioned = True
        self._healed.clear()

    def heal(self) -> None:
        """Let frames through again.

        Nothing re-associates by itself. A system that gave up on a peer stays
        given up on until `remote.reconnect` says otherwise, which is what
        this is for testing.
        """
        self._partitioned = False
        self._healed.set()

    async def wait_healed(self) -> None:
        """Wait until frames are allowed through again."""
        await self._healed.wait()

    def drop(self, frames: int) -> None:
        """Swallow the next few frames this system writes.

        Args:
            frames: How many to lose. Link frames count: a lost heartbeat is
                one of the things worth being able to arrange.
        """
        self._drops = frames

    def delay(self, seconds: float) -> None:
        """Hold every frame this system writes for a while before sending it.

        Args:
            seconds: How long. `0` stops delaying.
        """
        self._delay = seconds

    def wrap(self, link: FrameLink) -> Link:
        """Put a faulty link in front of a real one.

        Args:
            link: The link just opened or accepted.

        Returns:
            The link the association will use instead.
        """
        return _FaultyLink(link, self)

    async def allow_write(self) -> bool:
        """Decide what happens to a frame on its way out, and count it.

        Returns:
            Whether the frame should be written. A `False` means it is lost,
            and nothing anywhere is told: that is what makes it a fault worth
            injecting rather than an error worth handling.
        """
        if self._partitioned:
            return False
        if self._drops > 0:
            self._drops -= 1
            return False
        if self._delay > 0:
            await asyncio.sleep(self._delay)
        return not self._partitioned

    def __repr__(self) -> str:
        """Render what is currently wrong."""
        state = "partitioned" if self._partitioned else "connected"
        return f"LinkFaults({state}, drops={self._drops}, delay={self._delay:g}s)"


class _FaultyLink:
    """A link that loses frames on purpose.

    Losing is all it does. It never raises and never closes, because an error
    would tell the system what happened, and the whole difficulty of a
    partition is that nothing tells you anything.
    """

    __slots__ = ("_faults", "_link")

    def __init__(self, link: FrameLink, faults: LinkFaults) -> None:
        """Bind a wrapper to the link it stands in front of."""
        self._link = link
        self._faults = faults

    @property
    def peer(self) -> str:
        """The socket address on the other end."""
        return self._link.peer

    async def read_frame(self) -> bytes:
        """Read the next frame that is allowed to arrive.

        A frame that turns up during a partition is discarded and the read
        continues, which is what a lost packet looks like from up here. So is
        the end of the connection: it is held until the partition heals, so a
        peer that gave up and closed first does not announce it through a
        network that is supposed to be carrying nothing.
        """
        while True:
            try:
                frame = await self._link.read_frame()
            except (OSError, EOFError):
                if self._faults.partitioned:
                    await self._faults.wait_healed()
                raise
            if not self._faults.partitioned:
                return frame

    async def write_frame(self, data: bytes) -> None:
        """Write a frame, unless the faults say it never made it."""
        if await self._faults.allow_write():
            await self._link.write_frame(data)

    async def write_link(self, message: LinkFrame) -> None:
        """Write one of the transport's own frames, faults included.

        It frames the message here rather than delegating, so a heartbeat is
        subject to exactly the same faults as a message. A heartbeat that
        survives a partition would make the partition undetectable.
        """
        await self.write_frame(framed(message.model_dump_json().encode()))

    async def close(self) -> None:
        """Close the real link. A partition never closes anything by itself."""
        await self._link.close()

    def __repr__(self) -> str:
        """Render the socket and what is wrong with it."""
        return f"_FaultyLink({self._link.peer!r}, {self._faults!r})"


def link_faults(system: ActorSystem) -> LinkFaults:
    """Install fault injection on every link a system opens from now on.

    Args:
        system: The system to break links for. Call it before any traffic, so
            that every link is wrapped and a partition covers all of them.

    Returns:
        The controls.

    Raises:
        TapioError: If the system has remoting switched off, in which case it
            has no links to break.
    """
    endpoint = system.remote
    if endpoint is None:
        msg = (
            f"cannot inject link faults into {system.name!r}: it has remoting "
            "switched off, so it has no links. Pass "
            "TapioSettings(remote=RemoteSettings(...)) when constructing it."
        )
        raise TapioError(msg)
    faults = LinkFaults()
    endpoint.set_link_filter(faults.wrap)
    return faults


@final
class TwoNodes:
    """Two systems on loopback, and the network between them."""

    __slots__ = ("alpha", "alpha_faults", "beta", "beta_faults")

    def __init__(
        self,
        alpha: ActorSystem,
        beta: ActorSystem,
        alpha_faults: LinkFaults,
        beta_faults: LinkFaults,
    ) -> None:
        """Hold the pair and the faults on each side."""
        self.alpha = alpha
        """One system."""
        self.beta = beta
        """The other."""
        self.alpha_faults = alpha_faults
        """What is wrong with alpha's links."""
        self.beta_faults = beta_faults
        """What is wrong with beta's links."""

    def partition(self) -> None:
        """Cut both nodes off from each other, with both still running.

        Each side breaks its own links, so neither hears the other and
        neither has died. That is the case worth testing: both will declare
        the other unreachable, and both will be locally correct.
        """
        self.alpha_faults.partition()
        self.beta_faults.partition()

    def heal(self) -> None:
        """Let the packets flow again, which on its own repairs nothing.

        A node that gave up on its peer stays given up on. `remote.reconnect`
        is the repair, and it is explicit because a false alarm has already
        told watchers that live actors are gone.
        """
        self.alpha_faults.heal()
        self.beta_faults.heal()

    def __repr__(self) -> str:
        """Render both systems and the state of the network."""
        return f"TwoNodes({self.alpha.name!r}, {self.beta.name!r})"


@contextlib.asynccontextmanager
async def two_nodes(
    *,
    alpha: str = "alpha",
    beta: str = "beta",
    unreachable_after: timedelta = timedelta(milliseconds=300),
    heartbeat_interval: timedelta = timedelta(milliseconds=20),
) -> AsyncIterator[TwoNodes]:
    """Start two systems that can reach each other, and stop them afterwards.

    Both listen on loopback ports the OS picks, so nothing has to agree on a
    number in advance, and both are terminated however the block ends.

    The default timings are far shorter than production ones, because a test
    that waits ten seconds to see a quarantine is a test nobody runs. They are
    still a heartbeat interval well inside a detector window, which is the
    only relationship between the two that matters.

    Args:
        alpha: The first system's name.
        beta: The second system's name.
        unreachable_after: How long silence lasts before a peer is given up on.
        heartbeat_interval: How often an idle link says it is still there.

    Yields:
        The pair, and the controls for breaking the network between them.
    """
    # No env file on either, so a developer's environment cannot change what
    # a test is running against. The keyword is pydantic-settings' own and is
    # not in the generated signature, which is what mypy is objecting to.
    remote = RemoteSettings(
        _env_file=None,  # type: ignore[call-arg]
        bind_port=0,
        unreachable_after=unreachable_after,
        heartbeat_interval=heartbeat_interval,
    )
    settings = TapioSettings(_env_file=None, remote=remote)  # type: ignore[call-arg]
    first = ActorSystem(alpha, settings)
    try:
        second = ActorSystem(beta, settings)
        try:
            yield TwoNodes(first, second, link_faults(first), link_faults(second))
        finally:
            await second.terminate()
    finally:
        await first.terminate()
