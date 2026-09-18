"""One socket, its reader task, and the single close that ends both.

A socket used to be owned by whichever of four fields happened to be holding
it, and closed by whichever of five paths ran first. Each move was an
assignment plus a rule about who closes, so a new case was a new combination,
and there was nowhere the combinations were written down. Seven of thirty
commits were fixes in that set.

A handle is the answer to "who closes this", from before the socket exists
until it is shut. Whoever holds one owes the close, and closing it is
idempotent: a second caller waits out the close the first is doing rather than
racing it, so two paths owing the same socket is no longer a case to reason
about.
"""

import asyncio

from tapio.dispatch.tasks import cancel_and_wait
from tapio.remote.transport import Link

__all__ = ["LinkHandle"]


class LinkHandle:
    """A socket and the task reading it, closed together or not at all.

    Both halves are optional, and both windows are real. A link accepted and
    not yet handshaken has no reader. An association dialling out has a reader
    and no socket yet, and cancelling that reader is how the dial is called
    off, so the handle exists before the socket it will hold.
    """

    __slots__ = ("_closed", "_closing", "_link", "_reader")

    def __init__(self, link: Link | None, *, loop: asyncio.AbstractEventLoop) -> None:
        """Take ownership of a link, or of the dial that will produce one.

        Args:
            link: The socket this handle owes a close on, or `None` when it
                does not exist yet.
            loop: The loop whose future reports that close.
        """
        self._link = link
        self._reader: asyncio.Task[None] | None = None
        self._closing = False
        self._closed: asyncio.Future[None] = loop.create_future()

    @property
    def link(self) -> "Link | None":
        """The socket, or `None` while a dial is still in flight."""
        return self._link

    def holds(self, link: Link) -> None:
        """Take the socket a dial produced.

        Called from the reader, with no await between the dial returning and
        this, so a close cannot land in between and leave the socket owed by
        nobody.

        Args:
            link: The socket now open.
        """
        self._link = link

    @property
    def reader(self) -> "asyncio.Task[None] | None":
        """The task reading this link, or `None` if nothing is reading it yet."""
        return self._reader

    @property
    def is_closed(self) -> bool:
        """Whether the socket has been released."""
        return self._closed.done()

    def reads_with(self, reader: "asyncio.Task[None]") -> None:
        """Say which task is reading this link, so closing it ends that too.

        Args:
            reader: The task whose whole job is reading this link.
        """
        self._reader = reader

    def release(self) -> "Link | None":
        """Give the socket away, so this handle owes no close on it.

        What a handshake does when an association takes the link over. The
        reader is untouched: the task doing the handover is usually the
        reader, and it is about to end on its own. Closing the handle
        afterwards ends nothing, which is the point.

        Returns:
            The socket, or `None` if there was none to give.
        """
        link, self._link = self._link, None
        return link

    async def close(self) -> None:
        """Cancel the reader, wait for it however it ends, then close the link.

        Idempotent: a second call waits for the first one's close rather than
        closing again, so holding a handle twice costs nothing.

        The order matters. A read in flight ends before the socket does, or
        the reader wakes to a closed transport and raises where nobody is
        waiting for it.

        The socket is released before anything is re-raised. A caller whose
        own cancellation arrives while it waits for the reader still gets it,
        because a caller that resumes work afterwards has to stop, but it gets
        it with the close already done. That is the rule `cancel_and_wait`
        exists for, decided here once instead of at each call site.
        """
        if self._closing:
            # Shielded, so a caller cancelled here does not cancel the future
            # the close itself, and every other waiter, is reporting through.
            await asyncio.shield(self._closed)
            return
        self._closing = True
        reader, self._reader = self._reader, None
        try:
            if reader is not None and reader is not asyncio.current_task():
                await cancel_and_wait(reader)
        finally:
            link, self._link = self._link, None
            if link is not None:
                await link.close()
            if not self._closed.done():
                self._closed.set_result(None)
