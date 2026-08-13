"""Restarting an actor whose dependency keeps refusing, without thrashing.

Concepts: `Behaviors.supervise(...).on_failure(...)`, `Restart` with
exponential backoff, the restart window, and what happens to messages sent to
an actor that is between incarnations.

The uploader here stands in for anything that talks to a flaky dependency. Its
first two attempts fail and the third works. Restarting immediately would burn
the whole restart window in a millisecond and stop the actor for a fault that
was about to clear, so the strategy waits, and waits longer each time.

While it waits, the actor is absent, not dead. `tell` stays total, its mailbox
keeps filling, and work sent during the window is handled after the new
incarnation starts rather than dropped. On an unbounded mailbox that costs
memory in proportion to the inbound rate times the window, which is why an
actor that backs off usually wants a bounded mailbox.

The second scenario is the other half of the deal. An actor whose failures
never clear uses up its restart window and is stopped. A supervisor that
restarted forever would turn one bug into a busy one.

What to watch in the output: item 1 fails, and items 2 and 3 were sent while
nobody was there to receive them, yet all three are accounted for. The doomed
actor stops itself after its second failure instead of retrying forever.

Run it with `uv run python -m tapio_examples.supervision_backoff`.
"""

import asyncio
from datetime import timedelta

from tapio import ActorSystem, Behavior, Behaviors, Message
from tapio.actor import (
    ActorContext,
    Backoff,
    PostStop,
    Signal,
    SupervisorStrategy,
)

__all__ = ["Upload", "main"]

FAILING_ATTEMPTS = 2
"""How many attempts the simulated dependency refuses before it recovers."""

BACKOFF = Backoff(
    min_backoff=timedelta(milliseconds=20),
    max_backoff=timedelta(milliseconds=80),
    # No jitter, so the example is reproducible. Keep the default in
    # production. Without jitter, every actor that saw the same dependency
    # fail retries at the same moment, over and over.
    random_factor=0.0,
)


class Upload(Message):
    """One item to send to the flaky dependency."""

    item: int


def uploader(
    lines: list[str],
    attempts: list[int],
    failed: asyncio.Event,
    recovered: asyncio.Event,
) -> Behavior[Upload]:
    """An uploader whose dependency refuses the first two attempts.

    Args:
        lines: Where to record what happened.
        attempts: Every item attempted, across incarnations. It lives outside
            the behavior on purpose. A restart rebuilds the actor's own state,
            and the dependency it talks to does not reset.
        failed: Set after the first failure, so the example can send into the
            backoff window rather than sleeping and hoping.
        recovered: Set once every item has gone through.

    Returns:
        The supervised behavior.
    """

    def build(ctx: ActorContext[Upload]) -> Behavior[Upload]:
        lines.append(f"uploader: incarnation {len(attempts) + 1} ready")

        async def on_upload(message: Upload) -> Behavior[Upload]:
            attempts.append(message.item)
            if len(attempts) <= FAILING_ATTEMPTS:
                lines.append(f"uploader: item {message.item} failed")
                failed.set()
                msg = "the dependency refused the connection"
                raise ConnectionError(msg)
            lines.append(f"uploader: item {message.item} uploaded")
            recovered.set()
            return Behaviors.same()

        return Behaviors.receive_message(on_upload)

    return Behaviors.supervise(Behaviors.setup(build)).on_failure(
        SupervisorStrategy.restart(
            max_restarts=5, window=timedelta(seconds=1), backoff=BACKOFF
        ),
        # Only the failure this actor knows how to survive. Anything else falls
        # through to stop, which is what an unsupervised actor already does.
        on=ConnectionError,
    )


def doomed(lines: list[str], gave_up: asyncio.Event) -> Behavior[Upload]:
    """An actor whose failure never clears, so its restart window runs out."""

    def build(ctx: ActorContext[Upload]) -> Behavior[Upload]:
        async def on_upload(message: Upload) -> Behavior[Upload]:
            lines.append(f"doomed: item {message.item} failed")
            msg = "this one is never going to work"
            raise ConnectionError(msg)

        async def on_signal(
            ctx: ActorContext[Upload], signal: Signal
        ) -> Behavior[Upload]:
            if isinstance(signal, PostStop):
                lines.append("doomed: restart window exhausted, stopped")
                gave_up.set()
            return Behaviors.same()

        return Behaviors.receive_message(on_upload, on_signal=on_signal)

    return Behaviors.supervise(Behaviors.setup(build)).on_failure(
        # One restart per second. A second failure inside that window says the
        # fault is not transient after all.
        SupervisorStrategy.restart(max_restarts=1, window=timedelta(seconds=1)),
        on=ConnectionError,
    )


async def main() -> list[str]:
    """Run the example.

    Returns:
        One line per thing that happened, in order.
    """
    lines: list[str] = []
    attempts: list[int] = []
    failed, recovered, gave_up = (asyncio.Event(), asyncio.Event(), asyncio.Event())

    async with ActorSystem("supervision") as system:
        flaky = system.spawn(
            uploader(lines, attempts, failed, recovered), name="uploader"
        )

        flaky.tell(Upload(item=1))
        await failed.wait()
        # Sent into the backoff window, at an actor that does not currently
        # exist. Neither send raises, and neither message is lost.
        flaky.tell(Upload(item=2))
        flaky.tell(Upload(item=3))
        await recovered.wait()

        unlucky = system.spawn(doomed(lines, gave_up), name="doomed")
        unlucky.tell(Upload(item=4))
        unlucky.tell(Upload(item=5))
        await gave_up.wait()

    for line in lines:
        print(line)
    return lines


if __name__ == "__main__":
    asyncio.run(main())
