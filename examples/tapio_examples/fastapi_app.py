"""Where this goes in an application you already have.

Concepts: an `ActorSystem` in a FastAPI lifespan, a request handler that asks
an actor, and the two rules that keep the two halves from fighting.

tapio structures the inside of one service. It is not the service. The web
framework still owns the socket, the routing and the request, and the actor
system sits behind it holding the state that outlives a request: a rate
limiter, a session, a job that several requests can watch.

Two rules make that work, and both are visible below.

**One system, started with the app and stopped with it.** The lifespan is the
right place because it runs on the loop the requests run on, and an actor
system belongs to a loop. Starting one per request would be a new tree, a new
set of guardians and a new everything, for one request.

**The request handler asks, and handles the failure.** `ask` is the bridge
from a call stack that expects a return value to a mailbox that expects a
message. It has a deadline, and when the deadline passes the handler gets an
exception it can turn into a 503, rather than a request that never answers.

What to watch in the output: the last three lines. The slow request gets a 503
because the ask had a deadline, and then the actor carries on with the work
anyway, because **a timeout frees the caller and not the callee**. Its answer
arrives late, finds the promise gone, and becomes a dead letter. Waiting for
that dead letter is how the example knows the actor is free again, with no
sleeping and no guessing.

Before that, twenty requests arrive at once and the count is exactly right,
with no lock in the application code, because one actor handles one message at
a time.

Run it with:

```
uv run python -m tapio_examples.fastapi_app
```
"""

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import timedelta

import httpx
from fastapi import FastAPI, HTTPException

from tapio import ActorSystem, Behavior, Behaviors, DeadLetter, Message
from tapio.actor import ActorContext, ActorRef
from tapio.errors import AskTimeoutError

__all__ = ["Counted", "Hit", "Slow", "build_app", "counter", "main"]

ASK_TIMEOUT = timedelta(milliseconds=100)
"""How long a request waits for an actor before it gives up on it."""

SLOW_SECONDS = 0.3
"""How long the slow work takes: comfortably past the deadline above."""


class Counted(Message):
    """The answer: how many hits this actor has seen."""

    total: int


class Hit(Message):
    """One request, and where the answer goes."""

    reply_to: ActorRef[Counted]


class Slow(Message):
    """A request the actor will not answer in time, on purpose."""

    reply_to: ActorRef[Counted]


def counter() -> Behavior[Hit | Slow]:
    """Build the actor holding the state the requests share.

    The count is an ordinary integer in a closure. It needs no lock and no
    database round trip, because this actor handles one message at a time and
    two requests cannot be inside it at once.

    Returns:
        The behavior to spawn.
    """

    def build(ctx: ActorContext[Hit | Slow]) -> Behavior[Hit | Slow]:
        total = 0

        async def on_message(message: Hit | Slow) -> Behavior[Hit | Slow]:
            nonlocal total
            if isinstance(message, Slow):
                # Answers, but far too late. The caller has given up by then,
                # and the reply goes to a promise that is gone: a dead letter,
                # not an error in this actor. This actor is also not reading
                # its mailbox while it waits, so the next request queues.
                await asyncio.sleep(SLOW_SECONDS)
                message.reply_to.tell(Counted(total=total))
                return Behaviors.same()
            total += 1
            message.reply_to.tell(Counted(total=total))
            return Behaviors.same()

        return Behaviors.receive_message(on_message, msg_type=Hit | Slow)

    return Behaviors.setup(build)


def build_app() -> FastAPI:
    """Build the application, with one actor system behind it.

    Returns:
        The app, ready to serve.
    """

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
        # Started here, on the loop that will run the requests, and stopped
        # here. `async with` gives the tree its shutdown, so a task, a timer
        # or a blocking-pool thread cannot outlive the process's idea of
        # having stopped.
        async with ActorSystem("web") as system:
            app.state.system = system
            app.state.hits = system.spawn(counter(), "hits")
            yield

    app = FastAPI(lifespan=lifespan)

    @app.post("/hit")
    async def hit() -> dict[str, int]:
        """Count one request and report the total."""
        counted = await app.state.hits.ask(
            lambda reply_to: Hit(reply_to=reply_to),
            expect=Counted,
            timeout=ASK_TIMEOUT,
        )
        return {"total": counted.total}

    @app.post("/slow")
    async def slow() -> dict[str, int]:
        """Ask for something that will not arrive in time."""
        try:
            counted = await app.state.hits.ask(
                lambda reply_to: Slow(reply_to=reply_to),
                expect=Counted,
                timeout=ASK_TIMEOUT,
            )
        except AskTimeoutError as error:
            # A deadline the handler owns, turned into a status code the
            # caller understands. Without it this request would wait as long
            # as the actor took, which is not a decision anybody made.
            raise HTTPException(
                status_code=503, detail="the counter is busy"
            ) from error
        return {"total": counted.total}

    return app


async def main() -> list[str]:
    """Run the example against the app in this process.

    Returns:
        The lines the requests produced, in order.
    """
    lines: list[str] = []
    app = build_app()
    transport = httpx.ASGITransport(app=app)
    # `httpx` drives the app in-process, lifespan included, so the example
    # needs no port and no server to run.
    async with (
        httpx.AsyncClient(transport=transport, base_url="http://testserver") as client,
        app.router.lifespan_context(app),
    ):
        first = await client.post("/hit")
        lines.append(f"web: POST /hit -> {first.status_code} {first.json()}")

        # Twenty at once, through one actor. There is no lock in the
        # handler and none in the counter.
        together = await asyncio.gather(*(client.post("/hit") for _ in range(20)))
        totals = sorted(response.json()["total"] for response in together)
        lines.append(
            f"web: 20 concurrent hits ran {totals[0]} to {totals[-1]}, "
            f"{len(set(totals))} distinct"
        )

        # Subscribed before the slow request, because the thing being
        # waited for is the reply that arrives after nobody wants it.
        letters: asyncio.Queue[DeadLetter] = asyncio.Queue()
        app.state.system.dead_letters.subscribe(letters.put_nowait)

        slow = await client.post("/slow")
        lines.append(f"web: POST /slow -> {slow.status_code}, and the app serves on")

        late = await letters.get()
        lines.append(
            f"web: the counter answered anyway, and {type(late.message).__name__} "
            f"became a dead letter ({late.reason})"
        )

        after = await client.post("/hit")
        lines.append(f"web: POST /hit -> {after.status_code} {after.json()}")

    for line in lines:
        print(line)
    return lines


if __name__ == "__main__":
    asyncio.run(main())
