"""A token bucket in one actor, with no lock anywhere.

Concepts: `Behaviors.with_timers`, a fixed-rate refill, and the idea that the
mailbox is the mutex.

A rate limiter is shared mutable state under concurrent access, which is the
textbook case for a lock. Written as an actor it needs none. The bucket is an
ordinary variable inside one actor, and an actor handles one message at a
time, so the runtime provides the mutual exclusion. There is no critical
section here because there is no concurrency here. The concurrency is outside,
in the callers, and the mailbox puts it in order.

The refill is a timer, and a timer is not a callback running beside the
receive loop. It puts a `Refill` on this actor's own user lane, so it queues
like everything else and cannot land in the middle of a decision about a
request. That is why the bucket needs no locking even though requests read it
and a clock writes it.

`start_fixed_rate` is used here on purpose, rather than `start_fixed_delay`. A
limiter that let time slip when it was busy would hand out fewer permits than
it promised, and the promise is a rate. The docs warn about that catch-up
burst, and here catching up is the correct behaviour.

What to watch in the output: the first burst of five requests spends a bucket
holding two, and the other three are refused. That is the limiter working, not
failing. Then one tick of the refill puts a permit back and the next request
is allowed.

Run it with `uv run python -m tapio_examples.rate_limiter`.
"""

import asyncio
from datetime import timedelta

from tapio import ActorSystem, Behavior, Behaviors, Message
from tapio.actor import ActorContext, ActorRef, TimerScheduler

__all__ = ["Decision", "Refill", "Request", "main"]

CAPACITY = 2
"""How many permits the bucket holds when full."""

REFILL = timedelta(milliseconds=30)
"""How often one permit is put back."""


class Decision(Message):
    """What the limiter decided about one request."""

    label: str
    allowed: bool


class Request(Message):
    """Ask for a permit."""

    label: str
    reply_to: ActorRef[Decision]


class Refill(Message):
    """The timer, asking for one permit to be put back."""


Traffic = Request | Refill


def limiter(capacity: int, refill: timedelta) -> Behavior[Traffic]:
    """A token bucket that refuses what it cannot allow.

    A limiter should refuse rather than queue. Holding a request until a
    permit exists turns a rate limit into unbounded latency, and the caller
    usually has something better to do with the answer.
    """

    def with_scheduler(timers: TimerScheduler[Traffic]) -> Behavior[Traffic]:
        def build(ctx: ActorContext[Traffic]) -> Behavior[Traffic]:
            # Plain state on the closure. No lock, and none needed, because
            # nothing else in the process can reach it.
            tokens = capacity
            timers.start_fixed_rate("refill", Refill(), refill)

            async def on_message(message: Traffic) -> Behavior[Traffic]:
                nonlocal tokens
                match message:
                    case Refill():
                        tokens = min(capacity, tokens + 1)
                    case Request(label=label, reply_to=reply_to):
                        allowed = tokens > 0
                        if allowed:
                            tokens -= 1
                        reply_to.tell(Decision(label=label, allowed=allowed))
                return Behaviors.same()

            return Behaviors.receive_message(on_message)

        return Behaviors.setup(build)

    return Behaviors.with_timers(with_scheduler)


def caller(lines: list[str], seen: asyncio.Event, expected: int) -> Behavior[Decision]:
    """Records what the limiter decided, and says when it has heard enough."""

    def build(ctx: ActorContext[Decision]) -> Behavior[Decision]:
        heard = 0

        async def on_decision(message: Decision) -> Behavior[Decision]:
            nonlocal heard
            verdict = "allowed" if message.allowed else "throttled"
            lines.append(f"{message.label}: {verdict}")
            heard += 1
            if heard == expected:
                seen.set()
            return Behaviors.same()

        return Behaviors.receive_message(on_decision)

    return Behaviors.setup(build)


async def main() -> list[str]:
    """Run the example.

    Returns:
        One line per decision, in the order the limiter made them.
    """
    lines: list[str] = []
    burst_done = asyncio.Event()
    after_refill = asyncio.Event()

    async with ActorSystem("rate-limiter") as system:
        client = system.spawn(caller(lines, burst_done, expected=5), name="client")
        gate = system.spawn(limiter(CAPACITY, REFILL), name="limiter")

        # Five at once against a bucket of two. Sent in one go, so no refill
        # can land in the middle of the burst.
        for n in range(1, 6):
            gate.tell(Request(label=f"req-{n}", reply_to=client))
        await burst_done.wait()

        # Wait for the bucket to earn a permit back, then spend it.
        later = system.spawn(caller(lines, after_refill, expected=1), name="later")
        await asyncio.sleep(REFILL.total_seconds() * 1.5)
        gate.tell(Request(label="req-6", reply_to=later))
        await after_refill.wait()

    for line in lines:
        print(line)
    return lines


if __name__ == "__main__":
    asyncio.run(main())
