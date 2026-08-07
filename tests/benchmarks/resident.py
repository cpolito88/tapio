"""How much an idle actor costs to keep, and what latency looks like at scale.

This is the number behind the README's claim that an actor is a unit of
concurrency rather than a unit of deployment. It is not a pytest-benchmark
test, because what it measures is memory rather than time, and because the
largest size takes long enough that it should be run on purpose.

```
make bench-scale
```

Resident memory is read from `ru_maxrss`, which is a high-water mark: it never
goes down, so each size is measured in its own process and the number is the
peak that process reached. The measurement is deliberately crude. It counts
everything, the interpreter included, and the per-actor figure is the
difference from the baseline divided by the count, which is the number a
capacity plan actually needs.
"""

import asyncio
import json
import os
import resource
import subprocess
import sys
import time

from tests.benchmarks.machine import as_text

from tapio import Behavior, Behaviors, Message
from tapio.actor import ActorContext, ActorRef, ActorSystem
from tapio.settings import TapioSettings

SIZES = (1_000, 10_000, 100_000)
"""How many resident actors to measure, an order of magnitude apart."""

SAMPLES = 200
"""How many round trips the latency figure is the median of."""


class Ping(Message):
    """A message with somewhere to answer."""

    reply_to: ActorRef["Pong"]


class Pong(Message):
    """The answer."""


def idle() -> Behavior[Ping]:
    """Build an actor that holds a little state and answers when asked.

    Idle but not empty: an actor nobody can talk to would not need a mailbox,
    and the mailbox is part of what is being counted.

    Returns:
        The behavior to spawn.
    """

    def build(ctx: ActorContext[Ping]) -> Behavior[Ping]:
        seen = 0

        async def on_message(message: Ping) -> Behavior[Ping]:
            nonlocal seen
            seen += 1
            message.reply_to.tell(Pong())
            return Behaviors.same()

        return Behaviors.receive_message(on_message, msg_type=Ping)

    return Behaviors.setup(build)


def rss_bytes() -> int:
    """Read this process's peak resident memory.

    Returns:
        The high-water mark in bytes. `ru_maxrss` is kilobytes on Linux and
        bytes on macOS, which is the one platform difference worth handling.
    """
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak if sys.platform == "darwin" else peak * 1024


async def measure(count: int) -> dict[str, float]:
    """Start `count` actors, then measure memory and a round trip.

    Args:
        count: How many actors to keep resident.

    Returns:
        The measurements, as plain numbers for the parent process to print.
    """
    baseline = rss_bytes()
    async with ActorSystem("scale", TapioSettings(_env_file=None)) as system:
        refs = [system.spawn_anonymous(idle()) for _ in range(count)]

        # The last actor spawned, on the theory that if anything is slower to
        # reach it is the one at the end of the registry.
        target = refs[-1]
        samples = []
        for _ in range(SAMPLES):
            at = time.perf_counter()
            await target.ask(lambda reply_to: Ping(reply_to=reply_to), expect=Pong)
            samples.append(time.perf_counter() - at)
        samples.sort()

        return {
            "actors": count,
            "rss_bytes": rss_bytes(),
            "bytes_per_actor": (rss_bytes() - baseline) / count,
            "median_ask_seconds": samples[len(samples) // 2],
            "p99_ask_seconds": samples[int(len(samples) * 0.99)],
        }


def child(count: int) -> dict[str, float]:
    """Run one measurement in a process of its own.

    Peak memory never goes down, so measuring two sizes in one process would
    report the larger one twice.

    Args:
        count: How many actors to keep resident.

    Returns:
        What the child measured.
    """
    result = subprocess.run(
        [sys.executable, "-m", "tests.benchmarks.resident", str(count)],
        capture_output=True,
        text=True,
        check=True,
        env={**os.environ, "PYTHONPATH": os.getcwd()},
    )
    return dict(json.loads(result.stdout.strip().splitlines()[-1]))


def main() -> None:
    """Measure every size and print the table the README carries."""
    rows = [child(count) for count in SIZES]
    print(as_text())
    print()
    print(
        f"{'actors':>9}  {'RSS':>9}  {'per actor':>10}  {'ask p50':>9}  {'ask p99':>9}"
    )
    for row in rows:
        print(
            f"{int(row['actors']):>9,}  "
            f"{row['rss_bytes'] / 1e6:>7.0f}MB  "
            f"{row['bytes_per_actor'] / 1024:>8.1f}KB  "
            f"{row['median_ask_seconds'] * 1e6:>7.0f}us  "
            f"{row['p99_ask_seconds'] * 1e6:>7.0f}us"
        )


if __name__ == "__main__":
    if len(sys.argv) > 1:
        print(json.dumps(asyncio.run(measure(int(sys.argv[1])))))
    else:
        main()
