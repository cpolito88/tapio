"""What the library costs, measured rather than asserted.

Every number the README publishes comes from here. They are skipped in the
normal test run through `--benchmark-skip`, because a benchmark is a
measurement and not a pass or a fail. `make bench` runs them.

Each benchmark drives a real system on a real loop. There is no fake
dispatcher and nothing bypassed: the thing measured is the thing shipped. One
loop is created per benchmark and reused across its rounds, since an actor
system belongs to a loop and building one per round would time the setup
rather than the work.

Throughput is measured as a batch and reported per message. `msg_per_second`
in the extra info is the batch size divided by the mean, which is the number
the README carries.
"""

import asyncio
from collections.abc import Callable, Iterator
from datetime import timedelta
from typing import Any

import pytest

from tapio import Behavior, Behaviors, Message, register_message
from tapio.actor import ActorContext, ActorRef, ActorSystem
from tapio.settings import RemoteSettings, TapioSettings

BATCH = 10_000
"""How many messages one throughput sample sends."""

SPAWNS = 1_000
"""How many actors one spawn-cost sample starts."""

ROUNDS = 20
"""How many samples to take, fixed so two benchmarks are comparable."""


@register_message()
class Ping(Message):
    """One message with one small field, which is the common shape."""

    n: int


class Line(Message):
    """A nested model, so the wide message is not flat."""

    sku: str
    qty: int


@register_message()
class Wide(Message):
    """A bigger message, because what validation costs depends on the model.

    Ten fields, a nested model and a list of them: still small next to what a
    real domain message can be, and enough to show that the per-message cost
    is a property of the message rather than a constant.
    """

    order: str
    customer: str
    total_cents: int
    currency: str
    priority: int
    gift: bool
    note: str
    tags: list[str]
    lines: list[Line]
    billing: Line


@register_message()
class Done(Message):
    """An answer, for the ask benchmarks."""

    total: int


@register_message()
class Ask(Message):
    """A request carrying where the answer goes."""

    n: int
    reply_to: ActorRef[Done]


def counting(target: int, finished: "asyncio.Future[int]") -> Behavior[Ping | Wide]:
    """Build an actor that counts messages and resolves once it has them all.

    Args:
        target: How many to wait for.
        finished: Resolved with the count when the batch has arrived.

    Returns:
        The behavior to spawn.
    """

    def build(ctx: ActorContext[Ping | Wide]) -> Behavior[Ping | Wide]:
        seen = 0

        async def on_message(message: Ping | Wide) -> Behavior[Ping | Wide]:
            nonlocal seen
            seen += 1
            if seen < target:
                return Behaviors.same()
            if not finished.done():
                finished.set_result(seen)
            # Stops rather than lingering, so one round does not leave an actor
            # behind for the next round to share a loop with.
            return Behaviors.stopped()

        return Behaviors.receive_message(on_message, msg_type=Ping | Wide)

    return Behaviors.setup(build)


def wide_message() -> Wide:
    """Build the bigger message the wide benchmarks send.

    Returns:
        One message, reused, since a message is frozen.
    """
    return Wide(
        order="order-1",
        customer="customer-1",
        total_cents=1999,
        currency="EUR",
        priority=2,
        gift=False,
        note="leave it with a neighbour",
        tags=["web", "returning", "eu"],
        lines=[Line(sku="X-1", qty=2), Line(sku="Y-7", qty=1)],
        billing=Line(sku="postage", qty=1),
    )


def idle() -> Behavior[Ping]:
    """Build an actor that does nothing, for measuring what starting one costs."""

    async def on_message(message: Ping) -> Behavior[Ping]:
        return Behaviors.same()

    return Behaviors.receive_message(on_message, msg_type=Ping)


def answering() -> Behavior[Ask]:
    """Build an actor that answers immediately, for the ask benchmarks."""

    async def on_ask(message: Ask) -> Behavior[Ask]:
        message.reply_to.tell(Done(total=message.n))
        return Behaviors.same()

    return Behaviors.receive_message(on_ask, msg_type=Ask)


@pytest.fixture
def loop() -> Iterator[asyncio.AbstractEventLoop]:
    """A loop that outlives one benchmark's rounds.

    Yields:
        The loop, closed when the benchmark is done with it.
    """
    made = asyncio.new_event_loop()
    try:
        yield made
    finally:
        made.close()


def start(
    loop: asyncio.AbstractEventLoop, name: str, settings: TapioSettings
) -> ActorSystem:
    """Build a system on a loop that is not running yet.

    A system binds itself to the running loop when it is constructed, and
    remoting binds its port there too, so construction has to happen inside
    the loop even though nothing is awaited.

    Args:
        loop: The loop it will run on.
        name: What to call it.
        settings: What to build it with.

    Returns:
        The system.
    """

    async def build() -> ActorSystem:
        return ActorSystem(name, settings)

    return loop.run_until_complete(build())


def run(loop: asyncio.AbstractEventLoop, factory: Callable[[], Any]) -> Any:
    """Run one coroutine to completion on a loop.

    Args:
        loop: The loop to run it on.
        factory: Makes the coroutine. A fresh one is needed per round, since a
            coroutine cannot be awaited twice.

    Returns:
        Whatever the coroutine returned.
    """
    return loop.run_until_complete(factory())


def remote_settings() -> TapioSettings:
    """Settings for a benchmark node, on a port the OS picks.

    Returns:
        The settings.
    """
    return TapioSettings(
        _env_file=None,
        remote=RemoteSettings(bind_host="127.0.0.1", bind_port=0),
    )


def uri_of(system: ActorSystem, ref: ActorRef[Any]) -> str:
    """Write a ref's full address, for the other node to resolve.

    Args:
        system: The system the actor runs in.
        ref: The actor to address.

    Returns:
        The string form a peer resolves.
    """
    path = "/".join(ref.path.elements)
    host, port = system.address.host, system.address.port
    return f"tapio://{system.name}@{host}:{port}/{path}#{ref.path.uid}"


def measure_tell(
    benchmark: Any,
    loop: asyncio.AbstractEventLoop,
    settings: TapioSettings,
    message: Ping | Wide,
) -> None:
    """Time a batch of local sends, and report the per-message cost.

    The message is built once, outside the timing. Constructing a Pydantic
    model costs the same whatever `validate_on_tell` says, so leaving it in
    would dilute exactly the difference these two benchmarks exist to show.
    What is timed is the send, the delivery and the handler. A message is
    frozen, so sending the same one repeatedly is allowed and is what makes
    that possible.

    Args:
        benchmark: The pytest-benchmark fixture.
        loop: The loop to run on.
        settings: What to build the system with, which is the only thing a
            pair of validation benchmarks differ by.
        message: What to send ten thousand times.
    """
    system = start(loop, "bench", settings)
    try:

        def one_round() -> tuple[tuple[Any, ...], dict[str, Any]]:
            finished: asyncio.Future[int] = loop.create_future()
            sink = system.spawn_anonymous(counting(BATCH, finished))
            return (sink, finished), {}

        def send(sink: ActorRef[Ping | Wide], finished: "asyncio.Future[int]") -> int:
            async def batch() -> int:
                for _ in range(BATCH):
                    sink.tell(message)
                return await finished

            return loop.run_until_complete(batch())

        benchmark.extra_info["batch"] = BATCH
        benchmark.extra_info["message"] = type(message).__name__
        benchmark.extra_info["validate_on_tell"] = settings.validate_on_tell
        result = benchmark.pedantic(
            send, setup=one_round, rounds=ROUNDS, iterations=1, warmup_rounds=2
        )

        assert result == BATCH
        benchmark.extra_info["msg_per_second"] = BATCH / benchmark.stats["mean"]
    finally:
        loop.run_until_complete(system.terminate())


def test_tell_throughput_with_validation(
    benchmark: Any, loop: asyncio.AbstractEventLoop
) -> None:
    measure_tell(benchmark, loop, TapioSettings(_env_file=None), Ping(n=1))


def test_tell_throughput_without_validation(
    benchmark: Any, loop: asyncio.AbstractEventLoop
) -> None:
    measure_tell(
        benchmark,
        loop,
        TapioSettings(_env_file=None, validate_on_tell=False),
        Ping(n=1),
    )


def test_wide_tell_throughput_with_validation(
    benchmark: Any, loop: asyncio.AbstractEventLoop
) -> None:
    measure_tell(benchmark, loop, TapioSettings(_env_file=None), wide_message())


def test_wide_tell_throughput_without_validation(
    benchmark: Any, loop: asyncio.AbstractEventLoop
) -> None:
    measure_tell(
        benchmark,
        loop,
        TapioSettings(_env_file=None, validate_on_tell=False),
        wide_message(),
    )


def test_spawn_cost(benchmark: Any, loop: asyncio.AbstractEventLoop) -> None:
    """Time starting a thousand actors, in a system that starts empty.

    A fresh system per round, built in the untimed setup, because the cost of
    a spawn depends on how much is already live: a heap with tens of thousands
    of actors on it makes every allocation more expensive, and that is a
    property of the scale rather than of the call. The scale figures come from
    `resident.py`, which measures it on purpose.
    """
    live: list[ActorSystem] = []
    try:

        def one_round() -> tuple[tuple[Any, ...], dict[str, Any]]:
            while live:
                loop.run_until_complete(live.pop().terminate())
            system = start(loop, "bench", TapioSettings(_env_file=None))
            live.append(system)
            return (system,), {}

        def spawn_many(system: ActorSystem) -> int:
            async def go() -> int:
                refs = [system.spawn_anonymous(idle()) for _ in range(SPAWNS)]
                # One yield, which runs every cell task's first step, so the
                # number covers the actor existing and running rather than the
                # object having been built.
                await asyncio.sleep(0)
                return len(refs)

            return loop.run_until_complete(go())

        benchmark.extra_info["spawns"] = SPAWNS
        result = benchmark.pedantic(
            spawn_many, setup=one_round, rounds=ROUNDS, iterations=1, warmup_rounds=2
        )

        assert result == SPAWNS
        benchmark.extra_info["seconds_per_spawn"] = benchmark.stats["mean"] / SPAWNS
    finally:
        while live:
            loop.run_until_complete(live.pop().terminate())


def test_ask_latency(benchmark: Any, loop: asyncio.AbstractEventLoop) -> None:
    system = start(loop, "bench", TapioSettings(_env_file=None))
    try:
        answers = system.spawn(answering(), "answers")

        async def one_ask() -> int:
            reply = await answers.ask(
                lambda reply_to: Ask(n=1, reply_to=reply_to), expect=Done
            )
            return reply.total

        result = benchmark.pedantic(
            run, args=(loop, one_ask), rounds=ROUNDS * 10, iterations=1, warmup_rounds=5
        )

        assert result == 1
    finally:
        loop.run_until_complete(system.terminate())


def test_remote_tell_throughput(
    benchmark: Any, loop: asyncio.AbstractEventLoop
) -> None:
    sender = start(loop, "sender", remote_settings())
    receiver = start(loop, "receiver", remote_settings())
    message = Ping(n=1)
    try:

        def one_round() -> tuple[tuple[Any, ...], dict[str, Any]]:
            finished: asyncio.Future[int] = loop.create_future()
            sink = receiver.spawn_anonymous(counting(BATCH, finished))
            # Resolved outside the timing, and so is the association it
            # opens: what is measured is a link that is already up, which is
            # what a running service has.
            there = loop.run_until_complete(
                sender.resolve(uri_of(receiver, sink), expect=Ping)
            )
            return (there, finished), {}

        def send(there: ActorRef[Ping], finished: "asyncio.Future[int]") -> int:
            async def batch() -> int:
                for _ in range(BATCH):
                    there.tell(message)
                return await finished

            return loop.run_until_complete(batch())

        benchmark.extra_info["batch"] = BATCH
        result = benchmark.pedantic(
            send, setup=one_round, rounds=ROUNDS, iterations=1, warmup_rounds=2
        )

        assert result == BATCH
        benchmark.extra_info["msg_per_second"] = BATCH / benchmark.stats["mean"]
    finally:
        loop.run_until_complete(sender.terminate())
        loop.run_until_complete(receiver.terminate())


def test_remote_ask_latency(benchmark: Any, loop: asyncio.AbstractEventLoop) -> None:
    asker = start(loop, "asker", remote_settings())
    answerer = start(loop, "answerer", remote_settings())
    try:
        local = answerer.spawn(answering(), "answers")
        there: ActorRef[Ask] = loop.run_until_complete(
            asker.resolve(uri_of(answerer, local), expect=Ask)
        )

        async def one_ask() -> int:
            reply = await there.ask(
                lambda reply_to: Ask(n=1, reply_to=reply_to),
                expect=Done,
                timeout=timedelta(seconds=5),
            )
            return reply.total

        result = benchmark.pedantic(
            run, args=(loop, one_ask), rounds=ROUNDS * 10, iterations=1, warmup_rounds=5
        )

        assert result == 1
    finally:
        loop.run_until_complete(asker.terminate())
        loop.run_until_complete(answerer.terminate())
