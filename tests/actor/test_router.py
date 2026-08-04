"""Routers: one address in front of several identical actors.

Three claims are worth asserting and the rest follows from them: work is spread
round-robin and nothing is duplicated, the pool shrinks when a routee stops and
empties into a stopped router, and a routee that cannot take a message costs
that message rather than the pool.
"""

import asyncio

import pytest

from tapio import (
    ActorSystem,
    Behavior,
    Behaviors,
    DeadLetter,
    DeadLetterReason,
    MailboxConfig,
    Message,
    OverflowStrategy,
    RoundRobin,
    Routers,
    SupervisorStrategy,
)
from tapio.actor import (
    ActorContext,
    ActorPath,
    ActorRef,
    LocalActorRef,
    PostStop,
    Signal,
)
from tapio.errors import BehaviorTypeError
from tests.failures import BoomError, eventually


class Job(Message):
    """A unit of work, with a flag for the two things a routee can do wrong."""

    item: int
    fail: bool = False
    stop: bool = False


def worker(
    seen: list[tuple[str, int]],
    stall: asyncio.Event | None = None,
    entered: asyncio.Event | None = None,
) -> Behavior[Job]:
    """A routee that writes down its own name against everything it handled.

    It reports its own stop, so a test can wait for the pool to have shrunk
    without reaching into the router to look. The router hears about the same
    stop on its system lane, which outranks any work sent afterwards.
    """

    def build(ctx: ActorContext[Job]) -> Behavior[Job]:
        name = ctx.path.name

        async def on_job(message: Job) -> Behavior[Job]:
            if message.fail:
                raise BoomError("boom")
            if message.stop:
                return Behaviors.stopped()
            if stall is not None:
                if entered is not None:
                    entered.set()
                await stall.wait()
            seen.append((name, message.item))
            return Behaviors.same()

        async def on_signal(_: ActorContext[Job], signal: Signal) -> Behavior[Job]:
            if isinstance(signal, PostStop):
                seen.append((name, -1))
            return Behaviors.same()

        return Behaviors.receive_message(on_job, on_signal=on_signal)

    return Behaviors.setup(build)


async def test_work_goes_round_the_pool_in_turn(system: ActorSystem):
    seen: list[tuple[str, int]] = []
    router = system.spawn(Routers.pool(3, worker(seen)), name="workers")

    for item in range(1, 7):
        router.tell(Job(item=item))
    await eventually(lambda: len(seen) == 6)

    # Each routee saw two, in the order the rotation gives them, and no message
    # was handled twice.
    assert sorted(seen) == [
        ("routee-1", 1),
        ("routee-1", 4),
        ("routee-2", 2),
        ("routee-2", 5),
        ("routee-3", 3),
        ("routee-3", 6),
    ]


async def test_the_router_accepts_what_a_routee_accepts(system: ActorSystem):
    """It is read off the routees rather than declared twice, so it cannot drift."""
    seen: list[tuple[str, int]] = []
    router = system.spawn(Routers.pool(2, worker(seen)), name="workers")

    assert isinstance(router, LocalActorRef)
    assert router.cell.msg_type is Job


async def test_a_routee_that_stops_leaves_the_pool(system: ActorSystem):
    """Otherwise the router goes on sending work to an address nobody reads."""
    seen: list[tuple[str, int]] = []
    router = system.spawn(Routers.pool(3, worker(seen)), name="workers")

    # The rotation starts at the first routee, so this is the one that goes.
    router.tell(Job(item=0, stop=True))
    await eventually(lambda: ("routee-1", -1) in seen)

    for item in range(1, 5):
        router.tell(Job(item=item))
    await eventually(lambda: len([x for x in seen if x[1] > 0]) == 4)

    assert {name for name, item in seen if item > 0} == {"routee-2", "routee-3"}


async def test_an_emptied_pool_stops_the_router(system: ActorSystem):
    """A pool with nothing in it is an address that swallows work."""
    seen: list[tuple[str, int]] = []
    letters: list[DeadLetter] = []
    system.dead_letters.subscribe(letters.append)
    router = system.spawn(Routers.pool(2, worker(seen)), name="workers")

    router.tell(Job(item=0, stop=True))
    router.tell(Job(item=1, stop=True))
    await eventually(lambda: not _alive(router))

    router.tell(Job(item=2))
    await eventually(lambda: len(letters) == 1)
    assert letters[0].reason == DeadLetterReason.RECIPIENT_TERMINATED


async def test_a_failing_routee_is_supervised_where_it_was_declared(
    system: ActorSystem,
):
    """The router is the routees' parent, so the ordinary strategy applies.

    A restarting routee keeps its place in the pool: its path and uid are
    unchanged, and the router was told nothing, which is the whole point of a
    restart being invisible to watchers.
    """
    seen: list[tuple[str, int]] = []
    supervised = Behaviors.supervise(worker(seen)).on_failure(
        SupervisorStrategy.restart(), on=BoomError
    )
    router = system.spawn(Routers.pool(2, supervised), name="workers")

    router.tell(Job(item=0, fail=True))
    for item in (1, 2, 3, 4):
        router.tell(Job(item=item))
    await eventually(lambda: len(seen) == 4)

    # The restarted routee is still in the pool, still taking its turn.
    assert {name for name, _ in seen} == {"routee-1", "routee-2"}
    assert _alive(router)


async def test_a_routee_that_cannot_take_a_message_costs_the_message(
    system: ActorSystem,
):
    """And not the pool. A router forwards work it did not write.

    Failing here would take a whole pool down because one member of it was
    busy, so a routee at capacity is a recipient error like any other.
    """
    seen: list[tuple[str, int]] = []
    letters: list[DeadLetter] = []
    system.dead_letters.subscribe(letters.append)
    stall = asyncio.Event()
    entered = asyncio.Event()

    router = system.spawn(
        Routers.pool(
            1,
            worker(seen, stall, entered),
            routee_mailbox=MailboxConfig(capacity=1, on_overflow=OverflowStrategy.FAIL),
        ),
        name="workers",
    )

    # The first is waited for rather than assumed: the routee has to be inside
    # its handler before the next two land, or the queue depth below is a race
    # against the scheduler rather than a statement about capacity.
    router.tell(Job(item=1))
    await entered.wait()

    # One queued, and one with nowhere to go.
    router.tell(Job(item=2))
    router.tell(Job(item=3))
    await eventually(lambda: len(letters) == 1)

    assert letters[0].reason == DeadLetterReason.MAILBOX_FULL
    assert letters[0].message == Job(item=3)
    # The router is still there and still routing, which is the point.
    assert _alive(router)

    stall.set()
    await eventually(lambda: len([x for x in seen if x[1] > 0]) == 2)


async def test_a_pool_needs_a_routee():
    with pytest.raises(ValueError, match="at least one routee"):
        Routers.pool(0, Behaviors.empty())


def test_the_rotation_survives_the_pool_shrinking():
    """Removing a routee shifts the rotation rather than restarting it.

    The counter is kept, not the position, so an actor that has just been given
    work does not get more of it because the pool got smaller.
    """
    strategy = RoundRobin()
    three: list[ActorRef[Job]] = [_ref(n) for n in (1, 2, 3)]

    chosen = [strategy.select(three, Job(item=0)) for _ in range(3)]
    assert chosen == three

    two = [three[0], three[2]]
    assert strategy.select(two, Job(item=0)) is two[1]


def _ref(n: int) -> ActorRef[Job]:
    """A bare ref for the strategy test, which needs no live actors."""
    return ActorRef(ActorPath.root("test").child("user").child(f"routee-{n}"))


def _alive(ref: ActorRef[Job]) -> bool:
    """Whether the cell behind a ref is still reading its mailbox.

    Only a test asks this. Application code watches, because a liveness answer
    is stale by the time the caller reads it.
    """
    return isinstance(ref, LocalActorRef) and ref.cell.is_alive


async def test_a_router_needs_routees_that_declare_what_they_take(
    system: ActorSystem,
):
    """A routee that stopped during construction never declared one."""
    with pytest.raises(BehaviorTypeError, match="declares no message type"):
        system.spawn(
            Routers.pool(1, Behaviors.setup(lambda _: Behaviors.stopped())),
            name="workers",
        )


async def test_a_signal_the_router_has_nothing_to_say_about(system: ActorSystem):
    """Anything but a routee's death is somebody else's business."""
    seen: list[tuple[str, int]] = []
    router = system.spawn(Routers.pool(1, worker(seen)), name="workers")
    other = system.spawn(worker(seen), name="loner")

    # The router watches nothing but its routees, so this arrives as an
    # ordinary unhandled signal rather than shrinking the pool.
    assert isinstance(router, LocalActorRef)
    router.cell.watch(other)
    other.tell(Job(item=0, stop=True))
    await eventually(lambda: ("loner", -1) in seen)

    router.tell(Job(item=1))
    await eventually(lambda: ("routee-1", 1) in seen)


def test_the_rotation_says_how_far_it_has_gone():
    strategy = RoundRobin()
    strategy.select([_ref(1)], Job(item=0))

    assert repr(strategy) == "RoundRobin(sent=1)"
