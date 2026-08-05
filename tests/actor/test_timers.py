"""Tests for timers: an actor sending itself a message later.

The timings are deliberately coarse. What these tests are really about is
ownership: a timer belongs to the cell that scheduled it, so a tick can never
reach a stopped actor or a later incarnation.
"""

import asyncio
from datetime import timedelta

import pytest

from tapio import (
    ActorSystem,
    Behavior,
    Behaviors,
    DeadLetter,
    DeadLetterReason,
    MailboxConfig,
    Message,
    MessageTypeError,
    OverflowStrategy,
    SupervisorStrategy,
    TimerScheduler,
)
from tapio.actor import ActorContext, LocalActorRef
from tapio.testkit import assert_no_leaked_tasks
from tests.failures import BoomError, eventually


class Tick(Message):
    """What a timer sends."""

    label: str = "tick"


class Start(Message):
    """Ask the actor under test to schedule something."""

    key: str = "t"
    kind: str = "single"
    delay_ms: int = 5


class Fail(Message):
    """Ask the actor to blow up, so a restart can be observed."""


class Stop(Message):
    """Ask the actor to stop."""


Any_ = Tick | Start | Fail | Stop


def ticker(
    seen: list[str], *, strategy: SupervisorStrategy | None = None
) -> Behavior[Any_]:
    """An actor that schedules what it is told to and records what arrives."""

    def build(timers: TimerScheduler[Any_]) -> Behavior[Any_]:
        def setup(ctx: ActorContext[Any_]) -> Behavior[Any_]:
            seen.append("setup")

            async def on_message(message: Any_) -> Behavior[Any_]:
                match message:
                    case Start(key=key, kind=kind, delay_ms=ms):
                        every = timedelta(milliseconds=ms)
                        match kind:
                            case "single":
                                timers.start_single(key, Tick(label=key), every)
                            case "fixed_delay":
                                timers.start_fixed_delay(key, Tick(label=key), every)
                            case "fixed_rate":
                                timers.start_fixed_rate(key, Tick(label=key), every)
                    case Tick(label=label):
                        seen.append(f"tick {label}")
                    case Fail():
                        raise BoomError("boom")
                    case Stop():
                        return Behaviors.stopped()
                return Behaviors.same()

            return Behaviors.receive_message(on_message)

        return Behaviors.setup(setup)

    behavior: Behavior[Any_] = Behaviors.with_timers(build)
    if strategy is None:
        return behavior
    return Behaviors.supervise(behavior).on_failure(strategy, on=BoomError)


def scheduler_of(ref: object) -> TimerScheduler[Any_]:
    """The scheduler behind a ref, for the tests that assert on runtime state."""
    assert isinstance(ref, LocalActorRef)
    return ref.cell.timers


async def test_a_single_timer_fires_once(system: ActorSystem):
    seen: list[str] = []
    ref = system.spawn(ticker(seen), name="ticker")

    ref.tell(Start(kind="single", delay_ms=5))
    await eventually(lambda: seen.count("tick t") == 1)
    await asyncio.sleep(0.05)

    assert seen.count("tick t") == 1
    assert not scheduler_of(ref).is_active("t")


async def test_a_repeating_timer_keeps_firing(system: ActorSystem):
    seen: list[str] = []
    ref = system.spawn(ticker(seen), name="ticker")

    ref.tell(Start(kind="fixed_delay", delay_ms=2))
    await eventually(lambda: seen.count("tick t") >= 3)

    assert scheduler_of(ref).is_active("t")


async def test_a_fixed_rate_timer_catches_up_after_a_stall():
    """Missed ticks are sent, not skipped, which is what makes it a rate.

    The slow handler is the stall. The schedule keeps advancing while the
    actor is busy, so the ticks it missed go out afterwards.
    """
    seen: list[str] = []
    released = asyncio.Event()

    def build(timers: TimerScheduler[Tick | Start]) -> Behavior[Tick | Start]:
        async def on_message(message: Tick | Start) -> Behavior[Tick | Start]:
            if isinstance(message, Start):
                timers.start_fixed_rate(
                    "t", Tick(), timedelta(milliseconds=2), initial_delay=timedelta(0)
                )
                return Behaviors.same()
            seen.append("tick")
            if len(seen) == 1:
                # One slow message, long enough that several scheduled ticks
                # fall due while it runs.
                await asyncio.sleep(0.05)
                released.set()
            return Behaviors.same()

        return Behaviors.receive_message(on_message)

    async with ActorSystem("rate") as system:
        ref = system.spawn(Behaviors.with_timers(build), name="ticker")
        ref.tell(Start())
        await released.wait()
        # The ticks the stall cost were not dropped: they were queued while it
        # ran and arrive back to back once it is over.
        await eventually(lambda: len(seen) >= 10)


async def test_a_timer_is_cancelled_by_key(system: ActorSystem):
    seen: list[str] = []
    ref = system.spawn(ticker(seen), name="ticker")
    ref.tell(Start(kind="fixed_delay", delay_ms=2))
    await eventually(lambda: seen.count("tick t") >= 2)

    scheduler_of(ref).cancel("t")
    settled = len(seen)
    await asyncio.sleep(0.02)

    assert len(seen) == settled
    assert not scheduler_of(ref).is_active("t")


def test_cancelling_an_unknown_key_is_harmless(system: ActorSystem):
    ref = system.spawn(ticker([]), name="ticker")

    scheduler_of(ref).cancel("never-started")
    scheduler_of(ref).cancel_all()


async def test_starting_a_timer_under_a_live_key_replaces_it(system: ActorSystem):
    """Which is what makes "reset the idle timeout" one call rather than two."""
    seen: list[str] = []
    ref = system.spawn(ticker(seen), name="ticker")

    ref.tell(Start(key="t", kind="single", delay_ms=40))
    ref.tell(Start(key="t", kind="single", delay_ms=2))
    await eventually(lambda: seen.count("tick t") == 1)
    await asyncio.sleep(0.06)

    # The replaced timer never fired, so the second tick that a naive
    # implementation would deliver is absent.
    assert seen.count("tick t") == 1


async def test_a_restart_cancels_the_timers(system: ActorSystem):
    """The restart rule for timers, asserted here beside the feature.

    A tick scheduled by the incarnation that failed must not reach the one
    that replaced it. It was scheduled by state that no longer exists.
    """
    seen: list[str] = []
    ref = system.spawn(
        ticker(seen, strategy=SupervisorStrategy.restart()), name="ticker"
    )

    ref.tell(Start(key="t", kind="fixed_delay", delay_ms=40))
    await eventually(lambda: scheduler_of(ref).is_active("t"))

    ref.tell(Fail())
    await eventually(lambda: seen.count("setup") == 2)

    assert scheduler_of(ref).keys == ()
    # And it stays that way: the cancelled timer does not fire into the new
    # incarnation once its original delay elapses.
    await asyncio.sleep(0.06)
    assert "tick t" not in seen


async def test_stopping_cancels_the_timers(system: ActorSystem):
    """Cancelled in the termination sequence, like every other cell-owned task."""
    seen: list[str] = []

    with assert_no_leaked_tasks():
        ref = system.spawn(ticker(seen), name="ticker")
        ref.tell(Start(kind="fixed_delay", delay_ms=2))
        await eventually(lambda: seen.count("tick t") >= 1)
        ref.tell(Stop())
        await eventually(lambda: not scheduler_of(ref).keys)


async def test_a_tick_that_does_not_fit_dead_letters(system: ActorSystem):
    """A tick is an ordinary message, so it meets the mailbox like one.

    There is no sender to raise `MailboxFullError` into, the same situation a
    send from another thread is in. So a tick arriving at a full `FAIL`
    mailbox becomes a dead letter rather than an exception in a task nobody is
    watching. A fixed-rate timer against a slow handler produces that on
    purpose.
    """
    letters: list[DeadLetter] = []
    system.dead_letters.subscribe(letters.append)
    started = asyncio.Event()

    def build(timers: TimerScheduler[Tick]) -> Behavior[Tick]:
        async def on_message(message: Tick) -> Behavior[Tick]:
            started.set()
            await asyncio.sleep(0.05)
            return Behaviors.same()

        timers.start_fixed_rate(
            "t", Tick(), timedelta(milliseconds=1), initial_delay=timedelta(0)
        )
        return Behaviors.receive_message(on_message)

    system.spawn(
        Behaviors.with_timers(build),
        name="slow",
        mailbox=MailboxConfig(capacity=1, on_overflow=OverflowStrategy.FAIL),
    )

    await started.wait()
    await eventually(lambda: any(isinstance(x.message, Tick) for x in letters))

    shed = next(x for x in letters if isinstance(x.message, Tick))
    assert shed.reason == DeadLetterReason.MAILBOX_FULL


def test_a_timer_message_is_checked_when_it_is_scheduled(system: ActorSystem):
    """In the handler that scheduled it, not in a task with nobody watching."""
    scheduled: list[TimerScheduler[Tick]] = []

    def build(timers: TimerScheduler[Tick]) -> Behavior[Tick]:
        scheduled.append(timers)

        async def on_message(message: Tick) -> Behavior[Tick]:
            return Behaviors.same()

        return Behaviors.receive_message(on_message)

    system.spawn(Behaviors.with_timers(build), name="ticker")

    with pytest.raises(MessageTypeError):
        scheduled[0].start_single("t", Stop(), timedelta(milliseconds=1))  # type: ignore[arg-type]

    # And nothing was scheduled, so the rejected message left no task behind.
    assert scheduled[0].keys == ()


def test_a_negative_duration_is_refused(system: ActorSystem):
    scheduled: list[TimerScheduler[Tick]] = []

    def build(timers: TimerScheduler[Tick]) -> Behavior[Tick]:
        scheduled.append(timers)

        async def on_message(message: Tick) -> Behavior[Tick]:
            return Behaviors.same()

        return Behaviors.receive_message(on_message)

    system.spawn(Behaviors.with_timers(build), name="ticker")

    with pytest.raises(ValueError, match="cannot be negative"):
        scheduled[0].start_single("t", Tick(), timedelta(milliseconds=-1))
