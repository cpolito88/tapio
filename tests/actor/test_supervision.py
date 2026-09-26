"""Tests for supervision: what a restart does, and what escalation costs.

Two parts of the restart behaviour are asserted elsewhere, next to the feature
they belong to: timers are cancelled in `test_timers.py`, and the stash is
cleared in `test_stash.py`.
"""

import asyncio
from datetime import timedelta

import pytest

from tapio import ActorSystem, Behavior, Behaviors
from tapio.actor import (
    ActorContext,
    ActorRef,
    Backoff,
    Decision,
    Signal,
    SupervisorStrategy,
    Terminated,
)
from tapio.errors import BehaviorTypeError
from tapio.testkit import (
    NO_MESSAGE_WINDOW,
    IsolatedTapioSettings,
    assert_no_leaked_tasks,
)
from tests.failures import (
    BoomError,
    Job,
    OtherError,
    eventually,
    recording,
    well_inside,
)
from tests.messages import Ping

RESTART = SupervisorStrategy.restart()

MIN_BACKOFF = timedelta(milliseconds=50)
"""The first wait the layered backoff test expects, named so it can assert it."""

MAX_BACKOFF = timedelta(seconds=10)
"""That test's ceiling, which is where a shared restart count would head."""


async def test_an_unsupervised_failure_stops_the_actor(system: ActorSystem):
    seen: list[str] = []

    actor = system.spawn(recording(seen), name="worker")
    actor.tell(Job(fail=True))
    actor.tell(Job(item=1))
    await eventually(lambda: "PostStop" in seen)

    # Stop is the default because an actor that failed for a reason nobody
    # anticipated is in a state nobody described.
    assert seen == ["setup", "PostStop"]


async def test_resume_keeps_the_actor_and_its_state(system: ActorSystem):
    seen: list[str] = []

    actor = system.spawn(
        recording(seen, strategy=SupervisorStrategy.resume()), name="worker"
    )
    actor.tell(Job(fail=True))
    actor.tell(Job(item=1))
    await eventually(lambda: "job 1" in seen)

    # No second setup. The same incarnation carried on with the message after
    # the one that failed.
    assert seen == ["setup", "job 1"]


async def test_restart_preserves_the_mailbox_and_drops_the_failed_message(
    system: ActorSystem,
):
    seen: list[str] = []

    actor = system.spawn(recording(seen, strategy=RESTART), name="worker")
    actor.tell(Job(fail=True))
    actor.tell(Job(item=1))
    actor.tell(Job(item=2))
    await eventually(lambda: "job 2" in seen)

    # Both lanes survive the restart, so the two messages queued behind the
    # failure are still there afterwards. The one that failed is not.
    assert seen == ["setup", "PreRestart", "setup", "job 1", "job 2"]


async def test_restart_re_evaluates_the_original_behavior(system: ActorSystem):
    seen: list[str] = []

    async def switched(message: Job) -> Behavior[Job]:
        raise AssertionError("the switched-to behavior should not survive a restart")

    async def on_message(ctx: ActorContext[Job], message: Job) -> Behavior[Job]:
        if message.fail:
            raise BoomError("boom")
        seen.append(f"job {message.item}")
        return Behaviors.receive_message(switched, msg_type=Job)

    def build(ctx: ActorContext[Job]) -> Behavior[Job]:
        seen.append("setup")
        return Behaviors.receive(on_message)

    actor = system.spawn(
        Behaviors.supervise(Behaviors.setup(build)).on_failure(RESTART),
        name="worker",
    )
    actor.tell(Job(item=1))  # switches to the behavior that must not come back
    actor.tell(Job(fail=True))
    actor.tell(Job(item=2))
    await eventually(lambda: "job 2" in seen)

    # A cell keeps the behavior it was spawned with, not the one it has now.
    # An actor that switched several times still restarts to its original.
    assert seen == ["setup", "job 1", "setup", "job 2"]


async def test_restart_stops_children_and_respawns_the_ones_setup_made(
    system: ActorSystem,
):
    seen: list[str] = []
    children: list[ActorRef[Job]] = []

    def spawn_child(ctx: ActorContext[Job]) -> None:
        children.append(ctx.spawn(recording([]), name="child"))

    actor = system.spawn(
        recording(seen, strategy=RESTART, on_setup=spawn_child), name="parent"
    )
    actor.tell(Job(fail=True))
    await eventually(lambda: len(children) == 2)

    # Same name, new incarnation. The old child was stopped, and the setup
    # running again made a new one. A child spawned from a message handler
    # would just be gone, which is the part that surprises people.
    assert children[0].path.name == children[1].path.name == "child"
    assert children[0].path.uid != children[1].path.uid


async def test_restart_tells_watchers_nothing(system: ActorSystem):
    seen: list[str] = []
    watcher_saw: list[str] = []

    actor = system.spawn(recording(seen, strategy=RESTART), name="worker")

    async def on_signal(ctx: ActorContext[Job], signal: Signal) -> Behavior[Job]:
        watcher_saw.append(type(signal).__name__)
        return Behaviors.same()

    async def watch_it(ctx: ActorContext[Job], message: Job) -> Behavior[Job]:
        ctx.watch(actor)
        return Behaviors.same()

    watcher = system.spawn(
        Behaviors.receive(watch_it, on_signal=on_signal), name="watcher"
    )
    watcher.tell(Job())
    actor.tell(Job(fail=True))
    actor.tell(Job(item=1))
    await eventually(lambda: "job 1" in seen)

    # The ref, path and uid are unchanged, and only the incarnation is new. A
    # watcher has no reason to hear about that.
    assert watcher_saw == []

    # A real stop still fires, so the silence above is about restarts and not
    # about a watch that was never registered.
    actor.tell(Job(item=-1))
    await eventually(lambda: watcher_saw == ["Terminated"])


async def test_a_sibling_of_a_failed_child_keeps_running(system: ActorSystem):
    healthy_seen: list[str] = []
    doomed: list[ActorRef[Job]] = []
    healthy: list[ActorRef[Job]] = []

    def spawn_both(ctx: ActorContext[Job]) -> None:
        doomed.append(ctx.spawn(recording([]), name="doomed"))
        healthy.append(ctx.spawn(recording(healthy_seen), name="healthy"))

    system.spawn(recording([], on_setup=spawn_both), name="parent")
    doomed[0].tell(Job(fail=True))
    healthy[0].tell(Job(item=1))
    await eventually(lambda: "job 1" in healthy_seen)

    # Supervision is the inverse of a task group. One child failing must leave
    # its siblings untouched, which is why the runtime has no TaskGroup.
    healthy[0].tell(Job(item=2))
    await eventually(lambda: "job 2" in healthy_seen)


async def test_the_restart_window_is_exhausted_and_the_actor_stops(
    system: ActorSystem,
):
    seen: list[str] = []
    strategy = SupervisorStrategy.restart(max_restarts=2, window=timedelta(seconds=10))

    actor = system.spawn(recording(seen, strategy=strategy), name="worker")
    for _ in range(3):
        actor.tell(Job(fail=True))
    await eventually(lambda: "PostStop" in seen)

    # Two restarts are allowed, so a third failure inside the window says the
    # fault is not transient after all.
    assert seen.count("setup") == 3
    assert seen[-1] == "PostStop"


async def test_restarts_outside_the_window_do_not_count(system: ActorSystem):
    seen: list[str] = []
    strategy = SupervisorStrategy.restart(
        max_restarts=1, window=timedelta(seconds=0.05)
    )

    actor = system.spawn(recording(seen, strategy=strategy), name="worker")
    actor.tell(Job(fail=True))
    await eventually(lambda: seen.count("setup") == 2)
    await asyncio.sleep(0.06)
    actor.tell(Job(fail=True))
    await eventually(lambda: seen.count("setup") == 3)

    # The window measures a rate, not a total. Two failures an hour apart are
    # not the same as two in a row.
    assert "PostStop" not in seen


async def test_messages_arriving_during_backoff_are_buffered_not_dropped(
    system: ActorSystem,
):
    seen: list[str] = []
    strategy = SupervisorStrategy.restart(
        backoff=Backoff(
            min_backoff=timedelta(seconds=0.05),
            max_backoff=timedelta(seconds=0.05),
            random_factor=0.0,
        )
    )

    actor = system.spawn(recording(seen, strategy=strategy), name="worker")
    actor.tell(Job(fail=True))
    await eventually(lambda: "PreRestart" in seen)

    # The actor is absent, not dead. `tell` stays total and the mailbox keeps
    # filling, which is the memory risk the docs warn about for unbounded
    # mailboxes.
    actor.tell(Job(item=1))
    actor.tell(Job(item=2))
    assert "setup" not in seen[2:]

    await eventually(lambda: "job 2" in seen)
    assert seen == ["setup", "PreRestart", "setup", "job 1", "job 2"]


async def test_a_stop_during_backoff_is_not_waited_out():
    shutdown_timeout = timedelta(seconds=2)
    settings = IsolatedTapioSettings(shutdown_timeout=shutdown_timeout)
    seen: list[str] = []
    backoff_window = timedelta(seconds=30)
    strategy = SupervisorStrategy.restart(
        backoff=Backoff(
            min_backoff=backoff_window,
            max_backoff=backoff_window,
            random_factor=0.0,
        )
    )

    loop = asyncio.get_running_loop()
    with assert_no_leaked_tasks():
        system = ActorSystem("backing-off", settings)
        actor = system.spawn(recording(seen, strategy=strategy), name="worker")
        actor.tell(Job(fail=True))
        await eventually(lambda: "PreRestart" in seen)

        started = loop.time()
        await system.terminate()
        elapsed = loop.time() - started

    # A backing-off actor still reads its system lane, so shutdown does not
    # wait out a window it has no interest in. The bound is the system's own
    # shutdown deadline rather than a literal: past it the sweep would have
    # cancelled the cell, and the PostStop below would not be there.
    assert elapsed < shutdown_timeout.total_seconds()
    assert seen[-1] == "PostStop"


async def test_only_the_declared_exceptions_are_governed(system: ActorSystem):
    seen: list[str] = []

    actor = system.spawn(
        recording(seen, strategy=RESTART, on=BoomError, error=OtherError), name="worker"
    )
    actor.tell(Job(fail=True))
    await eventually(lambda: "PostStop" in seen)

    # A failure matching no wrapper falls through to stop, which is what an
    # unsupervised actor already does.
    assert seen == ["setup", "PostStop"]


async def test_the_outermost_wrapper_is_consulted_first(system: ActorSystem):
    seen: list[str] = []

    def build(ctx: ActorContext[Job]) -> Behavior[Job]:
        seen.append("setup")

        async def on_message(message: Job) -> Behavior[Job]:
            raise BoomError("boom")

        return Behaviors.receive_message(on_message)

    inner = Behaviors.supervise(Behaviors.setup(build)).on_failure(
        SupervisorStrategy.stop(), on=BoomError
    )
    outer = Behaviors.supervise(inner).on_failure(RESTART, on=BoomError)

    actor = system.spawn(outer, name="worker")
    actor.tell(Job(fail=True))
    await eventually(lambda: seen.count("setup") == 2)


async def test_supervision_survives_a_behavior_change(system: ActorSystem):
    seen: list[str] = []

    async def second(message: Job) -> Behavior[Job]:
        raise BoomError("boom")

    async def first(message: Job) -> Behavior[Job]:
        seen.append("switched")
        return Behaviors.receive_message(second, msg_type=Job)

    def build(ctx: ActorContext[Job]) -> Behavior[Job]:
        seen.append("setup")
        return Behaviors.receive_message(first)

    actor = system.spawn(
        Behaviors.supervise(Behaviors.setup(build)).on_failure(RESTART),
        name="worker",
    )
    actor.tell(Job())
    actor.tell(Job(fail=True))
    await eventually(lambda: seen.count("setup") == 2)

    # Supervision belongs to the actor, not to the behavior it happens to
    # hold, so returning an unwrapped behavior does not drop the strategy.
    assert seen == ["setup", "switched", "setup"]


async def test_escalation_makes_the_failure_the_parents_own(system: ActorSystem):
    parent_seen: list[str] = []
    child_seen: list[str] = []
    children: list[ActorRef[Job]] = []

    def spawn_child(ctx: ActorContext[Job]) -> None:
        children.append(
            ctx.spawn(
                recording(child_seen, strategy=SupervisorStrategy.escalate()),
                name="child",
            )
        )

    system.spawn(
        recording(parent_seen, strategy=RESTART, on_setup=spawn_child), name="parent"
    )
    children[0].tell(Job(fail=True))
    await eventually(lambda: len(children) == 2)

    # The child stops, the parent takes the failure as its own, and its own
    # decision rebuilds the subtree.
    assert child_seen.count("PostStop") == 1
    assert parent_seen == ["setup", "PreRestart", "setup"]


async def test_escalation_to_the_guardian_terminates_the_system():
    seen: list[str] = []

    with assert_no_leaked_tasks():
        system = ActorSystem("escalating")
        actor = system.spawn(
            recording(seen, strategy=SupervisorStrategy.escalate()), name="worker"
        )
        actor.tell(Job(fail=True))

        with pytest.raises(BoomError, match="boom") as failure:
            await system.when_terminated()

    # Nobody took responsibility, so the system came down and said why. The
    # chain is a note on the exception rather than a wrapper, so the reader
    # gets the original traceback.
    assert system.is_terminating
    notes = getattr(failure.value, "__notes__", [])
    assert any("escalated from" in note for note in notes)
    assert any("escalated to" in note and "/user" in note for note in notes)


async def test_a_guardian_failure_is_reported_once_however_long_you_wait():
    with assert_no_leaked_tasks():
        system = ActorSystem("escalating-twice")
        actor = system.spawn(
            recording([], strategy=SupervisorStrategy.escalate()), name="worker"
        )
        actor.tell(Job(fail=True))

        with pytest.raises(BoomError):
            await system.when_terminated()
        with pytest.raises(BoomError):
            await system.when_terminated()


async def test_a_clean_shutdown_raises_nothing(system: ActorSystem):
    system.spawn(recording([]), name="worker")

    await system.terminate()
    await system.when_terminated()


def test_backoff_doubles_up_to_its_ceiling():
    backoff = Backoff(
        min_backoff=timedelta(seconds=1),
        max_backoff=timedelta(seconds=4),
        random_factor=0.0,
    )

    schedule = [backoff.delay(n, jitter=0.0) for n in range(1, 6)]

    assert schedule == [1.0, 2.0, 4.0, 4.0, 4.0]


def test_backoff_jitter_only_ever_lengthens_the_wait():
    backoff = Backoff(
        min_backoff=timedelta(seconds=1),
        max_backoff=timedelta(seconds=1),
        random_factor=0.5,
    )

    # Jitter matters when a shared dependency fails. Without it, every actor
    # that noticed at the same moment retries at the same moment, over and
    # over.
    assert backoff.delay(1, jitter=0.0) == 1.0
    assert backoff.delay(1, jitter=1.0) == 1.5


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"min_backoff": timedelta(seconds=-1)}, "must not be negative"),
        ({"max_backoff": timedelta(seconds=0)}, "at least min_backoff"),
        ({"random_factor": 2.0}, r"must be in \[0, 1\]"),
    ],
)
def test_a_backoff_that_could_not_work_is_refused(kwargs, match):
    defaults = {
        "min_backoff": timedelta(seconds=1),
        "max_backoff": timedelta(seconds=1),
    }

    with pytest.raises(ValueError, match=match):
        Backoff(**{**defaults, **kwargs})


def test_restart_limits_belong_to_restart_alone():
    with pytest.raises(ValueError, match="restart limits do not apply"):
        SupervisorStrategy(Decision.RESUME, max_restarts=3)

    with pytest.raises(ValueError, match="max_restarts must be at least 1"):
        SupervisorStrategy.restart(max_restarts=0)


def test_a_strategy_reads_back_as_the_call_that_made_it():
    assert repr(SupervisorStrategy.stop()) == "SupervisorStrategy.stop()"
    assert "max_restarts=2" in repr(SupervisorStrategy.restart(max_restarts=2))


async def test_a_restart_whose_behavior_cannot_be_rebuilt_stops_the_actor(
    system: ActorSystem,
):
    # The restart runs the factory again, so a factory that worked once and
    # fails the second time leaves nothing to restart into. Failing the same
    # way forever is a loop, and stopping is the honest end of it.
    builds: list[int] = []

    def build(ctx: ActorContext[Job]) -> Behavior[Job]:
        builds.append(len(builds))
        if len(builds) > 1:
            raise OtherError("the factory cannot build this twice")

        async def on_message(message: Job) -> Behavior[Job]:
            raise BoomError("boom")

        return Behaviors.receive_message(on_message)

    ref = system.spawn(
        Behaviors.supervise(Behaviors.setup(build)).on_failure(RESTART, on=BoomError),
        name="fragile",
    )
    ref.tell(Job(item=1))

    # Built twice: once at spawn and once for the restart that then failed.
    await eventually(lambda: len(builds) == 2)
    # Stopped rather than restarted again, so the registry lets go of it.
    await eventually(lambda: system.refs.lookup(ref.path) is None)


async def test_a_restart_into_a_stopped_behavior_stops_the_actor(system: ActorSystem):
    # Deferred construction is allowed to decide there is nothing to run. On a
    # restart that means the actor goes rather than coming back inert.
    builds: list[int] = []

    def build(ctx: ActorContext[Job]) -> Behavior[Job]:
        builds.append(len(builds))
        if len(builds) > 1:
            return Behaviors.stopped()

        async def on_message(message: Job) -> Behavior[Job]:
            raise BoomError("boom")

        return Behaviors.receive_message(on_message)

    ref = system.spawn(
        Behaviors.supervise(Behaviors.setup(build)).on_failure(RESTART, on=BoomError),
        name="giving-up",
    )
    ref.tell(Job(item=1))

    await eventually(lambda: len(builds) == 2)
    await eventually(lambda: system.refs.lookup(ref.path) is None)


async def test_deferred_construction_that_never_settles_is_refused(
    system: ActorSystem,
):
    # A setup returning another setup forever would spin at spawn time. It is
    # called a loop after a bounded number of rounds and raises at the spawn,
    # which is where somebody can act on it.
    def build(ctx: ActorContext[Job]) -> Behavior[Job]:
        return Behaviors.setup(build)

    with pytest.raises(BehaviorTypeError, match="deferred construction"):
        system.spawn(Behaviors.setup(build), name="never-settles")


def layered(seen: list[str], strict: SupervisorStrategy) -> Behavior[Job]:
    """An actor with two restart strategies, one per exception type.

    `Job(fail=True)` raises `BoomError`, governed by `strict`. `Job(item=-1)`
    raises `OtherError`, governed by a strategy generous enough never to stop
    the actor itself. The outer wrapper is checked first, so `strict` is the
    outer one.
    """

    def build(ctx: ActorContext[Job]) -> Behavior[Job]:
        seen.append("setup")

        async def on_message(message: Job) -> Behavior[Job]:
            if message.fail:
                raise BoomError("boom")
            if message.item < 0:
                raise OtherError("other")
            return Behaviors.same()

        async def on_signal(ctx: ActorContext[Job], signal: Signal) -> Behavior[Job]:
            seen.append(type(signal).__name__)
            return Behaviors.same()

        return Behaviors.receive_message(on_message, on_signal=on_signal)

    lenient = Behaviors.supervise(Behaviors.setup(build)).on_failure(
        SupervisorStrategy.restart(max_restarts=50, window=timedelta(milliseconds=40)),
        on=OtherError,
    )
    return Behaviors.supervise(lenient).on_failure(strict, on=BoomError)


async def test_one_strategys_restarts_do_not_spend_anothers_allowance(
    system: ActorSystem,
):
    seen: list[str] = []
    strict = SupervisorStrategy.restart(max_restarts=3, window=timedelta(minutes=10))
    actor = system.spawn(layered(seen, strict), name="worker")

    # Three failures of the type the other layer governs.
    for _ in range(3):
        actor.tell(Job(item=-1))
    await eventually(lambda: seen.count("setup") == 4)

    # The first failure of the strict type. Its own budget is untouched, so
    # it must restart. Sharing one deque across layers stopped the actor here.
    actor.tell(Job(fail=True))
    await eventually(lambda: seen.count("setup") == 5)

    actor.tell(Job(item=1))
    # Proving a negative takes real time, and this is a wait spent even when
    # the test passes. The TestKit's window is the stated one, rather than a
    # literal chosen here: the case it catches is a stop that happens at once,
    # which is the case that happens.
    await asyncio.sleep(NO_MESSAGE_WINDOW.total_seconds())
    assert "PostStop" not in seen


async def test_a_short_window_does_not_prune_another_strategys_restarts(
    system: ActorSystem,
):
    seen: list[str] = []
    strict = SupervisorStrategy.restart(max_restarts=2, window=timedelta(minutes=10))
    actor = system.spawn(layered(seen, strict), name="worker")

    # Two strict failures spend the strict budget exactly.
    actor.tell(Job(fail=True))
    actor.tell(Job(fail=True))
    await eventually(lambda: seen.count("setup") == 3)

    # Long enough that those two are outside the other layer's 40ms window,
    # then a failure that layer governs. Pruning by the failure's own window
    # against a shared deque threw the strict timestamps away here.
    await asyncio.sleep(0.06)
    actor.tell(Job(item=-1))
    await eventually(lambda: seen.count("setup") == 4)

    # A third strict failure, still inside the strict ten-minute window and
    # over its limit of two. It must stop the actor.
    actor.tell(Job(fail=True))
    await eventually(lambda: "PostStop" in seen)

    assert seen.count("setup") == 4


async def test_a_backoff_counts_only_the_restarts_its_own_layer_made(
    system: ActorSystem,
):
    seen: list[str] = []
    strict = SupervisorStrategy.restart(
        backoff=Backoff(
            min_backoff=MIN_BACKOFF,
            max_backoff=MAX_BACKOFF,
            random_factor=0.0,
        )
    )
    actor = system.spawn(layered(seen, strict), name="worker")

    # Three restarts belonging to the other layer.
    for _ in range(3):
        actor.tell(Job(item=-1))
    await eventually(lambda: seen.count("setup") == 4)

    # The strict layer has restarted nothing yet, so its first backoff is the
    # minimum. Counting every layer's restarts made the exponent 3 here, and
    # this first wait eight times as long.
    started = asyncio.get_running_loop().time()
    actor.tell(Job(fail=True))
    await eventually(lambda: seen.count("setup") == 5, within=5.0)
    waited = asyncio.get_running_loop().time() - started

    # All this measures is that a wait happened rather than the restart going
    # straight through, which takes microseconds. Half the minimum is the
    # bound rather than the minimum itself: measured under a loaded machine
    # the wait comes back between 50.5ms and 53ms for a 50ms backoff, so
    # asserting the minimum exactly leaves half a millisecond of room and
    # fails the day a loop returns from a sleep a fraction early.
    #
    # Which wait it was is not measured here at all. `RestartLog` and
    # `Backoff.delay` decide that, and
    # `test_restarts.py::test_one_layers_restarts_never_lengthen_anothers_wait`
    # asserts the exact schedule against them, where no scheduler is
    # involved. The 150ms window this replaces measured by stopwatch what
    # that test now states.
    assert waited > MIN_BACKOFF.total_seconds() / 2
    assert waited < well_inside(MAX_BACKOFF)


def _failing_setup() -> Behavior[Ping]:
    """A behavior whose deferred construction raises."""

    def build(ctx: ActorContext[Ping]) -> Behavior[Ping]:
        raise BoomError("construction failed")

    return Behaviors.setup(build)


def _switching(seen: list[str]) -> Behavior[Ping]:
    """An actor that records its lifecycle and, on `Ping(n=1)`, returns a failing setup.

    Every other ping is recorded and handled in place.
    """

    def build(ctx: ActorContext[Ping]) -> Behavior[Ping]:
        seen.append("setup")

        async def on_message(ctx: ActorContext[Ping], message: Ping) -> Behavior[Ping]:
            seen.append(f"ping {message.n}")
            if message.n == 1:
                return _failing_setup()
            return Behaviors.same()

        async def on_signal(ctx: ActorContext[Ping], signal: Signal) -> Behavior[Ping]:
            seen.append(type(signal).__name__)
            return Behaviors.same()

        return Behaviors.receive(on_message, Ping, on_signal=on_signal)

    return Behaviors.setup(build)


async def test_a_setup_returned_from_a_handler_is_supervised(system: ActorSystem):
    seen: list[str] = []
    actor = system.spawn(
        Behaviors.supervise(_switching(seen)).on_failure(RESTART), name="actor"
    )

    actor.tell(Ping(n=1))
    actor.tell(Ping(n=2))

    # The failed construction is a restart like any other failure: the stop
    # hook of the failed incarnation runs, the original setup runs again, and
    # the next message reaches the new incarnation.
    await eventually(lambda: "ping 2" in seen)
    assert seen == ["setup", "ping 1", "PreRestart", "setup", "ping 2"]


async def test_a_setup_returned_from_a_handler_can_be_resumed(system: ActorSystem):
    seen: list[str] = []
    actor = system.spawn(
        Behaviors.supervise(_switching(seen)).on_failure(SupervisorStrategy.resume()),
        name="actor",
    )

    actor.tell(Ping(n=1))
    actor.tell(Ping(n=2))

    # Resuming keeps the behavior the actor had, since the new one was never
    # built.
    await eventually(lambda: "ping 2" in seen)
    assert seen == ["setup", "ping 1", "ping 2"]


async def test_an_unsupervised_setup_failure_from_a_handler_runs_the_stop_hook(
    system: ActorSystem,
):
    seen: list[str] = []
    actor = system.spawn(_switching(seen), name="actor")

    actor.tell(Ping(n=1))

    await eventually(lambda: "PostStop" in seen)
    assert seen == ["setup", "ping 1", "PostStop"]
    await eventually(lambda: system.refs.lookup(actor.path) is None)


async def test_a_setup_returned_from_a_signal_handler_is_supervised(
    system: ActorSystem,
):
    seen: list[str] = []

    def build(ctx: ActorContext[Ping]) -> Behavior[Ping]:
        seen.append("setup")
        child = ctx.spawn(
            Behaviors.receive_message(_stop_on_ping, msg_type=Ping), "child"
        )
        ctx.watch(child)

        async def on_message(ctx: ActorContext[Ping], message: Ping) -> Behavior[Ping]:
            seen.append(f"ping {message.n}")
            child.tell(message)
            return Behaviors.same()

        async def on_signal(ctx: ActorContext[Ping], signal: Signal) -> Behavior[Ping]:
            seen.append(type(signal).__name__)
            if isinstance(signal, Terminated):
                return _failing_setup()
            return Behaviors.same()

        return Behaviors.receive(on_message, Ping, on_signal=on_signal)

    actor = system.spawn(
        Behaviors.supervise(Behaviors.setup(build)).on_failure(RESTART), name="actor"
    )

    actor.tell(Ping(n=1))

    await eventually(lambda: seen.count("setup") == 2)
    assert seen == ["setup", "ping 1", "Terminated", "PreRestart", "setup"]


async def _stop_on_ping(message: Ping) -> Behavior[Ping]:
    return Behaviors.stopped()
