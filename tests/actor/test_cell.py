"""Tests for the cell's own runtime behaviour, with a live system.

The directive behaviors, `empty()` and `ignore()`, carry no signal handler of
their own. The cell keeps delivering signals to the last real behavior anyway,
so an actor that drains its work and steps back still runs its stop hook and
still hears about a death it was watching.
"""

from tapio import ActorSystem, Behavior, Behaviors, Message
from tapio.actor import ActorContext, ActorRef, Signal
from tests.failures import eventually


class Drain(Message):
    """Tells the actor to step back to a directive behavior."""


class Watch(Message):
    """Carries the actor to watch, so the test can then stop it."""

    target: ActorRef[Message]


def _stepping_back(seen: list[str], *, to: Behavior[Drain]) -> Behavior[Drain]:
    """A behavior that records its signals and becomes a directive on `Drain`."""

    async def on_message(ctx: ActorContext[Drain], message: Drain) -> Behavior[Drain]:
        seen.append("drained")
        return to

    async def on_signal(ctx: ActorContext[Drain], signal: Signal) -> Behavior[Drain]:
        seen.append(type(signal).__name__)
        return Behaviors.same()

    return Behaviors.receive(on_message, on_signal=on_signal)


async def test_an_actor_that_becomes_empty_still_runs_its_stop_hook(
    system: ActorSystem,
):
    seen: list[str] = []

    ref = system.spawn(_stepping_back(seen, to=Behaviors.empty()), name="holder")
    ref.tell(Drain())
    await eventually(lambda: seen == ["drained"])

    await system.terminate()

    # PostStop is where a held resource is released. An actor that stepped back
    # to empty() must still run it, or the resource leaks silently.
    assert seen == ["drained", "PostStop"]


async def test_an_actor_that_becomes_ignore_still_runs_its_stop_hook(
    system: ActorSystem,
):
    seen: list[str] = []

    ref = system.spawn(_stepping_back(seen, to=Behaviors.ignore()), name="holder")
    ref.tell(Drain())
    await eventually(lambda: seen == ["drained"])

    await system.terminate()

    assert seen == ["drained", "PostStop"]


async def test_an_actor_that_becomes_empty_still_hears_a_watched_death(
    system: ActorSystem,
):
    seen: list[str] = []

    async def on_message(ctx: ActorContext[Watch], message: Watch) -> Behavior[Watch]:
        ctx.watch(message.target)
        seen.append("watching")
        return Behaviors.empty()

    async def on_signal(ctx: ActorContext[Watch], signal: Signal) -> Behavior[Watch]:
        seen.append(type(signal).__name__)
        return Behaviors.same()

    async def be_stopped(message: Drain) -> Behavior[Drain]:
        return Behaviors.stopped()

    watcher = system.spawn(
        Behaviors.receive(on_message, on_signal=on_signal), name="watcher"
    )
    victim = system.spawn(Behaviors.receive_message(be_stopped), name="victim")

    watcher.tell(Watch(target=victim))  # type: ignore[arg-type]
    await eventually(lambda: seen == ["watching"])
    victim.tell(Drain())  # type: ignore[arg-type]

    # The watcher switched to empty() before the death. A Terminated dropped
    # here is a watcher that waits forever for an eviction it was promised.
    await eventually(lambda: "Terminated" in seen)
    assert seen == ["watching", "Terminated"]
