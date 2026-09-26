"""Tests for unwrapping a behavior: the loop the cell and the test kit share."""

from typing import Any

import pytest

from tapio import Behavior, Behaviors
from tapio.actor import ActorContext, StashBuffer, SupervisorStrategy
from tapio.actor.construction import MAX_SETUP_DEPTH, construct
from tapio.actor.timers import TimerScheduler
from tapio.errors import BehaviorTypeError
from tapio.testkit import BehaviorTestKit
from tests.messages import Ping


class _Host:
    """Hands out a stash buffer and records what it is asked to replay."""

    def __init__(self) -> None:
        self.ctx: ActorContext[Ping] = BehaviorTestKit(_sink()).ctx
        self.buffers: list[StashBuffer[Ping]] = []
        self.replayed: list[StashBuffer[Ping]] = []

    @property
    def timers(self) -> TimerScheduler[Ping]:
        raise AssertionError("no test here asks for timers")

    def stash_buffer(self, capacity: int) -> StashBuffer[Ping]:
        buffer: StashBuffer[Ping] = StashBuffer(capacity)
        self.buffers.append(buffer)
        return buffer

    def unstash(self, buffer: StashBuffer[Ping]) -> None:
        self.replayed.append(buffer)


def test_wrappers_come_off_in_any_order_and_strategies_read_outermost_first():
    sink = _sink()
    restart = SupervisorStrategy.restart()
    resume = SupervisorStrategy.resume()

    def build(ctx: ActorContext[Ping]) -> Behavior[Ping]:
        return Behaviors.supervise(sink).on_failure(resume)

    constructed = construct(
        Behaviors.supervise(Behaviors.setup(build)).on_failure(restart), _Host()
    )

    assert constructed.behavior is sink
    assert [layer.strategy for layer in constructed.supervision] == [restart, resume]


def test_a_replay_is_handed_to_the_host():
    sink = _sink()
    host = _Host()

    constructed = construct(
        Behaviors.with_stash(5, lambda stash: stash.unstash_all(sink)), host
    )

    assert constructed.behavior is sink
    assert host.replayed == host.buffers
    assert host.buffers[0].capacity == 5


def test_a_setup_that_returns_itself_is_refused():
    def build(ctx: ActorContext[Ping]) -> Behavior[Ping]:
        return Behaviors.setup(build)

    with pytest.raises(BehaviorTypeError, match=f"after {MAX_SETUP_DEPTH} rounds"):
        construct(Behaviors.setup(build), _Host())


def test_a_factory_that_raises_raises_to_the_caller():
    def build(ctx: ActorContext[Ping]) -> Behavior[Ping]:
        raise RuntimeError("construction failed")

    with pytest.raises(RuntimeError, match="construction failed"):
        construct(Behaviors.setup(build), _Host())


def _sink() -> Behavior[Ping]:
    async def on_message(message: Any) -> Behavior[Ping]:
        return Behaviors.same()

    return Behaviors.receive_message(on_message, msg_type=Ping)
