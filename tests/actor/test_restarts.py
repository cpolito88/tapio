"""Counting restarts, one budget per supervisor.

These pin the window: the limit and the backoff exponent read the same count,
so a failure old enough to stop counting against one stops counting against the
other. They also pin the split between layers, which is the other half of the
same question and the one an actor notices as a wait eight times too long.
"""

from datetime import timedelta

from tapio.actor import Backoff, SupervisorStrategy
from tapio.actor.restarts import RestartLog

_BACKOFF = Backoff(
    min_backoff=timedelta(seconds=1),
    max_backoff=timedelta(seconds=30),
    random_factor=0.0,
)


def test_the_backoff_exponent_forgets_restarts_the_window_forgot():
    strategy = SupervisorStrategy.restart(
        max_restarts=3, window=timedelta(seconds=1), backoff=_BACKOFF
    )
    log = RestartLog()

    # Isolated failures, an hour apart: none of them is inside the window, so
    # each restart is the first one as far as the window is concerned.
    for hour in range(6):
        assert log.record("layer", strategy, now=hour * 3600.0)

    assert log.count("layer") == 1
    assert _BACKOFF.delay(log.count("layer"), jitter=0.0) == 1.0


def test_restarts_inside_the_window_still_grow_the_exponent():
    strategy = SupervisorStrategy.restart(
        max_restarts=5, window=timedelta(seconds=10), backoff=_BACKOFF
    )
    log = RestartLog()

    # Three failures inside the same window: the count, and so the delay, climb.
    for tick in range(3):
        assert log.record("layer", strategy, now=float(tick))

    assert log.count("layer") == 3
    assert _BACKOFF.delay(log.count("layer"), jitter=0.0) == 4.0


def test_an_unlimited_strategy_counts_for_the_life_of_the_actor():
    # With no limit there is no window to age the count against, so it grows
    # with every restart, which is the only meaning backoff can have there.
    strategy = SupervisorStrategy.restart(backoff=_BACKOFF)
    log = RestartLog()

    for hour in range(4):
        assert log.record("layer", strategy, now=hour * 3600.0)

    assert log.count("layer") == 4


def test_one_layers_restarts_never_lengthen_anothers_wait():
    # Two supervisors over one actor keep separate budgets. Counting every
    # layer's restarts made a layer's first backoff its fourth, and the wait
    # eight times as long. `test_supervision.py` drives this through a running
    # actor; the schedule itself is decided here, where no clock is involved.
    strategy = SupervisorStrategy.restart(
        max_restarts=50, window=timedelta(seconds=10), backoff=_BACKOFF
    )
    log = RestartLog()

    for tick in range(3):
        assert log.record("lenient", strategy, now=float(tick))
    assert log.record("strict", strategy, now=3.0)

    # The strict layer has restarted once, so its wait is the minimum.
    assert log.count("strict") == 1
    assert _BACKOFF.delay(log.count("strict"), jitter=0.0) == 1.0

    # Shared, the same restart would have been the fourth.
    assert log.count("lenient") == 3
    assert _BACKOFF.delay(4, jitter=0.0) == 8.0
