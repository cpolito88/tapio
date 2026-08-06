"""Test support: helpers for asserting things about a running system.

Three pieces, for three kinds of test.

[TestProbe][tapio.testkit.probe.TestProbe] is an actor in a real system whose
job is to be asserted about. Hand its ref to the code under test and expect
what arrives.

[BehaviorTestKit][tapio.testkit.behavior.BehaviorTestKit] runs one behavior
with no system at all, for testing the function a handler is: what it returns,
what it sends, what it spawns.

`two_nodes` and the leak assertions are for the runtime itself: two systems on
loopback with a network you can break, and the checks that a block of work
left no task and no thread behind.

The pytest fixtures in [plugin][tapio.testkit.plugin] are registered through
an entry point, so they need no import and no `conftest.py`.
"""

from tapio.testkit.behavior import (
    BehaviorTestKit,
    Effect,
    RecordingRef,
    Spawned,
    Watched,
)
from tapio.testkit.leaks import assert_no_leaked_tasks, assert_no_leaked_threads
from tapio.testkit.probe import DEFAULT_TIMEOUT, NO_MESSAGE_WINDOW, TestProbe
from tapio.testkit.remote import LinkFaults, TwoNodes, link_faults, two_nodes

__all__ = [
    "DEFAULT_TIMEOUT",
    "NO_MESSAGE_WINDOW",
    "BehaviorTestKit",
    "Effect",
    "LinkFaults",
    "RecordingRef",
    "Spawned",
    "TestProbe",
    "TwoNodes",
    "Watched",
    "assert_no_leaked_tasks",
    "assert_no_leaked_threads",
    "link_faults",
    "two_nodes",
]
