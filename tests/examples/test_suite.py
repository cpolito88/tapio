"""Runs every example and asserts what it produced.

An example that breaks fails the build, which is the whole reason they are a
package rather than a folder of scripts. The last test here keeps the two in
step: a new example with no assertion is itself a failure.
"""

import pkgutil

import tapio_examples
from tapio.testkit import assert_no_leaked_tasks
from tapio_examples import counter, dead_letters, hello_world, ping_pong

ASSERTED = {"counter", "dead_letters", "hello_world", "ping_pong"}


async def test_hello_world():
    with assert_no_leaked_tasks():
        lines = await hello_world.main()

    assert lines == [
        "greeter: hello, world!",
        "listener: world has been greeted",
    ]


async def test_ping_pong():
    with assert_no_leaked_tasks():
        lines = await ping_pong.main()

    # Hops alternate and never overtake each other, and ping has the last word.
    assert lines[:4] == [
        "ping: hop 1",
        "pong: hop 2",
        "ping: hop 3",
        "pong: hop 4",
    ]
    assert lines[-1] == "ping: that is enough, stopping"


async def test_counter():
    with assert_no_leaked_tasks():
        value = await counter.main()

    # Both increments are applied before the query queued behind them.
    assert value == 3


async def test_dead_letters():
    with assert_no_leaked_tasks():
        lines = await dead_letters.main()

    # Three sends that did not arrive, and three different diagnoses. The
    # reasons are the lesson: a stopped actor, a mailbox that shed the stalest
    # work it was holding, and a send that outlived its system.
    assert len(lines) == 3
    assert "Work(item=1)" in lines[0]
    assert lines[0].endswith("(recipient-terminated)")
    assert "Work(item=3)" in lines[1]
    assert lines[1].endswith("(mailbox-full)")
    assert "Work(item=6)" in lines[2]
    assert lines[2].endswith("(system-terminated)")


def test_every_example_is_asserted():
    modules = {m.name for m in pkgutil.iter_modules(tapio_examples.__path__)}

    assert modules == ASSERTED, f"unasserted examples: {sorted(modules - ASSERTED)}"
