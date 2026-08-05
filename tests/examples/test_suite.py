"""Runs every example and asserts what it produced.

An example that breaks fails the build, which is the whole reason they are a
package rather than a folder of scripts. The last test here keeps the two in
step: a new example with no assertion is itself a failure.
"""

import pkgutil

import tapio_examples
from tapio.testkit import assert_no_leaked_tasks
from tapio_examples import (
    ask_timeout,
    counter,
    dead_letters,
    death_watch,
    escalation,
    graceful_shutdown,
    hello_world,
    ping_pong,
    rate_limiter,
    stash_on_startup,
    state_machine,
    supervision_backoff,
    two_nodes,
    worker_pool,
)

ASSERTED = {
    "ask_timeout",
    "counter",
    "dead_letters",
    "death_watch",
    "escalation",
    "graceful_shutdown",
    "hello_world",
    "ping_pong",
    "rate_limiter",
    "stash_on_startup",
    "state_machine",
    "supervision_backoff",
    "two_nodes",
    "worker_pool",
}


async def test_hello_world():
    with assert_no_leaked_tasks():
        lines = await hello_world.main()

    assert lines == [
        "greeter: hello, world!",
        "listener: world has been greeted",
    ]


async def test_two_nodes():
    with assert_no_leaked_tasks():
        lines = await two_nodes.main()

    # The same three beats as hello_world, with a link between the first and
    # the second: the request crosses, the greeting happens on the other node,
    # and the reply comes back to a ref that crossed with it.
    assert lines[0].startswith("home: resolving tapio://away@127.0.0.1:")
    assert lines[1:] == [
        "away: hello, world!",
        "home: world has been greeted",
    ]


async def test_ask_timeout():
    with assert_no_leaked_tasks():
        lines = await ask_timeout.main()

    # One reply, one deadline, one answer that arrived too late to be one, and
    # one failure that did not wait for the deadline it was given.
    assert lines == [
        "reader: 'Dune' is on shelf 3",
        "reader: gave up on 'Ulysses' after 0.05s",
        "dead letter: Shelf (ask-settled)",
        "reader: the desk closed, so there was no point waiting",
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


async def test_supervision_backoff():
    with assert_no_leaked_tasks():
        lines = await supervision_backoff.main()

    # Two failures, two restarts, then the work goes through. Items 2 and 3
    # were sent while the actor did not exist, and both were handled.
    assert lines[:6] == [
        "uploader: incarnation 1 ready",
        "uploader: item 1 failed",
        "uploader: incarnation 2 ready",
        "uploader: item 2 failed",
        "uploader: incarnation 3 ready",
        "uploader: item 3 uploaded",
    ]
    # And the other half of the bargain: a failure that never clears stops the
    # actor rather than restarting it forever.
    assert lines[-1] == "doomed: restart window exhausted, stopped"


async def test_death_watch():
    with assert_no_leaked_tasks():
        lines = await death_watch.main()

    assert lines == [
        "registry: registered ada, holding 1",
        "registry: registered grace, holding 2",
        "registry: ada stopped, holding 1",
        "registry: holding ['grace']",
    ]


async def test_escalation():
    with assert_no_leaked_tasks():
        lines = await escalation.main()

    # The worker escalated, so its parent's decision rebuilt the subtree.
    assert lines[:7] == [
        "pipeline: building, incarnation 1",
        "worker: ready",
        "worker: cannot parse an empty line",
        "worker: stopped",
        "pipeline: restarting after the worker escalated",
        "pipeline: building, incarnation 2",
        "worker: ready",
    ]
    # The ticker sits outside the restarted subtree and never noticed: a child
    # failing cancels nothing but itself and its supervisor's subtree.
    assert "ticker: tick 1" in lines
    assert "ticker: tick 2" in lines
    assert "worker: parsed 'ok'" in lines
    # And an escalation nobody catches ends the system with the cause intact,
    # carrying the path it climbed.
    assert lines[-3:] == [
        "system: terminated by empty input",
        "system: escalated from tapio://unsupervised/user/worker#1",
        "system: escalated to tapio://unsupervised/user",
    ]


async def test_graceful_shutdown():
    with assert_no_leaked_tasks():
        lines = await graceful_shutdown.main()

    # A real SIGINT, and then the drain: children before the parent that owns
    # them, and the pool's own close last.
    assert lines == [
        "conn-1: ran 'select 1'",
        "conn-2: ran 'select 1'",
        "signal: SIGINT, shutting down",
        "conn-1: closed",
        "conn-2: closed",
        "pool: closed, after every connection in it",
    ]


async def test_stash_on_startup():
    with assert_no_leaked_tasks():
        lines = await stash_on_startup.main()

    # The two that arrived before the template are answered first and in the
    # order they were sent, and the one that arrived after does not overtake
    # them despite the actor being ready by the time it landed.
    assert lines == [
        "greeter: loading, holding what arrives",
        "greeter: not ready, stashed ada",
        "greeter: not ready, stashed grace",
        "greeter: loaded, replaying 2 held",
        "greeter: hello, ada!",
        "greeter: hello, grace!",
        "greeter: hello, carol!",
    ]


async def test_rate_limiter():
    with assert_no_leaked_tasks():
        lines = await rate_limiter.main()

    # A bucket of two against a burst of five, then one refilled permit. The
    # burst is sent in a single go, so no refill can land inside it.
    assert lines == [
        "req-1: allowed",
        "req-2: allowed",
        "req-3: throttled",
        "req-4: throttled",
        "req-5: throttled",
        "req-6: allowed",
    ]


async def test_worker_pool():
    with assert_no_leaked_tasks():
        lines = await worker_pool.main()

    # Six jobs over three workers in strict rotation, never twice in a row on
    # the same one. Then the first worker goes, and the rotation carries on
    # across the two that are left rather than starting again.
    assert lines == [
        "routee-1: job 1",
        "routee-2: job 2",
        "routee-3: job 3",
        "routee-1: job 4",
        "routee-2: job 5",
        "routee-3: job 6",
        "routee-1: stopped, and left the pool",
        "routee-3: job 7",
        "routee-2: job 8",
        "routee-3: job 9",
        "routee-2: job 10",
    ]


async def test_state_machine():
    with assert_no_leaked_tasks():
        lines = await state_machine.main()

    # The same Send is refused twice and then goes through: the state is the
    # behavior, so what is legal is whatever the current one mentions. The
    # token reaches the connection translated, in its own vocabulary.
    assert lines == [
        "conn: refused Send, not open",
        "conn: connecting, asking for a token",
        "conn: refused Send, still connecting",
        "conn: authenticated with t-42",
        "conn: sent 'hello' with t-42",
        "conn: closing, draining what is queued",
        "conn: dropped Send, closing",
        "conn: closed",
    ]


def test_every_example_is_asserted():
    modules = {m.name for m in pkgutil.iter_modules(tapio_examples.__path__)}

    assert modules == ASSERTED, f"unasserted examples: {sorted(modules - ASSERTED)}"
