"""Runs every example and asserts what it printed.

A broken example fails the build, which is why the examples are a package
rather than a folder of scripts. The last test keeps this file in step with
them: a new example with no assertion here fails on purpose.
"""

import pkgutil

import tapio_examples
from tapio.testkit import assert_no_leaked_tasks
from tapio_examples import (
    ask_timeout,
    blocking_offload,
    chat_sessions,
    counter,
    dead_letters,
    death_watch,
    escalation,
    fastapi_app,
    graceful_shutdown,
    hello_world,
    node_failure,
    order_saga,
    partition,
    ping_pong,
    rate_limiter,
    remote_ask,
    remote_spawn,
    stash_on_startup,
    state_machine,
    supervision_backoff,
    two_nodes,
    worker_pool,
    worker_pool_remote,
)

ASSERTED = {
    "ask_timeout",
    "blocking_offload",
    "chat_sessions",
    "counter",
    "dead_letters",
    "death_watch",
    "escalation",
    "graceful_shutdown",
    "hello_world",
    "fastapi_app",
    "node_failure",
    "order_saga",
    "partition",
    "ping_pong",
    "rate_limiter",
    "remote_ask",
    "remote_spawn",
    "stash_on_startup",
    "state_machine",
    "supervision_backoff",
    "two_nodes",
    "worker_pool",
    "worker_pool_remote",
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

    # The same three steps as hello_world, with a link in between. The request
    # crosses, the greeting happens on the other node, and the reply comes
    # back to a ref that crossed with it.
    assert lines[0].startswith("home: resolving tapio://away@127.0.0.1:")
    assert lines[1:] == [
        "away: hello, world!",
        "home: world has been greeted",
    ]


async def test_ask_timeout():
    with assert_no_leaked_tasks():
        lines = await ask_timeout.main()

    # One reply, one deadline, one answer that arrived too late to count, and
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

    # Three sends that did not arrive, for three different reasons: a stopped
    # actor, a mailbox that dropped the oldest work it held, and a send that
    # outlived its system.
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
    # The other half: a failure that never clears stops the actor rather than
    # restarting it forever.
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
    # The ticker sits outside the restarted subtree and never noticed. A child
    # failing stops only itself and its supervisor's subtree.
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

    # A real SIGINT, then the drain: children before the parent that owns
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

    # The two that arrived before the template are answered first, in the
    # order they were sent. The one that arrived after does not overtake them,
    # even though the actor was ready when it landed.
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
    # burst is sent in one go, so no refill can land inside it.
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
    # the same one. Then the first worker stops, and the rotation carries on
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

    # The same Send is refused twice and then goes through. The state is the
    # behavior, so what is legal is whatever the current one handles. The
    # token reaches the connection already translated.
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


async def test_remote_ask():
    with assert_no_leaked_tasks():
        lines = await remote_ask.main()

    # One answer, and two failures that took about the same time for entirely
    # different reasons. The third is the one that could not happen locally:
    # the node stopped answering, so the ask failed on the peer rather than
    # waiting out the thirty seconds it was given.
    assert lines == [
        "asker: six by seven is 42",
        "asker: no answer in time, and the node is still there",
        "asker: the answering node is unreachable, so no waiting",
        "asker: after reconnecting, six by seven is still 42",
    ]


async def test_node_failure():
    with assert_no_leaked_tasks():
        lines = await node_failure.main()

    # The coordinator does not retry against the node that is gone. It
    # rebuilds the worker under itself and finishes the job there.
    assert lines == [
        "home: job 1 done by away",
        "home: the away node is gone, rebuilding here",
        "home: job 2 done by home",
    ]


async def test_partition():
    with assert_no_leaked_tasks():
        lines = await partition.main()

    # Both nodes give up on the other, both tell their watchers that a live
    # actor has stopped, and both keep working. The two beliefs are
    # contradictory and neither node can tell.
    home, away = lines[:6], lines[6:]
    assert home[0] == "home: poked by away, still working"
    assert home[1].startswith("home: gave up on tapio://away@")
    assert home[1].endswith("quarantined")
    assert home[2] == "home: told that tapio://away/user/steady#2 has stopped"
    assert home[3] == "home: poked by home itself, still working"
    assert home[4:] == [
        "home: network repaired, and still no association",
        "home: reconnected, because somebody decided to",
    ]
    assert away[0] == "away: poked by home, still working"
    assert away[1].startswith("away: gave up on tapio://home@")
    assert away[2] == "away: told that tapio://home/user/steady#2 has stopped"
    assert away[3] == "away: poked by away itself, still working"


async def test_chat_sessions():
    with assert_no_leaked_tasks():
        lines = await chat_sessions.main()

    # The model crashed while it was holding a request, so the ask timed out
    # and the session asked the restarted client instead. The turn count is
    # the proof that the session's own state was never touched.
    assert lines == [
        "chat: alice has a session at session-alice",
        "chat: alice heard \"about 'hello', then\" on turn 1",
        "chat: bob has a session at session-bob",
        "chat: bob heard \"about 'hi', then\" on turn 1",
        "chat: no answer for alice, so ask the new client",
        "chat: alice heard \"about 'again', then\" on turn 2",
        "chat: alice's session stopped, so it is evicted",
    ]


async def test_order_saga():
    with assert_no_leaked_tasks():
        lines = await order_saga.main()

    # Two steps ran, shipping refused, and exactly those two were undone, in
    # reverse. Nothing raised and nothing was left half done.
    assert lines == [
        "saga: payments did its part of order-1",
        "saga: inventory did its part of order-1",
        "saga: inventory undid its part of order-1",
        "saga: payments undid its part of order-1",
        "saga: order-1 failed because shipping refused",
        "saga: undone, newest first: inventory, payments",
        "saga: nothing was left half done, and nothing raised",
    ]


async def test_fastapi_app():
    with assert_no_leaked_tasks():
        lines = await fastapi_app.main()

    # Twenty concurrent requests through one actor, and every count handed out
    # exactly once. The 503 is the ask deadline the handler owns, and the dead
    # letter after it is the answer nobody was waiting for any more.
    assert lines[0] == "web: POST /hit -> 200 {'total': 1}"
    assert lines[1] == "web: 20 concurrent hits ran 2 to 21, 20 distinct"
    assert lines[2] == "web: POST /slow -> 503, and the app serves on"
    assert lines[3] == (
        "web: the counter answered anyway, and Counted became a dead letter "
        "(ask-settled)"
    )
    assert lines[4] == "web: POST /hit -> 200 {'total': 22}"


async def test_blocking_offload():
    with assert_no_leaked_tasks():
        lines = await blocking_offload.main()

    # The count is asserted and the elapsed times are not. A loaded CI runner
    # changes how long the call took; it does not change the fact that the
    # loop was frozen for all of it.
    assert lines[0] == (
        "on the loop: the rest of the system processed 0 messages during the call"
    )
    processed = int(lines[1].split("processed ")[1].split(" ")[0])
    assert processed > 10, lines[1]


async def test_remote_spawn():
    with assert_no_leaked_tasks():
        lines = await remote_spawn.main()

    # The worker was started on the other node, crashed, and was restarted
    # there. The job queued behind the crash came back through the same ref,
    # and the only trace of the restart is the job count starting again.
    assert lines[0].startswith("orders: compute started doubler-1 at tapio://compute/")
    assert lines[1:] == [
        "orders: 6 doubled is 12, job 1 for it",
        "orders: 7 doubled is 14, job 1 for it",
        "orders: the count restarted, and nothing told me why",
        "orders: compute cannot start 'tripler' (unknown-factory)",
        "orders: the worker is gone, so ask for another",
    ]


async def test_worker_pool_remote():
    with assert_no_leaked_tasks():
        lines = await worker_pool_remote.main()

    # The grant is the only thing bounding the work in flight. Nothing in the
    # transport enforces it, and the split between the two workers is whatever
    # the scheduling happened to be.
    assert lines[0] == "orders: 2 workers on compute, each granting 3 items at a time"
    first, second = (int(part) for part in lines[1].split() if part.isdigit())
    assert first + second == 12
    assert first > 0
    assert second > 0
    assert lines[2:] == [
        "orders: 12 items done, and never more than 3 outstanding at one worker",
        "orders: the grant is the backpressure; offer would have waited on "
        "this node's outbound buffer instead",
    ]


def test_every_example_is_asserted():
    modules = {m.name for m in pkgutil.iter_modules(tapio_examples.__path__)}

    assert modules == ASSERTED, f"unasserted examples: {sorted(modules - ASSERTED)}"
