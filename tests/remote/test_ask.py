"""Asking an actor on another system, and the three ways it can fail."""

import asyncio
from datetime import timedelta

import pytest

from tapio.actor import ActorSystem
from tapio.errors import (
    AskTargetTerminated,
    AskTargetUnreachable,
    AskTimeoutError,
    AskTypeError,
)
from tapio.testkit import assert_no_leaked_tasks, two_nodes
from tests.failures import eventually
from tests.remote.peers import Ping, Pong, collecting, echoing, ignoring, uri


async def test_asking_across_a_link_returns_the_reply(
    alpha: ActorSystem, beta: ActorSystem
):
    echo = beta.spawn(echoing(), "echo")
    remote = await alpha.resolve(uri(beta, echo), expect=Ping)

    answer = await remote.ask(
        lambda reply_to: Ping(n=7, reply_to=reply_to), expect=Pong
    )

    # Equal and not identical: the reply was rebuilt from JSON on the way
    # back, which is the one guarantee remoting weakens.
    assert answer == Pong(n=7)
    # The promise the reply came back through is deregistered, so an ask
    # leaves nothing addressable behind however it ended.
    assert not [path for path in alpha.refs.paths() if "promises" in str(path)]


async def test_a_remote_ask_that_gets_no_reply_times_out(
    alpha: ActorSystem, beta: ActorSystem
):
    seen: list[int] = []
    quiet = beta.spawn(ignoring(seen), "quiet")
    remote = await alpha.resolve(uri(beta, quiet), expect=Ping)

    with pytest.raises(AskTimeoutError, match="Pong"):
        await remote.ask(
            lambda reply_to: Ping(n=1, reply_to=reply_to),
            expect=Pong,
            timeout=timedelta(milliseconds=50),
        )

    # It arrived. Nobody answered it, which is a different problem from a
    # message that never crossed.
    await eventually(lambda: seen == [1])


async def test_a_remote_ask_fails_when_its_target_stops(
    alpha: ActorSystem, beta: ActorSystem
):
    echo = beta.spawn(echoing(), "echo")
    remote = await alpha.resolve(uri(beta, echo), expect=Ping)

    # A negative ping stops the target without an answer. The watch that
    # the ask registers over the link is what turns that into a failure
    # now rather than a wait for the full deadline.
    with pytest.raises(AskTargetTerminated, match="stopped before replying"):
        await remote.ask(
            lambda reply_to: Ping(n=-1, reply_to=reply_to),
            expect=Pong,
            timeout=timedelta(seconds=5),
        )


async def test_a_remote_ask_fails_fast_when_the_peer_disappears():
    with assert_no_leaked_tasks():
        async with two_nodes() as nodes:
            seen: list[int] = []
            quiet = nodes.beta.spawn(ignoring(seen), "quiet")
            remote = await nodes.alpha.resolve(uri(nodes.beta, quiet), expect=Ping)

            async def partition_once_it_has_arrived() -> None:
                await eventually(lambda: seen == [1])
                nodes.partition()

            watcher = asyncio.ensure_future(partition_once_it_has_arrived())
            started = asyncio.get_running_loop().time()
            try:
                with pytest.raises(AskTargetUnreachable, match="unreachable"):
                    await remote.ask(
                        lambda reply_to: Ping(n=1, reply_to=reply_to),
                        expect=Pong,
                        timeout=timedelta(seconds=30),
                    )
            finally:
                await watcher
            elapsed = asyncio.get_running_loop().time() - started

    # Well inside the deadline it was given, which is the whole point of
    # failing on the peer rather than on the clock.
    assert elapsed < 5.0


async def test_asking_a_quarantined_peer_fails_without_sending_anything():
    with assert_no_leaked_tasks():
        async with two_nodes() as nodes:
            seen: list[int] = []
            answers: list[Pong] = []
            quiet = nodes.beta.spawn(ignoring(seen), "quiet")
            listener = nodes.alpha.spawn(collecting(answers), "listener")
            remote = await nodes.alpha.resolve(uri(nodes.beta, quiet), expect=Ping)
            remote.tell(Ping(n=1, reply_to=listener))
            await eventually(lambda: seen == [1])
            nodes.partition()
            await eventually(lambda: nodes.alpha.remote.quarantined != (), within=5.0)

            with pytest.raises(AskTargetUnreachable, match="beyond reach"):
                await remote.ask(
                    lambda reply_to: Ping(n=2, reply_to=reply_to), expect=Pong
                )

            assert seen == [1]


async def test_a_remote_reply_of_the_wrong_type_fails_the_caller(
    alpha: ActorSystem, beta: ActorSystem
):
    echo = beta.spawn(echoing(), "echo")
    remote = await alpha.resolve(uri(beta, echo), expect=Ping)

    # The responder is right and the caller's expectation is wrong. That
    # surfaces here rather than as a value whose static type is a lie.
    with pytest.raises(AskTypeError, match="Ping"):
        await remote.ask(
            lambda reply_to: Ping(n=1, reply_to=reply_to),
            expect=Ping,
            timeout=timedelta(milliseconds=200),
        )
