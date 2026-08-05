"""The two-lane mailbox: precedence, ordering, and the wakeup invariants."""

import asyncio

import pytest

from tapio.actor.mailbox import Mailbox
from tapio.actor.signals import PostStop
from tests.messages import Increment


async def test_system_lane_drains_before_the_user_lane():
    mailbox = Mailbox()
    mailbox.put(Increment(by=1))
    mailbox.put_system(PostStop())

    assert isinstance(await mailbox.get(), PostStop)
    assert await mailbox.get() == Increment(by=1)


async def test_ordering_is_preserved_within_a_lane():
    mailbox = Mailbox()
    for i in range(5):
        mailbox.put(Increment(by=i))

    received = [await mailbox.get() for _ in range(5)]

    assert [m.by for m in received] == [0, 1, 2, 3, 4]


async def test_a_message_appended_while_waiting_is_never_lost():
    # Clearing the wakeup event after waking but before re-examining the
    # deques is what makes this safe. The reverse order drops the append that
    # lands in between.
    mailbox = Mailbox()
    loop = asyncio.get_running_loop()
    loop.call_soon(mailbox.put, Increment(by=7))

    message = await asyncio.wait_for(mailbox.get(), timeout=1)

    assert message == Increment(by=7)


async def test_repeated_waits_do_not_go_deaf():
    mailbox = Mailbox()
    for i in range(3):
        asyncio.get_running_loop().call_later(
            0.001 * (i + 1), mailbox.put, Increment(by=i)
        )

    received = [await asyncio.wait_for(mailbox.get(), timeout=1) for _ in range(3)]

    assert [m.by for m in received] == [0, 1, 2]


async def test_a_second_concurrent_consumer_is_refused():
    # The single-waiter design only works with one reader at a time, so the
    # mailbox checks that rather than trusting it.
    mailbox = Mailbox()
    first = asyncio.create_task(mailbox.get())
    await asyncio.sleep(0)

    with pytest.raises(RuntimeError, match="one consumer at a time"):
        await mailbox.get()

    mailbox.put(Increment())
    await first


async def test_sizes_and_repr_report_both_lanes():
    mailbox = Mailbox()
    mailbox.put(Increment())
    mailbox.put_system(PostStop())

    assert mailbox.user_size == 1
    assert mailbox.system_size == 1
    assert len(mailbox) == 2
    assert repr(mailbox) == "Mailbox(system=1, user=1)"
