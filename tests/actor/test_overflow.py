"""Bounded mailboxes: what happens to a message that arrives at a full one.

The four cases the plan promises are: raise in the sender, drop the arriving
message, drop the oldest queued one, and wait for room. Every drop is accounted
for as a dead letter, so none of these strategies loses a message silently.
"""

import asyncio

import pytest

from tapio.actor.mailbox import Mailbox, MailboxConfig, OverflowStrategy
from tapio.errors import MailboxFullError
from tests.messages import Ping


def bounded(capacity: int, strategy: OverflowStrategy) -> Mailbox:
    return Mailbox(MailboxConfig(capacity=capacity, on_overflow=strategy))


def test_a_capacity_below_one_is_rejected():
    with pytest.raises(ValueError, match="at least 1"):
        MailboxConfig(capacity=0)


def test_an_unbounded_lane_is_never_full():
    mailbox = Mailbox()
    for n in range(100):
        assert mailbox.put(Ping(n=n)) is None
    assert not mailbox.is_full


def test_fail_raises_in_the_sender_naming_the_capacity():
    mailbox = bounded(2, OverflowStrategy.FAIL)
    mailbox.put(Ping(n=1))
    mailbox.put(Ping(n=2))
    with pytest.raises(MailboxFullError, match="capacity 2"):
        mailbox.put(Ping(n=3))


def test_drop_new_returns_the_arriving_message_and_keeps_the_queue():
    mailbox = bounded(2, OverflowStrategy.DROP_NEW)
    mailbox.put(Ping(n=1))
    mailbox.put(Ping(n=2))

    displaced = mailbox.put(Ping(n=3))

    assert displaced == Ping(n=3)
    assert mailbox.user_size == 2


def test_drop_oldest_returns_the_head_and_enqueues_the_arrival():
    mailbox = bounded(2, OverflowStrategy.DROP_OLDEST)
    mailbox.put(Ping(n=1))
    mailbox.put(Ping(n=2))

    displaced = mailbox.put(Ping(n=3))

    assert displaced == Ping(n=1)
    assert mailbox.user_size == 2


async def test_the_system_lane_ignores_capacity_entirely():
    # A limit that could refuse a stop signal would make shutdown unreliable,
    # so backpressure is a property of the user lane only.
    from tapio.actor.signals import PostStop

    mailbox = bounded(1, OverflowStrategy.FAIL)
    mailbox.put(Ping(n=1))
    for _ in range(10):
        mailbox.put_system(PostStop())
    assert mailbox.system_size == 10


async def test_offer_waits_for_room_rather_than_dropping():
    mailbox = bounded(1, OverflowStrategy.FAIL)
    mailbox.put(Ping(n=1))

    sender = asyncio.create_task(mailbox.offer(Ping(n=2)))
    await asyncio.sleep(0)
    assert not sender.done()
    assert mailbox.waiting_senders == 1

    await mailbox.get()
    await sender
    assert mailbox.user_size == 1


async def test_blocked_senders_wake_one_per_slot_in_arrival_order():
    mailbox = bounded(1, OverflowStrategy.FAIL)
    mailbox.put(Ping(n=0))
    arrived: list[int] = []

    async def send(n: int) -> None:
        await mailbox.offer(Ping(n=n))
        arrived.append(n)

    senders = [asyncio.create_task(send(n)) for n in (1, 2, 3)]
    await asyncio.sleep(0)
    assert mailbox.waiting_senders == 3

    # One freed slot wakes exactly one sender, and it is the one that has
    # waited longest. An Event would wake all three for the same slot.
    await mailbox.get()
    await asyncio.sleep(0)
    assert arrived == [1]

    await mailbox.get()
    await mailbox.get()
    await asyncio.gather(*senders)
    assert arrived == [1, 2, 3]


async def test_a_cancelled_offer_leaves_no_future_behind():
    mailbox = bounded(1, OverflowStrategy.FAIL)
    mailbox.put(Ping(n=1))

    sender = asyncio.create_task(mailbox.offer(Ping(n=2)))
    await asyncio.sleep(0)
    assert mailbox.waiting_senders == 1

    sender.cancel()
    with pytest.raises(asyncio.CancelledError):
        await sender

    assert mailbox.waiting_senders == 0
    # And the slot it would have taken goes to the next sender, not nowhere.
    await mailbox.get()
    await mailbox.offer(Ping(n=3))
    assert mailbox.user_size == 1


async def test_cancelling_one_waiter_does_not_strand_the_others():
    mailbox = bounded(1, OverflowStrategy.FAIL)
    mailbox.put(Ping(n=0))

    first = asyncio.create_task(mailbox.offer(Ping(n=1)))
    second = asyncio.create_task(mailbox.offer(Ping(n=2)))
    await asyncio.sleep(0)

    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first

    await mailbox.get()
    await second
    assert mailbox.user_size == 1


async def test_closing_releases_every_blocked_sender():
    mailbox = bounded(1, OverflowStrategy.FAIL)
    mailbox.put(Ping(n=0))
    senders = [asyncio.create_task(mailbox.offer(Ping(n=n))) for n in (1, 2)]
    await asyncio.sleep(0)

    mailbox.close()
    await asyncio.gather(*senders)

    # Nobody is left awaiting a slot that a stopped actor will never free.
    assert mailbox.waiting_senders == 0
