"""The event stream: what a system says about itself."""

from tapio import Message
from tapio.actor.events import EventStream


class Started(Message):
    n: int


class Stopped(Message):
    n: int


def test_a_subscriber_hears_only_the_type_it_asked_for():
    stream = EventStream()
    started: list[Started] = []
    stream.subscribe(Started, started.append)

    stream.publish(Started(n=1))
    stream.publish(Stopped(n=2))

    assert started == [Started(n=1)]
    assert stream.total == 2


def test_unsubscribing_stops_the_events():
    stream = EventStream()
    seen: list[Started] = []
    subscription = stream.subscribe(Started, seen.append)

    stream.publish(Started(n=1))
    subscription.unsubscribe()
    stream.publish(Started(n=2))
    # Twice is harmless, since a handler that is already gone cannot be
    # removed twice differently.
    subscription.unsubscribe()

    assert seen == [Started(n=1)]


def test_a_subscriber_that_raises_does_not_stop_the_others():
    stream = EventStream()
    seen: list[Started] = []

    def explode(event: Started) -> None:
        raise RuntimeError("boom")

    stream.subscribe(Started, explode)
    stream.subscribe(Started, seen.append)

    stream.publish(Started(n=1))

    # One bad subscriber must not break the runtime event that reached it.
    assert seen == [Started(n=1)]


def test_publishing_with_nobody_listening_is_counted_and_dropped():
    stream = EventStream()

    stream.publish(Started(n=1))

    assert stream.total == 1
    assert list(stream) == []
