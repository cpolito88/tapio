"""The book an actor keeps of its death watches, without an actor around it.

These are about the bookkeeping alone: what a watch leaves behind, and that
stopping leaves nothing behind in either direction. How an actor learns that
another stopped is in `test_death_watch.py`, which needs a live tree.
"""

from typing import Any

from tapio.actor.path import ActorPath
from tapio.actor.watch import DeathWatch, Watcher, WatchTarget

ROOT = ActorPath.root("test")


class FakeWatcher:
    """Something that can be told, and remembers what it was told."""

    def __init__(self, name: str) -> None:
        """Name it, so it has a path to be keyed under."""
        self.path = ROOT.child(name)
        self.terminated: list[str] = []

    def notify_terminated(self, ref: Any) -> None:
        self.terminated.append(str(ref))

    def notify_unreachable(self, ref: Any, detail: str) -> None:
        self.terminated.append(f"{ref} unreachable: {detail}")


class FakeTarget:
    """Something that can be watched, and remembers who is watching."""

    def __init__(self, name: str) -> None:
        """Name it, so it has a path to be keyed under."""
        self.path = ROOT.child(name)
        self.is_alive = True
        self.watchers: list[ActorPath] = []

    def add_watcher(self, watcher: Any) -> None:
        self.watchers.append(watcher.path)

    def remove_watcher(self, watcher: Any) -> None:
        if watcher.path in self.watchers:
            self.watchers.remove(watcher.path)


def test_the_fakes_here_are_the_real_two_ends():
    # Both protocols are runtime-checkable, so this keeps the stand-ins below
    # honest: a method added to either end fails here rather than letting
    # these tests pass against a shape the cell no longer uses.
    assert isinstance(FakeWatcher("watcher"), Watcher)
    assert isinstance(FakeTarget("target"), WatchTarget)


def test_a_watcher_registered_twice_is_held_once():
    book = DeathWatch()
    watcher = FakeWatcher("watcher")

    book.add_watcher(watcher)
    book.add_watcher(watcher)

    # Keyed by path, so watching twice still delivers exactly one signal.
    assert book.watchers == (watcher.path,)


def test_removing_a_watcher_that_was_never_added_is_harmless():
    book = DeathWatch()

    book.remove_watcher(FakeWatcher("stranger"))

    assert book.watchers == ()


def test_a_watch_is_returned_once_and_then_forgotten():
    book = DeathWatch()
    target = FakeTarget("target")
    book.watching(target)

    assert book.stop_watching(target.path) is target
    # The caller deregisters from what it gets back, so handing the same
    # target out twice would deregister twice.
    assert book.stop_watching(target.path) is None


def test_stopping_tells_every_watcher_once_and_keeps_none():
    book = DeathWatch()
    first, second = FakeWatcher("first"), FakeWatcher("second")
    book.add_watcher(first)
    book.add_watcher(second)

    book.release(FakeWatcher("stopping"), "ref-to-me")
    book.release(FakeWatcher("stopping"), "ref-to-me")

    assert first.terminated == ["ref-to-me"]
    assert second.terminated == ["ref-to-me"]
    assert book.watchers == ()


def test_stopping_deregisters_from_everything_it_was_watching():
    book = DeathWatch()
    stopping = FakeWatcher("stopping")
    watched = [FakeTarget("one"), FakeTarget("two")]
    for target in watched:
        target.add_watcher(stopping)
        book.watching(target)

    book.release(stopping, "ref-to-me")

    # A registration outliving the actor it names is the leak death watch
    # exists to prevent, and it leaks in this direction too.
    assert [target.watchers for target in watched] == [[], []]
    assert book.stop_watching(watched[0].path) is None
