"""The table of peers a system has given up on, and the words that explain each."""

from tapio.remote.peers import StaticPeers
from tests.remote.peers import GHOST


def test_nothing_is_refused_until_something_gives_up():
    peers = StaticPeers()

    assert peers.refusal(GHOST) is None
    assert peers.refusals() == {}


def test_a_refusal_keeps_the_words_that_explain_it():
    peers = StaticPeers()
    peers.give_up(GHOST, "went silent for longer than the window")

    assert peers.refusal(GHOST) == "went silent for longer than the window"
    assert peers.refusals() == {GHOST: "went silent for longer than the window"}


def test_relenting_reports_what_it_cleared():
    peers = StaticPeers()
    peers.give_up(GHOST, "went silent")

    assert peers.relent(GHOST) == "went silent"
    assert peers.refusal(GHOST) is None
    assert peers.relent(GHOST) is None
