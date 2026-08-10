"""Who says which peers a system may associate with.

One system decides alone, from a table of the peers it gave up on. A
clustered one will decide from membership. The endpoint asks either of them
the same question, which is what these tests hold in place.
"""

from collections.abc import Mapping

from tapio import DeadLetter, DeadLetterReason
from tapio.remote.address import Address
from tapio.remote.peers import StaticPeers
from tapio.testkit import assert_no_leaked_tasks, two_nodes
from tests.failures import eventually
from tests.remote.peers import GHOST, Tick, counting, uri


class RefusingPeers:
    """A provider that refuses one address and nothing else.

    It stands in for the membership-backed one: the refusal comes from
    somewhere other than this system's own failure detector, and the endpoint
    is not supposed to be able to tell the difference.
    """

    def __init__(self, peer: Address, detail: str) -> None:
        """Refuse one peer from the start, for the stated reason."""
        self._peer = peer
        self._detail = detail

    def refusal(self, peer: Address) -> str | None:
        """Refuse the one address, and nothing else."""
        return self._detail if peer == self._peer else None

    def give_up(self, peer: Address, detail: str) -> None:
        """Take the address it already refuses, and no others."""

    def relent(self, peer: Address) -> str | None:
        """Never relent: what refused this peer is not this system."""
        return None

    def refusals(self) -> Mapping[Address, str]:
        """The one address, and why it is refused."""
        return {self._peer: self._detail}


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


async def test_the_endpoint_refuses_a_peer_because_the_provider_says_so():
    with assert_no_leaked_tasks():
        async with two_nodes() as nodes:
            letters: list[DeadLetter] = []
            nodes.alpha.dead_letters.subscribe(letters.append)
            ticks: list[int] = []
            worker = nodes.beta.spawn(counting(ticks), "worker")
            remote = await nodes.alpha.resolve(uri(nodes.beta, worker), expect=Tick)

            # Nothing here failed and nothing was quarantined. The peer is
            # refused because the authority for that question says it is,
            # which is how a downed member will read in a cluster.
            nodes.alpha.remote.use_peers(
                RefusingPeers(nodes.beta.address, "the cluster downed it")
            )
            remote.tell(Tick(n=1))

            await eventually(lambda: bool(letters))
            assert letters[0].reason == DeadLetterReason.QUARANTINED
            assert "the cluster downed it" in letters[0].detail
            assert nodes.alpha.remote.associations == ()
            assert ticks == []


async def test_installing_a_provider_carries_the_refusals_over():
    with assert_no_leaked_tasks():
        async with two_nodes() as nodes:
            nodes.alpha.remote.quarantine(nodes.beta.address, "went silent")

            nodes.alpha.remote.use_peers(StaticPeers())

            # A refusal that was acted on outlives whoever made it. Watchers
            # were already told the actors over there are gone, so a change of
            # authority must not quietly make the peer dialable again.
            assert nodes.alpha.remote.is_quarantined(nodes.beta.address)
            assert nodes.alpha.remote.refusal(nodes.beta.address) == "went silent"
            assert nodes.alpha.remote.quarantined == (nodes.beta.address,)
