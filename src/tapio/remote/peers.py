"""Which peers this system will talk to, and which it has given up on.

Remoting resolves a peer from an address somebody wrote down: a string in a
configuration file, or a ref that arrived in a message. Every such address is
dialable, and the only reason to refuse one is that this system decided the
peer is gone (`quarantine`, see
[failure][tapio.remote.failure]). That is what
[StaticPeers][tapio.remote.peers.StaticPeers] implements, and it is the whole
of the v0.1 answer.

Clustering answers the same question from membership instead: a member that
the cluster has downed is refused, and it is refused for a reason every node
agrees on rather than one this node reached alone. The consequences are
identical either way, which is why there is one lookup and not two: watchers
have been told the actors over there are gone, sends dead-letter, and nothing
is dialled again until somebody says so. Only the voter changes.

So the endpoint asks rather than checking a table of its own. A refusal
carries the words that explain it, because they end up in a log line and in
the dead letter for every message that was on its way there, and "quarantined"
on its own tells a reader nothing about which of the two decided it.
"""

from collections.abc import Mapping
from typing import Protocol, final, runtime_checkable

from tapio.remote.address import Address

__all__ = ["PeerProvider", "StaticPeers"]


@runtime_checkable
class PeerProvider(Protocol):
    """Which peers a system may associate with, and why not when it may not."""

    def refusal(self, peer: Address) -> str | None:
        """Say whether this system refuses to be associated with a peer.

        Args:
            peer: The peer's canonical address.

        Returns:
            Why the peer is refused, in words, or `None` if it is not
            refused and may be dialled.
        """
        ...

    def give_up(self, peer: Address, detail: str) -> None:
        """Refuse a peer from now on.

        Args:
            peer: The peer's canonical address.
            detail: Why, kept for the log, the dead letters, and whoever
                asks later what happened.
        """
        ...

    def relent(self, peer: Address) -> str | None:
        """Stop refusing a peer.

        Args:
            peer: The peer's canonical address.

        Returns:
            Why it was refused, or `None` if it was not refused at all.
        """
        ...

    def refusals(self) -> Mapping[Address, str]:
        """List every peer this system refuses, and why it refuses each.

        Returns:
            A snapshot, so a caller may read it while the answer changes.
        """
        ...


@final
class StaticPeers:
    """Every address is a peer, until this system gives up on one.

    The v0.1 answer, and the right one for a pair of systems that were told
    about each other. There is no membership to consult, so the only peers
    that are refused are the ones a failure detector here gave up on, and the
    only way back is for somebody to say so.
    """

    __slots__ = ("_refused",)

    def __init__(self) -> None:
        """Start with nothing refused, since nothing has failed yet."""
        self._refused: dict[Address, str] = {}

    def refusal(self, peer: Address) -> str | None:
        """Say why this system refuses a peer, or `None` if it does not."""
        return self._refused.get(peer)

    def give_up(self, peer: Address, detail: str) -> None:
        """Refuse a peer from now on, recording why."""
        self._refused[peer] = detail

    def relent(self, peer: Address) -> str | None:
        """Stop refusing a peer, returning why it was refused."""
        return self._refused.pop(peer, None)

    def refusals(self) -> Mapping[Address, str]:
        """List every refused peer with the words that explain it."""
        return dict(self._refused)

    def __repr__(self) -> str:
        """Render how many peers are refused, which is all the state there is."""
        return f"StaticPeers(refused={len(self._refused)})"
