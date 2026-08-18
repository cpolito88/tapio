"""Deciding that a peer is gone, and admitting what that decision is worth.

This is the part of remoting that cannot be made to feel local. A partition, a
long pause and a peer that died all look the same from one node: the frames
stop. So a system has to guess, and every guess it makes can be wrong.

The guess is split in two, so that each half can be replaced on its own:

* A [FailureDetector][tapio.remote.failure.FailureDetector] says whether a
  peer still looks alive from here. Today that is a fixed timeout. Clustering
  replaces it with phi-accrual, which reads the same interface.
* A [DownDecider][tapio.remote.failure.DownDecider] says what to do about it.
  Today it says yes, alone, immediately. Clustering replaces it with a
  strategy over converged membership, so that a minority partition stops
  itself rather than both halves declaring the other dead.

Writing the decider as an interface for a function that currently returns a
constant is the whole point. The association asks rather than deciding inline,
so the day the answer stops being a constant, nothing above it changes.

**The verdict can be false and there is no fix for that inside one node.**
Both sides of a partition will declare the other dead and both will be locally
correct. Resolving it needs membership and a quorum, which a single system does
not have, so it fails fast and stays failed instead. For request/response and
work distribution that is the better trade: wrongly deciding a peer is dead
costs a retry, which is recoverable, and waiting forever costs availability,
which often is not.
"""

import math
from collections import deque
from typing import Protocol, final, runtime_checkable

from tapio.message import Message
from tapio.remote.address import Address

__all__ = [
    "DeadlineDetector",
    "DownAlone",
    "DownDecider",
    "DownDecision",
    "FailureDetector",
    "PeerReachable",
    "PeerUnreachable",
    "PhiAccrualDetector",
]


@runtime_checkable
class FailureDetector(Protocol):
    """Whether a peer still looks alive, judged from the frames it sends."""

    def heartbeat(self, at: float) -> None:
        """Record that something arrived from the peer.

        Args:
            at: When, on the system loop's monotonic clock.
        """
        ...

    def is_available(self, now: float) -> bool:
        """Whether the peer still looks alive.

        Args:
            now: The current time, on the same clock.

        Returns:
            Whether frames are still arriving often enough to believe it.
        """
        ...


@final
class DeadlineDetector:
    """A fixed timeout: alive until nothing has arrived for long enough.

    The simplest detector that can work, and an honest one to start from. It
    has no opinion about how variable a network is, so the timeout has to be
    set well above the peer's heartbeat interval or a slow moment reads as a
    dead peer. Phi-accrual, which learns the distribution instead of being told
    a number, fits behind the same interface.
    """

    __slots__ = ("_last", "_window")

    def __init__(self, *, unreachable_after: float, started_at: float) -> None:
        """Start the clock on a peer that has just been heard from.

        Args:
            unreachable_after: Seconds of silence that mean the peer is gone.
            started_at: When the link came up, which counts as being heard
                from: a peer that has just handshaken is not a silent one.
        """
        self._window = unreachable_after
        self._last = started_at

    @property
    def last_heard(self) -> float:
        """When something last arrived, on the loop's clock."""
        return self._last

    def heartbeat(self, at: float) -> None:
        """Record that something arrived from the peer."""
        self._last = at

    def is_available(self, now: float) -> bool:
        """Whether the peer has been heard from inside the window."""
        return now - self._last < self._window

    def __repr__(self) -> str:
        """Render the window, which is the whole of the configuration."""
        return f"DeadlineDetector(unreachable_after={self._window:g}s)"


@final
class PhiAccrualDetector:
    """Alive until the peer's silence is longer than its own history explains.

    A fixed window has to be set well above the heartbeat interval or a slow
    moment reads as death, and that slack is latency a real failure waits out.
    This detector learns the spread of a peer's arrival times instead of being
    told a number, and reports a suspicion level `phi` that rises smoothly as
    silence outruns what the peer's own timing led it to expect. `phi` is on a
    log scale: phi around 1 is roughly a one-in-ten chance the next beat is
    merely late, phi around 2 about one in a hundred, and so on. One threshold
    therefore means the same confidence whether the link is fast and steady or
    slow and jittery, which is the whole reason to prefer it over a window.

    It reads the same interface as
    [DeadlineDetector][tapio.remote.failure.DeadlineDetector], so
    [RingMonitor][tapio.cluster.monitor.RingMonitor] and the association hold
    one without learning which.

    The estimate is seeded before any real interval is seen, so a peer picked
    up a moment ago is not suspected for never having answered a probe that has
    not been sent yet. One sample has no spread, so the seed is two synthetic
    intervals around a first estimate.
    """

    __slots__ = (
        "_intervals",
        "_last",
        "_max_samples",
        "_min_std",
        "_pause",
        "_sum",
        "_sum_sq",
        "_threshold",
    )

    def __init__(
        self,
        *,
        started_at: float,
        threshold: float,
        acceptable_pause: float,
        first_interval_estimate: float,
        max_samples: int = 200,
        min_std_deviation: float = 0.05,
    ) -> None:
        """Start suspecting a peer that has just been heard from.

        Args:
            started_at: When the link came up, which counts as being heard
                from, exactly as
                [DeadlineDetector][tapio.remote.failure.DeadlineDetector]
                treats it.
            threshold: The `phi` at which the peer is called unreachable.
                Higher is more patient and less likely to be wrong, at the
                cost of noticing a real death later.
            acceptable_pause: Seconds of extra silence tolerated on top of the
                learned mean, to ride out a scheduler or garbage-collection
                hiccup without relearning it as normal.
            first_interval_estimate: The interval assumed before any real one
                is seen, so a peer is not suspected during its first rounds.
                The probe interval is the natural value.
            max_samples: How many recent intervals shape the estimate. Older
                ones fall out, so the detector tracks a network whose timing
                drifts.
            min_std_deviation: A floor on the spread, in seconds. A peer that
                answers like a metronome would otherwise be suspected on a
                pause of milliseconds, which is too sharp to be safe.
        """
        self._last = started_at
        self._threshold = threshold
        self._pause = acceptable_pause
        self._max_samples = max_samples
        self._min_std = min_std_deviation
        self._intervals: deque[float] = deque(maxlen=max_samples)
        self._sum = 0.0
        self._sum_sq = 0.0
        std = first_interval_estimate / 4.0
        self._record(first_interval_estimate - std)
        self._record(first_interval_estimate + std)

    def _record(self, interval: float) -> None:
        """Add one inter-arrival interval, dropping the oldest past the window.

        The running sum and sum of squares are kept alongside the samples, so
        the mean and variance a probe needs cost nothing to read.

        Args:
            interval: The seconds between two arrivals.
        """
        if len(self._intervals) == self._max_samples:
            evicted = self._intervals[0]
            self._sum -= evicted
            self._sum_sq -= evicted * evicted
        self._intervals.append(interval)
        self._sum += interval
        self._sum_sq += interval * interval

    def heartbeat(self, at: float) -> None:
        """Record that something arrived, and learn the interval since the last.

        A zero or negative interval is not learned: two arrivals credited to
        the same instant say nothing about the peer's rhythm, and a clock that
        went backwards must not poison the estimate.

        Args:
            at: When, on the system loop's monotonic clock.
        """
        interval = at - self._last
        if interval > 0.0:
            self._record(interval)
        self._last = at

    def phi(self, now: float) -> float:
        """The suspicion that the peer is gone, given how long it has been quiet.

        Args:
            now: The current time, on the loop's monotonic clock.

        Returns:
            A value that is near zero while beats arrive on time and climbs
            without bound as silence outlasts the learned interval.
        """
        elapsed = now - self._last
        count = len(self._intervals)
        mean = self._sum / count + self._pause
        variance = self._sum_sq / count - (self._sum / count) ** 2
        std = max(math.sqrt(max(variance, 0.0)), self._min_std)
        # A logistic approximation to the normal distribution's tail, accurate
        # to a few thousandths and far cheaper than the error function. The
        # suspicion is -log10(P) for that tail, which reduces to log10(1 + e^p).
        # That single form is computed through a stable softplus so a long
        # silence, where p is large, neither overflows nor takes log10 of a
        # value that underflowed to zero.
        y = (elapsed - mean) / std
        p = y * (1.5976 + 0.070566 * y * y)
        softplus = p + math.log1p(math.exp(-p)) if p > 0.0 else math.log1p(math.exp(p))
        return softplus / math.log(10.0)

    def is_available(self, now: float) -> bool:
        """Whether suspicion is still below the threshold.

        Args:
            now: The current time, on the loop's monotonic clock.

        Returns:
            Whether the peer still looks alive.
        """
        return self.phi(now) < self._threshold

    def __repr__(self) -> str:
        """Render the threshold, which is the knob a reader reaches for first."""
        return f"PhiAccrualDetector(threshold={self._threshold:g})"


@final
class DownDecision(Message):
    """What to do about a peer the detector has given up on."""

    down: bool
    """Whether to treat the peer as gone."""

    detail: str
    """Why, for the log, the event and the errors that follow."""


@runtime_checkable
class DownDecider(Protocol):
    """What a system does when a peer stops looking alive."""

    async def decide(self, peer: Address) -> DownDecision:
        """Decide whether a peer that has gone quiet should be given up on.

        Args:
            peer: The peer in question.

        Returns:
            The decision.
        """
        ...


@final
class DownAlone:
    """Decide alone, immediately, and always yes.

    One node cannot do better. It has no membership to consult and no quorum
    to be part of, so "wait and see" would only mean waiting, and waiting is
    what the detector already did. Clustering replaces this with a strategy
    that knows how many nodes there are and which side of a partition it is
    on, and that is the entire difference.
    """

    async def decide(self, peer: Address) -> DownDecision:
        """Give up on the peer.

        Args:
            peer: The peer in question.

        Returns:
            A decision to down it, naming this system as the only voter.
        """
        return DownDecision(
            down=True,
            detail=(
                f"{peer} has gone silent, and this system decided alone that it "
                "is gone. With no membership to consult there is no other "
                "answer available"
            ),
        )

    def __repr__(self) -> str:
        """Render the class name; there is no state to show."""
        return "DownAlone()"


@final
class PeerUnreachable(Message):
    """Published when a peer can no longer be reached through its association.

    Subscribe to it on `system.events` to log, alarm, or shut a service down.
    It says nothing about whether the actors over there are running: a peer
    that terminated cleanly and one behind a partition produce the same event.
    """

    peer: str
    """The peer's canonical address."""

    uid: int
    """The incarnation that was associated, or `0` if the link never came up."""

    detail: str
    """What happened, in words."""

    quarantined: bool
    """Whether the address is now frozen.

    `True` after a detector gave up on a silent peer: nothing will be sent
    there and nothing dialled until `remote.reconnect` says so. `False` when
    the link merely ended, in which case the next send dials again.
    """


@final
class PeerReachable(Message):
    """Published when a link to a peer comes up and can carry traffic.

    The counterpart to
    [PeerUnreachable][tapio.remote.failure.PeerUnreachable], and the only
    thing that can retract one. It says a link is open, which is a weaker
    claim than the actors over there being the ones you remember: a peer that
    restarted comes back with a new incarnation uid, and `uid` is how a
    subscriber tells that apart from a link that merely reconnected.

    It is published every time a link opens, including the first, so a
    subscriber that only cares about recovery has to know whether it had
    written the peer off. Publishing only after an unreachability would need
    the association to remember a verdict that closed it, and the association
    that comes back is a new one.
    """

    peer: str
    """The peer's canonical address."""

    uid: int
    """The incarnation on the other end, as the handshake established it."""
