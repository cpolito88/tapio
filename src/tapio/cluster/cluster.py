"""Clustering, as an application sees it.

```python
system = ActorSystem("orders", TapioSettings(remote=RemoteSettings(bind_port=2551)))
cluster = Cluster(system)
await cluster.join_seed_nodes([
    "tapio://orders@10.0.0.1:2551",
    "tapio://orders@10.0.0.2:2551",
])
```

A cluster is started rather than configured, because joining takes a list of
seeds and happens after the system exists. Remoting is the exception: its port
has to be bound while the system is being constructed, since that is what
settles the canonical address before any ref can write itself down. A cluster
therefore requires a system that already has remoting on, and says so at
construction rather than at the first gossip round that goes nowhere.
"""

import asyncio
from collections.abc import Sequence
from datetime import timedelta
from typing import final

from tapio.actor.events import Subscription
from tapio.actor.ref import ActorRef
from tapio.actor.system import ActorSystem
from tapio.cluster.daemon import DAEMON_NAME, ClusterDaemon
from tapio.cluster.downing import DownStrategy
from tapio.cluster.gossip import Gossip
from tapio.cluster.member import Member, MemberStatus, rank_of
from tapio.cluster.messages import ClusterDowned, ClusterMessage, Leave, Seeds
from tapio.errors import ClusterError
from tapio.remote.address import Address
from tapio.settings import ClusterSettings

__all__ = ["Cluster"]


@final
class Cluster:
    """One node's membership in a cluster: how it joins, and what it sees."""

    def __init__(
        self,
        system: ActorSystem,
        settings: ClusterSettings | None = None,
        *,
        downing: DownStrategy | None = None,
        terminate_on_down: bool = False,
    ) -> None:
        """Start this node's cluster daemon, without joining anything yet.

        Args:
            system: The system to cluster. It must have remoting configured,
                since members address each other by their canonical addresses.
            settings: How often to gossip and how patient to be. The defaults
                when omitted.
            downing: What to do about an unreachable member. Omitted leaves one
                blocking convergence for ever, which is the safe default until
                an operator says how this cluster would rather resolve a split.
                Pass a strategy such as
                [KeepMajority][tapio.cluster.downing.KeepMajority] or
                [DownAll][tapio.cluster.downing.DownAll] to have the losing
                side downed and, for this node, to have it down itself.
            terminate_on_down: Whether to shut the whole system down when this
                node downs itself, rather than leaving that to the application.
                A downed member may not rejoin as itself, so a service whose
                only reason to run was the cluster wants this. One that does
                other work leaves it off and awaits
                [when_downed][tapio.cluster.cluster.Cluster.when_downed]
                instead. It has no effect without a `downing` strategy, since
                nothing then downs this node.

        Raises:
            ClusterError: If the system has remoting switched off, so it has
                no address other nodes could dial.
        """
        endpoint = system.remote
        if endpoint is None:
            msg = (
                f"cannot cluster {system.name}: it has remoting switched off, so "
                "it has no address another node could dial. Pass "
                "TapioSettings(remote=RemoteSettings(...)) when building the system."
            )
            raise ClusterError(msg)
        self._system = system
        self._settings = settings if settings is not None else ClusterSettings()

        def relent(address: str) -> None:
            """Stop refusing a member remoting gave up on."""
            endpoint.clear_quarantine(Address.parse(address))

        def linked(address: str) -> bool:
            """Whether remoting already holds an association with a peer."""
            return endpoint.association_for(Address.parse(address)) is not None

        self._daemon = ClusterDaemon(
            address=str(system.address),
            uid=system.uid,
            refs=system.refs,
            events=system.events,
            settings=self._settings,
            relent=relent,
            linked=linked,
            strategy=downing,
        )
        self._ref: ActorRef[ClusterMessage] = system.spawn_system_actor(
            self._daemon.behavior(), DAEMON_NAME
        )
        self._down_watch: Subscription | None = None
        self._shutdown: asyncio.Task[None] | None = None
        if downing is not None and terminate_on_down:
            self._down_watch = self._terminate_when_downed()

    def _terminate_when_downed(self) -> Subscription:
        """Shut the system down the moment this node downs itself.

        The daemon publishes
        [ClusterDowned][tapio.cluster.messages.ClusterDowned] from inside its
        own turn, so this cannot terminate inline: doing so would stop the
        daemon while it is mid publish. It spawns the shutdown as its own task
        instead, the same way a failure escalating past a guardian does, so the
        turn that decided to down this node finishes first. The task is held,
        since the event loop keeps only a weak reference to one and would let an
        unheld shutdown be collected before it finished. Once fired this stops
        listening, because a system downs itself once.

        Returns:
            The subscription, so construction can hold it.
        """
        system = self._system

        def shut_down(event: ClusterDowned) -> None:
            if self._down_watch is not None:
                self._down_watch.unsubscribe()
            system.log.warning(
                "%s downed itself, shutting the system down", event.address
            )
            self._shutdown = asyncio.ensure_future(system.terminate())

        return system.events.subscribe(ClusterDowned, shut_down)

    @property
    def address(self) -> str:
        """This node's canonical address, in the form members are named by."""
        return self._daemon.address

    @property
    def state(self) -> Gossip:
        """What this node currently believes about the cluster.

        A snapshot of a value, so reading it twice and comparing is a fair
        question to ask. It is this node's view and not the truth: another
        node may believe something newer, and convergence is the condition
        under which they are known to agree.
        """
        return self._daemon.state

    @property
    def self_member(self) -> Member | None:
        """This node as the cluster sees it, or `None` before it has joined."""
        return self._daemon.self_member

    @property
    def members(self) -> tuple[Member, ...]:
        """Every member that has not been downed or removed, in address order."""
        return self._daemon.state.alive

    @property
    def leader(self) -> str | None:
        """The address of the node allowed to act, as this node computes it."""
        return self._daemon.state.leader

    @property
    def gossip_rounds(self) -> int:
        """How many times this node has sent its view to a peer.

        One round is one peer, so this counts sends rather than messages
        multiplied by members. It is here because "how long did convergence
        take" is a question about rounds rather than about seconds.
        """
        return self._daemon.rounds

    @property
    def monitored(self) -> tuple[str, ...]:
        """The members this node watches, in address order.

        Every node sorts the member addresses, finds itself, and watches the
        few that follow it, so every member is watched by that many others
        however little traffic there is between them.
        """
        return self._daemon.monitored

    @property
    def heartbeats_sent(self) -> int:
        """How many probes this node has sent to the members it watches.

        One per watched member per round, which is what makes the traffic
        linear in the number of nodes rather than quadratic.
        """
        return self._daemon.heartbeats

    async def join_seed_nodes(
        self,
        seeds: Sequence[str],
        *,
        timeout: timedelta | None = None,  # noqa: ASYNC109 - how long to wait to join
    ) -> Member:
        """Join the cluster the seeds are in, or form it if this is the first.

        Every node passes the same list in the same order. A node asks each
        seed to let it in, and keeps asking until it sees itself in the gossip
        that comes back, because a join is delivered at most once like every
        other message. The first seed in the list, and only the first, may
        form a new cluster, and only after
        [seed_form_after][tapio.settings.ClusterSettings.seed_form_after] in
        which it has heard from nobody. That is the rule that stops a restart
        from producing a second cluster beside the first.

        Args:
            seeds: The seed addresses, in their string form, in the order
                every node lists them. This node's own address may be in the
                list and is skipped.
            timeout: How long to wait to reach `Up`. The
                [join_timeout][tapio.settings.ClusterSettings.join_timeout]
                setting when omitted.

        Returns:
            This node's member record, once the leader has accepted it.

        Raises:
            ClusterError: If the seed list is empty, or if this node has not
                reached `Up` within the timeout. Timing out does not stop the
                node trying: it keeps asking, and the caller decides whether
                to keep waiting or to give up on the process.
        """
        if not seeds:
            msg = (
                f"cannot join with no seed nodes: {self.address} would have "
                "nobody to ask. Pass the seed list every node uses, in the "
                "same order."
            )
            raise ClusterError(msg)
        self._ref.tell(Seeds(addresses=tuple(seeds)))
        patience = (timeout or self._settings.join_timeout).total_seconds()
        try:
            return await self._until(MemberStatus.UP, patience)
        except TimeoutError:
            msg = (
                f"{self.address} did not reach Up within {patience:g}s. It is "
                "still asking the seeds. Either no seed is reachable, or the "
                "first seed has not formed the cluster yet and is not one of "
                "the nodes this one can see."
            )
            raise ClusterError(msg) from None

    async def when_downed(self) -> None:
        """Wait until a downing strategy has made this node down itself.

        A downed member may not rejoin as itself, so the usual response is to
        shut the system down:

        ```python
        await cluster.when_downed()
        await system.terminate()
        ```

        Ending the process is left to the application on purpose, the same way
        [leave][tapio.cluster.cluster.Cluster.leave] leaves it, so a node that
        is only one part of a larger service decides for itself what a downing
        means for the rest of it.

        It never returns when downing is switched off, since nothing then downs
        this node, and never returns for a node that leaves gracefully: leaving
        walks a member out through the lattice and is not a downing.
        """
        await self._daemon.downed.wait()

    async def leave(
        self,
        *,
        timeout: timedelta | None = None,  # noqa: ASYNC109 - how long to wait to leave
    ) -> None:
        """Leave the cluster gracefully, and wait to be written off.

        The member walks out through the lattice rather than vanishing:
        `Leaving`, then `Exiting` once every node has seen it, then `Removed`.
        Each step needs a converged view, so leaving takes as long as
        agreement takes, and every node ends up holding the same tombstone.

        The system is not terminated. Ending the process is the application's
        decision, and doing it here would take the choice away from a node
        that is only leaving one cluster.

        Args:
            timeout: How long to wait to reach `Removed`. The
                [leave_timeout][tapio.settings.ClusterSettings.leave_timeout]
                setting when omitted.

        Raises:
            ClusterError: If this node never joined, or if it has not been
                removed within the timeout.
        """
        if self.self_member is None:
            msg = f"cannot leave: {self.address} is not a member of any cluster"
            raise ClusterError(msg)
        patience = (timeout or self._settings.leave_timeout).total_seconds()
        self._ref.tell(Leave(address=self.address))
        try:
            await self._until(MemberStatus.REMOVED, patience)
        except TimeoutError:
            msg = (
                f"{self.address} asked to leave and was not removed within "
                f"{patience:g}s. Leaving needs a converged view at each step, "
                "so an unreachable member blocks it until somebody decides "
                "about that member."
            )
            raise ClusterError(msg) from None

    async def _until(self, status: MemberStatus, within: float) -> Member:
        """Wait for this node's own status to reach a point in the lattice.

        Polling, because what is being waited for is a value that several
        messages move: an event to wait on would have to be published by every
        path that can change the state, and a missed one would hang.

        Args:
            status: The status to wait for, or anything past it.
            within: How long to wait, in seconds.

        Returns:
            This node's member record.

        Raises:
            TimeoutError: If the status was not reached in time.
        """
        target = rank_of(status)
        async with asyncio.timeout(within):
            while True:
                member = self.self_member
                if member is not None and member.rank >= target:
                    return member
                await asyncio.sleep(0.005)

    def __repr__(self) -> str:
        """Render this node's address and how many members it can see."""
        return f"Cluster({self.address}, members={len(self.members)})"
