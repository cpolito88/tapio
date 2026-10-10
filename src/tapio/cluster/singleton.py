"""ClusterSingleton: one instance of an actor, on the oldest member of a role.

Some work has to happen in exactly one place across a whole cluster: a
scheduler that must not fire twice, a coordinator that owns a piece of state, a
sequence nobody else may allocate. A cluster singleton is how tapio places one
such actor and moves it when its host goes away.

The design is the smallest one that is correct. Every node spawns a manager.
Each manager watches membership, and the manager on the oldest member of the
singleton's role, and only that one, runs the instance. "Oldest" is the member
with the lowest `up_number`, the order the leader accepted members in, which is
a total order every node computes the same way from the same gossip. So at a
converged view exactly one manager runs the instance, with no election and no
lock.

Handoff is triggered by a host going away, and a successor starts only once
the old host is removed: every manager hears
[MemberRemoved][tapio.cluster.events.MemberRemoved], recomputes the oldest, and
the new oldest starts the instance. A crash is only ever seen as removal.

A graceful leave is seen earlier, as
[MemberLeaving][tapio.cluster.events.MemberLeaving]. The leaving host lets its
instance go when it hears that about itself. Every other manager goes on
counting the leaving host as the oldest, so no successor starts until the
removal. The removal needs every member, the leaving host included, to have
seen the leave, so by then the host has asked its instance to stop. That holds
wherever the leave was asked for: on the host, on another node's management
port, or in a frame from a peer. A successor that started at `MemberLeaving`
instead could start before the host had heard of its own leave.

The host does not wait for its instance to finish stopping. An instance still
busy in a handler when the successor starts overlaps it until that handler
returns. Closing that gap needs a handover between the two managers, which this
design does not have.

A node that is downed lets its instance go at once, on
[SelfDown][tapio.cluster.events.SelfDown], and never hosts again. A downed node
is never removed from its own view, so without this its instance would run on
beside the successor the rest of the cluster starts.

This is not a proxy. It places the instance and keeps it placed; sending to
wherever it currently runs is a separate concern, which a group router over the
same role answers.
"""

from typing import Any, final

from tapio.actor.behavior import Behavior, Behaviors
from tapio.actor.context import ActorContext
from tapio.actor.ref import ActorRef
from tapio.actor.timers import TimerScheduler
from tapio.cluster.daemon import start_subscribing, subscribe_when_ready
from tapio.cluster.events import (
    ClusterEvent,
    MemberLeaving,
    MemberRemoved,
    MemberUp,
    SelfDown,
)
from tapio.cluster.member import Member, seniority
from tapio.logging import runtime_logger
from tapio.message import Message

__all__ = ["ClusterSingleton"]

_log = runtime_logger("cluster.singleton")

_SUBSCRIBE_TIMER = "singleton-subscribe"
_KEEPER_NAME = "instance"

_MANAGER_EVENTS: tuple[type[ClusterEvent], ...] = (
    MemberUp,
    MemberLeaving,
    MemberRemoved,
    SelfDown,
)


@final
class _Handoff(Message):
    """Tell a keeper to stop, taking the singleton instance with it."""


@final
class _Reconcile(Message):
    """Retry subscribing to the daemon until it has started."""


_ManagerMessage = MemberUp | MemberLeaving | MemberRemoved | SelfDown | _Reconcile


def ClusterSingleton(  # noqa: N802 - a factory named as the thing it builds
    behavior: Behavior[Any],
    *,
    name: str,
    role: str | None = None,
) -> Behavior[_ManagerMessage]:
    """Build a manager that runs one instance of a behavior across the cluster.

    ```python
    ctx.spawn(ClusterSingleton(coordinator(), name="coordinator", role="worker"))
    ```

    Spawn the same manager on every node. Each subscribes to membership, and
    the one on the oldest member of `role` runs `behavior` as an actor named
    `name`. When that member is removed, the next oldest takes over.

    A host that leaves lets its instance go as soon as it hears of its own
    leave, and a host that is downed lets it go on
    [SelfDown][tapio.cluster.events.SelfDown]. The handoff does not wait for
    the instance to finish stopping. On a downed host the successor can start
    before the downed side has noticed, so set `terminate_on_down` on the
    [Cluster][tapio.cluster.Cluster] or act on `when_downed` to end that
    process as well.

    The instance is spawned fresh wherever it runs, so pass a factory such as
    `Behaviors.setup(...)`, not an already-built behavior holding state: state
    that mattered on the old host does not cross to the new one, which is the
    honest shape of a singleton that survives its host going away. Supervise
    `behavior` the ordinary way for failures that do not end its node.

    Args:
        behavior: What the singleton instance does.
        name: The instance's actor name, under the manager that runs it.
        role: The role whose oldest member hosts the instance. `None` places it
            among all members, so a cluster with no roles still has one.

    Returns:
        The manager behavior, to spawn on every node.
    """
    return _Manager(behavior, name, role).behavior()


def _keeper(behavior: Behavior[Any], name: str) -> Behavior[_Handoff]:
    """Build the actor that holds a running singleton instance.

    The manager cannot stop one specific child on its own, so the instance runs
    under a keeper the manager can stop: a keeper that stops takes its child
    with it, which is how a handoff ends the old instance without ending the
    manager. The keeper does nothing else.

    Args:
        behavior: The singleton instance to run.
        name: The instance's actor name under the keeper.

    Returns:
        The keeper behavior.
    """

    def build(ctx: ActorContext[_Handoff]) -> Behavior[_Handoff]:
        ctx.spawn(behavior, name)

        async def on_message(message: _Handoff) -> Behavior[_Handoff]:
            return Behaviors.stopped()

        return Behaviors.receive_message(on_message, msg_type=_Handoff)

    return Behaviors.setup(build)


class _Manager:
    """One node's singleton manager: its membership view, and where it stands."""

    def __init__(self, behavior: Behavior[Any], name: str, role: str | None) -> None:
        """Describe the manager, before its actor exists."""
        self._behavior = behavior
        self._name = name
        self._role = role
        self._address = ""
        self._daemon: ActorRef[Any] | None = None
        # The role members this node has seen up and not seen removed, by
        # address. A member that restarts at the same address is never reported
        # removed as its old incarnation, since events follow the newest
        # incarnation at each address, so the newcomer's MemberUp has to
        # replace the old one. Keyed by member key, the old incarnation stayed
        # oldest for ever, at an address nobody would start the instance for.
        self._hosts: dict[str, Member] = {}
        # Addresses of hosts on their way out. They still count as the oldest,
        # so no successor starts before they are removed, but they host
        # nothing.
        self._leaving: set[str] = set()
        # Set once this node is downed. It never hosts again.
        self._downed = False
        self._keeper: ActorRef[_Handoff] | None = None

    def behavior(self) -> Behavior[_ManagerMessage]:
        """Build the manager actor."""

        def with_timers(
            timers: TimerScheduler[_ManagerMessage],
        ) -> Behavior[_ManagerMessage]:
            def build(
                ctx: ActorContext[_ManagerMessage],
            ) -> Behavior[_ManagerMessage]:
                self._address = str(ctx.self_ref.address)
                # Ask at once, then keep asking until the daemon has started,
                # which is usually the first tick.
                start_subscribing(timers, _SUBSCRIBE_TIMER, _Reconcile())

                async def on_message(
                    ctx: ActorContext[_ManagerMessage], message: _ManagerMessage
                ) -> Behavior[_ManagerMessage]:
                    return await self._receive(ctx, timers, message)

                return Behaviors.receive(on_message, _ManagerMessage)

            return Behaviors.setup(build)

        return Behaviors.with_timers(with_timers)

    async def _receive(
        self,
        ctx: ActorContext[_ManagerMessage],
        timers: TimerScheduler[_ManagerMessage],
        message: _ManagerMessage,
    ) -> Behavior[_ManagerMessage]:
        """Handle one message, then place the instance if this node should."""
        match message:
            case _Reconcile():
                if self._daemon is None:
                    self._daemon = await subscribe_when_ready(
                        ctx, timers, _SUBSCRIBE_TIMER, _MANAGER_EVENTS
                    )
                return Behaviors.same()
            case MemberUp():
                if self._in_role(message.member):
                    self._hosts[message.member.address] = message.member
                    self._leaving.discard(message.member.address)
            case MemberLeaving():
                member = message.member
                held = self._hosts.get(member.address)
                if held is None and self._in_role(member) and member.up_number:
                    # Replayed to a manager that arrived during the leave. It
                    # was up before it left, so it is still the oldest it was.
                    self._hosts[member.address] = member
                    held = member
                if held is not None and held.uid == member.uid:
                    self._leaving.add(member.address)
            case MemberRemoved():
                held = self._hosts.get(message.member.address)
                if held is not None and held.uid == message.member.uid:
                    del self._hosts[message.member.address]
                    self._leaving.discard(message.member.address)
            case SelfDown():
                self._downed = True
        self._reconcile(ctx)
        return Behaviors.same()

    def _in_role(self, member: Member) -> bool:
        """Whether a member can host this singleton."""
        return self._role is None or self._role in member.roles

    def _reconcile(self, ctx: ActorContext[_ManagerMessage]) -> None:
        """Start or hand off the instance to match who the oldest member is."""
        host = self._oldest()
        am_host = (
            not self._downed
            and host is not None
            and host.address == self._address
            and host.address not in self._leaving
        )
        if am_host and self._keeper is None:
            self._keeper = ctx.spawn(_keeper(self._behavior, self._name), _KEEPER_NAME)
            _log.info("%s runs cluster singleton %r", self._address, self._name)
        elif not am_host and self._keeper is not None:
            # This node is leaving, was downed, or is no longer the oldest. A
            # successor waits for this node's removal, which cannot happen
            # before this node has seen its own leave and got here.
            self._keeper.tell(_Handoff())
            self._keeper = None
            _log.info("%s hands off cluster singleton %r", self._address, self._name)

    def _oldest(self) -> Member | None:
        """The oldest role member, or `None` if there are none.

        Oldest by [seniority][tapio.cluster.member.seniority], the same
        definition a downing strategy uses, so a `KeepOldest` split and this
        singleton agree on which member that is. A leaving member still counts,
        which is what holds a successor back until it is removed. Only members
        that were up reach here, and an up member always carries an
        `up_number`, so the before-acceptance case seniority guards against does
        not arise here; the address still breaks a tie between equal numbers.
        """
        if not self._hosts:
            return None
        return min(self._hosts.values(), key=seniority)

    def __repr__(self) -> str:
        """Render the singleton name, its role, and how many hosts are known."""
        return (
            f"ClusterSingleton({self._name!r}, role={self._role!r}, "
            f"hosts={len(self._hosts)})"
        )
