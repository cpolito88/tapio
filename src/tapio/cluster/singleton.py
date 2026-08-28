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

Handoff is triggered by a host going away. A crash is only ever seen as
removal: every manager hears
[MemberRemoved][tapio.cluster.events.MemberRemoved], recomputes the oldest, and
the new oldest starts the instance. A graceful leave is seen earlier, as
[MemberLeaving][tapio.cluster.events.MemberLeaving], one or more converged
rounds before the removal. The leaving host drives its own transition, so its
manager hears `MemberLeaving` first and lets its instance go before any
successor starts. That order is what keeps the two from overlapping. Starting
the successor only at removal did not: leadership moves off a member once it
reaches `exiting`, so the successor learns of the removal first, from its own
leader actions, and would start while the old host, hearing the removal a round
or more later, was still running its instance. `MemberRemoved` still drives the
crash path, where there is no leave to hear.

This is not a proxy. It places the instance and keeps it placed; sending to
wherever it currently runs is a separate concern, which a group router over the
same role answers.
"""

from datetime import timedelta
from typing import Any, final

from tapio.actor.behavior import Behavior, Behaviors
from tapio.actor.context import ActorContext
from tapio.actor.ref import ActorRef
from tapio.actor.timers import TimerScheduler
from tapio.cluster.daemon import local_daemon
from tapio.cluster.events import MemberLeaving, MemberRemoved, MemberUp
from tapio.cluster.member import Member, seniority
from tapio.cluster.messages import Subscribe
from tapio.logging import runtime_logger
from tapio.message import Message

__all__ = ["ClusterSingleton"]

_log = runtime_logger("cluster.singleton")

_SUBSCRIBE_TIMER = "singleton-subscribe"
_KEEPER_NAME = "instance"
_RETRY_INTERVAL = timedelta(milliseconds=50)
"""How often a manager or router retries subscribing until the daemon exists.

A manager can be spawned in the same breath as the cluster, before the daemon
has registered its well-known name, so the first look may find nothing. This is
short because the daemon starts moments later, and it stops the moment the
subscription lands."""


@final
class _Handoff(Message):
    """Tell a keeper to stop, taking the singleton instance with it."""


@final
class _Reconcile(Message):
    """Retry subscribing to the daemon until it has started."""


_ManagerMessage = MemberUp | MemberLeaving | MemberRemoved | _Reconcile


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
        # The role members this node has seen up and not seen removed, by their
        # member key, so a restart at one address does not lose the newcomer.
        self._hosts: dict[tuple[str, int], Member] = {}
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
                timers.start_fixed_delay(
                    _SUBSCRIBE_TIMER,
                    _Reconcile(),
                    _RETRY_INTERVAL,
                    initial_delay=timedelta(0),
                )

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
                await self._ensure_subscribed(ctx, timers)
                return Behaviors.same()
            case MemberUp():
                if self._role is None or self._role in message.member.roles:
                    self._hosts[message.member.key] = message.member
            case MemberLeaving():
                # The predecessor lets go here rather than at MemberRemoved.
                # Leadership has moved on by the time it reaches exiting, so a
                # successor computed from the removal would otherwise start
                # while this one was still running.
                self._hosts.pop(message.member.key, None)
            case MemberRemoved():
                self._hosts.pop(message.member.key, None)
        self._reconcile(ctx)
        return Behaviors.same()

    async def _ensure_subscribed(
        self,
        ctx: ActorContext[_ManagerMessage],
        timers: TimerScheduler[_ManagerMessage],
    ) -> None:
        """Subscribe to the daemon once it exists, then stop retrying."""
        if self._daemon is not None:
            timers.cancel(_SUBSCRIBE_TIMER)
            return
        daemon = await local_daemon(ctx)
        if daemon is None:
            return
        self._daemon = daemon
        daemon.tell(
            Subscribe(
                subscriber=ctx.self_ref,
                events=(MemberUp, MemberLeaving, MemberRemoved),
            )
        )
        timers.cancel(_SUBSCRIBE_TIMER)

    def _reconcile(self, ctx: ActorContext[_ManagerMessage]) -> None:
        """Start or hand off the instance to match who the oldest member is."""
        host = self._oldest()
        am_host = host is not None and host.address == self._address
        if am_host and self._keeper is None:
            self._keeper = ctx.spawn(_keeper(self._behavior, self._name), _KEEPER_NAME)
            _log.info("%s runs cluster singleton %r", self._address, self._name)
        elif not am_host and self._keeper is not None:
            # The host is leaving or gone, so let the instance go. On a graceful
            # leave this node hears MemberLeaving first, since it drives its own
            # transition, so it releases before a successor computed from the
            # removal starts, and the two do not run at once.
            self._keeper.tell(_Handoff())
            self._keeper = None
            _log.info("%s hands off cluster singleton %r", self._address, self._name)

    def _oldest(self) -> Member | None:
        """The oldest role member, or `None` if there are none.

        Oldest by [seniority][tapio.cluster.member.seniority], the same
        definition a downing strategy uses, so a `KeepOldest` split and this
        singleton agree on which member that is. Only members seen `MemberUp`
        reach here, and an up member always carries an `up_number`, so the
        before-acceptance case seniority guards against does not arise here; the
        address still breaks a tie between equal numbers.
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
