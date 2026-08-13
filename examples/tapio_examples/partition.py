"""Two live nodes, a broken network, and both of them wrong about the other.

Concepts: the failure detector, `PeerUnreachable` on the system event stream,
quarantine, and `remote.reconnect` as the explicit repair.

This is the uncomfortable example, and it is here because the behaviour it
shows is the one people meet in production and do not expect. Nothing dies.
Both nodes run the whole time, both keep processing their own work, and the
only thing that breaks is the network between them. Each one then concludes
that the other is gone, tells its watchers so, and stops sending. Both
conclusions are wrong, and both are the best a single node can do.

The fix for this is a quorum: enough nodes to hold a vote, so that a minority
of a partition can find out that it is the minority and stand down. That is
clustering, and it is not in this version. What is here instead is a set of
defaults chosen so that being wrong is recoverable: fail fast, freeze the
address, and let a person or a supervisor decide when to try again.

What to watch in the output: the two blocks of node lines, printed side by
side. `home` says away is gone. `away` says home is gone. Both are still
answering their own callers while they say it. Then note that healing the
network on its own repairs nothing: the last two lines only happen because
somebody asked for them.

Run it with `uv run python -m tapio_examples.partition`.
"""

import asyncio
from datetime import timedelta

from tapio import (
    ActorSystem,
    Behavior,
    Behaviors,
    Message,
    Signal,
    Terminated,
    register_message,
)
from tapio.actor import ActorContext, ActorRef
from tapio.actor.events import Subscription
from tapio.remote.address import format_ref
from tapio.remote.failure import PeerUnreachable
from tapio.testkit import two_nodes

__all__ = ["Poke", "Poked", "main"]


@register_message()
class Poked(Message):
    """Proof that the actor on the other node is still working."""

    by: str


@register_message()
class Poke(Message):
    """A message with a reply address, to show the link working before it breaks."""

    by: str
    reply_to: ActorRef[Poked]


def steady(lines: list[str], node: str, answered: asyncio.Event) -> Behavior[Poke]:
    """Build an actor that answers, and keeps answering through everything.

    Args:
        lines: Where to record what it did.
        node: The node it runs on, for the output.
        answered: Set once it has answered something.

    Returns:
        The behavior to spawn.
    """

    async def on_poke(message: Poke) -> Behavior[Poke]:
        lines.append(f"{node}: poked by {message.by}, still working")
        message.reply_to.tell(Poked(by=node))
        answered.set()
        return Behaviors.same()

    return Behaviors.receive_message(on_poke, msg_type=Poke)


def mourner(
    remote: ActorRef[Poke], lines: list[str], node: str, bereaved: asyncio.Event
) -> Behavior[Poked]:
    """Build an actor that watches the other node's actor and reports its death.

    Args:
        remote: The actor on the other node.
        lines: Where to record what it was told.
        node: The node it runs on, for the output.
        bereaved: Set when `Terminated` arrives.

    Returns:
        The behavior to spawn.
    """

    def build(ctx: ActorContext[Poked]) -> Behavior[Poked]:
        ctx.watch(remote)

        async def on_message(message: Poked) -> Behavior[Poked]:
            return Behaviors.same()

        async def on_signal(
            ctx: ActorContext[Poked], signal: Signal
        ) -> Behavior[Poked]:
            if isinstance(signal, Terminated):
                # It is not dead. It is on the other side of a partition,
                # answering somebody else. Nothing in this signal says so, and
                # nothing could.
                lines.append(f"{node}: told that {signal.ref.path} has stopped")
                bereaved.set()
            return Behaviors.same()

        return Behaviors.receive_message(on_message, on_signal=on_signal)

    return Behaviors.setup(build)


def watch_events(system: ActorSystem, lines: list[str], node: str) -> Subscription:
    """Report every peer this node gives up on.

    Args:
        system: The node to subscribe to.
        lines: Where to record what it published.
        node: The node's name, for the output.

    Returns:
        The subscription, so the example can stop listening before the two
        systems shut down. A node going away makes its peer unreachable too,
        and that one is not news.
    """

    def record(event: PeerUnreachable) -> None:
        verdict = "quarantined" if event.quarantined else "link ended"
        lines.append(f"{node}: gave up on {event.peer}, {verdict}")

    return system.events.subscribe(PeerUnreachable, record)


async def main() -> list[str]:
    """Run the example.

    Returns:
        Both nodes' lines, home's first, so the two views can be read side by
        side.
    """
    home_lines: list[str] = []
    away_lines: list[str] = []
    home_answered, away_answered = asyncio.Event(), asyncio.Event()
    home_bereaved, away_bereaved = asyncio.Event(), asyncio.Event()

    # Short enough for an example. In production this is seconds, and it has
    # to be comfortably longer than the peer's heartbeat interval or an
    # ordinary slow moment reads as a dead node.
    async with two_nodes(
        alpha="home",
        beta="away",
        unreachable_after=timedelta(milliseconds=500),
        heartbeat_interval=timedelta(milliseconds=20),
    ) as nodes:
        here, there = nodes.alpha, nodes.beta
        listening = [
            watch_events(here, home_lines, "home"),
            watch_events(there, away_lines, "away"),
        ]

        here_actor = here.spawn(steady(home_lines, "home", home_answered), "steady")
        there_actor = there.spawn(steady(away_lines, "away", away_answered), "steady")
        to_there = await here.resolve(
            format_ref(there.address, there_actor.path), expect=Poke
        )
        to_here = await there.resolve(
            format_ref(here.address, here_actor.path), expect=Poke
        )
        here_watcher = here.spawn(
            mourner(to_there, home_lines, "home", home_bereaved), "mourner"
        )
        there_watcher = there.spawn(
            mourner(to_here, away_lines, "away", away_bereaved), "mourner"
        )

        to_there.tell(Poke(by="home", reply_to=here_watcher))
        to_here.tell(Poke(by="away", reply_to=there_watcher))
        await away_answered.wait()
        await home_answered.wait()

        # Nothing dies here. The two nodes simply stop being able to hear each
        # other, which is the case no single node can tell from the other one.
        nodes.partition()
        await home_bereaved.wait()
        await away_bereaved.wait()

        # Both are still doing their own work, each while believing the other
        # is gone. Poking locally proves it.
        home_answered.clear()
        away_answered.clear()
        here_actor.tell(Poke(by="home itself", reply_to=here_watcher))
        there_actor.tell(Poke(by="away itself", reply_to=there_watcher))
        await home_answered.wait()
        await away_answered.wait()

        # The network is fine again, and nothing reconnects. Both nodes have
        # already told their watchers that live actors are gone, so quietly
        # resuming would leave them holding contradictory beliefs with no way
        # to notice.
        nodes.heal()
        home_lines.append("home: network repaired, and still no association")

        there.remote.clear_quarantine(here.address)  # type: ignore[union-attr]
        await here.remote.reconnect(there.address)  # type: ignore[union-attr]
        home_lines.append("home: reconnected, because somebody decided to")

        for subscription in listening:
            subscription.unsubscribe()

    lines = home_lines + away_lines
    for line in lines:
        print(line)
    return lines


if __name__ == "__main__":
    asyncio.run(main())
