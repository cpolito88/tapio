"""Asking a question of an actor on another node, and the two ways it fails.

Concepts: `ask` across an association, the reply finding its way back through
`/system/promises`, and `AskTargetUnreachable` told apart from
`AskTimeoutError`.

The call is the same one `ask_timeout` makes locally. What changes is the
failures. A local ask has two: nobody answered in time, or the actor stopped.
A remote ask has a third, and it is the one worth understanding. The peer can
become unreachable, which means this node stopped hearing from it and decided
that it was gone. The actor over there may be perfectly healthy on the other
side of a partition.

The two failures are different errors on purpose. A timeout says the peer was
there and slow, so waiting longer might help. Unreachable says this node has
given up on the peer, so waiting will not help and retrying somewhere else
might.

What to watch in the output: the third and fourth lines. Both asks failed, in
about the same amount of time, for entirely different reasons. The last line
is the point of the whole example: the actor that "disappeared" answers a
question the moment the network is repaired.

Run it with `uv run python -m tapio_examples.remote_ask`.
"""

import asyncio
from datetime import timedelta

from tapio import (
    Behavior,
    Behaviors,
    Message,
    register_message,
)
from tapio.actor import ActorContext, ActorRef
from tapio.errors import AskTargetUnreachable, AskTimeoutError
from tapio.remote.address import format_ref
from tapio.testkit import two_nodes

__all__ = ["Answer", "Ask", "main"]


@register_message()
class Answer(Message):
    """What the oracle says."""

    question: str
    answer: str


@register_message()
class Ask(Message):
    """A question, and the ref the answer goes back to.

    `ask` builds the second field for you: the ref it passes is a promise on
    the asking node, addressed under `/system/promises` so that an answer
    coming back over the link finds the future somebody is awaiting.
    """

    question: str
    reply_to: ActorRef[Answer]


def oracle() -> Behavior[Ask]:
    """Build an actor that answers questions, except the ones it ignores.

    Returns:
        The behavior to spawn.
    """

    async def on_ask(ctx: ActorContext[Ask], message: Ask) -> Behavior[Ask]:
        if message.question == "the meaning of life":
            # Some questions take longer than anyone is prepared to wait.
            ctx.log.info("declining to answer %r", message.question)
            return Behaviors.same()
        message.reply_to.tell(Answer(question=message.question, answer="42"))
        return Behaviors.same()

    return Behaviors.receive(on_ask)


async def main() -> list[str]:
    """Run the example.

    Returns:
        The lines the two nodes produced, in the order they produced them.
    """
    lines: list[str] = []
    # A short window, so the example runs in well under a second. Production
    # values are seconds, not milliseconds: the window has to be long enough
    # that an ordinary slow moment is not read as a dead node.
    async with two_nodes(
        alpha="asker",
        beta="answers",
        unreachable_after=timedelta(milliseconds=500),
        heartbeat_interval=timedelta(milliseconds=20),
    ) as nodes:
        here, there = nodes.alpha, nodes.beta
        sage = there.spawn(oracle(), name="oracle")
        remote = await here.resolve(format_ref(there.address, sage.path), expect=Ask)

        answer = await remote.ask(
            lambda reply_to: Ask(question="six by seven", reply_to=reply_to),
            expect=Answer,
        )
        lines.append(f"asker: six by seven is {answer.answer}")

        try:
            await remote.ask(
                lambda reply_to: Ask(question="the meaning of life", reply_to=reply_to),
                expect=Answer,
                timeout=timedelta(milliseconds=100),
            )
        except AskTimeoutError:
            # The peer is there and nobody answered. Asking again later is a
            # reasonable thing to do.
            lines.append("asker: no answer in time, and the node is still there")

        # Now the network, rather than the actor, is the problem. The oracle
        # keeps running on its own node throughout.
        nodes.partition()
        try:
            await remote.ask(
                lambda reply_to: Ask(question="six by seven", reply_to=reply_to),
                expect=Answer,
                timeout=timedelta(seconds=30),
            )
        except AskTargetUnreachable:
            # Note the deadline above: thirty seconds, and the ask failed in a
            # fraction of one. It failed on the peer, not on the clock.
            lines.append("asker: the answering node is unreachable, so no waiting")

        nodes.heal()
        there.remote.clear_quarantine(here.address)  # type: ignore[union-attr]
        await here.remote.reconnect(there.address)  # type: ignore[union-attr]
        # Refs from before the quarantine name a session that is over, so the
        # address is resolved again rather than reused.
        again = await here.resolve(format_ref(there.address, sage.path), expect=Ask)
        repaired = await again.ask(
            lambda reply_to: Ask(question="six by seven", reply_to=reply_to),
            expect=Answer,
        )
        lines.append(
            f"asker: after reconnecting, six by seven is still {repaired.answer}"
        )

    for line in lines:
        print(line)
    return lines


if __name__ == "__main__":
    asyncio.run(main())
